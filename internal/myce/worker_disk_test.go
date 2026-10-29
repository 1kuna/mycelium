package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWorkerDiskSamplesDistinctVolumesAndRate(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	free := uint64(50 << 30)
	calls := 0
	w := Worker{Clock: clock, EnvironmentRoot: "env", Machine: machine("disk-worker", "fake"), DiskProbe: func(path string) (DiskReading, error) {
		calls++
		volume := "local"
		if path == "checkpoint" {
			volume = "nas"
		}
		return DiskReading{Volume: volume, TotalBytes: 100 << 30, FreeBytes: free}, nil
	}}
	first := w.sampleDisks("data", "checkpoint")
	if calls != 5 || len(first) != 2 || strings.Join(first[0].Roots, "+") != "data+environment+file-cache+model-cache" || first[0].ChangeBytesPerHour != nil {
		t.Fatalf("initial samples: %d %+v", calls, first)
	}
	clock.Advance(30 * time.Minute)
	free -= 1 << 30
	next := w.sampleDisks("data", "checkpoint")
	if calls != 10 || len(next) != 2 {
		t.Fatal(calls, next)
	}
	for _, v := range next {
		if v.ChangeBytesPerHour == nil || *v.ChangeBytesPerHour != -2*(1<<30) || v.SampledAt != clock.Now() {
			t.Fatalf("wrong worker clock rate: %+v", v)
		}
	}
	w.Machine.DiskVolumes = next
	text := strings.Join(CapabilityProfileLines(&w.Machine), "\n")
	if !strings.Contains(text, "data+environment+file-cache+model-cache disk: 49.0 / 100.0 GiB free; -2.00 GiB/h") {
		t.Fatal(text)
	}
	data, _ := json.Marshal(next)
	if strings.Contains(string(data), "nas") || strings.Contains(string(data), "local") {
		t.Fatal("volume identity leaked", string(data))
	}
	w.DiskProbe = func(string) (DiskReading, error) { return DiskReading{}, errors.New("private path") }
	failed := w.sampleDisks("data", "checkpoint")
	if len(failed) != 2 || failed[0].Error != "disk reading unavailable" || failed[0].FreeBytes != 49<<30 {
		t.Fatal(failed)
	}
}

func TestDiskHeadroomMeasuredSizesPrecedeHints(t *testing.T) {
	j := Job{Spec: Spec{Disk: &DiskNeeds{InputBytes: 100, OutputBytes: 200}, Files: FileManifest{"input": {SizeBytes: 10}}}, OutputRefs: FileManifest{"output": {SizeBytes: 20}}}
	if got := projectedDiskNeed(&j); got != 40 {
		t.Fatal(got)
	}
	j.Spec.Disk = nil
	if got := projectedDiskNeed(&j); got != 40 {
		t.Fatal(got)
	}
	j.Spec.Files, j.OutputRefs = nil, nil
	if got := projectedDiskNeed(&j); got != 0 {
		t.Fatal(got)
	}
}

// The measured successful CIFS Statfs took 8.233s, longer than the old
// 3.85s freshness window. Exercise the sampler and real queue disk predicate.
func TestWorkerDiskCheckpointProgressRetainsAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, clock := fixture(t)
		var mode, slowCalls, blockedCalls atomic.Int32
		release := make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		w := Worker{Clock: RealClock{}, Interval: time.Second, Machine: machine("disk-worker", "fake"), DiskProbe: func(path string) (DiskReading, error) {
			volume := "local"
			if path == "nas" {
				volume = "nas"
				switch mode.Load() {
				case 1:
					slowCalls.Add(1)
					time.Sleep(8233 * time.Millisecond)
				case 2:
					blockedCalls.Add(1)
					<-release
				case 3:
					return DiskReading{}, errors.New("I/O failure")
				}
			}
			return DiskReading{Volume: volume, TotalBytes: 100 << 30, FreeBytes: 40 << 30}, nil
		}}
		m := w.Machine
		j := &Job{Spec: spec("experiment")}
		j.Spec.Resumable = true
		sample := func() DiskVolume {
			t.Helper()
			volumes := w.sampleDisks("data", "nas")
			clock.now = time.Now()
			m.DiskReceivedAt = clock.Now()
			for _, v := range volumes {
				if strings.Contains(strings.Join(v.Roots, ","), "checkpoint") {
					m.DiskVolumes = []DiskVolume{v}
					return v
				}
			}
			t.Fatal("checkpoint sample missing")
			return DiskVolume{}
		}
		usable := func() {
			t.Helper()
			if reason := q.diskHeadroomReason(j, &m); reason != "" {
				t.Fatal(reason)
			}
		}
		first := sample()
		usable()
		mode.Store(1)
		for {
			v := sample()
			usable()
			if !v.Retained && v.SampledAt.After(first.SampledAt) {
				if v.AgeSeconds > 1 {
					t.Fatalf("successful sample timestamp predates completion: %+v", v)
				}
				break
			}
		}
		if slowCalls.Load() != 1 {
			t.Fatalf("duplicate slow Statfs: %d", slowCalls.Load())
		}
		mode.Store(2)
		retained := sample()
		usable()
		if !retained.Retained || retained.ProbeAgeSeconds == nil || retained.FreeBytes != 40<<30 {
			t.Fatalf("lost last good reading: %+v", retained)
		}
		time.Sleep(28 * time.Second)
		clock.now = time.Now()
		usable()
		// No further check-in can renew a pending operation forever: the queue
		// advances the reported probe age even when the worker stops reporting.
		time.Sleep(2 * time.Second)
		clock.now = time.Now()
		if reason := q.diskHeadroomReason(j, &m); !strings.Contains(reason, "disk reading unavailable") {
			t.Fatalf("queue retained stalled probe: %s", reason)
		}
		stalled := sample()
		if stalled.Retained || stalled.Error == "" || blockedCalls.Load() != 1 {
			t.Fatalf("stall not fenced or duplicated: %+v calls=%d", stalled, blockedCalls.Load())
		}
		close(release)
		recovered := sample()
		usable()
		if recovered.Error != "" || recovered.Retained {
			t.Fatalf("late success not recovered: %+v", recovered)
		}
		mode.Store(3)
		failed := sample()
		if failed.Retained || failed.FreeBytes != 40<<30 || !strings.Contains(q.diskHeadroomReason(j, &m), "disk reading unavailable") {
			t.Fatalf("completed error admitted work or erased reading: %+v", failed)
		}
		mode.Store(1)
		pendingAfterError := sample()
		if pendingAfterError.Retained || q.diskHeadroomReason(j, &m) == "" {
			t.Fatal("pending retry borrowed success from before completed error")
		}
		time.Sleep(9 * time.Second)
		sample()
		usable()
		cold := make(chan struct{})
		defer close(cold)
		w.DiskProbe = func(path string) (DiskReading, error) {
			if path == "other-nas" {
				<-cold
			}
			return DiskReading{Volume: path, TotalBytes: 100 << 30, FreeBytes: 40 << 30}, nil
		}
		for _, v := range w.sampleDisks("data", "other-nas") {
			if strings.Contains(strings.Join(v.Roots, ","), "checkpoint") && (v.Retained || v.TotalBytes != 0 || v.Error == "") {
				t.Fatalf("cold root borrowed prior reading: %+v", v)
			}
		}
	})
}
