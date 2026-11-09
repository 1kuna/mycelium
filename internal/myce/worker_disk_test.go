package myce

import (
	"net/http/httptest"
)

import (
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
	w := Worker{Clock: clock, EnvironmentRoot: "env", Machine: machine("disk-worker", "fake"), DiskProbe: func(path string) (DiskReading, error) {
		volume := "local"
		if path == "checkpoint" {
			volume = "nas"
		}
		return DiskReading{Volume: volume, TotalBytes: 100 << 30, FreeBytes: free}, nil
	}}
	w.sampleDisks("data", "checkpoint")
	clock.Advance(30 * time.Minute)
	free -= 1 << 30
	w.sampleDisks("data", "checkpoint")
	w.DiskProbe = func(string) (DiskReading, error) { return DiskReading{}, errors.New("private path") }
	failed := w.sampleDisks("data", "checkpoint")
	if len(failed) != 2 || failed[0].Error != "disk reading unavailable" || failed[0].FreeBytes != 49<<30 {
		t.Fatal(failed)
	}

	q, _ := fixture(t)
	w.Machine.DiskVolumes = failed
	check(t, q, w.Machine, nil)
	request := httptest.NewRequest("GET", "/api/status", nil)
	request.Header.Set("Authorization", "Bearer test")
	response := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatal(response.Code)
	}
	data := response.Body.String()
	if strings.Contains(data, "private path") || strings.Contains(data, `"volume":"nas"`) || strings.Contains(data, `"volume":"local"`) {
		t.Fatal("volume identity leaked", data)
	}
	if !strings.Contains(data, "disk reading unavailable") {
		t.Fatal("disk failure evidence hidden", data)
	}
}

// The measured successful CIFS Statfs took 8.233s, longer than the old
// 3.85s freshness window. Exercise the sampler and real queue disk predicate.
func TestWorkerDiskCheckpointProgressRetainsAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, clock := fixture(t)
		var mode atomic.Int32
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
					time.Sleep(8233 * time.Millisecond)
				case 2:
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
		if stalled.Retained || stalled.Error == "" {
			t.Fatalf("stall not fenced: %+v", stalled)
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
