package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
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
