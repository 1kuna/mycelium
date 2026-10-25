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

func reportDisk(t *testing.T, m Machine, c Clock, id string, roots []string, total, free uint64) Machine {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"disk_volumes": []any{map[string]any{"id": id, "roots": roots, "total_bytes": total, "free_bytes": free, "sampled_at": c.Now(), "sample_interval_seconds": 1}}})
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDiskHeadroomProductionRepins(t *testing.T) {
	q, c := fixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{"hold-host": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	a := reportDisk(t, machine("a", "fake", "hold-host"), c, "a-volume", []string{"data"}, 100<<30, 41<<30)
	check(t, q, a, nil)
	busy := submit(t, q, spec("experiment"))
	busy = check(t, q, a, nil).Job
	if busy == nil {
		t.Fatal("no initial occupancy")
	}
	s := spec("production")
	s.Needs.MemoryGB = 1
	s.Disk = &DiskNeeds{OutputBytes: 30 << 30}
	prod := submit(t, q, s)
	if job(t, q, prod.ID).ProductionHost != "hold-host" {
		t.Fatal("not pinned")
	}
	a = reportDisk(t, a, c, "a-volume", []string{"data"}, 100<<30, 2<<30)
	check(t, q, a, busy)
	held := job(t, q, prod.ID)
	if held.DiagnosticCode != "disk_headroom" || !strings.Contains(held.Reason, "disk_headroom:") || !strings.Contains(held.Reason, "hold-host") {
		t.Errorf("disk hold hidden by production hold: %s", held.Reason)
	}
	b := reportDisk(t, machine("b", "fake", "room-host"), c, "b-volume", []string{"data"}, 100<<30, 41<<30)
	got := check(t, q, b, nil).Job
	if got == nil || got.ID != prod.ID {
		t.Fatalf("production stayed pinned despite room: %+v", job(t, q, prod.ID))
	}
	if err := q.Started(result(got, true)); err != nil {
		t.Fatal(err)
	}
	c.Advance(ProductionStartTimeout)
	if got := job(t, q, prod.ID); got.State != "running" {
		t.Fatal("started job expired", got)
	}
}

func TestDiskHeadroomSharedHostReservations(t *testing.T) {
	q, c := fixture(t)
	a := reportDisk(t, machine("a", "fake", "shared-host"), c, "shared-volume", []string{"data"}, 100<<30, 41<<30)
	b := reportDisk(t, machine("b", "fake", "shared-host"), c, "shared-volume", []string{"data"}, 100<<30, 41<<30)
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 30 << 30}
	one := submit(t, q, s)
	two := submit(t, q, s)
	first := check(t, q, a, nil).Job
	if first == nil {
		t.Fatal("first job not admitted")
	}
	secondID := one.ID
	if first.ID == one.ID {
		secondID = two.ID
	}
	if got := check(t, q, b, nil).Job; got != nil {
		t.Fatalf("second job reused assigned disk headroom: %s", got.ID)
	}
	if err := q.Started(result(first, true)); err != nil {
		t.Fatal("own reservation blocked launch", err)
	}
	if got := check(t, q, b, nil).Job; got != nil {
		t.Fatalf("second job reused running disk headroom: %s", got.ID)
	}
	other := reportDisk(t, machine("other", "fake", "shared-host"), c, "different-volume", []string{"data"}, 100<<30, 41<<30)
	if got := check(t, q, other, nil).Job; got == nil || got.ID != secondID {
		t.Fatal("independent volume was charged", got)
	}
}

func TestDiskHeadroomCapacityRefusesImpossibleSubmission(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("a", "fake"), c, "volume", []string{"data"}, 100<<30, 2<<30)
	check(t, q, m, nil)
	s := spec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 100 << 30}
	_, err := q.Submit(s)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != "disk_headroom" {
		t.Fatalf("impossible size wasn't refused with disk reason: %v", err)
	}
	s.Disk.OutputBytes = 30 << 30
	s.RequireTarget = true
	if _, err := q.Submit(s); err != nil {
		t.Fatal("temporary free-space shortage became refusal", err)
	}
}

func TestDiskHeadroomChargesOnlyWrittenVolumes(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("a", "fake"), c, "volume", []string{"data"}, 100<<30, 5<<30)
	// Input+output writes fit data, while the input cache copy has its own volume.
	m.DiskVolumes = append(m.DiskVolumes, DiskVolume{Roots: []string{"file-cache"}, TotalBytes: 100 << 30, FreeBytes: 2 << 30, SampledAt: c.Now()}, DiskVolume{Roots: []string{"checkpoint"}, TotalBytes: 100 << 30, FreeBytes: 0, SampledAt: c.Now()})
	check(t, q, m, nil)
	s := spec("experiment")
	s.Resumable = false
	s.Disk = &DiskNeeds{InputBytes: 1 << 30, OutputBytes: 2 << 30}
	j := submit(t, q, s)
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("unwritten checkpoint root or duplicate data input charge blocked work")
	}
	// Hints for NAS/path inputs do not create an immutable cache copy.
	s.Inputs = map[string]string{"input": "shared/input"}
	m.DiskVolumes[1].FreeBytes = 1 << 30
	if reason := diskHeadroomReason(&Job{Spec: s}, &m); reason != "" {
		t.Fatalf("path input charged unused file cache: %s", reason)
	}
	// Manifest inputs really do need a cache copy on file-cache.
	s.Files = FileManifest{"input": {SizeBytes: 1 << 30}}
	if reason := diskHeadroomReason(&Job{Spec: s}, &m); !strings.Contains(reason, "file-cache") {
		t.Fatalf("cache copy was not charged to file-cache: %s", reason)
	}
	m.DiskVolumes[1].FreeBytes = 2 << 30
	s.Resumable = true
	s.Disk.CheckpointBytes = 1 << 30
	m.DiskVolumes[2].FreeBytes = 1 << 30
	if reason := diskHeadroomReason(&Job{Spec: s}, &m); !strings.Contains(reason, "checkpoint") {
		t.Fatalf("declared checkpoint bytes were not charged: %s", reason)
	}
}

func TestDiskHeadroomUnavailableAndStaleReadings(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("a", "fake"), c, "volume", []string{"data"}, 100<<30, 41<<30)
	c.Advance(10 * time.Second)
	check(t, q, m, nil)
	s := spec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 1 << 30}
	j := submit(t, q, s)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Error("stale disk sample admitted work")
	}
	held := job(t, q, j.ID)
	var status Snapshot
	if err := json.Unmarshal([]byte(statusViewResponse(t, q, "/api/status")), &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Machines) != 1 || status.Machines[0].DiskVolumes[0].AgeSeconds < 10 || status.Machines[0].DiskVolumes[0].Error != "disk reading unavailable" {
		t.Fatal("stale status lacks age/unavailable marker", status.Machines)
	}
	if !strings.Contains(held.Reason, "disk reading unavailable") {
		t.Error("stale reason", held.Reason)
	}
	m.DiskVolumes[0].Error = "disk reading unavailable"
	m.DiskVolumes[0].FreeBytes = 0
	if reason := diskHeadroomReason(&Job{Spec: s}, &m); !strings.Contains(reason, "disk reading unavailable") || strings.Contains(reason, "free=0") {
		t.Error("invented zero free bytes", reason)
	}
}

func TestDiskHeadroomUnknownManifestUsesHint(t *testing.T) {
	j := Job{Spec: Spec{Files: FileManifest{"input": {}}, Disk: &DiskNeeds{InputBytes: 100, OutputBytes: 20}}}
	if got := projectedDiskNeed(&j); got != 220 {
		t.Fatalf("unknown manifest suppressed input hint: %d", got)
	}
}

func TestWorkerDiskNASBudgetRetainsRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &fakeClock{now: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
		var slow atomic.Bool
		defer time.Sleep(2 * time.Second)
		w := Worker{Clock: c, Machine: machine("a", "fake"), DiskProbe: func(path string) (DiskReading, error) {
			if path == "nas" {
				if slow.Load() {
					time.Sleep(time.Second)
				} else {
					time.Sleep(100 * time.Millisecond)
				}
			}
			volume := "local"
			if path == "nas" {
				volume = "nas"
			}
			return DiskReading{Volume: volume, TotalBytes: 100 << 30, FreeBytes: 40 << 30}, nil
		}}
		first := w.sampleDisks("data", "nas")
		if len(first) != 2 || first[1].FreeBytes != 40<<30 || first[1].Error != "" {
			t.Fatalf("NAS used local deadline: %+v", first)
		}
		c.Advance(time.Second)
		slow.Store(true)
		retained := w.sampleDisks("data", "nas")
		if len(retained) != 2 || retained[1].FreeBytes != 40<<30 || retained[1].SampledAt != first[1].SampledAt || !retained[1].Retained || retained[1].AgeSeconds < 1 {
			t.Fatalf("timeout dropped last good reading: %+v", retained)
		}
		slow.Store(false)
		synctest.Wait()
		c.Advance(time.Second)
		next := w.sampleDisks("data", "nas")
		if next[1].ChangeBytesPerHour == nil {
			t.Fatalf("timeout erased rate baseline: %+v", next)
		}
	})
}

func TestWorkerDiskCacheVolumesAndOpaqueIdentity(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	makeWorker := func(name string) Worker {
		return Worker{Clock: c, Machine: machine(name, "fake", "same-host"), EnvironmentRoot: "data", FileCacheRoot: "files", CacheRoot: "models", DiskProbe: func(path string) (DiskReading, error) {
			return DiskReading{Volume: path, TotalBytes: 100 << 30, FreeBytes: 41 << 30}, nil
		}}
	}
	a, b := makeWorker("a"), makeWorker("b")
	av, bv := a.sampleDisks("data", ""), b.sampleDisks("data", "")
	if len(av) != 3 || len(bv) != 3 {
		t.Fatal("missing cache volumes", av, bv)
	}
	ids := map[string]bool{}
	for i, v := range av {
		if v.ID == "" || v.ID != bv[i].ID || v.ID == v.Roots[0] || ids[v.ID] {
			t.Fatal("volume identities are not opaque/stable/distinct", av, bv)
		}
		ids[v.ID] = true
	}
	if strings.Join(av[1].Roots, ",") != "file-cache" || strings.Join(av[2].Roots, ",") != "model-cache" {
		t.Fatal(av)
	}
}
