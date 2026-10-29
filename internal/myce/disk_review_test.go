package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
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

// Observed free space already includes the experiment's writes. Its projected
// output must not prevent ordinary GPU preemption for production (N1/N2).
func TestDiskHeadroomProductionPreemptsWithObservedSpace(t *testing.T) {
	q, c := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	m = reportDisk(t, m, c, "volume", []string{"data"}, 100<<30, 81<<30)
	s := spec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 60 << 30}
	submit(t, q, s)
	busy := assignedJob(machineCheck(t, q, m))
	if busy == nil {
		t.Fatal("experiment not assigned")
	}
	machineCheck(t, q, m, busy)
	m = reportDisk(t, m, c, "volume", []string{"data"}, 100<<30, 41<<30)
	machineCheck(t, q, m, busy)
	s = spec("production")
	s.Disk = &DiskNeeds{OutputBytes: 30 << 30}
	prod := submit(t, q, s)
	machineCheck(t, q, m, busy)
	if got := job(t, q, busy.ID); got.State != "stopping" || got.PreemptionCount != 1 {
		t.Fatalf("disk estimate prevented GPU preemption: state=%s preemptions=%d production=%+v", got.State, got.PreemptionCount, job(t, q, prod.ID))
	}
	finishInterrupted(t, q, busy)
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != prod.ID || !c.Now().Before(*next.Spec.StartBy) {
		t.Fatal("production did not start before its deadline", next)
	}
	if err := q.Started(result(next, true)); err != nil {
		t.Fatal(err)
	}
}

// A lost worker's exact running copy is recovery, not a new disk admission (N3).
func TestDiskHeadroomReconnectRecoversRunningCopy(t *testing.T) {
	q, c := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	m = reportDisk(t, m, c, "volume", []string{"data"}, 100<<30, 81<<30)
	s := spec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 60 << 30}
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil {
		t.Fatal("no initial assignment")
	}
	machineCheck(t, q, m, a)
	c.Advance(61 * time.Second)
	check(t, q, machine("healthy", "fake"), nil)
	requireState(t, q, a.ID, "queued")
	m = reportDisk(t, m, c, "volume", []string{"data"}, 100<<30, 0)
	r := machineCheck(t, q, m, a)
	if len(r.Attempts) != 1 || r.Attempts[0].Stop || r.Attempts[0].Kill || r.Attempts[0].Hold {
		t.Fatalf("disk admission killed a running copy: %+v", r.Attempts)
	}
	got := job(t, q, a.ID)
	if got.State != "running" || got.Attempt != a.Attempt || got.Machine != a.Machine || got.Session != a.Session || got.ReleaseConfirmed || got.StartedAt == nil {
		t.Fatal("exact running copy was not recovered", got)
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

func TestDiskHeadroomHTTPRefusesImpossibleSubmission(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("a", "fake"), c, "private-volume-id", []string{"data"}, 100<<30, 2<<30)
	check(t, q, m, nil)
	s := spec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 100 << 30}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	var body struct{ Code, Error string }
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	const reason = "disk_headroom: host a data free=107374182400 bytes floor=1073741824 bytes need=107374182400 bytes total=107374182400 bytes"
	if response.Code != http.StatusConflict || body.Code != "disk_headroom" || body.Error != reason {
		t.Errorf("expected sanitized disk capacity refusal, got HTTP %d: %s", response.Code, response.Body.String())
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	err = (Client{URL: server.URL, Token: "test"}).Call(context.Background(), http.MethodPost, "/api/jobs", s, nil)
	var apiError *HTTPError
	if !errors.As(err, &apiError) || apiError.Status != http.StatusConflict || apiError.Code != "disk_headroom" || apiError.Message != reason {
		t.Errorf("Go client lost typed disk capacity refusal: %v", err)
	}
	if len(q.s.Jobs) != 0 {
		t.Error("impossible submission created a job")
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
	c.Advance(31 * time.Second)
	m.DiskVolumes[0].AgeSeconds = 31
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
	if len(status.Machines) != 1 || status.Machines[0].DiskVolumes[0].AgeSeconds < 31 || status.Machines[0].DiskVolumes[0].Error != "disk reading unavailable" {
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

func TestDiskHeadroomFreshnessToleratesReportingDelay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		elapsed time.Duration
		stale   bool
	}{
		{"healthy at 3.1 seconds", 2250 * time.Millisecond, false},
		{"healthy at 8.2 seconds", 7350 * time.Millisecond, false},
		{"stale at 30 seconds", 29150 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, c := fixture(t)
			m := reportDisk(t, machine("a", "fake"), c, "volume", []string{"data"}, 100<<30, 41<<30)
			// Include observation age already accrued before queue receipt,
			// then advance across the reporting gap.
			m.DiskVolumes[0].AgeSeconds = 0.85
			check(t, q, m, nil)
			c.Advance(tc.elapsed)
			s := spec("experiment")
			s.Disk = &DiskNeeds{OutputBytes: 30 << 30}
			reason := q.diskHeadroomReason(&Job{Spec: s}, q.s.Machines[m.ID])
			if tc.stale {
				if !strings.Contains(reason, "disk reading unavailable") {
					t.Fatalf("expired disk reading remained usable: %q", reason)
				}
			} else if reason != "" {
				t.Fatalf("sampling time expired a healthy disk reading: %s", reason)
			}
		})
	}
}

// A native status snapshot lost checkpoint admission at 3.9866s even though
// the completed capacity sample still had 3.87 TB free and no pending metadata.
func TestDiskHeadroomCheckpointFreshnessWithoutPendingMetadata(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("a", "fake"), c, "nas", []string{"checkpoint"}, 7920000000000, 3870000000000)
	check(t, q, m, nil)
	c.Advance(3990 * time.Millisecond)
	j := &Job{Spec: spec("experiment")}
	j.Spec.Resumable = true
	j.Spec.Disk = &DiskNeeds{CheckpointBytes: 1 << 30}
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); reason != "" {
		t.Fatalf("successful capacity reading expired between samples: %s", reason)
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	v := snap.Machines[0].DiskVolumes[0]
	if v.Error != "" || v.ProbeAgeSeconds != nil || v.Retained || v.AgeSeconds != 3.99 {
		t.Fatalf("incorrect capacity status: %+v", v)
	}
	// Retention never invents free space: admission still uses the last value.
	j.Spec.Disk.CheckpointBytes = v.FreeBytes
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); !strings.Contains(reason, "free=") || strings.Contains(reason, "unavailable") {
		t.Fatalf("retained capacity did not enforce headroom: %s", reason)
	}
}

func TestDiskHeadroomCheckpointWithoutPendingMetadataStalls(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("a", "fake"), c, "nas", []string{"checkpoint"}, 100<<30, 41<<30)
	// Even a long reported cadence cannot extend the 30s no-progress bound.
	m.DiskVolumes[0].SampleIntervalSeconds = 60
	check(t, q, m, nil)
	j := &Job{Spec: spec("experiment")}
	j.Spec.Resumable = true
	c.Advance(29990 * time.Millisecond)
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); reason != "" {
		t.Fatalf("last good reading expired before stall bound: %s", reason)
	}
	// Re-reporting the same observation must not reset the progress deadline.
	m.DiskVolumes[0].AgeSeconds = 29.99
	check(t, q, m, nil)
	c.Advance(10 * time.Millisecond)
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); !strings.Contains(reason, "disk reading unavailable") {
		t.Fatalf("stalled sampler did not fence at 30s: %s", reason)
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	v := snap.Machines[0].DiskVolumes[0]
	if v.Error == "" || v.Retained || v.FreeBytes != 41<<30 {
		t.Fatalf("stall status lost evidence or retained admission: %+v", v)
	}
}

func TestDiskHeadroomFreshnessUsesReceiveTimeAndReportedAge(t *testing.T) {
	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		t.Run(skew.String(), func(t *testing.T) {
			q, c := fixture(t)
			m := reportDisk(t, machine("a", "fake"), c, "volume", []string{"data"}, 100<<30, 41<<30)
			m.DiskVolumes[0].SampledAt = c.Now().Add(skew)
			m.DiskVolumes[0].SampleIntervalSeconds = 10
			m.DiskVolumes[0].AgeSeconds = 20
			m.DiskVolumes[0].Retained = true
			m.DiskVolumes[0].Error = "disk reading unavailable"
			check(t, q, m, nil)
			s := spec("experiment")
			s.Disk = &DiskNeeds{OutputBytes: 30 << 30}
			j := &Job{Spec: s}
			if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); reason != "" {
				t.Fatalf("clock skew expired a recent sample: %s", reason)
			}
			c.Advance(11 * time.Second)
			// Other worker traffic must not refresh the disk report's receive time.
			if err := q.update(func() error { q.s.Machines[m.ID].LastSeen = c.Now(); return nil }); err != nil {
				t.Fatal(err)
			}
			if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); !strings.Contains(reason, "disk reading unavailable") {
				t.Fatalf("reported age plus time since receipt did not expire: %s", reason)
			}
			snap, err := q.Snapshot()
			if err != nil || len(snap.Machines) != 1 || snap.Machines[0].DiskVolumes[0].AgeSeconds != 31 || snap.Machines[0].DiskVolumes[0].Retained {
				t.Fatal("status failed to age the detached sample", snap.Machines, err)
			}
		})
	}
}

// Review probe A1: routing to a ready server writes no new model/data bytes.
func TestDiskHeadroomRoutesReadyModelBelowFloor(t *testing.T) {
	for _, hold := range []bool{false, true} {
		t.Run("production-hold="+stringBool(hold), func(t *testing.T) {
			q, c, m := modelFixture(t, "model-host")
			if hold {
				if err := q.ConfigureHosts(map[string]HostPolicy{"model-host": {ProductionHold: true}}); err != nil {
					t.Fatal(err)
				}
			}
			first := addModelCall(t, q, "production")
			reportModel(t, q, m, "ready")
			if err := q.finishCall(first.ID); err != nil {
				t.Fatal(err)
			}
			m = reportDisk(t, m, c, "volume", []string{"data", "model-cache"}, 100<<30, 512<<20)
			reportModel(t, q, m, "ready")
			prod := addModelCall(t, q, "production")
			if prod.Machine != m.ID || prod.Attempt == "" {
				t.Fatalf("disk floor blocked a ready server: %+v", prod)
			}
		})
	}
}

func TestDiskHeadroomPinnedEngineRestartsWithCachedFiles(t *testing.T) {
	q, c, m := modelFixture(t)
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	slot := q.s.ModelSlots[slotKey(m.ID, "test")]
	recoveryReport(t, q, m, slot, "ready")
	old := slot.Attempt
	m.CachedModels = []string{"test"}
	m = reportDisk(t, m, c, "volume", []string{"data", "model-cache"}, 100<<30, 512<<20)
	recoveryReport(t, q, m, slot, "unloading")
	recoveryReport(t, q, m, slot, "failed")
	c.Advance(time.Second)
	m = reportDisk(t, m, c, "volume", []string{"data", "model-cache"}, 100<<30, 512<<20)
	recoveryReport(t, q, m, slot, "failed")
	if slot.State != "loading" || slot.Attempt == old {
		t.Fatalf("disk floor blocked cached pinned-engine restart: %+v", slot)
	}
}

func TestDiskHeadroomNewModelLoadChargesCacheSize(t *testing.T) {
	q, c, m := modelFixture(t)
	config := q.models
	config.Models[0].SizeBytes = 2 << 30
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	m = reportDisk(t, m, c, "data", []string{"data"}, 100<<30, 5<<30)
	cache := DiskVolume{ID: "cache", Roots: []string{"model-cache"}, TotalBytes: 100 << 30, FreeBytes: 2 << 30, SampledAt: c.Now()}
	m.DiskVolumes = append(m.DiskVolumes, cache)
	check(t, q, m, nil)
	call := addModelCall(t, q, "production")
	if slot := q.s.ModelSlots[slotKey(m.ID, "test")]; slot != nil {
		t.Fatal("new load ignored model size plus cache floor", slot)
	}
	m.DiskVolumes[0].FreeBytes = 512 << 20
	m.DiskVolumes[1].FreeBytes = 4 << 30
	check(t, q, m, nil)
	if slot := q.s.ModelSlots[slotKey(m.ID, "test")]; slot == nil || slot.State != "loading" || call.Machine != "" {
		t.Fatal("cache space did not admit new load despite unwritten data root", slot)
	}
}

// A short control interval cannot make a healthy reading expire before the
// worker's existing bounded filesystem observations and start handoff finish.
func TestDiskHeadroomFastHeartbeatAllowsStartHandoff(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("fast", "fake"), c, "volume", []string{"data"}, 100<<30, 41<<30)
	m.DiskVolumes[0].SampleIntervalSeconds = 0.01
	check(t, q, m, nil)
	submit(t, q, spec("experiment"))
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("no initial assignment")
	}
	c.Advance(2 * localObservationBudget)
	if err := q.Started(result(a, true)); err != nil {
		t.Fatalf("healthy sample expired during bounded handoff: %v", err)
	}
}

// s029: a production attempt was refused at the start fence twice (HTTP 409
// request_stale) and charged two losses. A reading that merely aged out while
// the attempt waited is not evidence of exhaustion; disk admission belongs at
// dispatch, so the start fence must not refuse on staleness.
func TestDiskHeadroomStaleReadingDoesNotRefuseStartFence(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("spark", "fake"), c, "volume", []string{"data", "model-cache"}, 100<<30, 41<<30)
	check(t, q, m, nil)
	prod := submit(t, q, spec("production"))
	a := check(t, q, m, nil).Job
	if a == nil || a.ID != prod.ID {
		t.Fatal("production not assigned")
	}
	// Keep worker contact fresh while the disk sampler stops making progress.
	if err := q.update(func() error {
		q.s.Machines[m.ID].DiskVolumes[0].AgeSeconds = 31
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if reason := q.diskHeadroomReason(a, q.s.Machines[m.ID]); !strings.Contains(reason, "disk reading unavailable") {
		t.Fatalf("disk reading did not expire before start fence: %s", reason)
	}
	launch := HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatalf("stale disk reading refused launch authorization: %v", err)
	}
	if err := q.Started(result(a, true)); err != nil {
		t.Fatalf("stale disk reading refused start hand-off: %v", err)
	}
}

// A fresh observation below the floor still refuses, names its predicate, and
// requeues without spending loss or infrastructure-retry budget, like a
// worker-side launch_refused disk withdrawal.
func TestDiskHeadroomStartFenceRefusalIsNotLoss(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("spark", "fake"), c, "volume", []string{"data"}, 100<<30, 41<<30)
	check(t, q, m, nil)
	prod := submit(t, q, spec("production"))
	a := check(t, q, m, nil).Job
	if a == nil || a.ID != prod.ID {
		t.Fatal("production not assigned")
	}
	if err := q.update(func() error {
		owner := q.s.Machines[a.Machine]
		owner.DiskVolumes[0].FreeBytes, owner.DiskVolumes[0].SampledAt, owner.DiskReceivedAt = 512<<20, c.Now(), c.Now()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	launch := HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)}
	if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
		t.Fatalf("fresh below-floor reading did not refuse launch: %v", err)
	}
	if !strings.Contains(logs.String(), "event=launch_placement_rejected") || !strings.Contains(logs.String(), "predicate=disk_floor") {
		t.Fatalf("placement rejection did not name its predicate: %s", logs.String())
	}
	r := result(a, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "io_error"}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, prod.ID)
	if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("start-fence placement refusal charged as a loss: state=%s losses=%d retries=%d reason=%q", got.State, got.Losses, got.InfrastructureRetries, got.Reason)
	}
}

// The start fence refuses on the free floor alone, using the same current or
// retained readings dispatch trusts: admitted need never re-refuses Started.
func TestDiskHeadroomStartFenceUsesCurrentFloor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		free     uint64
		retained bool
		want     error
	}{
		{"above floor below need", 2 << 30, false, nil},
		{"retained below floor", 512 << 20, true, ErrStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, c := fixture(t)
			m := reportDisk(t, machine("spark", "fake"), c, "volume", []string{"data"}, 100<<30, 41<<30)
			check(t, q, m, nil)
			s := spec("production")
			s.Disk = &DiskNeeds{OutputBytes: 2 << 30}
			prod := submit(t, q, s)
			a := check(t, q, m, nil).Job
			if a == nil || a.ID != prod.ID {
				t.Fatal("production not assigned")
			}
			if err := q.update(func() error {
				owner := q.s.Machines[a.Machine]
				v := &owner.DiskVolumes[0]
				v.FreeBytes, v.SampledAt, owner.DiskReceivedAt = tc.free, c.Now(), c.Now()
				if tc.retained {
					v.Retained, v.Error = true, "probe failed"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := q.Started(result(a, true)); !errors.Is(err, tc.want) {
				t.Fatalf("Started = %v, want %v", err, tc.want)
			}
		})
	}
}

// Busy workers reported healthy local capacity 4.8-8.2 seconds old while the
// three-heartbeat window held admission despite tens of GiB of free space.
func TestDiskHeadroomLocalFreshnessUntilStall(t *testing.T) {
	q, c := fixture(t)
	m := reportDisk(t, machine("a", "fake"), c, "local", []string{"data", "environment", "file-cache", "model-cache"}, 245<<30, 55<<30)
	check(t, q, m, nil)
	j := &Job{Spec: spec("experiment")}
	j.Spec.Disk = &DiskNeeds{OutputBytes: 1 << 30}
	c.Advance(8200 * time.Millisecond)
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); reason != "" {
		t.Fatalf("healthy local capacity expired between reports: %s", reason)
	}
	// The retained reading must still enforce actual free space.
	j.Spec.Disk.OutputBytes = 55 << 30
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); !strings.Contains(reason, "free=") || strings.Contains(reason, "unavailable") {
		t.Fatalf("last good local capacity did not enforce headroom: %s", reason)
	}
	j.Spec.Disk.OutputBytes = 1 << 30
	c.Advance(21790 * time.Millisecond)
	m.DiskVolumes[0].AgeSeconds = 29.99
	// Neither re-reporting nor a long nominal cadence renews the observation.
	m.DiskVolumes[0].SampleIntervalSeconds = 60
	check(t, q, m, nil)
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); reason != "" {
		t.Fatalf("local capacity expired before stall deadline: %s", reason)
	}
	c.Advance(10 * time.Millisecond)
	if reason := q.diskHeadroomReason(j, q.s.Machines[m.ID]); !strings.Contains(reason, "disk reading unavailable") {
		t.Fatalf("repeated report renewed stalled local capacity: %s", reason)
	}
}
