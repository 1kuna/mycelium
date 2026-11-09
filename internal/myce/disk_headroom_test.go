package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Fake worker inventory must withhold work temporarily, never reject submission.
func TestDiskHeadroomHoldsAndRecovers(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, c := fixture(t)
		m := machine("disk-worker", "fake", "disk-host")
		setFree := func(free uint64) {
			b, _ := json.Marshal(map[string]any{"disk_volumes": []any{map[string]any{"roots": []string{"data"}, "total_bytes": uint64(100 << 30), "free_bytes": free, "sampled_at": c.Now()}}})
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
		}
		setFree(2 << 30)
		check(t, q, m, nil)
		s := spec("experiment")
		// A hint is optional; this one makes the job need 2 GiB beyond the floor.
		if err := json.Unmarshal([]byte(`{"disk":{"output_bytes":2147483648}}`), &s); err != nil {
			t.Fatal(err)
		}
		j := submit(t, q, s)
		if got := check(t, q, m, nil).Job; got != nil {
			t.Fatalf("low-space host admitted job: %s", got.ID)
		}
		var snap Snapshot
		if err := json.Unmarshal([]byte(statusViewResponse(t, q, "/api/status")), &snap); err != nil {
			t.Fatal(err)
		}
		if len(snap.Machines) != 1 || len(snap.Machines[0].DiskVolumes) != 1 || snap.Machines[0].DiskVolumes[0].FreeBytes != 2<<30 {
			t.Fatal("API omitted worker disk inventory", snap.Machines)
		}
		held := cpuIsolationJob(t, snap, j.ID)
		if held.State != "queued" || held.DiagnosticCode != "disk_headroom" {
			t.Fatalf("not a temporary typed hold: %+v", held)
		}
		for _, part := range []string{"disk-host", "free=2147483648", "floor=1073741824", "need=2147483648"} {
			if !strings.Contains(held.Reason, part) {
				t.Fatalf("missing %q: %s", part, held.Reason)
			}
		}
		setFree(10 << 30)
		admitted := check(t, q, m, nil).Job
		if admitted == nil || admitted.ID != j.ID {
			t.Fatal("recovered space did not admit queued work", admitted)
		}
		// Need was admitted at dispatch; the start fence refuses only a current
		// reading below the free floor (s029).
		setFree(512 << 20)
		check(t, q, m, admitted)
		if err := q.Started(result(admitted, true)); err != ErrStale {
			t.Fatalf("launch ignored disk drift: %v", err)
		}
		refused := result(admitted, false)
		refused.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "launch_refused"}
		if err := q.Complete(refused); err != nil {
			t.Fatal(err)
		}
		held = job(t, q, j.ID)
		if held.State != "queued" || held.Losses != 0 || held.InfrastructureRetries != 0 {
			t.Fatalf("prelaunch disk hold charged a failure: %+v", held)
		}
		setFree(10 << 30)
		admitted = check(t, q, m, nil).Job
		if admitted == nil {
			t.Fatal("disk drift did not recover")
		}
		if err := q.Started(result(admitted, true)); err != nil {
			t.Fatalf("launch did not recover: %v", err)
		}

	})

	t.Run("DiskHeadroomProductionRepins", func(t *testing.T) {
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

	})

	t.Run("DiskHeadroomProductionPreemptsWithObservedSpace", func(t *testing.T) {
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

	})

	t.Run("DiskHeadroomCapacityRefusesImpossibleSubmission", func(t *testing.T) {
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

	})

	t.Run("DiskHeadroomHTTPRefusesImpossibleSubmission", func(t *testing.T) {
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

	})
}
