package myce

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInventoryObservationRestartAdmission(t *testing.T) {
	for _, state := range []string{"pending", "failed", "stale"} {
		t.Run(state, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			m.Devices = m.Devices[:1]
			// Retained capacities are deliberately healthy: contact and saved devices
			// cannot substitute for a completed observation in this new session.
			m.Session = "restarted-worker"
			m.InventoryProbe = &InventoryObservation{State: state}
			gpu := submit(t, q, spec("experiment"))
			cpu := submit(t, q, cpuSpec(1, 1))
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != cpu.ID {
				t.Fatal("CPU backfill stopped with device probe", run)
			}
			finish(t, q, run, true)
			for i := 0; i < 3; i++ {
				clock.Advance(5 * time.Minute)
				if a := assignedJob(machineCheck(t, q, m)); a != nil {
					t.Fatal("GPU assigned without fresh inventory", a)
				}
			}
			got := job(t, q, gpu.ID)
			if got.State != "queued" || got.Attempt != 0 || !strings.Contains(got.Reason, "fresh accelerator inventory") {
				t.Fatalf("wrong inventory wait: state=%s attempt=%d reason=%s", got.State, got.Attempt, got.Reason)
			}
			m.InventoryProbe = &InventoryObservation{At: clock.Now(), State: "ready", Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}}
			a := assignedJob(machineCheck(t, q, m))
			if a == nil || a.ID != gpu.ID || a.Attempt != 1 {
				t.Fatal("fresh probe did not recover first GPU assignment", a)
			}
		})
	}
}

func TestInventoryObservationExpiryAndLaunchRefusal(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	m.InventoryProbe = &InventoryObservation{At: clock.Now().Add(24 * time.Hour), State: "ready", Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}}
	machineCheck(t, q, m)
	// Receipt uses queue time, not the worker's wall clock. Withhold as soon as
	// the actual observation expires, even without a new failed check-in.
	clock.Advance(2 * time.Second)
	gpu := submit(t, q, spec("experiment"))
	if got := job(t, q, gpu.ID); got.Attempt != 0 || !strings.Contains(got.Reason, "fresh accelerator inventory") {
		t.Fatal("cached contact renewed expired discovery", got.Reason)
	}
	m.InventoryProbe.At = clock.Now()
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != gpu.ID {
		t.Fatal("fresh report missing", a)
	}
	clock.Advance(time.Millisecond)
	r := result(a, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: "device_probe_unavailable", PrelaunchRefused: true}
	r.ExecutionEvidence = unstartedExecution(a.Attempt)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// Replaying the same good discovery after a newer failed launch must not
	// immediately reassign the job. Another CPU command still gets the host.
	m.InventoryProbe.AgeSeconds = clock.Now().Sub(m.InventoryProbe.At).Seconds()
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatal("cached scan erased newer launch failure", got)
	}
	// Delivery jitter can move the queue-rebased scan time forward, but the
	// original worker scan identity must still prevent cache replay.
	clock.Advance(time.Millisecond)
	m.InventoryProbe.AgeSeconds = 0
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatal("receipt delay renewed a refused scan", got.ID)
	}
	cpu := submit(t, q, cpuSpec(1, 1))
	c := assignedJob(machineCheck(t, q, m))
	if c == nil || c.ID != cpu.ID {
		t.Fatal("device failure blocked CPU", c)
	}
	finish(t, q, c, true)
	clock.Advance(time.Millisecond)
	m.InventoryProbe.At, m.InventoryProbe.AgeSeconds = clock.Now(), 0
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != gpu.ID || next.Attempt != 2 {
		t.Fatal("new successful scan did not clear launch fence", next)
	}
	if got := job(t, q, gpu.ID); got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatal("probe refusal consumed execution failure budget")
	}
}

func TestInventoryObservationPredecessorQueue(t *testing.T) {
	var reports []Checkin
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
		}
		reports = append(reports, c)
		raw, _ := json.Marshal(c)
		if strings.Contains(string(raw), "inventory_probe_failed_scan") || strings.Contains(string(raw), "inventory_probe_after") {
			t.Error("queue-owned fence leaked into predecessor check-in")
		}
		if c.Machine.InventoryProbe != nil {
			w.WriteHeader(400)
			io.WriteString(w, `{"code":"request_invalid","error":"unknown field"}`)
			return
		}
		json.NewEncoder(w).Encode(Reply{AttemptProtocol: keyedAttemptProtocol})
	}))
	defer server.Close()
	w := Worker{Client: Client{URL: server.URL, inventoryWire: &atomic.Bool{}}}
	w.Client.inventoryWire.Store(true)
	m := m3Machine()
	m.InventoryProbe = &InventoryObservation{State: "failed"}
	m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "device_probe_unavailable", 0
	for range 2 {
		if _, err := w.checkin(t.Context(), Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
	}
	if len(reports) != 3 || reports[0].Machine.InventoryProbe == nil || reports[1].Machine.InventoryProbe != nil || reports[2].Machine.InventoryProbe != nil {
		t.Fatal("predecessor fallback not retained")
	}
	for _, c := range reports {
		if c.Machine.Devices[0].Reason != "device_probe_unavailable" || c.Machine.Devices[0].FreeMemoryGB != 0 {
			t.Fatal("compatibility stripped device refusal")
		}
	}
}

func TestInventoryObservationPartialAndPersistence(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.InventoryProbe = &InventoryObservation{At: clock.Now(), State: "partial", Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}}
	m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "device_probe_unavailable", 0
	gpu := submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != gpu.ID || len(a.AllocatedDevices) != 1 || a.AllocatedDevices[0] != m.Devices[1].ID {
		t.Fatal("failed family blocked healthy sibling", a)
	}
	if d := durableMachine(&m); d.InventoryProbe == nil || d.InventoryProbe.State != "stale" || inventoryObservationFresh(d.InventoryProbe, clock.Now()) {
		t.Fatal("queue reopen could trust saved discovery")
	}
	m.GPU = "cpu" // Degraded inventory projects the host to CPU capacity.
	for _, state := range []string{"pending", "failed", "stale"} {
		m.InventoryProbe.State = state
		if why := HyphaAvailability(&m); !strings.Contains(why, "fresh accelerator inventory: "+state) {
			t.Fatal("host availability omitted discovery wait", why)
		}
	}
}
