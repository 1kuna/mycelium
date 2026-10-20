package myce

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func m3Spec(priority, device string, memory, ram float64) Spec {
	s := spec(priority)
	s.Needs.MemoryGB, s.Needs.HostMemoryGB = memory, ram
	if device != "" {
		s.Needs.GPU = &GPUSelection{Devices: []string{device}}
	}
	return s
}

func TestM3SelectorsAndTypedErrors(t *testing.T) {
	for _, tc := range []struct{ name, selector, code string }{
		{"count", `{"count":2}`, "gpu_count_unsupported"},
		{"zero", `{"count":0}`, "gpu_selector_invalid"},
		{"fraction", `{"count":1.5}`, "gpu_selector_invalid"},
		{"empty", `{"devices":[]}`, "gpu_selector_invalid"},
		{"duplicate", `{"devices":["0","0"]}`, "gpu_selector_invalid"},
		{"noncanonical", `{"devices":["01"]}`, "gpu_selector_invalid"},
		{"number", `{"devices":[0]}`, "gpu_selector_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			b, _ := json.Marshal(spec("experiment"))
			var raw map[string]any
			_ = json.Unmarshal(b, &raw)
			raw["needs"].(map[string]any)["gpu"] = json.RawMessage(tc.selector)
			b, _ = json.Marshal(raw)
			r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(b))
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			API{q, "test"}.Handler().ServeHTTP(w, r)
			var body struct{ Code string }
			_ = json.Unmarshal(w.Body.Bytes(), &body)
			if w.Code != 400 || body.Code != tc.code {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	q, _ := fixture(t)
	m := m3Machine()
	m.Devices[0].Platform = "cuda"
	profile := *m.CapabilityProfile
	profile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 8 << 30}}
	m.Devices[0].CapabilityProfile = &profile
	machineCheck(t, q, m)
	j := submit(t, q, m3Spec("experiment", "1", 2, 1))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != j.ID || !slices.Equal(a.AllocatedDevices, []string{"1"}) {
		t.Fatal("heterogeneous selector", a)
	}
	unknown := submit(t, q, m3Spec("experiment", "9", 2, 1))
	if got := job(t, q, unknown.ID); got.State != "queued" || got.DiagnosticCode != "no_matching_device" {
		t.Fatalf("unknown selection: %+v", got)
	}
	s := cpuSpec(1, 1)
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	_, err := q.Submit(s)
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Code != "gpu_selector_invalid" {
		t.Fatal(err)
	}
}

func TestM3RetiredNamesPreserveReplay(t *testing.T) {
	q, _ := fixture(t)
	s := m3Spec("experiment", "", 1, 1)
	s.SubmissionKey, s.AllowedMachines = "before-migration", []string{"b70-gpu0"}
	original := submit(t, q, s)
	q.mu.Lock()
	q.s.Machines["b70-gpu0"] = &Machine{ID: "b70-gpu0", Replacement: &MachineReplacement{Machine: "b70", Device: "0"}}
	q.mu.Unlock()
	replayed, err := q.Submit(s)
	if err != nil || replayed.ID != original.ID {
		t.Fatal("key replay changed", replayed, err)
	}
	s.SubmissionKey = "new-intent"
	_, err = q.Submit(s)
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Status != 400 || typed.Code != "machine_renamed" || !strings.Contains(typed.Message, `needs.gpu.devices: ["0"]`) {
		t.Fatal(err)
	}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: s.AllowedMachines}}); !errors.As(err, &typed) || typed.Code != "machine_renamed" {
		t.Fatal(err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"b70-gpu0": {}}); !errors.As(err, &typed) || typed.Code != "machine_renamed" {
		t.Fatal(err)
	}
	s.SubmissionKey, s.AllowedMachines = "literal-name", []string{"arbitrary-gpu0"}
	if _, err := q.Submit(s); err != nil {
		t.Fatal("arbitrary suffix treated as retired", err)
	}
}

func TestM3CrossDeviceRAMVictimAndExactAck(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB = 10
	submit(t, q, m3Spec("experiment", "1", 2, 8))
	victim := assignedJob(machineCheck(t, q, m))
	if victim == nil {
		t.Fatal("missing victim")
	}
	demand := submit(t, q, m3Spec("production", "0", 2, 5))
	reply := machineCheck(t, q, m, victim)
	if job(t, q, victim.ID).State != "stopping" || assignedJob(reply) != nil {
		t.Fatal("RAM release must stop neighbor and wait for acknowledgement", reply)
	}
	machineCheck(t, q, m) // Omission is not an acknowledgement.
	requireState(t, q, demand.ID, "queued")
	stale := result(victim, false)
	stale.Session = "stale"
	stale.ReleaseOnly = true
	if err := q.Complete(stale); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, victim.ID); got.ReleaseConfirmed || got.State != "stopping" {
		t.Fatal("stale acknowledgement discharged custody", got)
	}
	finishInterrupted(t, q, victim)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != demand.ID || !slices.Equal(a.AllocatedDevices, []string{"0"}) {
		t.Fatal("exact ack did not free shared RAM", a)
	}
	if got := job(t, q, victim.ID); got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatal("planned stop charged retry", got)
	}
}

func TestM3UnifiedMemoryDebtAndDeterminism(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB = 12
	m.Devices[1].UnifiedMemory = true
	submit(t, q, m3Spec("experiment", "1", 6, 4))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil {
		t.Fatal("first assignment")
	}
	submit(t, q, m3Spec("experiment", "0", 1, 4))
	if b := assignedJob(machineCheck(t, q, m, a)); b != nil {
		t.Fatal("unified allocation charged only dedicated RAM", b)
	}
	snap, _ := q.Snapshot()
	if len(snap.Machines) != 1 || snap.Machines[0].HostMemoryInUseGB != 10 || len(snap.Machines[0].Devices[1].Attempts) != 1 {
		t.Fatal("component/host status", snap.Machines)
	}
	q.mu.Lock()
	demand := &Job{Spec: m3Spec("production", "0", 1, 8)}
	for range 3 {
		target := q.machineTargets(q.s.Machines[m.ID])[1]
		p := q.roomPlan(demand, target, true)
		if p == nil || len(p.jobs) != 1 || p.jobs[0].ID != a.ID {
			q.mu.Unlock()
			t.Fatal("heuristic did not select useful cross-device unified victim", p)
		}
	}
	q.mu.Unlock()
}

func TestM3StopEventsCoverMachineSession(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	submit(t, q, m3Spec("experiment", "0", 1, 1))
	a := assignedJob(machineCheck(t, q, m))
	submit(t, q, m3Spec("experiment", "1", 1, 1))
	b := assignedJob(machineCheck(t, q, m, a))
	if a == nil || b == nil {
		t.Fatal("missing concurrent attempts")
	}
	demand := &Job{ID: "event-production", Spec: spec("production")}
	q.stopBatchFor(q.s.Jobs[a.ID], demand)
	if err := q.Cancel(b.ID); err != nil {
		t.Fatal(err)
	}
	read := func(session string) WorkerEvent {
		r := httptest.NewRequest("GET", "/api/workers/"+m.ID+"/events?session="+session, nil)
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		w := httptest.NewRecorder()
		API{q, "test"}.Handler().ServeHTTP(w, r)
		var e WorkerEvent
		if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := read(m.Session)
	if len(e.Events) != 2 || e.Events[0].Session != m.Session || e.Events[1].Session != m.Session {
		t.Fatal("stop starvation", e)
	}
	for _, event := range e.Events {
		if event.ID == a.ID {
			if event.PriorityStop == nil || event.PriorityStop.Demand != demand.ID {
				t.Fatal("event omitted priority stop provenance", event)
			}
			p := &process{job: *a}
			control := processControl{clock: clock, processes: map[attemptKey]*process{jobKey(*a): p}, pendingEvents: map[attemptKey]WorkerEvent{jobKey(*a): event}}
			control.apply()
			if p.priorityStop == nil || p.priorityStop.Demand != demand.ID {
				t.Fatal("event did not record intent before delivery")
			}
		}
	}
	if again := read(m.Session); again.Cursor != e.Cursor {
		t.Fatal("unstable cursor")
	}
	clock.Advance(StopGrace + time.Second)
	if kill := read(m.Session); kill.Cursor == e.Cursor || !kill.Events[0].Kill || !kill.Events[1].Kill {
		t.Fatal("kill transition", kill)
	}
	if wrong := read("other-session"); len(wrong.Events) != 0 || wrong.ID != "" {
		t.Fatal("session fence", wrong)
	}
}

func TestM3SharedLossShortestAndPerAttemptBound(t *testing.T) {
	q, clock := fixture(t)
	q.lossTimeouts = [3]time.Duration{9 * time.Second, 3 * time.Second, 7 * time.Second}
	m := m3Machine()
	submit(t, q, m3Spec("production", "0", 1, 1))
	a := assignedJob(machineCheck(t, q, m))
	submit(t, q, m3Spec("deadline", "1", 1, 1))
	b := assignedJob(machineCheck(t, q, m, a))
	if a == nil || b == nil {
		t.Fatal("missing attempts")
	}
	q.mu.Lock()
	timeout := q.machineLossTimeout(m.ID)
	q.mu.Unlock()
	if timeout != 3*time.Second {
		t.Fatal("not shortest", timeout)
	}
	clock.Advance(3 * time.Second)
	check(t, q, machine("healthy", "fake"), nil)
	_, _ = q.Snapshot()
	if got := job(t, q, a.ID); got.InfrastructureRetries != 1 {
		t.Fatal("production loss not charged", got)
	}
	// Every production attempt on this machine has its own two-requeue bound.
	for cycle := 0; cycle < 2; cycle++ {
		m.Session = "reconnected-" + string(rune('a'+cycle))
		c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Fresh: true, PreviousSession: a.Session}
		r, err := q.Checkin(c)
		if err != nil {
			t.Fatal(err)
		}
		a = assignedJob(r)
		if a == nil {
			t.Fatal("no infrastructure retry", r)
		}
		machineCheck(t, q, m, a)
		clock.Advance(10 * time.Second)
		check(t, q, machine("healthy", "fake"), nil)
		_, _ = q.Snapshot()
	}
	if got := job(t, q, a.ID); got.State != "failed" || got.InfrastructureRetries != 2 {
		t.Fatal("production retry bound", got)
	}
}

func TestM3HoldMaintenanceAndRepeatedPlannedStops(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	if err := q.ConfigureHosts(map[string]HostPolicy{m.ID: {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	submit(t, q, m3Spec("experiment", "0", 2, 1))
	a := assignedJob(machineCheck(t, q, m))
	submit(t, q, m3Spec("experiment", "1", 2, 1))
	b := assignedJob(machineCheck(t, q, m, a))
	submit(t, q, cpuSpec(1, 1))
	cpu := assignedJob(machineCheck(t, q, m, a, b))
	if a == nil || b == nil || cpu == nil {
		t.Fatal("initial host allocations")
	}
	p := submit(t, q, m3Spec("production", "0", 2, 1))
	for _, j := range []*Job{a, b, cpu} {
		requireState(t, q, j.ID, "stopping")
	}
	finishInterrupted(t, q, a)
	finishInterrupted(t, q, b)
	if assignedJob(machineCheck(t, q, m, cpu)) != nil {
		t.Fatal("free device bypassed host hold")
	}
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, cpu)
	if assignedJob(machineCheck(t, q, m)) != nil {
		t.Fatal("hold bypassed maintenance")
	}
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, p.ID); got.State != "failed" || got.InfrastructureRetries != 0 {
		t.Fatal("maintenance did not retain fail-closed production admission", got)
	}
	p = submit(t, q, m3Spec("production", "0", 2, 1))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != p.ID {
		t.Fatal("new production after resume", run)
	}
	finish(t, q, run, true)
	// More than two queue-recorded maintenance stops remain planned, including
	// a concurrent neighbor, without consuming either infrastructure counter.
	for range 3 {
		a = assignedJob(machineCheck(t, q, m))
		if a == nil {
			t.Fatal("no resumable work")
		}
		b = assignedJob(machineCheck(t, q, m, a))
		if b == nil {
			t.Fatal("no neighbor")
		}
		if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
			t.Fatal(err)
		}
		for _, j := range []*Job{a, b} {
			finishInterrupted(t, q, j)
			got := job(t, q, j.ID)
			if got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatal("planned stop consumed retry", got)
			}
		}
		if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestM3ProductionLossChargesEveryAttempt(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	q.lossTimeouts = [3]time.Duration{5 * time.Second, 3 * time.Second, 9 * time.Second}
	submit(t, q, m3Spec("production", "0", 1, 1))
	a := assignedJob(machineCheck(t, q, m))
	submit(t, q, m3Spec("production", "1", 1, 1))
	b := assignedJob(machineCheck(t, q, m, a))
	machineCheck(t, q, m, a, b)
	clock.Advance(6 * time.Second)
	check(t, q, machine("healthy", "fake"), nil)
	for _, j := range []*Job{a, b} {
		if got := job(t, q, j.ID); got.InfrastructureRetries != 1 || got.Losses != 1 {
			t.Fatal("shared loss did not charge each production attempt", got)
		}
	}
}

func TestM3SharedLossRecoversEveryRetainedDevice(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	submit(t, q, m3Spec("experiment", "0", 1, 1))
	a := assignedJob(machineCheck(t, q, m))
	submit(t, q, m3Spec("experiment", "1", 1, 1))
	b := assignedJob(machineCheck(t, q, m, a))
	machineCheck(t, q, m, a, b)
	clock.Advance(61 * time.Second)
	check(t, q, machine("healthy", "fake"), nil)
	requireState(t, q, a.ID, "queued")
	requireState(t, q, b.ID, "queued")
	r := machineCheck(t, q, m, a, b)
	for _, directive := range r.Attempts {
		if directive.Stop || directive.Hold {
			t.Fatal("neighbor prevented exact device recovery", directive)
		}
	}
	for _, prior := range []*Job{a, b} {
		recovered := job(t, q, prior.ID)
		if recovered.State != "running" || recovered.Attempt != prior.Attempt || !slices.Equal(recovered.AllocatedDevices, prior.AllocatedDevices) {
			t.Fatal("exact device custody was not recovered", recovered)
		}
	}
}

func TestM3HeterogeneousCopyPreparation(t *testing.T) {
	q, _ := fixture(t)
	config := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{Name: "cuda-copy", Path: "weights", Engine: "fake", MemoryGB: 2, Platforms: []string{"cuda"}, SizeBytes: 1024, SHA256: strings.Repeat("a", 64)}}}
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.Devices[1].Platform = "cuda"
	profile := *m.CapabilityProfile
	profile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 8 << 30}}
	m.Devices[1].CapabilityProfile = &profile
	m.ModelConfigHash = modelConfigHash(config)
	machineCheck(t, q, m)
	if err := q.SyncModel(SyncRequest{Model: "cuda-copy", To: "cuda"}); err != nil {
		t.Fatal(err)
	}
	r := machineCheck(t, q, m)
	if len(r.CopyPlan) != 1 || r.CopyPlan[0].Launch == nil || r.CopyPlan[0].Launch.Device != "1" {
		t.Fatal("second-platform copy was not offered once", r.CopyPlan)
	}
	s := m3Spec("experiment", "1", 1, 1)
	s.Needs.Platform = Platform{Required: []string{"cuda"}}
	queued := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil {
		t.Fatal("missing neighboring execution", job(t, q, queued.ID).Reason)
	}
	if got := machineCheck(t, q, m, a); len(got.CopyPlan) != 0 {
		t.Fatal("background copy ignored busy machine", got.CopyPlan)
	}
}

func TestM3ExtensionDeviceIntersection(t *testing.T) {
	e := compositionExtension(t, false)
	e.Variants[0].Needs.GPU = &GPUSelection{Devices: []string{"0", "1"}}
	j := &Job{State: "queued", Extensions: []Extension{e}, Spec: Spec{Extension: extensionKey(e), Needs: ResourceNeeds{GPU: &GPUSelection{Devices: []string{"1", "2"}}}}}
	m := m3Machine()
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
	for _, id := range []string{"0", "1", "2"} {
		target := m.Devices[0].machine(m)
		target.Device = id
		choices := extensionJobs(j, &target, false)
		if id != "1" {
			if len(choices) != 0 {
				t.Fatalf("extension broadened device intersection to %s", id)
			}
		} else if len(choices) != 1 || choices[0].Spec.Needs.GPU == nil || !slices.Equal(choices[0].Spec.Needs.GPU.Devices, []string{"1"}) {
			t.Fatal("resolved intersection was not reserved", choices)
		}
	}
}

func TestM3QualificationAndModelSlotsStayOnDevice(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	target := m.Devices[0].machine(m)
	target.ExtensionFingerprint = "exact-device-0"
	r := newExtensionReport(e, &target, "kind:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "check-0"
	m.Devices[0].ExtensionFingerprint = target.ExtensionFingerprint
	m.Devices[0].ExtensionReports = []ExtensionReport{r}
	m.Devices[1].ExtensionFingerprint = "exact-device-1"
	m.Devices[1].ExtensionReports = []ExtensionReport{r}
	j := submit(t, q, Spec{Name: "qualified", Project: "test", Kind: "sample"})
	q.mu.Lock()
	resolved := q.s.Jobs[j.ID]
	zero, one := m.Devices[0].machine(m), m.Devices[1].machine(m)
	ok0, ok1 := qualifiedExtension(resolved, &zero) != nil, qualifiedExtension(resolved, &one) != nil
	q.mu.Unlock()
	if !ok0 || ok1 {
		t.Fatal("device 0 qualification authorized its neighbor", ok0, ok1)
	}
	// Two pins may own the same weights but have distinct execution identities.
	config := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{Name: "shared", Path: "fake", Engine: "fake", MemoryGB: 2, HostMemoryGB: 1, Platforms: []string{"fake"}}}}
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	m.ModelURL = "http://127.0.0.1:12345"
	m.ModelConfigHash = modelConfigHash(q.models)
	machineCheck(t, q, m)
	for i, project := range []string{"first", "second"} {
		if err := q.Pin(ModelPin{Project: project, Model: "shared", Machine: m.ID, Device: strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
	}
	q.mu.Lock()
	s0, s1 := q.s.ModelSlots[slotKey(m.ID, "shared", "0")], q.s.ModelSlots[slotKey(m.ID, "shared", "1")]
	q.mu.Unlock()
	if s0 == nil || s1 == nil || s0.Attempt == s1.Attempt {
		t.Fatal("model slot identities", s0, s1)
	}
	_, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "shared", Device: "0", Attempt: s0.Attempt, State: "ready"}}})
	if err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	ready0, ready1 := s0.State == "ready", s1.State == "ready"
	q.mu.Unlock()
	if !ready0 || ready1 {
		t.Fatal("model report crossed device", ready0, ready1)
	}
	config.Models[0].Device = "0,1"
	if err := q.ConfigureModels(config); err == nil {
		t.Fatal("multi-card model accepted")
	}
}

func TestM3QueueRestartRetainsMissingDeviceCustody(t *testing.T) {
	root := t.TempDir()
	db := filepath.Join(root, "queue.db")
	clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	q, err := Open(db, filepath.Join(root, "checkpoints"), clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	q.checkpointProbe = func() bool { return true }
	q.nasAvailable = true
	m := m3Machine()
	m.Devices[1].UnifiedMemory = true
	submit(t, q, m3Spec("experiment", "0", 2, 1))
	a := assignedJob(machineCheck(t, q, m))
	submit(t, q, m3Spec("experiment", "1", 2, 1))
	b := assignedJob(machineCheck(t, q, m, a))
	if a == nil || b == nil {
		t.Fatal("initial assignments")
	}
	missing := m
	missing.Devices = slices.Clone(m.Devices[:1])
	machineCheck(t, q, missing, a, b)
	snap, _ := q.Snapshot()
	if len(snap.Machines[0].Devices) != 2 || snap.Machines[0].HostMemoryInUseGB != 4 {
		t.Fatal("missing device erased unified custody", snap.Machines)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, filepath.Join(root, "checkpoints"), clock)
	if err != nil {
		t.Fatal(err)
	}
	q.checkpointProbe = func() bool { return true }
	q.nasAvailable = true
	third := submit(t, q, m3Spec("experiment", "0", 2, 1))
	r := machineCheck(t, q, m) // Missing reports redeliver exact old assignments.
	seen := map[string]bool{}
	for _, d := range r.Attempts {
		if d.Job != nil {
			seen[d.Job.ID] = true
		}
	}
	if !seen[a.ID] || !seen[b.ID] || seen[third.ID] {
		t.Fatal("restart lost omitted custody", r)
	}
	if err := q.Cancel(a.ID); err != nil {
		t.Fatal(err)
	}
	machineCheck(t, q, m, b)
	requireState(t, q, third.ID, "queued")
	ack := result(a, false)
	ack.ReleaseOnly = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	next := assignedJob(machineCheck(t, q, m, b))
	if next == nil || next.ID != third.ID {
		t.Fatal("exact ack did not admit replacement", next)
	}
	if got := job(t, q, b.ID); got.Attempt != b.Attempt || got.State != "running" {
		t.Fatal("neighbor custody changed", got)
	}
}
