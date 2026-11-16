package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func residentFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m := modelFixture(t)
	m.FreeMemoryGB = 32
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	m.Capabilities = append(m.Capabilities, residentJobsCapability)
	check(t, q, m, nil)
	s := spec("experiment")
	s.Resumable = false
	s.Needs.MemoryGB, s.Needs.HostMemoryGB = 1, 1
	s.Resident = &ResidentBinding{Model: "test"}
	return q, clock, m, s
}

func residentAssigned(t *testing.T, q *Queue, m Machine, s Spec) *Job {
	t.Helper()
	j := submit(t, q, s)
	r := check(t, q, m, nil)
	if r.Job == nil || r.Job.ID != j.ID {
		t.Fatalf("resident driver not assigned: %+v; job=%+v", r, job(t, q, j.ID))
	}
	return r.Job
}

func residentReport(t *testing.T, q *Queue, m Machine, j *Job, state string) *ModelSlot {
	t.Helper()
	name := j.Spec.Resident.Engine.Model.Name
	slot := q.s.ModelSlots[targetSlotKey(&m, name)]
	if slot == nil {
		t.Fatal("resident engine not reserved")
	}
	c := Checkin{Machine: m, ModelReports: []ModelReport{{Name: name, Attempt: slot.Attempt, Device: slot.Device, State: state, CallsMeasured: true}}, period: time.Second}
	if j.State == "running" {
		c.Running = &Execution{ID: j.ID, Attempt: j.Attempt}
	}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	return slot
}

func TestResidentJobsReuseChargeAndMeasuredIdle(t *testing.T) {
	q, clock, m, s := residentFixture(t)
	first := residentAssigned(t, q, m, s)
	clock.Advance(4 * time.Second) // Measured load cost, not a production timeout.
	slot := residentReport(t, q, m, first, "ready")
	attempt := slot.Attempt
	if q.modelMemory(m.ID) != 8 || len(q.s.ModelSlots) != 1 {
		t.Fatal("cold engine was not charged once")
	}
	finish(t, q, first, true)
	s.Files = nil
	second := residentAssigned(t, q, m, s)
	if second.Spec.Resident.Engine.Model.Name != slot.Model || slot.Attempt != attempt || slot.State != "ready" || q.modelMemory(m.ID) != 8 {
		t.Fatalf("same stage reloaded or double charged: %+v", slot)
	}
	finish(t, q, second, true)
	clock.Advance(time.Hour) // No configured ten-minute retention for this opt-in.
	q.Tick()
	if slot.State != "draining" || q.modelMemory(m.ID) != 8 {
		t.Fatal("idle release must retain charge until acknowledgement")
	}
	second.State = "succeeded"
	residentReport(t, q, m, second, "unloaded")
	if q.modelMemory(m.ID) != 0 {
		t.Fatal("acknowledged unload retained memory")
	}
}

func TestResidentJobsSeparateKindsAndFreezeCatalog(t *testing.T) {
	q, _, _, s := residentFixture(t)
	a := submit(t, q, s)
	s.Name = "other-stage"
	b := submit(t, q, s)
	if a.Spec.Resident.Engine.Model.Name == b.Spec.Resident.Engine.Model.Name {
		t.Fatal("different stages shared an engine")
	}
	original := a.Spec.Resident.Engine.Model.StartCommand[0]
	q.models.Models[0].StartCommand[0] = "changed-runtime"
	if a.Spec.Resident.Engine.Model.StartCommand[0] != original {
		t.Fatal("frozen engine aliases mutable catalog data")
	}
	s.Name = "job"
	c := submit(t, q, s)
	if c.Spec.Resident.Engine.Model.Name == a.Spec.Resident.Engine.Model.Name {
		t.Fatal("changed runtime reused an old engine")
	}
}

func TestResidentJobsReserveColdLoadAndYieldUnderPressure(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, clock, m, s := residentFixture(t)
			m.FreeMemoryGB = 8 // Driver alone fits; driver plus cold engine does not.
			check(t, q, m, nil)
			j := submit(t, q, s)
			if r := check(t, q, m, nil); r.Job != nil || len(q.s.ModelSlots) != 0 {
				t.Fatal("admitted driver without room for engine")
			}
			m.FreeMemoryGB = 32
			r := check(t, q, m, nil)
			if r.Job == nil || r.Job.ID != j.ID {
				t.Fatal("driver did not start when both allocations fit")
			}
			j = r.Job
			clock.Advance(4 * time.Second)
			slot := residentReport(t, q, m, j, "ready")
			finish(t, q, j, true)
			production := spec(priority)
			production.Needs.MemoryGB = 32
			production.Needs.HostMemoryGB = 1
			p := submit(t, q, production)
			if slot.State != "draining" || q.modelMemory(m.ID) != 8 {
				t.Fatal("production pressure did not drain engine with custody")
			}
			j.State = "succeeded"
			residentReport(t, q, m, j, "unloaded")
			if got := job(t, q, p.ID); got.State != "running" && check(t, q, m, nil).Job == nil {
				t.Fatal("production did not start after unload acknowledgement")
			}
		})
	}
}

func TestResidentJobsPendingDemandAndEphemeralRefusal(t *testing.T) {
	q, clock, m, s := residentFixture(t)
	j := residentAssigned(t, q, m, s)
	clock.Advance(time.Second)
	slot := residentReport(t, q, m, j, "ready")
	finish(t, q, j, true)
	waiting := submit(t, q, s)
	clock.Advance(time.Hour)
	// No heartbeat is required to prove the idle-decision contract itself.
	if q.modelIdleExpired(slot) {
		t.Fatal("queued same-stage demand lost its warm engine")
	}
	q.s.Jobs[waiting.ID].State = "cancelled"
	if !q.modelIdleExpired(slot) {
		t.Fatal("cancelled demand held memory indefinitely")
	}
	s.WorkerData = "ephemeral"
	s.Outputs = map[string]string{"result.json": "results"}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("resident engine escaped ephemeral cleanup contract")
	}
}

// Real HTTP process, queue call admission and attempt socket. Independent
// accepted results stay in SQLite; invalid/stale calls cannot affect siblings.
func TestResidentJobsRealCPUProcessAndAttemptBridge(t *testing.T) {
	q, clock, m, s := residentFixture(t)
	model := serverTestModel(t, "ready")
	model.Name, model.Device, model.DeviceEnv = "test", "", nil
	model.SizeBytes, model.SHA256 = 1, strings.Repeat("0", 64)
	if err := q.ConfigureModels(ModelConfig{Models: []Model{model}}); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	storeRoot := t.TempDir()
	if err := q.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{"input": putPieceTestFile(t, q, "managed input")}
	s.Outputs = map[string]string{"out": "results"}
	first := residentAssigned(t, q, m, s)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{s.Project: {Weight: 1, MaxConcurrent: 1, MaxConcurrentExperiments: 1}}); err != nil {
		t.Fatal(err)
	}
	frozen := first.Spec.Resident.Engine.Model
	engine := NewModelEngine(frozen, t.TempDir(), RealClock{})
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := engine.Load(ctx, frozen); err != nil {
		t.Fatal(err)
	}
	defer engine.Unload()
	pid := engine.(*LlamaEngine).cmd.Process.Pid
	slot := q.s.ModelSlots[targetSlotKey(&m, frozen.Name)]
	rt := &modelRuntime{models: map[string]*workerModel{frozen.Name: {model: frozen, engine: engine, state: "ready", attempt: slot.Attempt}}, session: m.Session, token: "test"}
	worker := workerTestServer(http.HandlerFunc(rt.serve))
	defer worker.Close()
	m.ModelURL = worker.URL
	clock.Advance(time.Second)
	residentReport(t, q, m, first, "ready")
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	w := Worker{Client: Client{URL: api.URL, Token: "test"}, Clock: clock}
	endpoint, expectedStatus, cancelBefore := "/files/a%3Fb%2F%25?x=1", 200, false
	invoke := func(j *Job, payload string) string {
		t.Helper()
		if err := q.Started(result(j, true)); err != nil {
			t.Fatal(err)
		}
		// Pipe names also work under arbitrarily long managed attempt roots.
		work, err := os.MkdirTemp("", "myce-r-test-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(work)
		env, close, err := w.startResidentBridge(ctx, *j, work)
		if err != nil {
			t.Fatal(err)
		}
		defer close()
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://resident"+endpoint, strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+env["MYCE_RESIDENT_KEY"])
		req.Header.Set("X-Myce-Project", "forged")
		if cancelBefore {
			if _, err := q.CancelJob(j.ID); err != nil {
				t.Fatal(err)
			}
		}
		response, err := ResidentRequest(ctx, env["MYCE_RESIDENT_PIPE"], env["MYCE_RESIDENT_KEY"], req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		if response.StatusCode != expectedStatus {
			t.Fatalf("bridge status=%d: %s", response.StatusCode, data)
		}
		if strings.Contains(string(data), "test|") {
			t.Fatal("fleet token reached engine")
		}
		if expectedStatus == 200 && !strings.Contains(string(data), "/files/a%3Fb%2F%25?x=1|") {
			t.Fatalf("escaped endpoint lost: %s", data)
		}
		return string(data)
	}
	accept := func(j *Job, data string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "out")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Client.UploadFile(ctx, path, ref); err != nil {
			t.Fatal(err)
		}
		r := result(j, true)
		r.OutputRefs = FileManifest{"out": ref}
		if err = q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	dependent := pieceConsumer(s, first.ID, "")
	dependent.Resident = nil
	dependent.Files = nil
	consumer := submit(t, q, dependent)
	if dependencyReady(consumer) {
		t.Fatal("dependent ready before accepted artifact")
	}
	one := invoke(first, "one")
	if !strings.HasSuffix(one, "||one") {
		t.Fatal(one)
	}
	accept(first, one)
	if !dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("accepted resident output did not release dependency")
	}
	if _, err := q.CancelJob(consumer.ID); err != nil {
		t.Fatal(err)
	}
	second := residentAssigned(t, q, m, s)
	two := invoke(second, "two")
	if !strings.HasSuffix(two, "||two") {
		t.Fatal(two)
	}
	if engine.(*LlamaEngine).cmd.Process.Pid != pid || q.modelMemory(m.ID) != model.MemoryGB {
		t.Fatal("second job replaced process or double charged")
	}
	accept(second, two)
	bad := residentAssigned(t, q, m, s)
	endpoint, expectedStatus = "/invalid", 422
	invoke(bad, "bad input")
	finish(t, q, bad, false)
	cancelled := residentAssigned(t, q, m, s)
	expectedStatus, cancelBefore = 409, true
	invoke(cancelled, "cancelled input")
	finish(t, q, cancelled, false)
	dying := residentAssigned(t, q, m, s)
	endpoint, expectedStatus, cancelBefore = "/exit", 502, false
	invoke(dying, "interrupted input")
	finish(t, q, dying, false)
	await(t, func() bool { reports, _ := rt.report(); return len(reports) == 1 && reports[0].State == "unloaded" })
	reports, _ := rt.report()
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: reports, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	if job(t, q, first.ID).State != "succeeded" || job(t, q, second.ID).State != "succeeded" || !dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("failed/cancelled/dead engine damaged accepted siblings")
	}

	// Calls are ephemeral; accepted jobs and their frozen dependencies survive reopen.
	api.Close()
	q = reopenCancellationQueue(t, q, clock)
	if err := q.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	api = httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	w.Client.URL = api.URL
	if job(t, q, first.ID).OutputRefs["out"].SHA256 == "" || !dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("restart lost accepted artifacts/dependency")
	}
	stale := residentAttempt{first.ID, first.Machine, first.Session, first.Attempt}
	if q.residentAttemptLive(stale) {
		t.Fatal("completed job retained invocation authority")
	}
	if err := q.Complete(result(first, true)); err != nil && !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if job(t, q, second.ID).State != "succeeded" {
		t.Fatal("stale completion damaged accepted sibling")
	}
}

func TestResidentJobsIdentityIgnoresPerItemMemory(t *testing.T) {
	q, _, _, s := residentFixture(t)
	s.PrepareSource = true
	s.Files = FileManifest{".myce-source.tar": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	a := &Job{Spec: s}
	if err := q.freezeResident(a); err != nil {
		t.Fatal(err)
	}
	s.ProfileMemory = strings.Repeat("b", 64)
	s.Run.Command = append(s.Run.Command, "another-recording")
	s.Files = FileManifest{".myce-source.tar": s.Files[".myce-source.tar"], "input": {SHA256: strings.Repeat("c", 64), SizeBytes: 2}}
	b := &Job{Spec: s}
	if err := q.freezeResident(b); err != nil {
		t.Fatal(err)
	}
	if a.Spec.Resident.Engine.Model.Name != b.Spec.Resident.Engine.Model.Name {
		t.Fatal("per-item arguments/files/memory split the common engine")
	}
	s.Files[".myce-source.tar"] = FileRef{SHA256: strings.Repeat("d", 64), SizeBytes: 1}
	c := &Job{Spec: s}
	if err := q.freezeResident(c); err != nil {
		t.Fatal(err)
	}
	if a.Spec.Resident.Engine.Model.Name == c.Spec.Resident.Engine.Model.Name {
		t.Fatal("changed source reused engine")
	}
}

func TestResidentJobsLocalAliasBudgets(t *testing.T) {
	_, clock := fixture(t)
	m := sharedUnifiedMachine()
	w := sharedWorker(m, clock)
	w.Machine.HostMemoryBudgetGB = 14
	runtime := &modelRuntime{models: map[string]*workerModel{"derived": {model: Model{Name: "derived", MemoryGB: 8, HostMemoryGB: 4}, state: "loading", device: "0"}}}
	reports, device := runtime.report()
	if device != 8 {
		t.Fatal("runtime budget not charged once")
	}
	next := Job{Spec: sharedSpec("experiment", 1), AllocatedDevices: []string{"0"}}
	if err := localAttemptAdmission(w, nil, next, reports); err != nil {
		t.Fatal("fitting alias allocation refused", err)
	}
	w.Machine.HostMemoryBudgetGB = 4
	if err := localAttemptAdmission(w, nil, next, reports); err == nil {
		t.Fatal("resident alias missing from host guard")
	}
	w.Machine.HostMemoryBudgetGB = 14
	next.Spec.Needs.MemoryGB = 33
	if err := localAttemptAdmission(w, nil, next, reports); err == nil || !strings.Contains(err.Error(), "device_budget") {
		t.Fatal("alias missing from card guard", err)
	}
	encoded, _ := json.Marshal(reports)
	var untrusted []ModelReport
	_ = json.Unmarshal(encoded, &untrusted)
	if _, ok := w.localModelBudget(untrusted[0]); ok {
		t.Fatal("wire report supplied trusted local budget")
	}
}

// Runs inside the same kernel boundary as a real stage; pathname sockets are
// still denied, while the managed request/response pipes are usable.
func TestResidentJobsPayloadChild(t *testing.T) {
	if os.Getenv("RESIDENT_PAYLOAD_CHILD") == "" {
		return
	}
	if os.Getenv("MYCE_TOKEN") != "" {
		os.Exit(8)
	}
	req, _ := http.NewRequest("POST", "http://resident/infer", strings.NewReader("sandboxed input"))
	response, err := ResidentRequest(context.Background(), os.Getenv("MYCE_RESIDENT_PIPE"), os.Getenv("MYCE_RESIDENT_KEY"), req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 || string(data) != "accepted response" {
		t.Fatalf("%s %v", data, err)
	}
}
func TestResidentJobsSandboxedPayload(t *testing.T) {
	w := recoveryWorker(t)
	api := httptest.NewServer(workerTestHandler(http.HandlerFunc(func(out http.ResponseWriter, in *http.Request) {
		if strings.HasPrefix(in.URL.Path, "/api/workers/resident/") {
			if in.Header.Get("Authorization") != "Bearer worker-private" || in.Header.Get("X-Myce-Job-Attempt") != "1" {
				t.Error("bridge lost attempt authorization")
			}
			data, _ := io.ReadAll(in.Body)
			if string(data) != "sandboxed input" {
				t.Error("request body changed")
			}
			out.Write([]byte("accepted response"))
			return
		}
		out.Header().Set("Content-Type", "application/json")
		out.Write([]byte(`{"ok":true,"start_gate":"` + startGateProtocol + `"}`))
	})))
	defer api.Close()
	w.Client = Client{URL: api.URL, Token: "worker-private"}
	exe, _ := os.Executable()
	j := recoveryJob(t, []string{exe, "-test.run=^TestResidentJobsPayloadChild$"})
	j.Spec.Isolation = "sandboxed"
	j.Spec.Resumable = false
	j.Spec.Run.Env = map[string]string{"RESIDENT_PAYLOAD_CHILD": "1", "GORACE": "atexit_sleep_ms=0"}
	j.Spec.Resident = &ResidentBinding{Model: "test", Engine: &ResidentEngine{Model: Model{Name: "derived"}}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	var p *process
	await(t, func() bool { p, err = w.start(store, j); return !errors.Is(err, errWorkerCheckpoint) })
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(w.Clock.Now(), 0, true)
	select {
	case finished := <-p.done:
		if finished.err != nil || !finished.result.Success {
			data, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
			t.Fatalf("payload: %+v %s", finished, data)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("sandboxed resident payload stalled")
	}
}

func TestResidentJobsFrozenEnvironmentRetention(t *testing.T) {
	q, _, m, s := residentFixture(t)
	recipe := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"engine-env": {"fake": recipe}}}); err != nil {
		t.Fatal(err)
	}
	config := q.models
	config.Models[0].Environment = "engine-env"
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	j := residentAssigned(t, q, m, s)
	plan := q.modelPlan(m.ID)
	if len(plan) != 1 || plan[0].EnvironmentRecipe == nil || plan[0].EnvironmentRecipe.Key != recipe.Key {
		t.Fatal("load plan lost frozen recipe")
	}
	// The original catalog can change while the frozen job remains live.
	if err := q.ConfigureModels(ModelConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{}); err != nil {
		t.Fatal(err)
	}
	if !q.environmentReferenced(context.Background(), recipe.Key) {
		t.Fatal("live resident environment was prunable")
	}
	model, ok := q.model(j.Spec.Resident.Engine.Model.Name)
	if !ok || model.Environment != "engine-env" {
		t.Fatal("catalog replacement lost frozen model")
	}
}
