package myce

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestResidentJobsReserveColdLoadAndYieldToProduction(t *testing.T) {
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
	production := spec("production")
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
	first := residentAssigned(t, q, m, s)
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
	invoke := func(j *Job, payload string) string {
		t.Helper()
		if err := q.Started(result(j, true)); err != nil {
			t.Fatal(err)
		}
		// A short owned path also exercises Unix-domain length limits on macOS.
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
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://resident/files/a%3Fb%2F%25?x=1", strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+env["MYCE_RESIDENT_KEY"])
		req.Header.Set("X-Myce-Project", "forged")
		response, err := ResidentRequest(ctx, env["MYCE_RESIDENT_PIPE"], env["MYCE_RESIDENT_KEY"], req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		if response.StatusCode != 200 {
			t.Fatalf("bridge status=%d: %s", response.StatusCode, data)
		}
		if strings.Contains(string(data), "test|") {
			t.Fatal("fleet token reached engine")
		}
		return string(data)
	}
	if data := invoke(first, "one"); !strings.HasSuffix(data, "||one") {
		t.Fatal(data)
	}
	finish(t, q, first, true)
	second := residentAssigned(t, q, m, s)
	if data := invoke(second, "two"); !strings.HasSuffix(data, "||two") {
		t.Fatal(data)
	}
	if engine.(*LlamaEngine).cmd.Process.Pid != pid || q.modelMemory(m.ID) != model.MemoryGB {
		t.Fatal("second job replaced process or double charged")
	}
	finish(t, q, second, true)
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
