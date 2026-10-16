package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the check-in wire format, including compatibility with old workers.
func entryApprovalState(t *testing.T, m Machine, state string) Machine {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	fields["model_approval_states"] = json.RawMessage(`{"approved":"` + state + `"}`)
	data, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEntryUnknownApprovalKeepsReadySlots(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "pinned"}[pinned], func(t *testing.T) {
			q, clock, m := modelFixture(t)
			cache, model, _, _ := localApprovalFixture(t)
			model.MemoryGB = 8
			q.models.Models[0] = model
			cache.Clock = clock
			m.ModelConfigHash = modelConfigHash(q.models)
			m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals(q.models.Models)
			check(t, q, m, nil)
			if pinned {
				if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			} else {
				c := addModelCall(t, q, "experiment")
				reportModel(t, q, m, "ready")
				if err := q.finishCall(c.ID); err != nil {
					t.Fatal(err)
				}
			}
			reportModel(t, q, m, "ready")
			if pinned {
				if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&m): {ProductionHold: true}}); err != nil {
					t.Fatal(err)
				}
			}
			release, finished := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }); <-finished })
			cache.localMetadataProbe = func(ctx context.Context, path, root string) ([]localModelMetadata, error) {
				defer close(finished)
				<-release
				return localMetadata(ctx, path, root)
			}
			m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals(q.models.Models)
			if m.ModelApprovalStates[model.LocalSource] != approvalUnknown || len(m.ApprovedModels) != 0 {
				t.Fatal("stalled walk not reported as unknown")
			}
			data, err := json.Marshal(Checkin{Machine: m})
			if err != nil {
				t.Fatal(err)
			}
			var wire Checkin
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			m = wire.Machine
			reply := reportModel(t, q, m, "ready")
			slot := q.s.ModelSlots[slotKey(m.ID, "test")]
			if slot == nil || slot.State != "ready" || len(reply.ModelPlan) != 1 || reply.ModelPlan[0].State != "ready" {
				t.Fatal("unknown approval drained resident slot")
			}
			if pinned && q.s.Pins[0].Assigned != m.ID {
				t.Fatal("unknown approval displaced pin")
			}
			// A still-blocked check-in must preserve the same ready residency.
			m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals(q.models.Models)
			reportModel(t, q, m, "ready")
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			for _, priority := range []string{"experiment", "production"} {
				c := addModelCall(t, q, priority)
				if c.Machine != m.ID {
					t.Fatal("unknown approval blocked ready routing", priority)
				}
				if err := q.finishCall(c.ID); err != nil {
					t.Fatal(err)
				}
			}
			if q.modelMemory(m.ID) != 8 {
				t.Fatal("resident custody changed")
			}
		})
	}
}

func TestEntryGenericMissingHeadersMessage(t *testing.T) {
	q, _ := fixture(t)
	r := httptest.NewRequest("POST", "/models/test/upload", strings.NewReader("unused"))
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	(API{Queue: q, Token: "test"}).Handler().ServeHTTP(w, r)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "X-Myce-Project and X-Myce-Priority are required") || strings.Contains(w.Body.String(), "default_call_priority") || strings.Contains(w.Body.String(), "entry") {
		t.Fatal("generic route suggests unsupported defaults:", w.Body.String())
	}
}

func TestEntryUnknownApprovalBlocksNewLoadsAndCopies(t *testing.T) {
	q, clock, m := modelFixture(t)
	q.models.Models[0].LocalSource = "approved"
	m.ModelConfigHash = modelConfigHash(q.models)
	// Unknown overrides even a stale approved identity.
	m.ApprovedModels = map[string]string{"approved": q.models.Models[0].SHA256}
	m = entryApprovalState(t, m, "unknown")
	check(t, q, m, nil)
	c := addModelCall(t, q, "experiment")
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	if err := q.SyncModel(SyncRequest{Model: "test", To: "all"}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	reply := check(t, q, m, nil)
	if len(reply.ModelPlan) != 0 || len(reply.CopyPlan) != 0 || c.Machine != "" || len(q.s.ModelSlots) != 0 || q.s.Pins[0].Assigned != "" {
		t.Fatal("unknown approval admitted new work")
	}
	// Check the planner independently of the check-in response.
	if plan, _, _ := q.copyPlan(q.s.Machines[m.ID]); len(plan) != 0 {
		t.Fatal("unknown approval scheduled pre-copy")
	}
	// A batch model need must not use the ready-call exception either.
	batch := spec("production")
	batch.Needs.Model = "test"
	j := submit(t, q, batch)
	if check(t, q, m, nil).Job != nil || job(t, q, j.ID).State != "queued" {
		t.Fatal("unknown approval admitted model batch")
	}
}

func TestEntryInvalidApprovalStillDrains(t *testing.T) {
	for _, change := range []string{"changed", "revoked", "old-worker"} {
		t.Run(change, func(t *testing.T) {
			q, clock, m := modelFixture(t)
			cache, model, snapshot, _ := localApprovalFixture(t)
			model.MemoryGB = 8
			q.models.Models[0] = model
			cache.Clock = clock
			m.ModelConfigHash = modelConfigHash(q.models)
			m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals(q.models.Models)
			check(t, q, m, nil)
			if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
				t.Fatal(err)
			}
			reportModel(t, q, m, "ready")
			switch change {
			case "changed":
				if err := os.WriteFile(filepath.Join(snapshot, "config.json"), []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				// Fixture-owned approval is disposable; revocation is the behavior under test.
				if err := os.Remove(filepath.Join(cache.ApprovalRoot, model.LocalSource+".json")); err != nil {
					t.Fatal(err)
				}
			}
			if change == "old-worker" {
				m.ApprovedModels = nil
				m.ModelApprovalStates = nil
			} else {
				m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals(q.models.Models)
				if m.ModelApprovalStates[model.LocalSource] != approvalInvalid {
					t.Fatal("change not reported invalid")
				}
			}
			reply := reportModel(t, q, m, "ready")
			slot := q.s.ModelSlots[slotKey(m.ID, "test")]
			if slot == nil || slot.State != "draining" || len(reply.ModelPlan) != 1 || reply.ModelPlan[0].State != "draining" || q.modelMemory(m.ID) != 8 {
				t.Fatal("invalid approval failed to drain with custody")
			}
			c := addModelCall(t, q, "production")
			if c.Machine != "" {
				t.Fatal("invalid approval routed")
			}
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if q.modelMemory(m.ID) != 8 {
				t.Fatal("drain prematurely released custody")
			}
			reportModel(t, q, m, "unloaded")
			if q.modelMemory(m.ID) != 0 {
				t.Fatal("unload acknowledgement retained custody")
			}
		})
	}
}

type entryApprovalEngine struct {
	FakeEngine
	unloads atomic.Int32
}

func (e *entryApprovalEngine) Unload() error { e.unloads.Add(1); return nil }

func TestEntryWorkerUnknownApprovalKeepsServingButBlocksLaunch(t *testing.T) {
	cache, model, _, _ := localApprovalFixture(t)
	rt := &modelRuntime{cache: cache, config: ModelConfig{Models: []Model{model}}, models: map[string]*workerModel{}, copies: map[string]*workerCopy{}, copyParallel: 1, session: "session", token: "test"}
	states := map[string]ModelApprovalState{model.LocalSource: approvalUnknown}
	rt.setLocalApprovals(nil, states)
	plan := []ModelSlot{{Model: model.Name, Session: rt.session, State: "loading"}}
	rt.sync(plan)
	rt.syncCopies([]CopyTask{{Model: model.Name}}, nil, nil, false)
	if len(rt.models) != 0 || len(rt.copies) != 0 {
		t.Fatal("unknown approval started load/copy")
	}
	engine := &entryApprovalEngine{FakeEngine: FakeEngine{Model: model.Name}}
	rt.models[model.Name] = &workerModel{model: model, engine: engine, state: "ready"}
	plan[0].State = "ready"
	rt.sync(plan)
	call := func() int {
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("X-Myce-Session", rt.session)
		w := httptest.NewRecorder()
		rt.serve(w, r)
		return w.Code
	}
	if code := call(); code != 200 {
		t.Fatal("unknown approval blocked resident engine", code)
	}
	if engine.unloads.Load() != 0 || rt.models[model.Name].state != "ready" {
		t.Fatal("unknown approval unloaded engine")
	}
	rt.setLocalApprovals(nil, map[string]ModelApprovalState{model.LocalSource: approvalInvalid})
	if code := call(); code != 503 {
		t.Fatal("invalid approval kept serving", code)
	}
	plan[0].State = "draining"
	rt.sync(plan)
	rt.wg.Wait()
	if engine.unloads.Load() != 1 {
		t.Fatal("verified invalid plan did not unload")
	}
}

func TestEntryUnknownApprovalRetainsOtherFences(t *testing.T) {
	for _, fence := range []string{"entry", "owner", "platform", "log-policy", "session", "loading", "draining", "invalid", "missing-state"} {
		t.Run(fence, func(t *testing.T) {
			q, _, m := modelFixture(t)
			model := q.models.Models[0]
			model.LocalSource = "approved"
			m.ModelApprovalStates = map[string]ModelApprovalState{"approved": approvalUnknown}
			slot := &ModelSlot{Model: model.Name, Machine: m.ID, Session: m.Session, State: "ready"}
			q.s.ModelSlots[slotKey(m.ID, model.Name)] = slot
			if !q.modelUsableMachine(model, &m) {
				t.Fatal("unknown ready baseline excluded")
			}
			switch fence {
			case "entry":
				model.AllowedMachines = []string{"another"}
			case "owner":
				model.Project = "owner"
				q.s.Policies = map[string]ProjectPolicy{"owner": {Weight: 1, AllowedMachines: []string{"another"}}}
			case "platform":
				model.Platforms = []string{"cuda"}
			case "log-policy":
				model.Logs = "none"
				m.Capabilities = nil
			case "session":
				slot.Session = "previous-session"
			case "loading":
				slot.State = "loading"
			case "draining":
				slot.State = "draining"
			case "invalid":
				m.ModelApprovalStates["approved"] = approvalInvalid
			case "missing-state":
				m.ModelApprovalStates = nil
			}
			if q.modelUsableMachine(model, &m) {
				t.Fatal("unknown bypassed fence", fence)
			}
		})
	}
}
