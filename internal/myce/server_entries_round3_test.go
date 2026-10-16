package myce

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func entryHoldFixture(t *testing.T, fence string) (*Queue, *fakeClock, Machine, Machine) {
	t.Helper()
	q, clock, a := modelFixture(t)
	a.HostID = "ha"
	b := a
	b.ID, b.Session, b.HostID = "two", "session-two", "hb"
	model := &q.models.Models[0]
	model.Project = "owner"
	switch fence {
	case "entry":
		model.AllowedMachines = []string{b.ID}
	case "owner":
		if err := q.ConfigureProjects(map[string]ProjectPolicy{"owner": {Weight: 1, AllowedMachines: []string{b.ID}}}); err != nil {
			t.Fatal(err)
		}
	case "approval":
		model.LocalSource = "approved"
		b.ApprovedModels = map[string]string{"approved": model.SHA256}
	}
	a.ModelConfigHash, b.ModelConfigHash = modelConfigHash(q.models), modelConfigHash(q.models)
	if err := q.ConfigureHosts(map[string]HostPolicy{"ha": {ProductionHold: true}, "hb": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	return q, clock, a, b
}

func TestEntryProductionBatchHoldRespectsModelFences(t *testing.T) {
	for _, fence := range []string{"entry", "owner", "approval"} {
		t.Run(fence, func(t *testing.T) {
			q, clock, a, b := entryHoldFixture(t, fence)
			s := spec("production")
			s.Needs.Model = "test"
			j := submit(t, q, s)
			if got := job(t, q, j.ID).ProductionHost; got != b.HostID {
				t.Fatalf("targeted %q, want %s", got, b.HostID)
			}
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if q.held(a.HostID) || !q.held(b.HostID) {
				t.Fatal("hold escaped model fence")
			}
			if check(t, q, a, nil).Job != nil {
				t.Fatal("excluded worker received model batch")
			}
			got := check(t, q, b, nil).Job
			if got == nil || got.ID != j.ID {
				t.Fatal("eligible worker left idle")
			}
		})
	}
}

func TestEntryPinsAndProductionHoldRespectModelFences(t *testing.T) {
	for _, fence := range []string{"entry", "owner", "approval"} {
		t.Run(fence, func(t *testing.T) {
			q, clock, a, b := entryHoldFixture(t, fence)
			if err := q.Pin(ModelPin{Project: "caller", Model: "test", Platform: "fake"}); err != nil {
				t.Fatal(err)
			}
			if q.s.Pins[0].Assigned != b.ID || q.s.ModelSlots[slotKey(a.ID, "test")] != nil {
				t.Fatal("pin escaped fence")
			}
			reportModel(t, q, b, "ready")
			c := addModelCall(t, q, "production")
			if c.ProductionHost != b.HostID || c.Machine != b.ID {
				t.Fatal("production call escaped fence or failed to reuse pin")
			}
			if err := q.finishCall(c.ID); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			slot := q.s.ModelSlots[slotKey(b.ID, "test")]
			if slot == nil || slot.State != "ready" || q.modelMemory(b.ID) != 8 {
				t.Fatal("hold released pinned custody")
			}
			if q.held(a.HostID) {
				t.Fatal("excluded host held")
			}
		})
	}
}

type entryProbeReader func([]byte) (int, error)

func (f entryProbeReader) Read(p []byte) (int, error) { return f(p) }

func TestEntryGenericHeadersBeforeSpooling(t *testing.T) {
	q, _ := fixture(t)
	dir := t.TempDir()
	if err := q.ConfigureUploadSpool(dir, 1024); err != nil {
		t.Fatal(err)
	}
	for _, headers := range []http.Header{{}, {"X-Myce-Project": {"test"}}, {"X-Myce-Project": {"test"}, "X-Myce-Priority": {"bad"}}} {
		read, spooled := false, false
		body := entryProbeReader(func([]byte) (int, error) {
			read = true
			files, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			spooled = len(files) > 0
			return 0, io.EOF
		})
		r := httptest.NewRequest("POST", "/models/test/upload", body)
		r.Header = headers
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		(API{Queue: q, Token: "test"}).Handler().ServeHTTP(w, r)
		files, err := os.ReadDir(dir)
		if w.Code != 400 || read || spooled || err != nil || len(files) != 0 || q.uploadSpool.used != 0 {
			t.Fatalf("status=%d read=%t spooled=%t files=%d err=%v", w.Code, read, spooled, len(files), err)
		}
		if !strings.Contains(w.Body.String(), "X-Myce-") {
			t.Fatal("routing error lacks actionable header names")
		}
	}
}

func TestEntryRoutingCompatibilityAndCallerProductionDefault(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"caller": {Weight: 1, DefaultCallPriority: "production"}}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		headers      http.Header
		wantPriority string
		wantErr      bool
	}{
		{"other-owner", http.Header{"X-Myce-Project": {"caller"}}, "production", false},
		{"trimmed-caller-default", http.Header{"X-Myce-Project": {" caller "}}, "production", false},
		{"ignored-malformed-deadline", http.Header{"X-Myce-Project": {"caller"}, "X-Myce-Priority": {"experiment"}, "X-Myce-Deadline": {"bad"}}, "experiment", false},
		{"required-deadline", http.Header{"X-Myce-Project": {"caller"}, "X-Myce-Priority": {"deadline"}, "X-Myce-Deadline": {"bad"}}, "", true},
		{"duplicate-project", http.Header{"X-Myce-Project": {"caller", "caller"}}, "", true},
		{"duplicate-priority", http.Header{"X-Myce-Project": {"caller"}, "X-Myce-Priority": {"experiment", "experiment"}}, "", true},
		{"duplicate-deadline", http.Header{"X-Myce-Project": {"caller"}, "X-Myce-Deadline": {"bad", "bad"}}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &ModelCall{Project: tc.headers.Get("X-Myce-Project"), Priority: tc.headers.Get("X-Myce-Priority")}
			err := q.callDefaults(c, Model{Project: "another-owner"}, tc.headers, true)
			if (err != nil) != tc.wantErr {
				t.Fatalf("routing compatibility: %v", err)
			}
			if !tc.wantErr && (c.Project != "caller" || c.Priority != tc.wantPriority || c.Deadline != nil) {
				t.Fatalf("wrong caller defaults: %+v", c)
			}
		})
	}
}

func TestEntryApprovalHeartbeatOneWalkAndBoundedTimeout(t *testing.T) {
	c, model, _, _ := localApprovalFixture(t)
	c.Limit = 1024
	if err := c.init(); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)}
	c.Clock = clock
	var calls atomic.Int32
	var blocked atomic.Bool
	release, finished := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	c.localMetadataProbe = func(ctx context.Context, path, root string) ([]localModelMetadata, error) {
		calls.Add(1)
		if blocked.Load() {
			<-release
			defer close(finished)
		}
		return localMetadata(ctx, path, root)
	}
	models := []Model{model, model} // Shared approval is walked only once, too.
	approved := c.ApprovedModels(models)
	if approved[model.LocalSource] != model.SHA256 || calls.Load() != 1 {
		t.Fatal("healthy approval not checked exactly once")
	}
	if names, _, err := c.report(models, approved); err != nil || !contains(names, model.Name) || calls.Load() != 1 {
		t.Fatal("cache report repeated the heartbeat walk", err)
	}
	blocked.Store(true)
	heartbeat := func() {
		t.Helper()
		done := make(chan map[string]string, 1)
		go func() { done <- c.ApprovedModels(models) }()
		select {
		case approved := <-done:
			if len(approved) != 0 {
				t.Fatal("timed-out approval advertised")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("filesystem stalled the heartbeat")
		}
	}
	heartbeat()
	firstWarning := c.nextApprovalWarning
	clock.Advance(time.Second)
	heartbeat()
	if calls.Load() != 2 {
		t.Fatal("blocked filesystem scans accumulated")
	}
	if c.nextApprovalWarning != firstWarning {
		t.Fatal("approval warnings were not rate limited")
	}
	clock.Advance(time.Minute)
	heartbeat()
	if !c.nextApprovalWarning.After(firstWarning) {
		t.Fatal("approval warning did not recur after its interval")
	}
	blocked.Store(false)
	releaseOnce.Do(func() { close(release) })
	<-finished
	// The completed but expired result must never revive approval readiness.
	if len(c.ApprovedModels(models)) != 0 {
		t.Fatal("expired check revived approval")
	}
	if c.ApprovedModels(models)[model.LocalSource] != model.SHA256 || calls.Load() != 3 {
		t.Fatal("fresh check did not recover")
	}
}

func TestEntryUnverifiableApprovalWithholdsLoadsAndRoutingKeepsCustody(t *testing.T) {
	c, model, _, _ := localApprovalFixture(t)
	rt := &modelRuntime{cache: c, config: ModelConfig{Models: []Model{model}}, models: map[string]*workerModel{}, session: "session", token: "test"}
	rt.setLocalApprovals(nil, nil)
	rt.sync([]ModelSlot{{Model: model.Name, State: "loading", Session: rt.session}})
	if len(rt.models) != 0 {
		t.Fatal("unverified source started a load")
	}
	engine := &lifecycleEngine{}
	rt.models[model.Name] = &workerModel{model: model, engine: engine, state: "ready", queueFlight: 1}
	reports, memory := rt.report()
	if memory != model.MemoryGB || len(reports) != 1 || reports[0].State != "ready" {
		t.Fatal("unverifiable approval released local custody")
	}
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
	request.Header.Set("Authorization", "Bearer test")
	request.Header.Set("X-Myce-Session", rt.session)
	response := httptest.NewRecorder()
	rt.serve(response, request)
	if response.Code != 503 || engine.unloads.Load() != 0 {
		t.Fatal("unverified source routed or lost custody")
	}

	q, clock, m := modelFixture(t)
	q.models.Models[0].LocalSource = model.LocalSource
	q.models.Models[0].SHA256 = model.SHA256
	m.ModelConfigHash = modelConfigHash(q.models)
	m.ApprovedModels = map[string]string{model.LocalSource: model.SHA256}
	check(t, q, m, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Platform: "fake"}); err != nil {
		t.Fatal(err)
	}
	reportModel(t, q, m, "ready")
	m.ApprovedModels = nil
	reportModel(t, q, m, "ready")
	if slot := q.s.ModelSlots[slotKey(m.ID, "test")]; slot == nil || slot.State != "draining" || q.modelMemory(m.ID) != 8 {
		t.Fatal("missing approval released queue custody")
	}
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	call := addModelCall(t, q, "experiment")
	if call.Machine != "" || q.modelMemory(m.ID) != 8 {
		t.Fatal("unverified source routed or elapsed time released custody")
	}
	reportModel(t, q, m, "unloaded")
	if q.modelMemory(m.ID) != 0 {
		t.Fatal("confirmed unload did not release custody")
	}
}

func TestEntryApprovalHeartbeatRejectsExpiredCompletedSnapshot(t *testing.T) {
	c, model, _, _ := localApprovalFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	close(done)
	// Completion and the heartbeat timeout can race; cancellation wins even if
	// the filesystem computed a valid manifest just before the timeout returned.
	c.localProbe.key = modelConfigHash(ModelConfig{Models: []Model{model}})
	c.localProbe.pending = &checkpointIOResult[localApprovalSnapshot]{done: done, value: localApprovalSnapshot{models: map[string]string{model.LocalSource: model.SHA256}, ctx: ctx}}
	cancel()
	if len(c.ApprovedModels([]Model{model})) != 0 {
		t.Fatal("expired completed result revived readiness")
	}
	if c.ApprovedModels([]Model{model})[model.LocalSource] != model.SHA256 {
		t.Fatal("fresh metadata check failed to recover")
	}
}
