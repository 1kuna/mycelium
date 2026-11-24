package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestInputStagingTimestampPrecedesMemoryAndLaunch(t *testing.T) {
	q, initial := fixture(t)
	clock := &workerFakeClock{now: initial.Now()}
	q.clock = clock
	m := machine("staging", "fake")
	m.HostMemoryMeasured = true
	m.FreeHostMemoryGB, m.HostAvailableMemoryGB = 64, 64
	m.ExtensionFingerprint, _ = ExtensionFingerprint(context.Background(), "fake", m.Device)
	e := fakeExtension(t)
	e.Run = []string{"sh", "-ec", `test "$(cat upstream.json)" = synthetic && test "$(cat request.json)" = request`}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	extensionRoot := t.TempDir()
	m.ExtensionReports = installQualified(t, extensionRoot, e, "")
	s := spec("production")
	s.Resumable = false
	s.Kind, s.Run = "sample", RunSpec{}
	s.Needs = ResourceNeeds{}
	s.Inputs = map[string]string{"upstream.json": "upstream.json", "request.json": "request.json"}
	if err := os.WriteFile(filepath.Join(q.root, "upstream.json"), []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(q.root, "request.json"), []byte("request"), 0600); err != nil {
		t.Fatal(err)
	}
	submitted := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatalf("staging job not assigned: %+v", job(t, q, submitted.ID))
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/started" {
			clock.Advance(7 * time.Second)
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	c := Client{URL: server.URL, Token: "test", stagingWire: &atomic.Bool{}, launchTimingWire: &atomic.Bool{}}
	w := Worker{StateDir: newTestStateRoot(t), Machine: m, Clock: clock, Client: c, JobFileRoot: q.root, ExtensionRoot: extensionRoot, extensionFingerprint: m.ExtensionFingerprint,
		HostMemorySource: func(context.Context) (float64, error) { return 64, nil },
		MemorySource: func(context.Context) (float64, error) {
			clock.Advance(20 * time.Second) // A later launch check must not move staging completion.
			return 64, nil
		}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	received := clock.Now().Add(-time.Second)
	assigned.LaunchTimings = &LaunchTimings{AssignmentReceivedAt: received, ObservedAt: received, CurrentPhase: launchDispatch}
	store.state.Session, store.state.Job = assigned.Session, assigned
	store.extensionFiles.timeout = testWaitTimeout // Local fixture I/O, not a timing test.
	want := clock.Now()
	p, err := startAfterFixtureInputs(t, w, store, *assigned)
	if err != nil || p == nil {
		t.Fatalf("start: process=%v err=%v", p, err)
	}
	select {
	case finished := <-p.done:
		if finished.err != nil || !finished.result.Success {
			t.Fatalf("payload did not read the staged input: %+v", finished)
		}
		if err := q.Complete(finished.result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		p.stop(clock.Now(), 0, true)
		t.Fatal("payload did not finish")
	}
	got, err := c.GetJob(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.InputsStagedAt == nil || !got.InputsStagedAt.Equal(want) || got.StartedAt == nil || !got.StartedAt.Equal(want.Add(27*time.Second)) {
		t.Fatalf("get lost the staging/launch boundary: staged=%v started=%v", got.InputsStagedAt, got.StartedAt)
	}
	var saved workerState
	data, err := os.ReadFile(filepath.Join(store.dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if got.StagingTimings == nil || got.StagingTimings.CompletedAt == nil || !got.StagingTimings.CompletedAt.Equal(want) || got.StagingTimings.Phases[stageFiles].Calls != 1 {
		t.Fatal("worker did not send staging phases before launch", got.StagingTimings)
	}
	if len(got.AttemptHistory) != 1 {
		t.Fatal("completed attempt missing from history")
	}
	launch := got.AttemptHistory[0].LaunchTimings
	if launch == nil || !launch.AssignmentReceivedAt.Equal(received) || launch.InputsReadyAt == nil ||
		!launch.InputsReadyAt.Equal(want) || launch.PreparationReadyAt == nil ||
		!launch.PreparationReadyAt.Equal(want.Add(20*time.Second)) ||
		launch.CommandStartAt == nil || !launch.CommandStartAt.Equal(want.Add(27*time.Second)) ||
		launch.CommandStartBoundary != launchExecConfirmed || launch.CurrentPhase != "" {
		t.Fatalf("incomplete launch boundaries: %+v", launch)
	}
	var elapsed float64
	seconds := map[launchPhase]float64{}
	for _, phase := range launch.Phases {
		elapsed += phase.Seconds
		seconds[phase.Phase] = phase.Seconds
	}
	if elapsed != 28 || seconds[launchDispatch] != 1 || seconds[launchPrechecks] != 20 ||
		seconds[launchStartedRPC] != 7 {
		t.Errorf("launch partition accounts for %v of 28 seconds: %+v", elapsed, launch.Phases)
	}
	if saved.Job == nil || saved.Job.StagingTimings == nil {
		t.Fatal("worker recovery lost staging phases")
	}
	if saved.Job == nil || saved.Job.InputsStagedAt == nil || !saved.Job.InputsStagedAt.Equal(want) {
		t.Fatal("worker recovery lost staging completion")
	}
}

func TestInputStagingReportsRejectStaleAttemptsAndResetOnRetry(t *testing.T) {
	q, clock := fixture(t)
	m := machine("staging", "fake")
	submitted := submit(t, q, spec("experiment"))
	assigned := check(t, q, m, nil).Job
	staged := clock.Now().Add(time.Second)
	report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt + 1}, Session: assigned.Session, InputsStagedAt: &staged}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, submitted.ID); got.InputsStagedAt != nil {
		t.Fatal("stale report supplied staging evidence")
	}
	report.Attempt = assigned.Attempt
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, submitted.ID)
	if got.InputsStagedAt == nil || !got.InputsStagedAt.Equal(staged) || got.StartedAt != nil {
		t.Fatal("prelaunch staging report was lost or started the payload", got)
	}
	r := result(assigned, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, submitted.ID); got.State != "queued" || got.InputsStagedAt != nil {
		t.Fatal("retry retained previous-attempt staging evidence", got)
	}
}
