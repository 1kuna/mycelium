package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
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
	case <-time.After(testWaitTimeout):
		p.stop(clock.Now(), 0, true)
		t.Fatal("payload did not finish")
	}
	got, err := c.GetJob(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.InputsStagedAt == nil || !got.InputsStagedAt.Equal(want) || got.StartedAt == nil || !got.StartedAt.Equal(want.Add(20*time.Second)) {
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

func TestInputStagingTimestampWaitsForBothNASCopies(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)}
	entered := make(chan string, 2)
	firstDone := make(chan struct{})
	releaseSecond := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(releaseSecond) }) }
	t.Cleanup(release)
	type completion struct {
		inputs stagedInputs
		err    error
	}
	done := make(chan completion, 1)
	go func() {
		inputs, err := captureStagedInputs(clock, func() (string, error) {
			err := stageJobInputs(map[string]string{"request": "request-source", "upstream": "upstream-source"}, func(name, source string) error {
				entered <- name
				if name == "request" {
					close(firstDone)
				} else {
					<-releaseSecond
				}
				return nil
			})
			return "complete-snapshot", err
		})
		done <- completion{inputs, err}
	}()
	// Both copiers entered, and the first returned; the second still owns its
	// input. No timestamp or completion may escape while that peer is active.
	for range 2 {
		select {
		case <-entered:
		case <-time.After(testWaitTimeout):
			t.Fatal("NAS copies did not overlap")
		}
	}
	<-firstDone
	select {
	case got := <-done:
		t.Fatalf("staging timestamp escaped before the peer joined: %+v", got)
	default:
	}
	clock.Advance(7 * time.Second)
	want := clock.Now()
	release()
	select {
	case got := <-done:
		if got.err != nil || got.inputs.directory != "complete-snapshot" || !got.inputs.at.Equal(want) {
			t.Fatalf("staging must capture completion after both copies join: %+v, want %v", got, want)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("joined staging did not complete")
	}
}
