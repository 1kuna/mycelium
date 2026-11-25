package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func warmFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	m := machine("worker-a", "fake")
	m.Capabilities = append(m.Capabilities, warmPiecesCapability)
	check(t, q, m, nil)
	s := spec("experiment")
	s.WarmPieces = warmPiecesFixed
	s.SubmissionKey = "warm-pieces-test"
	s.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "a", Args: []string{"first"}}, {Key: "b"}, {Key: "c"}}}
	s.Outputs = map[string]string{"out": "results"}
	return q, clock, m, s
}

func warmRequest(j *Job, op string, d *WarmPieceDelivery) warmPieceRequest {
	r := warmPieceRequest{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Op: op}
	if d != nil {
		r.Key, r.Sequence = d.Key, d.Sequence
	}
	return r
}

func warmCall(t *testing.T, q *Queue, r warmPieceRequest) warmPieceReply {
	t.Helper()
	reply, err := q.warmPiece(t.Context(), r)
	if err != nil {
		t.Fatalf("%s %s: %v", r.Op, r.Key, err)
	}
	return reply
}

func warmStart(t *testing.T, q *Queue, m Machine, id string) *Job {
	t.Helper()
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != id || j.Batch != nil || len(j.FanOut.Children) != 0 {
		t.Fatalf("expected one warm parent placement: %+v", j)
	}
	if err := q.Started(result(j, true)); err != nil {
		t.Fatal(err)
	}
	return j
}

func warmNext(t *testing.T, q *Queue, j *Job, key string) *WarmPieceDelivery {
	t.Helper()
	next := warmRequest(j, "next", nil)
	offer := warmCall(t, q, next).Delivery
	duplicate := warmCall(t, q, next).Delivery
	if offer == nil || offer.Key != key || duplicate == nil || *offer != *duplicate {
		t.Fatalf("wanted replay of offer %s, got %v and %v", key, offer, duplicate)
	}
	start := warmRequest(j, "start", offer)
	started := warmCall(t, q, start).Delivery
	replayed := warmCall(t, q, start).Delivery
	if started.StartedAt == nil || !started.StartedAt.Equal(*replayed.StartedAt) {
		t.Fatalf("start retransmission changed the authorization: %v %v", started, replayed)
	}
	return started
}

func warmSuccess(t *testing.T, q *Queue, j *Job, d *WarmPieceDelivery) warmPieceRequest {
	t.Helper()
	r := warmRequest(j, "success", d)
	r.OutputRefs = FileManifest{"out": putPieceTestFile(t, q, d.Key+" result")}
	warmCall(t, q, r)
	return r
}

func reopenWarmQueue(t *testing.T, q *Queue, clock Clock) *Queue {
	t.Helper()
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root, files := q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := Open(path, root, clock, WithFileStore(files, 1<<20, true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { out.Close() })
	fakeCheckpointAvailability(t, out, true)
	return out
}

func TestWarmPiecesReceiptsAndDependencies(t *testing.T) {
	q, clock, m, s := warmFixture(t)
	j := submit(t, q, s)
	if replay := submit(t, q, s); replay.ID != j.ID {
		t.Fatalf("replay returned %s, want %s", replay.ID, j.ID)
	}
	changed := s
	changed.FanOut = &FanOut{Pieces: slices.Clone(s.FanOut.Pieces)}
	changed.FanOut.Pieces[0].Args = []string{"different"}
	if _, err := q.Submit(changed); err == nil {
		t.Fatal("changed piece intent was replayed")
	}
	run := warmStart(t, q, m, j.ID)
	consumerSpec := spec("experiment")
	consumerSpec.Resumable = false
	consumerSpec.After = &PieceDependency{Job: j.ID, Piece: "a", Outputs: map[string]string{"in": "out"}}
	consumer := submit(t, q, consumerSpec)
	if got := job(t, q, consumer.ID); dependencyReady(got) {
		t.Fatal("dependent was ready before receipt")
	}
	clock.Advance(time.Second)
	d := warmNext(t, q, run, "a")
	clock.Advance(2 * time.Second)
	r := warmSuccess(t, q, run, d)
	if got := job(t, q, consumer.ID); !dependencyReady(got) || got.Dependency.Execution != r.Execution {
		t.Fatalf("dependent did not bind the exact accepted execution: %+v", got.Dependency)
	}
	q = reopenWarmQueue(t, q, clock)
	warmCall(t, q, r) // The committed receipt survived a lost reply and queue restart.
	changedReceipt := r
	changedReceipt.OutputRefs = maps.Clone(r.OutputRefs)
	changedReceipt.OutputRefs["out"] = putPieceTestFile(t, q, "changed")
	if _, err := q.warmPiece(t.Context(), changedReceipt); !errors.Is(err, ErrStale) {
		t.Fatalf("changed receipt returned %v", err)
	}
	clock.Advance(3 * time.Second)
	b := warmNext(t, q, run, "b")
	warmCall(t, q, warmRequest(run, "failure", b))
	warmSuccess(t, q, run, warmNext(t, q, run, "c"))
	if !warmCall(t, q, warmRequest(run, "next", nil)).End {
		t.Fatal("settled list kept offering work")
	}
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	final := job(t, q, j.ID)
	if final.State != "failed" || !strings.Contains(final.Reason, "b") ||
		len(final.FanOut.Results) != 2 || len(final.FanOut.Warm.Failures) != 1 || final.OutputRefs["a/out"] != r.OutputRefs["out"] {
		t.Fatalf("wrong settled parent: %s %q %+v", final.State, final.Reason, final.FanOut)
	}
	if p := final.FanOut.Warm; p.StartupSeconds != 1 || p.ItemSeconds != 2 || p.BetweenItemsSeconds != 3 {
		t.Fatalf("wrong warm timings after retransmission and restart: %+v", p)
	}
}

func TestWarmPiecesLossResumesFirstUnsettled(t *testing.T) {
	q, clock, m, s := warmFixture(t)
	j := submit(t, q, s)
	run := warmStart(t, q, m, j.ID)
	cached := warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	b := warmNext(t, q, run, "b")
	save := putPieceTestFile(t, q, `{"myce_checkpoint":{"generation":2}}`)
	pub := checkpointPublication{Result: result(run, false), Piece: b.Key, Sequence: b.Sequence, Checkpoint: FileCheckpoint{Generation: 2, Files: FileManifest{"progress.json": save}}}
	if err := q.publishCheckpoint(t.Context(), pub); err != nil {
		t.Fatal(err)
	}
	clock.Advance(LossAfter + time.Second)
	replacement := machine("worker-b", m.GPU)
	replacement.Capabilities = append(replacement.Capabilities, warmPiecesCapability)
	newRun := warmStart(t, q, replacement, j.ID)
	if newRun.Attempt <= run.Attempt || newRun.ResumeFiles == nil || newRun.ResumeFiles.Generation != 2 {
		t.Fatalf("replacement missed the piece checkpoint: attempt %d, checkpoint %v", newRun.Attempt, newRun.ResumeFiles)
	}
	if _, err := q.warmPiece(t.Context(), cached); !errors.Is(err, ErrStale) {
		t.Fatalf("old receipt replay after replacement returned %v", err)
	}
	stale := warmRequest(run, "failure", b)
	if _, err := q.warmPiece(t.Context(), stale); !errors.Is(err, ErrStale) {
		t.Fatalf("old active delivery returned %v", err)
	}
	if err := q.publishCheckpoint(t.Context(), pub); !errors.Is(err, ErrStale) {
		t.Fatalf("old checkpoint returned %v", err)
	}
	warmSuccess(t, q, newRun, warmNext(t, q, newRun, "b"))
	warmSuccess(t, q, newRun, warmNext(t, q, newRun, "c"))
	if err := q.Complete(result(newRun, true)); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "succeeded" || got.FanOut.Results["a"].Execution.Attempt != run.Attempt || len(got.OutputRefs) != 3 ||
		len(got.FileCheckpoints) != 0 {
		t.Fatalf("recovery replaced prior progress or leaked a checkpoint: %+v", got.FanOut)
	}
}

func TestWarmPiecesCancellationDuringVerification(t *testing.T) {
	q, _, m, s := warmFixture(t)
	j := submit(t, q, s)
	run := warmStart(t, q, m, j.ID)
	success := warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	b := warmNext(t, q, run, "b")
	r := warmRequest(run, "success", b)
	r.OutputRefs = success.OutputRefs
	entered, resume := make(chan struct{}), make(chan struct{})
	syncDir := q.files.syncDir
	q.files.syncDir = func(path string) error {
		if path == filepath.Join(q.files.root, "blobs") {
			close(entered)
			<-resume
		}
		return syncDir(path)
	}
	done := make(chan error, 1)
	go func() { _, err := q.warmPiece(context.Background(), r); done <- err }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("verification did not begin: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("verification did not reach the file store")
	}
	err := q.Cancel(j.ID)
	close(resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrStale) {
		t.Fatalf("cancelled publication returned %v", err)
	}
	rRelease := result(run, false)
	rRelease.ReleaseOnly = true
	if err := q.Complete(rRelease); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "cancelled" || len(got.FanOut.Results) != 1 || got.OutputRefs["a/out"] != success.OutputRefs["out"] {
		t.Fatalf("cancellation changed accepted outputs: %s %+v", got.State, got.FanOut)
	}
}

func TestWarmPiecesPreemptionFencesItemStartAndResumes(t *testing.T) {
	q, _, m, s := warmFixture(t)
	j := submit(t, q, s)
	run := warmStart(t, q, m, j.ID)
	warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	b := warmNext(t, q, run, "b")
	higher := submit(t, q, spec("production"))
	if got := job(t, q, j.ID); got.State != "stopping" || priorityStop(got) == nil {
		t.Fatalf("production did not request a fenced stop: %s %q", got.State, got.Reason)
	}
	if reply, err := q.warmPiece(t.Context(), warmRequest(run, "start", b)); err != nil || !reply.End || reply.Delivery != nil {
		t.Fatalf("stopped delivery should end the loop: %+v, %v", reply, err)
	}
	wrong := warmRequest(run, "start", b)
	wrong.Sequence++
	if _, err := q.warmPiece(t.Context(), wrong); !errors.Is(err, ErrStale) {
		t.Fatalf("mismatched start was treated as a clean stop: %v", err)
	}
	if _, err := q.warmPiece(t.Context(), warmRequest(run, "failure", b)); !errors.Is(err, ErrStale) {
		t.Fatalf("stopped item settled as a failure: %v", err)
	}
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	first := check(t, q, m, nil).Job
	if first == nil || first.ID != higher.ID {
		t.Fatalf("expected priority work after release, got %v", first)
	}
	if err := q.Complete(result(first, true)); err != nil {
		t.Fatal(err)
	}
	restarted := warmStart(t, q, m, j.ID)
	warmNext(t, q, restarted, "b")
	if got := job(t, q, j.ID); got.InfrastructureRetries != 0 || got.Losses != 0 {
		t.Fatalf("priority stop spent retry budget: %+v", got)
	}
}

func TestWarmPiecesNextAfterPriorityStop(t *testing.T) {
	q, _, m, s := warmFixture(t)
	parent := submit(t, q, s)
	run := warmStart(t, q, m, parent.ID)
	receipt := warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	demand := submit(t, q, spec("production"))
	if got := job(t, q, parent.ID); got.State != "stopping" || !plannedPriorityStop(got) {
		t.Fatalf("expected production stop before the next request: %s %q", got.State, got.Reason)
	}
	if got := warmCall(t, q, warmRequest(run, "next", nil)); !got.End || got.Delivery != nil {
		t.Fatalf("stop boundary returned an item: %+v", got)
	}
	// Main can exit cleanly before the worker receives the stop directive.
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, parent.ID); got.State != "queued" || got.FanOut.Next != 1 ||
		got.OutputRefs["a/out"] != receipt.OutputRefs["out"] || len(got.FanOut.Warm.Failures) != 0 ||
		got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("boundary exit did not retain the unsettled tail: %s %+v", got.State, got.FanOut)
	}
	first := check(t, q, m, nil).Job
	if first == nil || first.ID != demand.ID {
		t.Fatalf("production did not get the released turn: %+v", first)
	}
	if err := q.Complete(result(first, true)); err != nil {
		t.Fatal(err)
	}
	warmNext(t, q, warmStart(t, q, m, parent.ID), "b")
}

func TestWarmPiecesFinalCheckpointRetriesBeforeRelease(t *testing.T) {
	testWarmPiecesFinalCheckpoint(t, false)
}

func TestWarmPiecesInvalidFinalCheckpointReleases(t *testing.T) {
	testWarmPiecesFinalCheckpoint(t, true)
}

func testWarmPiecesFinalCheckpoint(t *testing.T, invalid bool) {
	t.Helper()
	q, clock, m, s := warmFixture(t)
	s.Resumable = true
	parent := submit(t, q, s)
	run := warmStart(t, q, m, parent.ID)
	warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	b := warmNext(t, q, run, "b")
	handler := API{Queue: q, Token: "test"}.Handler()
	var publications atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/files/checkpoint" && publications.Add(1) == 1 && !invalid {
			http.Error(w, "fixture checkpoint service unavailable", http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(api.Close)
	w := recoveryWorker(t)
	w.Machine, w.Clock, w.control = m, clock, &processControl{clock: clock}
	w.Client = Client{URL: api.URL, Token: "test"}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	run.Checkpoint = t.TempDir()
	store, err := newAttemptStore(root, *run)
	if err != nil {
		t.Fatal(err)
	}
	save := warmCheckpointPath(*run, b.Key)
	publication := attemptPublicationRoot(*run, store.dir)
	for _, dir := range []string{save, publication} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(save, "progress.json"), []byte(`{"myce_checkpoint":{"generation":7}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publication, "warm-checkpoint.json"), []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	if invalid {
		if generation, err := w.publishFileCheckpoint(*run, store.dir); err != nil || generation != 7 {
			t.Fatalf("publish initial checkpoint: generation=%d, err=%v", generation, err)
		}
		if err := os.WriteFile(filepath.Join(save, "progress.json"), []byte(`{"myce_checkpoint":{"generation":-1}}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	submit(t, q, spec("production"))
	stopping := job(t, q, parent.ID)
	stop := priorityStop(stopping)
	if stopping.State != "stopping" || stop == nil {
		t.Fatalf("expected a priority stop, got %s", stopping.State)
	}
	r := result(run, false)
	r.Interrupted, r.PriorityStop = true, stop
	actorErr := runWarmAttemptResult(t, w, store, clock, r, AttemptDirective{Stop: true, PriorityStop: stop})
	final := job(t, q, parent.ID)
	if invalid && (publications.Load() != 1 || final.CleanupCause == nil || final.CleanupCause.Finalization == nil ||
		final.CleanupCause.Finalization.Error != "invalid_checkpoint_generation" || !slices.ContainsFunc(final.Copies, func(copy Copy) bool {
		return copy.Attempt == run.Attempt && copy.Machine == run.Machine && copy.Session == run.Session && copy.ReleaseConfirmed
	})) {
		t.Fatalf("invalid save did not release with a checkpoint diagnostic: copies=%+v cause=%+v posts=%d",
			final.Copies, final.CleanupCause, publications.Load())
	}
	if actorErr != nil || !invalid && publications.Load() < 2 || final.State != "queued" || final.FanOut.Next != 1 ||
		final.FileCheckpoints[run.Attempt].Generation != 7 {
		t.Fatalf("checkpoint lost across publication retry: state=%s reason=%q posts=%d generation=%d actor=%v",
			final.State, final.Reason, publications.Load(), final.FileCheckpoints[run.Attempt].Generation, actorErr)
	}
}

func runWarmAttemptResult(t *testing.T, w Worker, store *workerStore, clock *fakeClock, r Result, directive AttemptDirective) error {
	t.Helper()
	store.state.Result = &r
	if err := store.save(); err != nil {
		t.Fatal(err)
	}
	a := &attemptSupervisor{store: store, job: *store.state.Job, worker: w, connected: true, publication: &publicationProgress{},
		directive: directive, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	go a.run(ctx)
	defer func() { cancel(); <-a.done }()
	await(t, func() bool {
		clock.Advance(5 * time.Millisecond)
		select {
		case <-a.done:
			return true
		default:
			return false
		}
	})
	return a.err
}

type warmRefusal struct {
	name, reason string
	change       func(*Spec)
}

func warmRefusals() []warmRefusal {
	multiple := 2
	return []warmRefusal{
		{"elastic", "accepts fixed", func(s *Spec) { s.WarmPieces = "elastic" }},
		{"no pieces", "plain-command pieces", func(s *Spec) { s.FanOut = nil }},
		{"extension", "plain-command pieces", func(s *Spec) { s.Kind = "extension" }},
		{"dependency", "after inputs", func(s *Spec) { s.After = &PieceDependency{Job: "producer", Piece: "a"} }},
		{"multi-device", "one accelerator", func(s *Spec) { s.Needs.GPU = &GPUSelection{Count: &multiple} }},
		{"NAS inputs", "file-store outputs", func(s *Spec) { s.Inputs = map[string]string{"in": "inputs"} }},
		{"async delivery", "file-store outputs", func(s *Spec) { s.OutputDelivery = asyncNAS }},
		{"ephemeral data", "managed worker data", func(s *Spec) { s.WorkerData = "ephemeral" }},
		{"checkpoint mode", "checkpoint generations", func(s *Spec) { s.CheckpointPolicy = "local_first" }},
		{"concurrency", "omit max_concurrency", func(s *Spec) { s.FanOut.MaxConcurrency = 2 }},
		{"shared XPU", "omit max_concurrency", func(s *Spec) { s.FanOut.ShareXPU = true }},
		{"workdir", "workdir", func(s *Spec) { s.Run.Workdir = "folder" }},
		{"reserved outputs", "worker-owned prefixes", func(s *Spec) { s.Outputs = map[string]string{".myce-internal": "out"} }},
	}
}

func TestWarmPiecesRefusals(t *testing.T) {
	q, _, _, s := warmFixture(t)
	for _, tc := range warmRefusals() {
		t.Run(tc.name, func(t *testing.T) {
			copy := cloneJob(&Job{Spec: s}).Spec
			tc.change(&copy)
			if err := copy.Validate(); err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("wanted %q, got %v", tc.reason, err)
			}
		})
	}
	nasCheckpoints, _ := fixture(t)
	if _, err := nasCheckpoints.Submit(s); err == nil || !strings.Contains(err.Error(), "requires file-store checkpoints") {
		t.Fatalf("resumable warm submission with NAS checkpoints returned %v", err)
	}
	unqualified := machine("old-worker", "fake")
	j := submit(t, q, s)
	if got := check(t, q, unqualified, nil).Job; got != nil {
		t.Fatalf("old worker received warm job %s", got.ID)
	}
	if got := job(t, q, j.ID); got.State != "queued" {
		t.Fatalf("unsupported worker changed state to %s", got.State)
	}
}

func TestWarmPiecesSameTierBoundaryYield(t *testing.T) {
	q, clock, _, s := warmFixture(t)
	m := cpuMachine("cpu-worker", "cpu-host", "cpu", 2, 8)
	m.Capabilities = append(m.Capabilities, warmPiecesCapability)
	check(t, q, m, nil)
	s.Needs = ResourceNeeds{CPUCores: 2, HostMemoryGB: 1}
	j := submit(t, q, s)
	run := warmStart(t, q, m, j.ID)
	a := warmNext(t, q, run, "a")
	competing := cpuSpec(2, 1)
	competing.Project = "another-project"
	competing.AllowedMachines = []string{m.ID}
	waiting := submit(t, q, competing)
	clock.Advance(time.Second)
	boundary := warmRequest(run, "next", nil)
	warmSuccess(t, q, run, a)

	if got := warmCall(t, q, boundary); !got.End {
		t.Fatalf("warm continuation did not yield contested CPU capacity: %+v", got)
	}
	if got := check(t, q, m, run).Job; got != nil {
		t.Fatalf("peer admitted ahead of release: %+v", got)
	}
	release := result(run, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	peer := check(t, q, m, nil).Job
	if peer == nil || peer.ID != waiting.ID {
		t.Fatalf("contender did not get released resources: %+v", peer)
	}
	if err := q.Complete(result(peer, true)); err != nil {
		t.Fatal(err)
	}
	next := warmStart(t, q, m, j.ID)
	warmNext(t, q, next, "b")
	if got := job(t, q, j.ID); got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("boundary yield charged a failure: %+v", got.FanOut)
	}
}

func TestWarmPiecesBoundaryYieldsSharedRAMAcrossCards(t *testing.T) {
	q, clock, _, s := warmFixture(t)
	m := m3Machine()
	m.Capabilities = append(m.Capabilities, warmPiecesCapability)
	machineCheck(t, q, m)
	s.Needs.HostMemoryGB = 24
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	s.AllowedMachines = []string{m.ID}
	j := submit(t, q, s)
	run := warmStart(t, q, m, j.ID)
	if !slices.Equal(run.AllocatedDevices, []string{"0"}) {
		t.Fatalf("warm attempt is on %v, want card 0", run.AllocatedDevices)
	}
	a := warmNext(t, q, run, "a")
	peer := spec("experiment")
	peer.Project = "another-project"
	peer.Needs.HostMemoryGB = 16
	peer.Needs.GPU = &GPUSelection{Devices: []string{"1"}}
	peer.AllowedMachines = []string{m.ID}
	waiting := submit(t, q, peer)
	clock.Advance(time.Second)
	warmSuccess(t, q, run, a)
	if got := warmCall(t, q, warmRequest(run, "next", nil)); !got.End || got.Delivery != nil {
		t.Fatalf("warm continuation did not yield shared host RAM: %+v", got)
	}
	if got := assignedJob(machineCheck(t, q, m, run)); got != nil {
		t.Fatalf("peer admitted before exact release: %+v", got)
	}
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != waiting.ID || !slices.Equal(next.AllocatedDevices, []string{"1"}) {
		t.Fatalf("peer did not get the released host RAM on card 1: %+v", next)
	}
}

func TestWarmPiecesEmptyListAfterAttemptLossFinalizes(t *testing.T) {
	q, clock, m, s := warmFixture(t)
	s.FanOut.Pieces = s.FanOut.Pieces[:1]
	j := submit(t, q, s)
	run := warmStart(t, q, m, j.ID)
	warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	clock.Advance(LossAfter + time.Second)
	replacement := machine("replacement", m.GPU)
	replacement.Capabilities = append(replacement.Capabilities, warmPiecesCapability)
	next := warmStart(t, q, replacement, j.ID)
	if !warmCall(t, q, warmRequest(next, "next", nil)).End {
		t.Fatal("finalization replay offered a settled item")
	}
	if err := q.Complete(result(next, true)); err != nil {
		t.Fatal(err)
	}
	final := job(t, q, j.ID)
	if final.State != "succeeded" || final.Attempt != next.Attempt || final.FanOut.Results["a"].Execution.Attempt != run.Attempt {
		t.Fatalf("finalization replay changed the receipt: %s %+v", final.State, final.FanOut)
	}
}

func TestWarmPiecesSuccessObservedBeforePriorityStopDelivery(t *testing.T) {
	q, _, m, s := warmFixture(t)
	j := submit(t, q, s)
	run := warmStart(t, q, m, j.ID)
	a := warmNext(t, q, run, "a")
	submit(t, q, spec("production"))
	r := warmRequest(run, "success", a)
	r.OutputRefs = FileManifest{"out": putPieceTestFile(t, q, "before the stop signal")}
	if _, err := q.warmPiece(t.Context(), r); !errors.Is(err, ErrStale) {
		t.Fatalf("success without prerelease evidence returned %v", err)
	}
	r.BeforeStop = true
	warmCall(t, q, r)
	if got := job(t, q, j.ID); got.State != "stopping" || got.FanOut.Results["a"].Execution != r.Execution {
		t.Fatalf("completion changed either the stop or the accepted receipt: %s %+v", got.State, got.FanOut)
	}
}

func TestWarmPiecesCleanPressureStopRequeues(t *testing.T) {
	q, _, m, s := warmFixture(t)
	parent := submit(t, q, s)
	run := warmStart(t, q, m, parent.ID)
	receipt := warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	if err := q.updateRequest(t.Context(), func() error {
		q.stopForPoolPressure(q.s.Jobs[parent.ID], "fixture demand")
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if reply := warmCall(t, q, warmRequest(run, "next", nil)); !reply.End {
		t.Fatalf("pressure stop offered more work: %+v", reply)
	}
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, parent.ID)
	if got.State != "queued" || got.FanOut.Next != 1 ||
		got.OutputRefs["a/out"] != receipt.OutputRefs["out"] || len(got.FanOut.Warm.Failures) != 0 || got.Losses != 0 {
		t.Fatalf("clean pressure stop = %s (%s), progress=%+v", got.State, got.Reason, got.FanOut)
	}
	warmNext(t, q, warmStart(t, q, m, parent.ID), "b")
}

func TestWarmPiecesCleanPressureStopReportsFullResult(t *testing.T) {
	q, clock, m, s := warmFixture(t)
	s.Resumable = false
	parent := submit(t, q, s)
	run := warmStart(t, q, m, parent.ID)
	warmSuccess(t, q, run, warmNext(t, q, run, "a"))
	if err := q.updateRequest(t.Context(), func() error {
		q.stopForPoolPressure(q.s.Jobs[parent.ID], "fixture demand")
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	var reports atomic.Int32
	var releaseOnly atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/complete" {
			data, err := io.ReadAll(r.Body)
			var got Result
			if err != nil || json.Unmarshal(data, &got) != nil {
				http.Error(w, "invalid completion fixture", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			reports.Add(1)
			if got.ReleaseOnly {
				releaseOnly.Store(true)
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(api.Close)
	w := recoveryWorker(t)
	w.Machine, w.Clock, w.control = m, clock, &processControl{clock: clock}
	w.Client = Client{URL: api.URL, Token: "test"}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	store, err := newAttemptStore(root, *run)
	if err != nil {
		t.Fatal(err)
	}
	if err := runWarmAttemptResult(t, w, store, clock, result(run, true), AttemptDirective{}); err != nil {
		t.Fatal(err)
	}
	final := job(t, q, parent.ID)
	if reports.Load() != 1 || releaseOnly.Load() || final.State != "queued" || final.FanOut.Next != 1 {
		t.Fatalf("pressure completion bypassed result classification: reports=%d release-only=%t state=%s next=%d",
			reports.Load(), releaseOnly.Load(), final.State, final.FanOut.Next)
	}
}

func TestWarmPiecesBoundaryYieldsToCardGroup(t *testing.T) {
	q, clock, _, s := warmFixture(t)
	m := m3Machine()
	m.Capabilities = append(m.Capabilities, warmPiecesCapability)
	s.Needs.MemoryGB = 6
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	s.AllowedMachines = []string{m.ID}
	parent := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != parent.ID || !slices.Equal(run.AllocatedDevices, []string{"0"}) {
		t.Fatalf("expected warm work on card 0, got %+v", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	a := warmNext(t, q, run, "a")
	peer := spec("experiment")
	peer.Project, peer.AllowedMachines = "another-project", []string{m.ID}
	peer.Needs.MemoryGB = 12 // Six GiB on each eight-GiB card.
	waiting := submit(t, q, peer)
	clock.Advance(time.Second)
	warmSuccess(t, q, run, a)
	if reply := warmCall(t, q, warmRequest(run, "next", nil)); !reply.End || reply.Delivery != nil {
		t.Fatalf("warm work did not yield to the two-card contender: %+v", reply)
	}
	if got := assignedJob(machineCheck(t, q, m, run)); got != nil {
		t.Fatalf("card group was assigned before exact release: %+v", got)
	}
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	got := assignedJob(machineCheck(t, q, m))
	if got == nil || got.ID != waiting.ID || !slices.Equal(got.AllocatedDevices, []string{"0", "1"}) {
		t.Fatalf("two-card contender did not get the released group: %+v", got)
	}
}

func TestWarmPiecesFileCheckpointWithoutNAS(t *testing.T) {
	q, _, m, s := warmFixture(t)
	fakeCheckpointAvailability(t, q, false)
	m.CheckpointUnavailable = true
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint"}}
	s.Resumable = true
	parent := submit(t, q, s)
	if !parent.FileTransport {
		t.Fatal("warm checkpoint was submitted without file transport")
	}
	workerReason := (Worker{Machine: m}).storageRootReason(parent)
	if got := check(t, q, m, nil).Job; got == nil || got.ID != parent.ID || workerReason != "" {
		t.Fatalf("file-store checkpoint required the NAS: assigned=%t worker reason=%q", got != nil, workerReason)
	}
}
