package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublicationTimingsExactAttempt(t *testing.T) {
	for _, checkpoints := range []bool{false, true} {
		t.Run(fmt.Sprintf("file-checkpoints=%t", checkpoints), func(t *testing.T) {
			testPublicationTimingsExactAttempt(t, checkpoints)
		})
	}
}

func testPublicationTimingsExactAttempt(t *testing.T, checkpoints bool) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, checkpoints); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(api.Close)
	client := Client{URL: api.URL, Token: "test"}
	_, input := retentionUpload(t, client, "input")
	_, output := retentionUpload(t, client, "output bytes")
	spec := spec("experiment")
	spec.Resumable = false
	spec.Files = FileManifest{"input": input}
	spec.Outputs = map[string]string{"output": "results"}
	queued := submit(t, q, spec)
	m := machine("publisher", "fake")
	check(t, q, m, nil)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID {
		t.Fatal("missing attempt")
	}
	if j.FileTransport != checkpoints || !j.storedFileResults() {
		t.Fatalf("wrong transport for stored result: file checkpoints=%t", j.FileTransport)
	}
	checkpoint := attemptCheckpoint(*j)
	pending := filepath.Join(j.Checkpoint, "attempt-pending")
	unrelated := filepath.Join(t.TempDir(), "attempt-other")
	q.nasProgress.readMu.Lock()
	q.nasProgress.recent = map[string]checkpointProgressObservation{
		checkpoint: {value: 1},
		pending:    {pending: true},
		unrelated:  {value: 2},
	}
	q.nasProgress.readMu.Unlock()
	checkObservations := func(wantSettled bool) {
		t.Helper()
		q.nasProgress.readMu.Lock()
		defer q.nasProgress.readMu.Unlock()
		_, settledPresent := q.nasProgress.recent[checkpoint]
		_, pendingPresent := q.nasProgress.recent[pending]
		_, unrelatedPresent := q.nasProgress.recent[unrelated]
		if settledPresent != wantSettled || !pendingPresent || !unrelatedPresent {
			t.Fatalf("unexpected checkpoint observations after completion request: settled=%t pending=%t unrelated=%t",
				settledPresent, pendingPresent, unrelatedPresent)
		}
	}
	var planningCalls atomic.Int32
	q.mu.Lock()
	q.observeBodyPhase = func(name string, _ time.Duration) {
		if name == "maintain_models" {
			planningCalls.Add(1)
		}
	}
	q.mu.Unlock()
	p := &publicationProgress{}
	p.resultReady(clock.Now())
	clock.Advance(time.Second)
	release := result(j, true)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	if planningCalls.Swap(0) != 0 {
		t.Error("release acknowledgement ran fleet planning")
	}
	checkObservations(true)
	p.releaseAcknowledged(clock.Now())
	manifest := p.begin(clock, "filesystem/output_manifest")
	clock.Advance(200 * time.Millisecond)
	manifest()
	manifest() // A deferred close after an explicit close counts once.
	clock.Advance(time.Second)
	create := p.request(clock, "POST", "/api/files/uploads/")
	clock.Advance(time.Second)
	create()
	patch := p.request(clock, "PATCH", "/api/files/uploads/"+output.SHA256)
	clock.Advance(time.Second)
	patch()
	p.confirm(output)
	p.confirm(output)
	client.publication = p
	q.files.syncDir = func(string) error { clock.Advance(2 * time.Second); return nil }
	finished := result(j, true)
	finished.OutputRefs = FileManifest{"output": output}
	if err := client.Call(context.Background(), "POST", "/api/workers/complete", finished, nil); err != nil {
		t.Fatal(err)
	}
	if planningCalls.Load() != 0 {
		t.Fatal("successful stored result ran fleet planning")
	}
	checkObservations(false)
	got := job(t, q, j.ID)
	timing := got.PublicationTimings
	if timing == nil || len(got.AttemptHistory) != 1 || got.AttemptHistory[0].PublicationTimings == nil {
		t.Fatalf("timings unavailable for attempt %d: %+v", j.Attempt, timing)
	}
	if timing.VerificationSeconds == nil || *timing.VerificationSeconds != 2 || timing.CompletionQueueSeconds != 2 ||
		timing.ReleaseToAcceptSeconds == nil || *timing.ReleaseToAcceptSeconds != 5.2 {
		t.Fatalf("wrong queue timings: %+v", timing)
	}
	worker := timing.Worker
	if worker == nil || worker.ResultReadyToReleaseAckSeconds == nil || *worker.ResultReadyToReleaseAckSeconds != 1 ||
		worker.ManifestSeconds != .2 || worker.UploadCreates != 1 || worker.UploadPatches != 1 ||
		worker.UploadCreateSeconds != 1 || worker.UploadPatchSeconds != 1 || worker.UploadWindowSeconds != 2 ||
		worker.PublishedBytes != output.SizeBytes {
		t.Fatalf("wrong worker timings: %+v", worker)
	}
	// A delayed retry cannot overwrite the accepted observation.
	clock.Advance(time.Second)
	client.publication = nil
	if err := client.Call(context.Background(), "POST", "/api/workers/complete", finished, nil); err != nil {
		t.Fatal(err)
	}
	if replay := job(t, q, j.ID).PublicationTimings; replay.Worker == nil || replay.CompletionQueueSeconds != 2 {
		t.Fatalf("replay overwrote accepted timing: %+v", replay)
	}
}

// An old result deliberately cannot finish log replay. Actual queue admission
// semantics in the fixture withhold a replacement while Recovering is true.
// A replacement must run in the same session without dropping old custody.
func TestReleasedRecoveryAdmitsNewWorkBeforePublication(t *testing.T) {
	for _, mode := range []string{"log", "output"} {
		t.Run(mode, func(t *testing.T) { testReleasedRecoveryAdmitsNewWork(t, mode) })
	}
}

func testReleasedRecoveryAdmitsNewWork(t *testing.T, mode string) {
	h := &machineHarness{controls: map[string]AttemptDirective{}, blockLog: "saved-result", logEntered: make(chan struct{}), unblock: make(chan struct{})}
	w := harnessWorker(t, h)
	clock := &fakeClock{now: time.Now()}
	w.Clock = clock
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	root.state.Session, root.state.Fresh = "saved-session", false
	if err := root.save(); err != nil {
		t.Fatal(err)
	}
	old := harnessJob(t, "saved-result", "", "false")
	old.Machine, old.Session = w.Machine.ID, root.state.Session
	const output = "saved output"
	if mode == "output" {
		h.blockLog = ""
		old.Spec.Run.Workdir = t.TempDir()
		path := filepath.Join(old.Spec.Run.Workdir, "output.txt")
		if err := os.WriteFile(path, []byte(output), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		old.Spec.Files = FileManifest{"input": ref}
		old.Spec.Outputs = map[string]string{"output.txt": "results"}
	}
	child, err := newAttemptStore(root, old)
	if err != nil {
		t.Fatal(err)
	}
	r := result(&old, true)
	child.state.Result, child.state.Log = &r, "saved.log"
	if err := os.WriteFile(filepath.Join(root.dir, child.state.Log), []byte("saved log"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := child.save(); err != nil {
		t.Fatal(err)
	}
	root.close()
	next := harnessJob(t, "replacement", "", "true")
	var offered atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if mode == "output" && req.Method == "POST" && req.URL.Path == "/api/files/uploads/" {
			body, err := io.ReadAll(req.Body)
			if err != nil || string(body) != output || req.Header.Get("Content-Type") != "application/offset+octet-stream" {
				t.Errorf("unexpected creation upload: body=%q, err=%v", body, err)
				http.Error(rw, "invalid creation upload", http.StatusBadRequest)
				return
			}
			h.once.Do(func() { close(h.logEntered) })
			select {
			case <-h.unblock:
			case <-req.Context().Done():
				return
			}
			rw.Header().Set("Upload-Offset", fmt.Sprint(len(output)))
			rw.WriteHeader(http.StatusCreated)
			return
		}
		if req.URL.Path != "/api/workers/checkin" {
			h.serve(rw, req)
			return
		}
		var c Checkin
		if err := json.NewDecoder(req.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		h.mu.Lock()
		h.reports = append(h.reports, c)
		h.mu.Unlock()
		if len(c.Attempts) > 0 && c.Machine.Session != old.Session {
			t.Error("rotated session before old result delivery")
		}
		reply := Reply{AttemptProtocol: keyedAttemptProtocol}
		if !c.Recovering && offered.CompareAndSwap(false, true) {
			j := next
			j.Machine, j.Session = c.Machine.ID, c.Machine.Session
			reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Job: &j})
		}
		json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	w.Client.URL = server.URL
	if mode == "output" {
		// This fixture controls when the upload request completes. Keep loopback
		// HTTP and owner cancellation without the socket stall detector racing
		// the replacement's startup on a loaded test host.
		w.Client.HTTP = &http.Client{Transport: fetchTransport(server.Client().Transport.RoundTrip)}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		close(h.unblock)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("worker did not stop")
		}
	}()
	await(t, func() bool { clock.Advance(time.Millisecond * 10); return h.completed(next.ID) })
	blocking := "POST /api/workers/log"
	if mode == "output" {
		blocking = "POST /api/files/uploads/"
	}
	// Replacement completion does not synchronize publication or the next
	// heartbeat. Wait for the blocked request and its reported operation.
	await(t, func() bool {
		clock.Advance(time.Millisecond * 10)
		select {
		case <-h.logEntered:
		default:
			return false
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, c := range h.reports {
			for _, p := range c.Machine.ResultPublications {
				if p.ID == old.ID && p.Attempt == old.Attempt && p.Blocking == blocking {
					return true
				}
			}
		}
		return false
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	releases := 0
	for _, r := range h.results {
		if r.ID == old.ID {
			if !r.ReleaseOnly {
				t.Fatal("old publication unexpectedly finished")
			}
			releases++
		}
	}
	if mode == "output" && releases != 1 {
		t.Fatalf("release acknowledged %d times while upload blocked", releases)
	}
}

// Reports retain accepted bytes across repeated HEADs and use the injected
// clock to distinguish one still-running syscall from restarted work.
func TestPublicationProgressConfirmedBytesAndBlockingOperation(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, ref := retentionUpload(t, c, "finished output")
	p := &publicationProgress{}
	c.publication, c.Clock = p, clock
	j := Job{ID: "saved-result", Attempt: 2}
	finish := p.begin(clock, "filesystem/output_validation")
	clock.Advance(37 * time.Second)
	if r := p.snapshot(j, clock.Now()); r == nil || r.WaitingSeconds != 37 || r.Blocking != "filesystem/output_validation" {
		t.Fatal(r)
	}
	finish()
	for range 2 {
		if err := c.hasFile(context.Background(), ref, ""); err != nil {
			t.Fatal(err)
		}
	}
	finish = p.request(clock, "HEAD", "/api/files/blobs/"+ref.SHA256)
	defer finish()
	clock.Advance(time.Minute)
	r := p.snapshot(j, clock.Now())
	if r.PublishedBytes != ref.SizeBytes || r.Blocking != "HEAD /api/files/blobs/:id" || r.WaitingSeconds != 60 {
		t.Fatal(r)
	}
	lines := strings.Join(NewCompactStatus(Snapshot{Now: clock.Now(), Machines: []*Machine{{ID: "worker", ResultPublications: []ResultPublication{*r}}}}).Lines(false), "\n")
	if !strings.Contains(lines, "published_bytes=15 blocking=HEAD /api/files/blobs/:id") || strings.Contains(lines, ref.SHA256) {
		t.Fatal(lines)
	}
}

// Finalization API calls use the queue's measured progress watch, not a new
// total publication deadline. Recovery can retry once that evidence says stuck.
func TestRecoveryPublicationRequestUsesQueueProgress(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	var starts atomic.Int32
	p := &publicationProgress{}
	c := Client{URL: "http://fixture.invalid", Clock: clock, publication: p,
		Pause: func(_ context.Context, d time.Duration) error { clock.Advance(d); return nil },
		HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/api/ping" {
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"queue_progress":{"releases":7,"waiting":2,"p99_ms":3000}}`))}, nil
			}
			starts.Add(1)
			<-r.Context().Done()
			return nil, r.Context().Err()
		})},
	}.WithProgressWatch()
	for range 2 {
		err := c.Call(context.Background(), "POST", "/api/workers/complete", Result{}, nil)
		var stalled *QueueStalledError
		if !errors.As(err, &stalled) || stalled.Window != 12*time.Second {
			t.Fatal(err)
		}
		p.failed(err)
	}
	if starts.Load() != 2 || p.snapshot(Job{ID: "saved", Attempt: 1}, clock.Now()).LastError != "queue_stalled" {
		t.Fatal("request not retryable or cause lost")
	}
}

// #1792: one lost acknowledgement after minutes of successful replay used to
// schedule minutes of idle retry time. Exercise the actor's actual retry gate,
// retaining the failed chunk boundary and the original successful result.
func TestRecoveryLogReplayRetryExcludesAcknowledgedChunks(t *testing.T) {
	w := recoveryWorker(t)
	clock := &fakeClock{now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}
	w.Clock, w.control = clock, &processControl{}
	w.Client.Jitter = func() float64 { return 0 }
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	j := recoveryJob(t, []string{"true"})
	j.Machine, j.Session = w.Machine.ID, "saved-session"
	root.state.Session = j.Session
	s, err := newAttemptStore(root, j)
	if err != nil {
		t.Fatal(err)
	}
	r := result(&j, true)
	s.state.Result, s.state.Log = &r, "saved-replay.log"
	if err := os.WriteFile(filepath.Join(s.dir, s.state.Log), []byte(strings.Repeat("x", logReplayBatchSize)+"last chunk"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var failedChunk LogChunk
	retried := make(chan struct{})
	w.Client.HTTP = &http.Client{Transport: clientTestTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api/workers/log" || req.URL.Path == "/api/workers/log-batch" {
			var chunk LogChunk
			if req.URL.Path == "/api/workers/log-batch" {
				var batch logBatch
				if err := json.NewDecoder(req.Body).Decode(&batch); err != nil {
					t.Error(err)
				}
				chunk = batch.Chunks[0]
			} else if err := json.NewDecoder(req.Body).Decode(&chunk); err != nil {
				t.Error(err)
			}
			switch calls.Add(1) {
			case 1:
				clock.Advance(5 * time.Minute)
			case 2:
				failedChunk = chunk
				clock.Advance(2 * time.Second)
				return nil, &QueueStalledError{Method: req.Method, Path: req.URL.Path, Waited: 2 * time.Second}
			case 3:
				if chunk.Offset != failedChunk.Offset || chunk.Text != failedChunk.Text || chunk.Session != j.Session || chunk.Attempt != j.Attempt {
					t.Error("retry changed the durable chunk identity")
				}
				close(retried)
			}
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	a := &attemptSupervisor{store: s, job: j, worker: w, connected: true, publication: &publicationProgress{}, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	go a.run(ctx)
	defer func() { cancel(); <-a.done }()
	await(t, func() bool {
		p := a.publication.snapshot(j, clock.Now())
		return p != nil && p.Blocking == "retry" && p.LastError == "queue_stalled"
	})
	failedAt := clock.Now()
	await(t, func() bool {
		clock.Advance(10 * time.Millisecond)
		select {
		case <-retried:
			return true
		default:
			if clock.Now().Sub(failedAt) > 10*time.Second {
				t.Fatal("successful replay time inflated the retry pause")
			}
			return false
		}
	})
	cancel()
	<-a.done
	if s.state.Result != nil && !s.state.Result.Success {
		t.Fatal("retry lost the saved successful outcome")
	}
}

func TestRecoveryPublicationFieldsRetryPredecessorQueue(t *testing.T) {
	var mu sync.Mutex
	var reports []Checkin
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c Checkin
		json.NewDecoder(r.Body).Decode(&c)
		mu.Lock()
		reports = append(reports, c)
		mu.Unlock()
		if len(c.Machine.ResultPublications) > 0 {
			w.WriteHeader(400)
			io.WriteString(w, `{"code":"request_invalid","error":"unknown field"}`)
			return
		}
		json.NewEncoder(w).Encode(Reply{AttemptProtocol: keyedAttemptProtocol})
	}))
	defer server.Close()
	w := Worker{Client: Client{URL: server.URL, publicationWire: &atomic.Bool{}}}
	w.Client.publicationWire.Store(true)
	c := Checkin{Machine: Machine{ID: "worker", Session: "session", ResultPublications: []ResultPublication{{Execution: Execution{ID: "old", Attempt: 1}, Blocking: "retry"}}}}
	for range 2 {
		if _, err := w.checkin(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	if len(reports) != 3 || len(reports[0].Machine.ResultPublications) != 1 || len(reports[1].Machine.ResultPublications) != 0 || len(reports[2].Machine.ResultPublications) != 0 {
		t.Fatal(reports)
	}
}

func TestPublicationProgressAcknowledgementClearsError(t *testing.T) {
	for _, status := range []int{200, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			p := &publicationProgress{}
			p.failed(&QueueStalledError{})
			c := Client{URL: "http://fixture.invalid", publication: p, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			})}}
			_ = c.Call(context.Background(), "POST", "/api/workers/log", LogChunk{}, nil)
			got := p.snapshot(Job{}, c.clock().Now()).LastError
			if status == 200 && got != "" || status == 503 && got != "queue_stalled" {
				t.Fatal("wrong retained error", got)
			}
		})
	}
}
