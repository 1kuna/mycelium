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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
	var ref FileRef
	if mode == "output" {
		h.blockLog = ""
		old.Spec.Run.Workdir = t.TempDir()
		path := filepath.Join(old.Spec.Run.Workdir, "output.txt")
		if err := os.WriteFile(path, []byte("saved output"), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err = IdentifyFile(path)
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
		if mode == "output" && req.Method == "HEAD" && strings.HasPrefix(req.URL.Path, "/api/files/blobs/") {
			h.once.Do(func() { close(h.logEntered) })
			select {
			case <-h.unblock:
			case <-req.Context().Done():
				return
			}
			rw.Header().Set("Content-Length", "12")
			rw.Header().Set("ETag", `"`+ref.SHA256+`"`)
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
	select {
	case <-h.logEntered:
	default:
		t.Fatal("old result was never replayed")
	}
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
	found := false
	blocking := "POST /api/workers/log"
	if mode == "output" {
		blocking = "HEAD /api/files/blobs/:id"
	}
	for _, c := range h.reports {
		for _, p := range c.Machine.ResultPublications {
			if p.ID == old.ID && p.Attempt == old.Attempt && p.Blocking == blocking {
				found = true
			}
		}
	}
	if !found {
		t.Error("heartbeat omitted blocked publication request")
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
