package myce

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #2166: TERM while a finished attempt's result is still uploading must not
// wait for the remaining bytes. The upload's second chunk stays blocked for the
// rest of the first run, so the worker returning at all proves its exit does
// not depend on the upload. The restarted worker keeps the session, resumes at
// the queue's accepted tus offset and completes the same attempt.
func TestWorkerTermDuringResultUploadResumesAfterRestart(t *testing.T) {
	for _, mode := range []string{"installed", "foreground"} {
		t.Run(mode, func(t *testing.T) { testWorkerTermDuringResultUpload(t, mode) })
	}
}

func testWorkerTermDuringResultUpload(t *testing.T, mode string) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<30, true); err != nil {
		t.Fatal(err)
	}
	const size = 3*fileUploadChunkSize + 1
	s := spec("experiment")
	s.Resumable = false
	s.Needs.MemoryGB = 1
	s.Run.Command = []string{"sh", "-c", "echo ran; head -c " + strconv.Itoa(size) + " /dev/zero > output"}
	s.Outputs = map[string]string{"output": "results"}

	var blocking atomic.Bool
	blocking.Store(true)
	held, release := make(chan struct{}), make(chan struct{})
	var heldOnce sync.Once
	var mu sync.Mutex
	var resumedAt []string
	api := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method == "PATCH" && strings.HasPrefix(r.URL.Path, "/api/files/uploads/") {
			if blocking.Load() {
				if r.Header.Get("Upload-Offset") != "0" {
					// A slow store: the second chunk does not finish while the first run lives.
					heldOnce.Do(func() { close(held) })
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
			} else {
				mu.Lock()
				resumedAt = append(resumedAt, r.Header.Get("Upload-Offset"))
				mu.Unlock()
			}
		}
		api.ServeHTTP(rw, r)
	}))
	// Release the held chunk before Close waits for its handler.
	defer func() { close(release); server.Close() }()
	// A stored-file job: its output publishes through the queue's tus store.
	seed := filepath.Join(t.TempDir(), "seed")
	if err := os.WriteFile(seed, []byte("seed"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := (Client{URL: server.URL, Token: "test"}).UploadFile(context.Background(), seed, ref); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{"seed": ref}
	j := submit(t, q, s)

	state := newTestStateRoot(t)
	newWorker := func() Worker {
		w := Worker{EnvironmentLimit: 1 << 20, StateDir: state, Interval: 20 * time.Millisecond, Clock: RealClock{},
			Machine: machine("term", "fake"), Client: Client{URL: server.URL, Token: "test"},
			HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
		if mode == "installed" {
			w.ServiceConfig, w.memoryCeilings, w.devEnvironmentSizing = &ServiceConfig{Role: "worker"}, fakeCeilings(t), true
		}
		return w
	}
	// Installed, signal.NotifyContext delivers SIGTERM by canceling Run's
	// context. Foreground myce join routes SIGTERM to Restart instead; its
	// Run context is the Ctrl-C planned leave, which stays uncanceled here.
	run := func(w Worker) (context.CancelFunc, chan error) {
		ctx, term := context.WithCancel(context.Background())
		done := make(chan error, 1)
		leave := ctx
		if mode == "foreground" {
			w.Restart, leave = ctx, context.Background()
		}
		go func() { done <- w.Run(leave) }()
		return term, done
	}

	term, done := run(newWorker())
	defer term()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("worker exited before publishing: %v", err)
	case <-time.After(testWaitTimeout):
		t.Fatalf("result upload never reached its second chunk: %+v", job(t, q, j.ID))
	}
	started := time.Now()
	term()
	returnsWhileBlocked(t, "worker TERM during result upload", func() {
		if err := <-done; err != nil {
			t.Errorf("TERM exit: %v", err)
		}
	})
	t.Logf("%s worker exited %s after TERM with the upload still blocked", mode, time.Since(started).Round(time.Millisecond))
	if got := job(t, q, j.ID); got.State != "running" || got.Attempt != 1 {
		t.Fatalf("quick exit changed the queue's attempt: state=%s attempt=%d", got.State, got.Attempt)
	}
	// A leaving check-in would make the restarted session leave again.
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range snap.Machines {
		if m.Leaving || m.Gone {
			t.Fatalf("TERM told the queue the worker left: %+v", m)
		}
	}

	blocking.Store(false)
	term, done = run(newWorker())
	defer term()
	await(t, func() bool {
		select {
		case err := <-done:
			t.Fatalf("restarted worker exited: %v", err)
		default:
		}
		return job(t, q, j.ID).State == "succeeded"
	})
	got := job(t, q, j.ID)
	if got.Attempt != 1 || len(got.AttemptHistory) > 1 || got.Losses != 0 {
		t.Fatalf("restart re-ran the attempt: attempt=%d history=%d losses=%d", got.Attempt, len(got.AttemptHistory), got.Losses)
	}
	if _, ok := got.OutputRefs["output"]; !ok {
		t.Fatal("restart did not publish the output")
	}
	mu.Lock()
	first := append([]string(nil), resumedAt...)
	mu.Unlock()
	if len(first) == 0 || first[0] != strconv.Itoa(fileUploadChunkSize) {
		t.Fatalf("restart did not resume at the accepted offset %d: PATCH offsets %v", fileUploadChunkSize, first)
	}
	logs, _ := q.Logs(j.ID, 0)
	runs := 0
	for _, line := range logs {
		if strings.TrimSpace(line.Text) == "ran" {
			runs++
		}
	}
	if runs != 1 {
		t.Fatalf("command ran %d times", runs)
	}
	term()
	returnsWhileBlocked(t, "restarted worker stop", func() { <-done })
}
