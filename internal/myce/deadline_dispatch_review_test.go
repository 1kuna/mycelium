package myce

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestReviewDeadlineDelayedGateMustNotExecute(t *testing.T) {
	q, initial := fixture(t)
	c := &dockerTestClock{now: initial.Now()}
	q.clock = c
	s := spec("production")
	s.Run.Command = []string{"/bin/sh", "-c", `echo ran > "$MYCE_CHECKPOINT_DIR/command-ran"`}
	submit(t, q, s)
	assigned := check(t, q, machine("worker", "fake"), nil).Job
	ready, release := make(chan struct{}), make(chan struct{})
	var readyOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	api := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/started" {
			// The disposable helper has reported ready, but the queue has not yet
			// durably authorized execution. Delay this real boundary, not PATH lookup
			// of a trusted supervisor shell.
			readyOnce.Do(func() { close(ready) })
			<-release
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: machine("worker", "fake"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	w = workerWithPreparedCheckpoint(t, w, store, *assigned)
	marker := filepath.Join(attemptCheckpoint(*assigned), "command-ran")
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() {
		unblock()
		select {
		case <-finished:
		case <-time.After(testWaitTimeout):
			t.Error("gate startup did not finish during cleanup")
		}
	}()
	go func() {
		defer close(finished)
		p, err := w.start(store, *assigned)
		if p != nil {
			<-p.done
		}
		done <- err
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("worker exited before gate readiness: %v", err)
	case <-time.After(testWaitTimeout):
		t.Fatal("gate never became ready")
	}
	c.advance(ProductionStartTimeout)
	requireState(t, q, assigned.ID, "failed")
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("dispatch stalled")
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("command executed after durable deadline failure")
	}
}
