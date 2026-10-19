package myce

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReviewDeadlineDelayedGateMustNotExecute(t *testing.T) {
	q, initial := fixture(t)
	c := &dockerTestClock{now: initial.Now()}
	q.clock = c
	dir := t.TempDir()
	ready, release, marker := filepath.Join(dir, "ready"), filepath.Join(dir, "release"), filepath.Join(dir, "command-ran")
	// Pipes make readiness and release explicit, independent of shell polling.
	openGate := func(path string) *os.File {
		t.Helper()
		if err := syscall.Mkfifo(path, 0600); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}
	readyPipe, releasePipe := openGate(ready), openGate(release)
	shim := "#!/bin/sh\nread gate || exit 125\nprintf R > " + ready + "\nread release < " + release + "\nexec /bin/sh \"$@\" <<EOF\nstart\nEOF\n"
	if e := os.WriteFile(filepath.Join(dir, "sh"), []byte(shim), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	s := spec("production")
	s.Run.Command = []string{"/bin/sh", "-c", "echo ran > " + marker}
	submit(t, q, s)
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer store.close()
	store.state.Session = assigned.Session
	w = workerWithPreparedCheckpoint(t, w, store, *assigned)
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() {
		// Also release and join startup on an assertion failure before store teardown.
		_, _ = releasePipe.WriteString("release\n")
		select {
		case <-finished:
		case <-time.After(testWaitTimeout):
			t.Error("gate startup did not finish during cleanup")
		}
	}()
	go func() {
		defer close(finished)
		p, e := w.start(store, *assigned)
		if p != nil {
			<-p.done
		}
		done <- e
	}()
	readyDone := make(chan error, 1)
	go func() { _, err := io.ReadFull(readyPipe, make([]byte, 1)); readyDone <- err }()
	select {
	case err := <-readyDone:
		if err != nil {
			t.Fatal(err)
		}
	case err := <-done:
		t.Fatalf("worker exited before gate readiness: %v", err)
	case <-time.After(testWaitTimeout):
		t.Fatal("gate never became ready")
	}
	c.advance(ProductionStartTimeout)
	requireState(t, q, assigned.ID, "failed")
	if _, e := releasePipe.WriteString("release\n"); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("dispatch stalled")
	}
	if _, e := os.Stat(marker); e == nil {
		t.Fatal("command executed after durable deadline failure")
	}
}
