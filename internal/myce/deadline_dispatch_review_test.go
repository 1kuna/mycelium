package myce

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReviewDeadlineDelayedGateMustNotExecute(t *testing.T) {
	q, initial := fixture(t)
	c := &dockerTestClock{now: initial.Now()}
	q.clock = c
	dir := t.TempDir()
	ready, release, marker := filepath.Join(dir, "ready"), filepath.Join(dir, "release"), filepath.Join(dir, "command-ran")
	shim := "#!/bin/sh\nread gate || exit 125\necho ready > " + ready + "\nwhile [ ! -f " + release + " ]; do /bin/sleep 0.005; done\nexec /bin/sh \"$@\" <<EOF\nstart\nEOF\n"
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
	w := Worker{StateDir: t.TempDir(), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer store.close()
	store.state.Session = assigned.Session
	done := make(chan error, 1)
	go func() {
		p, e := w.start(store, *assigned)
		if p != nil {
			<-p.done
		}
		done <- e
	}()
	await(t, func() bool { _, e := os.Stat(ready); return e == nil })
	c.advance(ProductionStartTimeout)
	requireState(t, q, assigned.ID, "failed")
	if e := os.WriteFile(release, nil, 0600); e != nil {
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
