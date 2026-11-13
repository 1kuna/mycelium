package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitParentDeath(t *testing.T) {
	var parent atomic.Int64
	parent.Store(42)
	ctx, cancel := waitParentContext(context.Background(), 42, time.Millisecond, func() int { return int(parent.Load()) })
	defer cancel()
	if ctx.Err() != nil {
		t.Fatal("living parent cancelled")
	}
	parent.Store(1)
	select {
	case <-ctx.Done():
	case <-time.After(devTestTimeout):
		t.Fatal("parent death did not end wait")
	}
}
func TestWaitParentAlreadyOrphaned(t *testing.T) {
	ctx, cancel := waitParentContext(context.Background(), 1, time.Second, func() int { return 1 })
	defer cancel()
	if ctx.Err() == nil {
		t.Fatal("already orphaned wait continued")
	}
}

func TestWaitParentProcessHelper(t *testing.T) {
	mode := os.Getenv("MYCE_PARENT_TEST_MODE")
	if mode == "" {
		return
	}
	dir := os.Getenv("MYCE_PARENT_TEST_DIR")
	if mode == "parent" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWaitParentProcessHelper$")
		cmd.Env = append(os.Environ(), "MYCE_PARENT_TEST_MODE=child")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				_ = cmd.Process.Release()
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("child never read job")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if os.Getenv("MYCE_PARENT_TEST_COMMAND") == "run" {
		err = runCommand(ctx, []string{"--queue", os.Getenv("MYCE_PARENT_TEST_QUEUE"), "--token", "fixture", "--", "true"}, io.Discard, io.Discard)
	} else {
		err = waitCommand(ctx, []string{"job", "--queue", os.Getenv("MYCE_PARENT_TEST_QUEUE"), "--token", "fixture", "--timeout", "5s"}, io.Discard, io.Discard)
	}
	// Publish only complete contents; the parent reads as soon as exit exists.
	exit := filepath.Join(dir, "exit")
	if e := os.WriteFile(exit+".tmp", []byte(strconv.Itoa(exitCode(err))), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(exit+".tmp", exit); e != nil {
		t.Fatal(e)
	}
}
func TestWaitExitsAfterRealParentExit(t *testing.T) { testObserverParentExit(t, "wait") }
func TestRunExitsAfterRealParentExit(t *testing.T)  { testObserverParentExit(t, "run") }
func testObserverParentExit(t *testing.T, command string) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
			t.Error(err)
		}
		if r.URL.Path == "/api/jobs/cancel" || r.URL.Path == "/api/jobs/job/cancel" {
			t.Error("parent death cancelled job")
		}
		if r.URL.Path == "/api/jobs/job/logs" {
			io.WriteString(w, `[]`)
			return
		}
		if r.URL.Path == "/api/wait" {
			io.WriteString(w, `{"jobs":[{"id":"job","state":"running","revision":1,"spec":{}}]}`)
			return
		}
		io.WriteString(w, `{"id":"job","state":"running"}`)
	}))
	defer server.Close()
	parent := exec.Command(os.Args[0], "-test.run=^TestWaitParentProcessHelper$")
	parent.Env = append(os.Environ(), "MYCE_PARENT_TEST_COMMAND="+command, "MYCE_PARENT_TEST_MODE=parent", "MYCE_PARENT_TEST_DIR="+dir, "MYCE_PARENT_TEST_QUEUE="+server.URL)
	if out, err := parent.CombinedOutput(); err != nil {
		t.Fatalf("parent helper: %v %s", err, out)
	}
	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(filepath.Join(dir, "exit")); err == nil {
			expected := 4
			if command == "run" {
				expected = runDetachedExit
			}
			if string(data) != strconv.Itoa(expected) {
				t.Fatalf("orphan exit code %s, want %d", data, expected)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("wait did not exit after its real parent died")
}
