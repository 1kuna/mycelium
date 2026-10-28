package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Crash the real dev process, without deferred queue or worker shutdown. The
// restarted worker cleans the orphaned command and reports its interrupted
// attempt through the real API. No fleet, daemon, or dev-only control is used.
func TestProductionDevQueueCrash(t *testing.T) {
	const childRoot = "MYCE_TEST_PRODUCTION_DEV_ROOT"
	if root := os.Getenv(childRoot); root != "" {
		if err := run([]string{"dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "state", "queue.db"), "--checkpoints", filepath.Join(root, "checkpoints"), "--host-id", "retry-host", "--workers", "1", "--memory-gb", "8"}); err != nil {
			t.Fatal(err)
		}
		return
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wait := func(ready func() bool) {
		t.Helper()
		for !ready() {
			select {
			case <-ctx.Done():
				t.Fatal("dev crash deadline", ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	start := func(index int) (myce.Client, func(bool)) {
		t.Helper()
		logPath := filepath.Join(root, fmt.Sprintf("dev-%d.log", index))
		log, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProductionDevQueueCrash$", "-test.count=1")
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "TMPDIR=" + root, "MYCE_TOKEN=dev-crash-fixture", childRoot + "=" + root}
		cmd.Dir, cmd.Stdout, cmd.Stderr, cmd.WaitDelay = root, log, log, time.Second
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		active := true
		stop := func(crash bool) {
			t.Helper()
			if !active {
				return
			}
			active = false
			if crash {
				_ = cmd.Process.Kill()
			} else {
				_ = cmd.Process.Signal(os.Interrupt)
			}
			err := cmd.Wait()
			log.Close()
			if crash && err == nil || !crash && err != nil {
				data, _ := os.ReadFile(logPath)
				t.Fatalf("dev exit: crash=%v err=%v\n%s", crash, err, data)
			}
		}
		t.Cleanup(func() { stop(false) })
		client := myce.Client{Token: "dev-crash-fixture"}
		wait(func() bool {
			data, _ := os.ReadFile(logPath)
			for _, line := range strings.Split(string(data), "\n") {
				fields := strings.Fields(line)
				if len(fields) >= 3 && fields[0] == "Mycelium" && fields[1] == "dev:" {
					client.URL = fields[2]
					return true
				}
			}
			return false
		})
		return client, stop
	}
	client, crash := start(1)
	marker := filepath.Join(root, "started")
	s := myce.Spec{Name: "crash-stage", Project: "example", Priority: "production", SubmissionKey: "one-stage"}
	s.Needs.MemoryGB, s.Needs.Platform.Any = 1, true
	s.Run.Command = []string{"sh", "-c", `printf 'myce fixture work: %s\n' "$PWD"; printf 'myce fixture checkpoint: %s\n' "$MYCE_CHECKPOINT_DIR"; test -z "$MYCE_TOKEN" || exit 99; if [ "$MYCE_ATTEMPT" = 1 ]; then printf started > "$1"; while :; do sleep 1; done; fi; printf x >> "$MYCE_CHECKPOINT_DIR/output"`, "fixture", "{work}/started"}
	var accepted myce.Job
	if err := client.Call(ctx, "POST", "/api/jobs", s, &accepted); err != nil {
		t.Fatal(err)
	}
	readJob := func(client myce.Client) myce.Job {
		t.Helper()
		var snap myce.Snapshot
		if err := client.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		for _, j := range snap.Jobs {
			if j.ID == accepted.ID {
				return *j
			}
		}
		t.Fatal("accepted job missing from status")
		return myce.Job{}
	}
	var old myce.Job
	wait(func() bool {
		work := commandFixturePath(t, ctx, client, accepted.ID, "work")
		if work == "" {
			return false
		}
		marker = filepath.Join(work, "started")
		if _, err := os.Stat(marker); err != nil {
			return false
		}
		old = readJob(client)
		return old.State == "running" && old.StartedAt != nil
	})
	crash(true)
	client, stop := start(2)
	var final myce.Job
	wait(func() bool {
		final = readJob(client)
		if final.State == "failed" || final.State == "cancelled" {
			t.Fatal("restarted job did not complete", final.State, final.Reason)
		}
		return final.State == "succeeded" && final.ReleaseConfirmed
	})
	if final.ID != accepted.ID || final.Attempt != 2 || final.InfrastructureRetries != 1 {
		t.Fatal("wrong recovered job", final)
	}
	var replay myce.Job
	if err := client.Call(ctx, "POST", "/api/jobs", s, &replay); err != nil || replay.ID != accepted.ID || replay.Spec.SubmissionKey != s.SubmissionKey || replay.State != "succeeded" {
		t.Fatal("key replay", replay, err)
	}
	stale := myce.Result{Execution: myce.Execution{ID: old.ID, Attempt: old.Attempt}, Machine: old.Machine, Session: old.Session, Success: true}
	if err := client.Call(ctx, "POST", "/api/workers/complete", stale, nil); err == nil {
		t.Fatal("late old result accepted")
	}
	var snapshot myce.Snapshot
	if err := client.Call(ctx, "GET", "/api/status", nil, &snapshot); err != nil || len(snapshot.Jobs) != 1 || snapshot.Jobs[0].State != "succeeded" || snapshot.Jobs[0].Attempt != 2 {
		t.Fatal("duplicate completion", snapshot.Jobs, err)
	}
	checkpoint := commandFixturePath(t, ctx, client, accepted.ID, "checkpoint")
	if checkpoint == "" {
		t.Fatal("command did not report its checkpoint dir")
	}
	if data, err := commandFixtureAppendBytes(filepath.Join(checkpoint, "output")); err != nil || string(data) != "x" {
		t.Fatal("output was not produced once", string(data), err)
	}
	stop(false)
	if ctx.Err() != nil {
		t.Fatal("dev crash total budget exceeded", ctx.Err())
	}
}
