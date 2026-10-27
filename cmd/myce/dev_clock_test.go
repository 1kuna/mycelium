package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestDevClockSubmissionExpiryEndToEnd(t *testing.T) {
	root := currentTestStateRoot(t)
	binary := devTestBinary(t)
	d, err := myce.StartDev(myce.DevConfig{Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "clock-test", Workers: 1, MemoryGB: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	c := d.Client()
	c.HTTP = &http.Client{Timeout: 10 * time.Second}
	cancelKey := func(key string) myce.Cancellation {
		t.Helper()
		ack, err := c.CancelSubmission(ctx, "test", key)
		if err != nil {
			t.Fatal(err)
		}
		if ack.Project != "test" || ack.SubmissionKey == nil || *ack.SubmissionKey != key || !ack.WillNotRun {
			t.Fatalf("bad ack: %+v", ack)
		}
		return ack
	}
	spec := myce.Spec{Name: "clock-test", Project: "test", Priority: "experiment"}
	spec.Run.Command = []string{"echo", "done"}
	spec.Needs.MemoryGB = 8
	// No worker fits, so cancellation and expiry need no process timing assumptions.
	spec.Needs.Platform = myce.Platform{Required: []string{"xpu"}}
	submit := func(key string) (myce.Job, error) {
		s := spec
		s.SubmissionKey = key
		var j myce.Job
		err := c.Call(ctx, "POST", "/api/jobs", s, &j)
		return j, err
	}
	if ack := cancelKey("expires"); ack.State != "absent" || ack.JobID != nil {
		t.Fatal(ack)
	}
	if ack := cancelKey("binds"); ack.State != "absent" {
		t.Fatal(ack)
	}
	bound, err := submit("binds")
	if err != nil || bound.ID == "" || bound.State != "cancelled" || bound.Attempt != 0 {
		t.Fatal(bound, err)
	}
	live, err := submit("lifetime")
	if err != nil {
		t.Fatal(err)
	}
	var cancelled myce.Cancellation
	if err := c.Call(ctx, "POST", "/api/jobs/"+live.ID+"/cancel", nil, &cancelled); err != nil || cancelled.State != "cancelled" {
		t.Fatal(cancelled, err)
	}
	for _, duration := range []string{"30d", "1s"} {
		command := exec.CommandContext(ctx, binary, "dev", "clock", "advance", duration, "--queue", d.URL, "--token", "clock-test")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("advance: %v %s", err, output)
		}
		var reply struct {
			Now time.Time `json:"now"`
		}
		if err := json.Unmarshal(output, &reply); err != nil || reply.Now.IsZero() {
			t.Fatalf("reply: %s %v", output, err)
		}
	}
	_, err = submit("expires")
	var apiError *myce.HTTPError
	if !errors.As(err, &apiError) || apiError.Status != 410 || apiError.Code != "submission_key_expired" {
		t.Fatalf("expired submit: %v", err)
	}
	if ack := cancelKey("expires"); ack.State != "unknown" || ack.JobID != nil || ack.Release != nil || ack.AcknowledgedAt != nil {
		t.Fatal(ack)
	}
	if ack := cancelKey("never-seen"); ack.State != "absent" || ack.JobID != nil {
		t.Fatal(ack)
	}
	for key, id := range map[string]string{"binds": bound.ID, "lifetime": live.ID} {
		replay, err := submit(key)
		if err != nil || replay.ID != id || replay.State != "cancelled" || replay.Attempt != 0 {
			t.Fatal(replay, err)
		}
	}
	var snapshot myce.Snapshot
	if err := c.Call(ctx, "GET", "/api/status", nil, &snapshot); err != nil {
		t.Fatal(err)
	}
	// Replay answers survive by key while the old terminal job rows retire.
	if len(snapshot.Jobs) != 0 {
		t.Fatalf("expired/unseen key created a job after history retirement: %d", len(snapshot.Jobs))
	}
}

func TestDevClockCLIProductionIsolation(t *testing.T) {
	root := currentTestStateRoot(t)
	q, err := myce.Open(filepath.Join(root, "queue.db"), filepath.Join(root, "checkpoints"), myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	var out bytes.Buffer
	if err := devClockCommand([]string{"advance", "1s", "--queue", server.URL, "--token", "test"}, &out); err == nil {
		t.Fatal("dev CLI reached production clock")
	}
	t.Setenv("MYCE_TOKEN", "test")
	if err := run([]string{"queue", "clock", "advance", "1s"}); err == nil || err.Error() != "queue accepts flags only" {
		t.Fatalf("production CLI: %v", err)
	}
	for _, duration := range []string{"0s", "-1s", "91d", "NaNd", "1e100d", "0.000000000000001d"} {
		if err := devClockCommand([]string{"advance", duration, "--queue", server.URL, "--token", "test"}, &out); err == nil {
			t.Fatalf("accepted %s", duration)
		}
	}
}
