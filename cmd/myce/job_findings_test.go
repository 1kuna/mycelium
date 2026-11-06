package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func testJobFinding() myce.JobFinding {
	return myce.JobFinding{Code: "memory_over_declaration", Classification: "behaviour", Attempt: 1, Message: "Measured RSS sum reached 64 GiB above a 2 GiB declaration.", Cause: "unknown", Confidence: "measured", Evidence: []myce.FindingEvidence{{Metric: "host_memory", Source: "process-group RSS sum"}}, Action: "retained observation", NextStep: "inspect child-process concurrency"}
}

func TestJobFindingsCLIWaitStatusResultAndCallback(t *testing.T) {
	const id = "1234567890abcdef"
	f := testJobFinding()
	body := []byte("verified\n")
	hash := sha256.Sum256(body)
	j := myce.Job{ID: id, State: "succeeded", Findings: []myce.JobFinding{f}, OutputRefs: myce.FileManifest{"result.txt": {SHA256: hex.EncodeToString(hash[:]), SizeBytes: int64(len(body))}}}
	batch := false
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/jobs/"+id:
			reads++
			w.Header().Set("X-Myce-Wait", "batch-v1")
			if batch && reads == 1 {
				copy := j
				copy.State = "running"
				copy.Findings = nil
				json.NewEncoder(w).Encode(copy)
			} else {
				json.NewEncoder(w).Encode(j)
			}
		case r.URL.Path == "/api/wait":
			json.NewEncoder(w).Encode(myce.WaitReply{Jobs: []myce.WaitState{{ID: id, State: "succeeded", Revision: 1}}})
		case strings.HasPrefix(r.URL.Path, "/api/files/blobs/"):
			w.Header().Set("ETag", `"`+j.OutputRefs["result.txt"].SHA256+`"`)
			w.Write(body)
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client := myce.Client{URL: server.URL, Token: "fixture"}
	clock := &waitClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	for _, useBatch := range []bool{false, true} {
		batch, reads = useBatch, 0
		var out, stderr bytes.Buffer
		if err := waitJobs(context.Background(), client, []string{id}, time.Hour, &out, &stderr, clock, func(context.Context, time.Duration) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), id+" succeeded") || strings.Count(stderr.String(), "finding attempt 1") != 1 || !strings.Contains(stderr.String(), "Cause: unknown") {
			t.Fatalf("%s / %s", out.String(), stderr.String())
		}
		want := 1
		if useBatch {
			want = 2
		}
		if reads != want {
			t.Fatalf("full receipt reads %d want %d", reads, want)
		}
	}
	batch = false
	t.Setenv("MYCE_TOKEN", "fixture")
	status := captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", server.URL, id}) })
	if !strings.Contains(status, "finding attempt 1") {
		t.Fatal(status)
	}
	statusJSON := captureJoiningOutput(t, func() error { return run([]string{"status", "--json", "--queue", server.URL, id}) })
	var decoded myce.Job
	if err := json.Unmarshal([]byte(statusJSON), &decoded); err != nil || !reflect.DeepEqual(decoded.Findings, j.Findings) {
		t.Fatal(statusJSON, err)
	}
	var warning bytes.Buffer
	dest := t.TempDir()
	if err := resultCommand(context.Background(), []string{id, "--queue", server.URL, "--out", dest}, &warning); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(warning.String(), "succeeded with findings") || !strings.Contains(warning.String(), f.NextStep) {
		t.Fatal(warning.String())
	}
	if got, err := os.ReadFile(filepath.Join(dest, "result.txt")); err != nil || !bytes.Equal(got, body) {
		t.Fatal(string(got), err)
	}
	dir := t.TempDir()
	payload := filepath.Join(dir, "callback.json")
	hook := filepath.Join(dir, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat > "+shellArg(payload)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{JobID: id, Project: "fixture", Key: "key", Nonce: "nonce"}
	if err := finishProfileObservation(context.Background(), dir, receipt, profileObserver{Notify: hook}, &j, nil); err != nil {
		t.Fatal(err)
	}
	var notification profileNotification
	if err := readProfileJSON(payload, &notification); err != nil || !reflect.DeepEqual(notification.Findings, j.Findings) {
		t.Fatal(notification, err)
	}
	// Warning output remains empty for a healthy successful job.
	j.Findings = nil
	warning.Reset()
	if err := resultCommand(context.Background(), []string{id, "--queue", server.URL, "--out", t.TempDir()}, &warning); err != nil || warning.Len() != 0 {
		t.Fatal(warning.String(), err)
	}
}
