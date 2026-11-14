package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

type stagingTimeoutReader struct{}

func (stagingTimeoutReader) Read([]byte) (int, error) {
	return 0, fmt.Errorf("private-path private-credential: %w", errFileTransferStalled)
}

// #246: the worker can acknowledge and discard a prelaunch failure while the
// queue must retain the safe, exact-attempt diagnosis across reopen.
func TestInputStagingOutageSurvivesAcknowledgementAndReopen(t *testing.T) {
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	j := harnessJob(t, ID(), "", "touch {work}/launched")
	j.Spec.Files = FileManifest{"input": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	h.jobs = []Job{j}
	w := harnessWorker(t, h)
	marker := filepath.Join(w.StateDir, "job-files", j.ID, "1", "launched")
	var gets atomic.Int32
	w.Client.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasPrefix(r.URL.Path, "/api/files/blobs/") {
			if r.Method == "HEAD" {
				return &http.Response{StatusCode: 200, ContentLength: 1, Header: http.Header{"Etag": {`"` + strings.Repeat("a", 64) + `"`}}, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			if gets.Add(1) > 1 {
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
			}
			return &http.Response{StatusCode: 200, ContentLength: 1, Header: http.Header{"Etag": {`"` + strings.Repeat("a", 64) + `"`}}, Body: io.NopCloser(stagingTimeoutReader{})}, nil
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	startHarness(t, w, h)
	await(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, r := range h.results {
			if r.ID == j.ID && !r.ReleaseOnly {
				return true
			}
		}
		return false
	})
	await(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.reports) > 0 && len(h.reports[len(h.reports)-1].Attempts) == 0
	})
	var r Result
	h.mu.Lock()
	for _, value := range h.results {
		if value.ID == j.ID && !value.ReleaseOnly {
			r = value
		}
	}
	h.mu.Unlock()
	if r.ExecutionCause == nil || r.ExecutionCause.Operation != "input_staging" || r.ExecutionCause.Staging == nil || r.ExecutionCause.Staging.Error != "storage_unreachable" || r.ExecutionCause.ExitCode != nil || r.Success || r.ReleasePending || gets.Load() != 2 {
		t.Fatalf("lost prelaunch diagnosis: %+v", r)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("payload launched", err)
	}
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(path, root, w.Clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	fakeCheckpointAvailability(t, q, true)
	j.Machine, j.Session, j.State = r.Machine, r.Session, "running"
	q.s.Jobs[j.ID] = &j
	m := machine(r.Machine, "fake")
	m.Session = r.Session
	check(t, q, m, &j)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, w.Clock)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	// #1045: the idle stall resumes within the attempt; a subsequent completed
	// dial failure still ends it as infrastructure and retains its exact cause.
	if got.Attempt != j.Attempt || got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != 1 || !strings.Contains(got.AttemptHistory[0].Reason, "operation=cache_download error=storage_unreachable") || got.StartedAt != nil || got.ExecutionCause == nil || got.ExecutionCause.Operation != "input_staging" || got.ExecutionCause.ExitCode != nil || got.ExecutionCause.Staging == nil {
		t.Fatalf("GET lost exact prelaunch diagnosis: %+v", got)
	}
	data, _ := json.Marshal(got)
	if strings.Contains(string(data), "private-") {
		t.Fatal("GET exposed upstream text", string(data))
	}
	stale := r
	stale.Attempt++
	stale.ExecutionCause = &RuntimeCause{Operation: "command", Class: "io_error"}
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatal("stale diagnosis accepted", err)
	}
}
