package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// wait keeps its "ID state" stdout contract and puts the finding on stderr;
// result refuses with the same finding instead of a bare "no accepted result".
func TestTerminalFindingInWaitAndResult(t *testing.T) {
	finding := &myce.JobFinding{Code: "output_not_regular", Owner: "job", What: `Your command exited 0, but "result" broke the output contract.`, Action: `write "result" as one regular file.`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/wait" {
			json.NewEncoder(w).Encode(myce.WaitReply{Jobs: []myce.WaitState{{ID: "job", State: "failed", Revision: 1, Spec: &myce.WaitSpec{}, Finding: finding}}})
			return
		}
		json.NewEncoder(w).Encode(myce.Job{ID: "job", State: "failed", Finding: finding})
	}))
	defer server.Close()
	clock := &waitClock{now: time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)}
	var out, progress bytes.Buffer
	err := waitJobsReporting(context.Background(), myce.Client{URL: server.URL, Token: "fixture"}, nil, []string{"job"}, 0, &out, &progress, clock, func(context.Context, time.Duration) error { return nil }, false)
	if err == nil || out.String() != "job failed\n" || progress.String() != "job "+finding.Text()+"\n" {
		t.Fatalf("wait: %v %q %q", err, out.String(), progress.String())
	}
	var resultErr *ResultError
	err = resultCommand(context.Background(), []string{"job", "--queue", server.URL, "--token", "fixture", "--out", filepath.Join(t.TempDir(), "out")}, io.Discard)
	if !errors.As(err, &resultErr) || resultErr.Code != "result_not_accepted" || !strings.Contains(err.Error(), finding.Text()) {
		t.Fatalf("result: %v", err)
	}
}
