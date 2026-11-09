package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStatusJSONPressureView(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	release := myce.RunningRelease{Version: "v1.2.3", Commit: "0123456789abcdef"}
	m := myce.Machine{CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 16 << 30}, ID: "worker", Session: "running-session", GPU: "fake", FreeMemoryGB: 8, Release: &release}
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	live := myce.API{Queue: q, Token: "test-only"}.Handler()
	snapshot := myce.Snapshot{Machines: []*myce.Machine{{ID: "host", HostAvailableMemoryGB: 3, HostMemoryInUseGB: 1, FreeHostMemoryGB: 2}}, Jobs: []*myce.Job{{ID: "12345678abcdef", State: "queued", DiagnosticCode: "host_memory_headroom"}}}

	exit := 1
	history := myce.Snapshot{Now: time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC), Jobs: []*myce.Job{
		{ID: "1234567890abcdef", State: "failed", Attempt: 2, Starts: 2, AttemptHistory: []myce.AttemptOutcome{
			{Attempt: 1, ExecutionCause: &myce.RuntimeCause{Operation: "launch", Class: "launch_refused", Predicate: "native_workdir_containment"}, Machine: "one", Session: "worker-session", State: "queued", Interrupted: true, OperatorStop: true, Reason: "requeued: stopped by operator"},
			{Attempt: 2, Machine: "one", Session: "worker-session", State: "failed", ExecutionCause: &myce.RuntimeCause{Operation: "command", Class: "command_exit", ExitCode: &exit}},
		}},
		{ID: "unrelated-job", State: "queued", Spec: myce.Spec{Name: "unrelated"}},
	}}

	releaseState := "pending"
	history.Jobs[0].Cancellation = &myce.Cancellation{WillNotRun: true, Release: &releaseState}
	history.Jobs[0].CancellationScope = &myce.CancellationScope{Pieces: []myce.CancellationPiece{{Key: "future", WillNotRun: true, Release: "not_applicable", Dependents: []string{"consumer"}}}, Members: []myce.CancellationMember{{JobID: "consumer", State: "stopping", WillNotRun: true, Release: "pending"}}}
	snapshot.Jobs = append(snapshot.Jobs, history.Jobs[0])
	snapshot.Jobs[0].State = "running"
	snapshot.Jobs[0].Attempt = 1
	snapshot.Jobs[0].Spec.AuthoringOrigin = myce.OriginAdapter
	snapshot.Jobs = append(snapshot.Jobs, &myce.Job{ID: "waiting-job", State: "queued", Reason: "example: hypha on battery; waiting for external power"})
	snapshot.FileStore = &myce.FileStoreStatus{Store: "queue-file-store", UsedBytes: 13, FreeBytes: 40, ReclaimableBytes: 9}
	snapshot.HealthAlertDelivery = &myce.HealthAlertDelivery{Pending: 3}
	snapshot.JobsPage = &myce.StatusJobsPage{Total: 3, Returned: 1, Mode: "recent", NextBefore: "cursor"}
	snapshot.QueueRelease = myce.CurrentRelease()
	release := myce.RunningRelease{Version: "v1.2.3", Commit: "0123456789abcdef"}
	snapshot.Machines[0].ID = "worker"
	snapshot.Machines[0].Release = &release
	snapshot.Machines[0].ExtensionReports = []myce.ExtensionReport{{Name: "sample", Version: "1", Work: "kind:sample", Qualified: true}}
	snapshot.Jobs[0].Spec.Logs = "none"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/live/") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/live")
			live.ServeHTTP(w, r)
			return
		}
		if r.Method != "GET" {
			t.Errorf("unexpected request: %s", r.Method)
		}
		switch r.URL.Path {
		case "/api/jobs/job/ledger":
			if r.URL.Query().Get("after") != "2" || r.URL.Query().Get("high_water") != "5" || r.URL.Query().Get("limit") != "1" {
				t.Errorf("unexpected request %s", r.URL)
			}
			if r.Header.Get("Authorization") != "Bearer synthetic" {
				t.Error("missing auth")
			}
			io.WriteString(w, `{"job_id":"job","complete":true,"high_water":5,"next":3,"has_more":true,"entries":[{"sequence":3}]}`)
			return
		case "/api/environments/tools":
			if r.Header.Get("Authorization") != "Bearer fixture" {
				t.Errorf("unexpected request: %s", r.Method)
			}
			json.NewEncoder(w).Encode(myce.EnvironmentEntry{Name: "tools", Platforms: []myce.EnvironmentPlatform{{Platform: "cpu", Backend: "uv", Key: "key", InputDigests: map[string]string{"requirements.txt": "digest"}}}})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/jobs/") {
			if r.URL.Path == "/api/jobs/history" {
				_ = json.NewEncoder(w).Encode(history.Jobs[0])
			} else {
				_ = json.NewEncoder(w).Encode(snapshot.Jobs[0])
			}
			return
		}
		_ = json.NewEncoder(w).Encode(snapshot)
	}))
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test")
	out := captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", server.URL, "--json"}) })
	var got myce.Snapshot
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatal(err, out)
	}
	if len(got.Machines) != 1 || got.Machines[0].HostAvailableMemoryGB != 3 || got.Machines[0].HostMemoryInUseGB != 1 || got.Jobs[0].DiagnosticCode != "host_memory_headroom" {
		t.Fatal(out)
	}
	out = captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", server.URL, "--json", "12345678"}) })
	var job myce.Job
	if err := json.Unmarshal([]byte(out), &job); err != nil || job.ID != snapshot.Jobs[0].ID {
		t.Fatal(err, out)
	}

	out = captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", server.URL, "--full"}) })
	if !strings.Contains(out, "sample@1 · kind of work sample · qualified") {
		t.Errorf("missing %q", "sample@1 · kind of work sample · qualified")
	}
	if !strings.Contains(out, "queue-file-store: used=13 bytes free=40 bytes reclaimable=9 bytes") {
		t.Fatal(out)
	}
	full := true
	if strings.Contains(out, "12345678abcdef") != full {
		t.Fatal(out)
	}
	if !strings.Contains(out, "running · attempt 1") || strings.Contains(out, "attempt 0") {
		t.Fatal(out)
	}
	if !strings.Contains(out, "authoring origin: adapter") {
		t.Fatal(out)
	}
	if !strings.Contains(out, "health notifier is not configured") || !strings.Contains(out, "3 retained alerts have no delivery acknowledgement") {
		t.Fatal(out)
	}
	if !strings.Contains(out, "1 of 3 retained jobs shown") || !strings.Contains(out, "status JOB_ID") || !strings.Contains(out, "status --history") {
		t.Fatalf("partial history was presented as exhaustive: %s", out)
	}
	for _, want := range []string{"hypha on battery; waiting for external power", "queue release: " + myce.CurrentRelease().Version + " (" + myce.CurrentRelease().Commit + ")", "worker worker release: v1.2.3 (0123456789abcdef)"} {
		if !strings.Contains(out, want) {
			t.Errorf("status missing %q: %s", want, out)
		}
	}
	out = captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", server.URL, "history"}) })
	for _, want := range []string{"attempt 1", "state=queued", "operator=true", "attempt 2", "command_exit (exit 1)", "native_workdir_containment", "workdir beneath {work}"} {
		if !strings.Contains(out, want) {
			t.Errorf("status missing %q: %s", want, out)
		}
	}
	if strings.Contains(out, "unrelated") {
		t.Fatal("status ignored job selector", out)
	}

	for _, want := range []string{"cancellation will_not_run=true release=pending", "piece future materialized=false accepted=false", "release=not_applicable dependents=consumer", "member consumer state=stopping will_not_run=true release=pending"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q: %s", want, out)
		}
	}

	{
		var out bytes.Buffer
		if err := environmentCommand(context.Background(), []string{"get", "tools", "--queue", server.URL, "--token", "fixture"}, &out); err != nil {
			t.Fatal(err)
		}
		if !json.Valid(out.Bytes()) || !bytes.Contains(out.Bytes(), []byte("digest")) {
			t.Fatal(out.String())
		}
	}
	{
		output, _, err := captureUsageOutput(t, []string{"ledger", "job", "--queue", server.URL, "--token", "synthetic", "--after", "2", "--high-water", "5", "--limit", "1"})
		if err != nil {
			t.Fatal(err)
		}
		var page struct {
			JobID   string `json:"job_id"`
			Next    int    `json:"next"`
			HasMore bool   `json:"has_more"`
		}
		if err = json.Unmarshal([]byte(output), &page); err != nil || page.JobID != "job" || page.Next != 3 || !page.HasMore {
			t.Fatal("bad ledger output", page, err)
		}
	}
	{
		out, _, err := captureUsageOutput(t, []string{"logs", "12345678", "--follow", "--queue", server.URL, "--token", "test"})
		if err != nil {
			t.Fatal(err)
		}
		command := "logs"
		if command == "logs" && !strings.Contains(out, "logs are disabled") {
			t.Fatal(out)
		}
	}

	{
		snapshot, err := status(context.Background(), myce.Client{URL: server.URL + "/live", Token: "test-only"})
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.QueueRelease != myce.CurrentRelease() || len(snapshot.Machines) != 1 || snapshot.Machines[0].Release == nil || *snapshot.Machines[0].Release != release {
			t.Fatalf("running releases missing from API: %+v", snapshot)
		}
		out, _, err := captureUsageOutput(t, []string{"status", "--queue", server.URL + "/live", "--token", "test-only"})
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"queue release: " + myce.CurrentRelease().Version + " (" + myce.CurrentRelease().Commit + ")", "worker worker release: v1.2.3 (0123456789abcdef)"} {
			if !strings.Contains(out, want) {
				t.Errorf("status missing %q: %s", want, out)
			}
		}
	}
}
