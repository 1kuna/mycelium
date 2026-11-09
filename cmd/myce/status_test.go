package main

import (
	"context"
	"mycelium/internal/myce"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatusQueueAndWorkerRunningReleases(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	release := myce.RunningRelease{Version: "v1.2.3", Commit: "0123456789abcdef"}
	m := myce.Machine{CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 16 << 30}, ID: "worker", Session: "running-session", GPU: "fake", FreeMemoryGB: 8, Release: &release}
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	snapshot, err := status(context.Background(), myce.Client{URL: server.URL, Token: "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.QueueRelease != myce.CurrentRelease() || len(snapshot.Machines) != 1 || snapshot.Machines[0].Release == nil || *snapshot.Machines[0].Release != release {
		t.Fatalf("running releases missing from API: %+v", snapshot)
	}
	out, _, err := captureUsageOutput(t, []string{"status", "--queue", server.URL, "--token", "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"queue release: " + myce.CurrentRelease().Version + " (" + myce.CurrentRelease().Commit + ")", "worker worker release: v1.2.3 (0123456789abcdef)"} {
		if !strings.Contains(out, want) {
			t.Errorf("status missing %q: %s", want, out)
		}
	}
}
