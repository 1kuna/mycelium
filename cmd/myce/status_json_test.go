package main

import (
	"encoding/json"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusJSONPressureView(t *testing.T) {
	snapshot := myce.Snapshot{Machines: []*myce.Machine{{ID: "host", HostAvailableMemoryGB: 3, HostMemoryInUseGB: 1, FreeHostMemoryGB: 2}}, Jobs: []*myce.Job{{ID: "1234567890abcdef", State: "queued", DiagnosticCode: "host_memory_headroom"}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(snapshot) }))
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
}
