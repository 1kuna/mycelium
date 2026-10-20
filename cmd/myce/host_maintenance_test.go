package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func maintenanceCLIWorker(id string) myce.Machine {
	return myce.Machine{ID: id, HostID: "host", Device: id, Session: "session-" + id, GPU: "fake", FreeMemoryGB: 8, HostMemoryMeasured: true, HostAvailableMemoryGB: 16, CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 64, MemoryLayout: "separate", SystemMemoryBytes: 32 << 30}}
}
func TestHostMaintenanceCLIAndWaitCustody(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	a, b := maintenanceCLIWorker("gpu0"), maintenanceCLIWorker("gpu1")
	submitted := waitSubmit(t, q)
	reply, err := q.Checkin(myce.Checkin{Machine: a})
	if err != nil || reply.Job == nil {
		t.Fatal(reply, err)
	}
	execution := myce.Result{Machine: a.ID, Session: a.Session, Execution: myce.Execution{ID: submitted.ID, Attempt: reply.Job.Attempt}}
	if err = q.Started(execution); err != nil {
		t.Fatal(err)
	}
	if _, err = q.Checkin(myce.Checkin{Machine: b}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((myce.API{Queue: q, Token: "test-only"}).Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test-only")
	var out bytes.Buffer
	if err = hostMaintenanceCommand(context.Background(), []string{"drain", "host", "--queue", server.URL}, &out); err != nil {
		t.Fatal(err)
	}
	var status myce.HostMaintenanceStatus
	if err = json.Unmarshal(out.Bytes(), &status); err != nil || status.Policy.Mode != "drained" || len(status.Attempts) != 1 {
		t.Fatal(out.String(), err)
	}
	client := myce.Client{URL: server.URL, Token: "test-only"}
	status, err = waitHostMaintenance(context.Background(), client, "host", 6*time.Second, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if err == nil || !strings.Contains(err.Error(), "timed out") || len(status.Attempts) != 1 || status.Quiescent {
		t.Fatal(status, err)
	}
	current, err := q.HostMaintenanceStatus("host")
	if err != nil || len(current.Attempts) != 1 || len(current.PendingReleases) != 1 {
		t.Fatal("wait changed custody", current, err)
	}
	execution.Success = true
	if err = q.Complete(execution); err != nil {
		t.Fatal(err)
	}
	for _, m := range []myce.Machine{a, b} {
		if _, err = q.Checkin(myce.Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
	}
	status, err = waitHostMaintenance(context.Background(), client, "host", time.Second, clock, func(context.Context, time.Duration) error { t.Fatal("quiescent host waited"); return nil })
	if err != nil || !status.Quiescent {
		t.Fatal(status, err)
	}
	out.Reset()
	if err = hostMaintenanceCommand(context.Background(), []string{"budget", "--queue", server.URL, "host", "--memory-budget-gb", "4", "--cpu-budget-cores", "3"}, &out); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(out.Bytes(), &status); err != nil || status.Policy.MemoryBudget.Bytes != 4<<30 || status.Policy.CPUBudgetCores == nil || *status.Policy.CPUBudgetCores != 3 {
		t.Fatal(out.String(), err)
	}
	for _, args := range [][]string{{"status", "host", "--stop"}, {"resume", "host", "--wait"}, {"status", "host", "--memory-budget-gb", "4"}, {"disable", "host", "--timeout", "0s"}, {"budget", "host"}, {"status", "host", "--cpu-budget-cores", "3"}} {
		if err = hostMaintenanceCommand(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatal("accepted invalid args", args)
		}
	}
	out.Reset()
	if err = hostMaintenanceCommand(context.Background(), []string{"status", "unknown", "--queue", server.URL}, &out); err == nil || !strings.Contains(out.String(), `"healthy":false`) {
		t.Fatal("unknown worker health passed", out.String(), err)
	}
	server.Close()
	if _, err = waitHostMaintenance(context.Background(), client, "host", time.Second, clock, func(context.Context, time.Duration) error { t.Fatal("queue outage was ignored"); return nil }); err == nil {
		t.Fatal("queue outage passed wait")
	}
}
