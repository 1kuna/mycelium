package main

import (
	"bytes"
	"context"
	"encoding/json"
	"mycelium/internal/myce"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func maintenanceCLIWorker(id string) myce.Machine {
	return myce.Machine{Capabilities: []string{"native_isolation_landlock"}, ID: id, HostID: "host", Device: id, Session: "session-" + id, GPU: "fake", FreeMemoryGB: 8, HostMemoryMeasured: true, HostAvailableMemoryGB: 16, CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 64, MemoryLayout: "separate", SystemMemoryBytes: 32 << 30}}
}

func TestHostMaintenanceCLIAndWaitCustody(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	a, b := maintenanceCLIWorker("gpu0"), maintenanceCLIWorker("gpu1")
	submitted := waitSubmit(t, q)
	reply, err := legacyCLICheckin(q, clock, myce.Checkin{Machine: a})
	if err != nil || reply.Job == nil {
		t.Fatal(reply, err)
	}
	execution := myce.Result{Machine: a.ID, Session: a.Session, Execution: myce.Execution{ID: submitted.ID, Attempt: reply.Job.Attempt}}
	if err = q.Started(execution); err != nil {
		t.Fatal(err)
	}
	if _, err = legacyCLICheckin(q, clock, myce.Checkin{Machine: b}); err != nil {
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
		if _, err = legacyCLICheckin(q, clock, myce.Checkin{Machine: m}); err != nil {
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

	out.Reset()
	if err = hostMaintenanceCommand(context.Background(), []string{"status", "unknown", "--queue", server.URL}, &out); err == nil || !strings.Contains(out.String(), `"healthy":false`) {
		t.Fatal("unknown worker health passed", out.String(), err)
	}
	if err := hostMaintenanceCommand(context.Background(), []string{"resume", "host", "--queue", server.URL}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{true} {
		var out bytes.Buffer
		flag := "--last-resort=false"
		if enabled {
			flag = "--last-resort=true"
		}
		if err := hostMaintenanceCommand(context.Background(), []string{"placement", "host", flag, "--queue", server.URL}, &out); err != nil {
			t.Fatal(err)
		}
		var status myce.HostMaintenanceStatus
		if err := json.Unmarshal(out.Bytes(), &status); err != nil || status.Policy.LastResort != enabled || status.Policy.Mode != "enabled" {
			t.Fatal("placement did not edit enabled host", out.String(), err)
		}
		if err := hostMaintenanceCommand(context.Background(), []string{"status", "host", "--queue", server.URL}, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		clock.advance(time.Second) // Global status may lag by one cache lifetime.
		output, _, err := captureUsageOutput(t, []string{"status", "--queue", server.URL, "--token", "test-only"})
		if err != nil || strings.Contains(output, "last-resort placement") != enabled {
			t.Fatal("CLI status hid placement policy", output, err)
		}
	}
	{
		m := maintenanceCLIWorker("machine")
		m.Devices, m.MemoryError, m.InventoryReason = []myce.MachineDevice{}, "device_probe_unavailable", "device_probe_unavailable"
		if _, err := legacyCLICheckin(q, clock, myce.Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := hostMaintenanceCommand(context.Background(), []string{"disable", "host", "--stop", "--wait", "--timeout", "100ms", "--queue", server.URL}, &out); err != nil {
			t.Fatalf("zero-custody disable wait failed: %v %s", err, out.String())
		}
		var status myce.HostMaintenanceStatus
		if err := json.Unmarshal(out.Bytes(), &status); err != nil || !status.Quiescent || status.Healthy || status.Policy.Mode != "disabled" || status.Policy.StopAt == nil {
			t.Fatalf("unexpected status: %+v %v", status, err)
		}
	}
	server.Close()
	if _, err = waitHostMaintenance(context.Background(), client, "host", time.Second, clock, func(context.Context, time.Duration) error { t.Fatal("queue outage was ignored"); return nil }); err == nil {
		t.Fatal("queue outage passed wait")
	}
}

// #215: an enrolled operator machine manages hosts through installed, pinned
// authority with no MYCE_TOKEN, like status and cancel (#207).
