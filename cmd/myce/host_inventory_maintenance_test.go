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

func TestHostDisableStopWaitDegradedInventory(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	m := maintenanceCLIWorker("machine")
	m.Devices, m.MemoryError, m.InventoryReason = []myce.MachineDevice{}, "device_probe_unavailable", "device_probe_unavailable"
	if _, err := currentCLICheckin(q, clock, myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((myce.API{Queue: q, Token: "test-only"}).Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test-only")
	var out bytes.Buffer
	if err := hostMaintenanceCommand(context.Background(), []string{"disable", "host", "--stop", "--wait", "--timeout", "100ms", "--queue", server.URL}, &out); err != nil {
		t.Fatalf("zero-custody disable wait failed: %v %s", err, out.String())
	}
	var status myce.HostMaintenanceStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil || !status.Quiescent || status.Healthy || status.Policy.Mode != "disabled" || status.Policy.StopAt == nil {
		t.Fatalf("unexpected status: %+v %v", status, err)
	}
	out.Reset()
	err := hostMaintenanceCommand(context.Background(), []string{"status", "host", "--queue", server.URL}, &out)
	status = myce.HostMaintenanceStatus{}
	if err == nil || exitCode(err) != 1 || json.Unmarshal(out.Bytes(), &status) != nil || status.Healthy || status.Host != "host" {
		t.Fatal("unhealthy host status must retain stdout JSON and exit 1", out.String(), err)
	}
}

// #1187: a fresh worker with one quarantined device once printed
// "missing, stale or inconsistent worker evidence" and exit 1, so automation
// refused a supported clear. Degraded now names the device, reason and clear
// with exit 3; genuinely stale evidence keeps the refusal and exit 1.
func TestHostStatusQuarantinedDeviceIsDegradedNotStale(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	m := maintenanceCLIWorker("machine")
	m.Devices = []myce.MachineDevice{
		{ID: "0", Platform: "fake", Identity: "fake-a", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, Reason: "device_fault_xe"},
		{ID: "1", Platform: "fake", Identity: "fake-b", Selector: "1", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8},
	}
	if _, err := currentCLICheckin(q, clock, myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((myce.API{Queue: q, Token: "test-only"}).Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test-only")
	var out bytes.Buffer
	var status myce.HostMaintenanceStatus
	err := hostMaintenanceCommand(context.Background(), []string{"status", "host", "--queue", server.URL}, &out)
	if json.Unmarshal(out.Bytes(), &status) != nil || status.State != "degraded" || len(status.Workers) != 1 || !status.Workers[0].Fresh {
		t.Fatalf("fixture is not a fresh degraded worker: %s %v", out.String(), err)
	}
	if err == nil || exitCode(err) != hostDegradedExit || strings.Contains(err.Error(), "stale") {
		t.Fatalf("degraded host reported as missing/stale evidence: exit %d %v", exitCode(err), err)
	}
	for _, want := range []string{"machine fresh", "device 0", "device_fault_xe", "myce device clear-quarantine --state-dir DIR --device 0"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("degraded message lacks %q: %v", want, err)
		}
	}
	clock.advance(time.Hour)
	out.Reset()
	err = hostMaintenanceCommand(context.Background(), []string{"status", "host", "--queue", server.URL}, &out)
	if err == nil || exitCode(err) != 1 || !strings.Contains(err.Error(), "missing, stale or inconsistent worker evidence") {
		t.Fatalf("stale worker must keep the evidence refusal: exit %d %v %s", exitCode(err), err, out.String())
	}
}
