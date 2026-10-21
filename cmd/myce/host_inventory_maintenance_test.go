package main

import (
	"bytes"
	"context"
	"encoding/json"
	"mycelium/internal/myce"
	"net/http/httptest"
	"testing"
)

func TestHostDisableStopWaitDegradedInventory(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	m := maintenanceCLIWorker("machine")
	m.Devices, m.MemoryError, m.InventoryReason = []myce.MachineDevice{}, "device_probe_unavailable", "device_probe_unavailable"
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
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
}
