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

func TestHostDeviceCLI(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	m := maintenanceCLIWorker("worker")
	m.Devices = []myce.MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-stable", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	if _, err := fixtureQueueCheckin(q, myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test-only")
	var out bytes.Buffer
	if err := hostMaintenanceCommand(context.Background(), []string{"device", "disable", "host", "0", "--reason", "recurring driver faults", "--queue", server.URL}, &out); err != nil {
		t.Fatal(err)
	}
	var status myce.HostMaintenanceStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil || len(status.DeviceMaintenance) != 1 || !status.DeviceMaintenance[0].Disabled || status.DeviceMaintenance[0].Identity != "fake-stable" || status.DeviceMaintenance[0].Reason != "recurring driver faults" || !status.DeviceMaintenance[0].UpdatedAt.Equal(clock.Now()) {
		t.Fatal("CLI did not submit resolved identity and reason", out.String(), err)
	}
	clock.advance(time.Second)
	printed, _, err := captureUsageOutput(t, []string{"status", "--queue", server.URL, "--token", "test-only"})
	if err != nil || !strings.Contains(printed, "fake-stable") || !strings.Contains(printed, "recurring driver faults") || !strings.Contains(printed, clock.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)) {
		t.Fatal("status hid policy identity/reason/time", printed, err)
	}
	if err := hostMaintenanceCommand(context.Background(), []string{"device", "enable", "host", "fake-stable", "--queue", server.URL}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	status, err = q.HostMaintenanceStatus("host")
	if err != nil || status.DeviceMaintenance[0].Disabled || status.DeviceMaintenance[0].Reason != "" || status.DeviceMaintenance[0].Revision != 2 {
		t.Fatal("enable did not clear policy with retained revision", status, err)
	}

	pending := waitSubmit(t, q)
	reply, err := fixtureQueueCheckin(q, myce.Checkin{Machine: m, AttemptProtocol: "keyed-attempt-v1"})
	if err != nil || len(reply.Attempts) != 1 || reply.Attempts[0].ID != pending.ID {
		t.Fatal("missing usage assignment", reply, err)
	}
	out.Reset()
	if err := hostMaintenanceCommand(context.Background(), []string{"status", "host", "--queue", server.URL}, &out); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out.Bytes(), &status); err != nil || len(status.DeviceUse) != 1 || status.DeviceUse[0].Identity != "fake-stable" || status.DeviceUse[0].Assignments != 1 || !status.DeviceUse[0].LastAssignedAt.Equal(clock.Now()) {
		t.Fatal("host status hid assignment history", out.String(), err)
	}
	for _, args := range [][]string{{"device"}, {"device", "disable", "host"}, {"device", "enable", "host", "0", "--reason", "wrong"}, {"device", "disable", "host", "0", "--stop"}} {
		if err := hostMaintenanceCommand(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatal("invalid device command accepted", args)
		}
	}
}
