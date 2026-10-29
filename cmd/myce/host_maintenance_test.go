package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func maintenanceCLIWorker(id string) myce.Machine {
	return myce.Machine{Capabilities: []string{"native_isolation_landlock"}, ID: id, HostID: "host", Device: id, Session: "session-" + id, GPU: "fake", FreeMemoryGB: 8, HostMemoryMeasured: true, HostAvailableMemoryGB: 16, CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 64, MemoryLayout: "separate", SystemMemoryBytes: 32 << 30}}
}

func TestHostMaintenancePlacementCLI(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	if _, err := q.Checkin(myce.Checkin{Machine: maintenanceCLIWorker("worker")}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((myce.API{Queue: q, Token: "test-only"}).Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test-only")
	for _, enabled := range []bool{true, false} {
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
	for _, args := range [][]string{{"placement", "host"}, {"placement", "host", "--last-resort", "--wait"}, {"status", "host", "--last-resort"}, {"resume", "host", "--last-resort=false"}} {
		if err := hostMaintenanceCommand(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatal("accepted invalid placement arguments", args)
		}
	}
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

// #215: an enrolled operator machine manages hosts through installed, pinned
// authority with no MYCE_TOKEN, like status and cancel (#207).
func TestHostMaintenanceInstalledAuthority(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	m := maintenanceCLIWorker("worker")
	m.Devices = []myce.MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-stable", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	const token = "host-fixture-token"
	server := httptest.NewTLSServer(myce.API{Queue: q, Token: token}.Handler())
	defer server.Close()
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	joining := filepath.Join(home, "joining.json")
	hash := sha256.Sum256(server.Certificate().Raw)
	invitation := myce.Invitation{Version: 1, Token: token, Address: server.URL, Fingerprint: hex.EncodeToString(hash[:])}
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(myce.ServiceConfig{Role: "worker", JoiningFile: joining})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		ok   func(myce.HostMaintenanceStatus) bool
	}{
		{[]string{"device", "disable", "host", "0", "--reason", "driver faults"}, func(s myce.HostMaintenanceStatus) bool {
			return len(s.DeviceMaintenance) == 1 && s.DeviceMaintenance[0].Disabled
		}},
		{[]string{"device", "enable", "host", "fake-stable"}, func(s myce.HostMaintenanceStatus) bool {
			return len(s.DeviceMaintenance) == 1 && !s.DeviceMaintenance[0].Disabled
		}},
		{[]string{"drain", "host"}, func(s myce.HostMaintenanceStatus) bool { return s.Policy.Mode == "drained" }},
		{[]string{"budget", "host", "--memory-budget-gb", "4"}, func(s myce.HostMaintenanceStatus) bool {
			return s.Policy.MemoryBudget != nil && s.Policy.MemoryBudget.Bytes == 4<<30
		}},
		{[]string{"placement", "host", "--last-resort=true"}, func(s myce.HostMaintenanceStatus) bool { return s.Policy.LastResort }},
		{[]string{"resume", "host"}, func(s myce.HostMaintenanceStatus) bool { return s.Policy.Mode == "enabled" }},
	} {
		var out bytes.Buffer
		var status myce.HostMaintenanceStatus
		if err = hostMaintenanceCommand(context.Background(), tc.args, &out); err != nil || json.Unmarshal(out.Bytes(), &status) != nil || !tc.ok(status) {
			t.Fatalf("host %v: %s %v", tc.args, out.String(), err)
		}
		if strings.Contains(out.String(), token) {
			t.Fatal("host maintenance exposed authority")
		}
	}
	// A wrong installed pin must refuse before the policy changes.
	invitation.Fingerprint = strings.Repeat("0", 64)
	if err = myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	if err = hostMaintenanceCommand(context.Background(), []string{"device", "disable", "host", "0", "--config", config}, &bytes.Buffer{}); err == nil {
		t.Fatal("host maintenance accepted a mismatched certificate pin")
	}
	if current, err := q.HostMaintenanceStatus("host"); err != nil || current.Policy.Mode != "enabled" {
		t.Fatalf("pin refusal changed policy: %+v %v", current.Policy, err)
	}
}
