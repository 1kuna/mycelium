package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// The review's fast HTTP polling reproduction, with drain and readiness time
// advanced by a fake clock. The setup context expires at 15 fake seconds.
func TestEnrollmentDrainHasSeparateInstallBudget(t *testing.T) {
	previousVersion := myce.BuildVersion
	myce.BuildVersion = "v1.0.1"
	t.Cleanup(func() { myce.BuildVersion = previousVersion })
	clock := &waitClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	started := clock.Now()
	type phaseDeadline struct {
		at     time.Time
		cancel context.CancelFunc
	}
	var deadlines []phaseDeadline
	setupTimeout := func(parent context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		deadlines = append(deadlines, phaseDeadline{clock.Now().Add(15 * time.Second), cancel})
		return ctx, cancel
	}
	wait := func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		clock.advance(d)
		for _, deadline := range deadlines {
			if !clock.Now().Before(deadline.at) {
				deadline.cancel()
			}
		}
		return nil
	}
	home := t.TempDir()
	cpuBudget := 0
	machine := myce.Machine{ID: "hypha", HostID: "hypha", Session: "session", HostMemoryMeasured: true, HostAvailableMemoryGB: 16,
		CapabilityProfile: &myce.CapabilityProfile{OS: "linux", Arch: "amd64", LogicalCPUs: 8, SystemMemoryBytes: 32 << 30},
		Enrollment:        &myce.EnrollmentProof{Version: myce.BuildVersion, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol, RunnerSmoke: true}}
	status := myce.HostMaintenanceStatus{Host: "hypha", Healthy: true, Policy: myce.HostMaintenance{Mode: "enabled", UpdatedAt: started, CPUBudgetCores: &cpuBudget, MemoryBudget: &myce.HostMemoryBudget{}}, Workers: []myce.MaintenanceWorker{{Machine: machine, Fresh: true}}}
	var mu sync.Mutex
	var drainedAt time.Time
	var drainedFor time.Duration
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(r.URL.Path, "/enrollment") {
			_ = json.NewEncoder(w).Encode(myce.EnrollmentObservation{Fresh: true, Machine: machine, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol, Policy: status.Policy})
			return
		}
		if r.Method == "POST" {
			var req myce.HostMaintenanceRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			actions = append(actions, req.Action)
			switch req.Action {
			case "drain":
				status.Policy.Mode, status.Policy.DrainOwner = "drained", req.DrainOwner
				drainedAt = clock.Now()
			case "resume":
				status.Policy.Mode, status.Policy.DrainOwner = "enabled", ""
			}
			status.Policy.UpdatedAt = clock.Now()
		}
		status.Quiescent = status.Policy.Mode == "drained" && clock.Now().Sub(drainedAt) >= 16*time.Second
		if status.Quiescent && drainedFor == 0 {
			drainedFor = clock.Now().Sub(drainedAt)
		}
		_ = json.NewEncoder(w).Encode(status)
	}))
	t.Cleanup(server.Close)
	c, err := myce.PlanInstall("worker", home, "", home, server.URL, "hypha", myce.Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 8})
	if err != nil {
		t.Fatal(err)
	}
	// The old non-enrollment worker has no prior receipt. The injected manager
	// publishes an exact replacement receipt when it activates the new install.
	c.EnvironmentFile, c.JoiningFile, c.HostID, c.AllowDeviceless = "", filepath.Join(home, "joining.json"), "hypha", true
	roots, err := c.StateRoots()
	if err != nil {
		t.Fatal(err)
	}
	if err = myce.PrepareStateRoots(roots...); err != nil {
		t.Fatal(err)
	}
	if err = myce.WriteInvitation(c.JoiningFile, myce.Invitation{Version: 1, Address: server.URL, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(server.Certificate().Raw)), Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(c.Binary, []byte("#!/bin/sh\necho '{\"version\":\"v1.0.0\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	var installedAt time.Time
	probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if name != "systemctl" {
			return nil, fmt.Errorf("unexpected command %s", name)
		}
		text := strings.Join(args, " ")
		switch {
		case strings.Contains(text, "--property=UnitFileState"):
			return []byte("enabled"), nil
		case strings.Contains(text, "--property=ActiveState,"):
			return []byte("ActiveState=active\nSubState=running\nMainPID=123\nNRestarts=0\nResult=success\n"), nil
		case strings.Contains(text, " enable --now "):
			installedAt = clock.Now()
			receipt := myce.EnrollmentReceipt{ProcessID: 123, Session: machine.Session, Version: myce.BuildVersion, LogicalCPUs: 8, SystemMemoryBytes: 32 << 30}
			data, err := json.Marshal(receipt)
			if err != nil {
				return nil, err
			}
			return nil, os.WriteFile(filepath.Join(c.State, "enrollment.json"), data, 0600)
		default:
			return nil, nil
		}
	}}
	args := []string{"install", "worker", "--enroll", "--config", c.Config, "--allow-downgrade"}
	if err := enrollmentCommandWithTiming(args, probe, home, nil, clock, wait, setupTimeout); err != nil {
		t.Fatalf("upgrade after a 16-second drain must succeed: %v", err)
	}
	if installedAt.Sub(started) != 16*time.Second || len(deadlines) != 3 {
		t.Fatalf("installation did not start with its own budget after drain: at=%s phases=%d", installedAt.Sub(started), len(deadlines))
	}
	mu.Lock()
	defer mu.Unlock()
	if drainedFor != 16*time.Second || strings.Join(actions, ",") != "drain,resume" || status.Policy.Mode != "enabled" {
		t.Fatalf("drain/resume incomplete: waited=%s actions=%v mode=%s", drainedFor, actions, status.Policy.Mode)
	}
	if _, err := os.Stat(filepath.Join(c.State, "enrollment-resume.json")); !os.IsNotExist(err) {
		t.Fatal("completed upgrade retained resume authority", err)
	}
}
