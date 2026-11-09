package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readyEnrollmentMachine() Machine {
	return Machine{ID: "new-hypha", HostID: "new-hypha", Session: "exact-session", GPU: "fake", Devices: []MachineDevice{{ID: "0", Identity: "test-card", Platform: "fake", BudgetGB: 8, TotalMemoryGB: 8}}, HostMemoryMeasured: true, HostAvailableMemoryGB: 16, CapabilityProfile: &CapabilityProfile{OS: "darwin", Arch: "arm64", LogicalCPUs: 8, SystemMemoryBytes: 32 << 30}, Enrollment: &EnrollmentProof{Version: BuildVersion, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, RunnerSmoke: true}}
}

func TestEnrollmentRunnerSmokeStripsTokenAndRetainsNoCustody(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "private-smoke-test-token")
	dir := t.TempDir()
	if err := enrollmentRunnerSmoke(context.Background(), dir, RealClock{}); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "enrollment-command-*.json"))
	if err != nil || len(files) != 0 {
		t.Fatal("smoke custody retained", files, err)
	}
}

func TestEnrollmentWaitRequiresStableManagerAndSupportsPoliteWaiting(t *testing.T) {
	for _, mode := range []string{"ready", "polite_waiting", "start-loop", "previous-process"} {
		t.Run(mode, func(t *testing.T) {
			m := readyEnrollmentMachine()
			if mode == "polite_waiting" {
				m.CapabilityProfile.Availability = &Availability{Mode: "daily", Reason: "waiting for owner inactivity"}
			}
			dir := t.TempDir()
			if err := saveEnrollmentReceipt(dir, m); err != nil {
				t.Fatal(err)
			}
			clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
			budget := 0
			observations := 0
			result, err := WaitEnrollment(context.Background(), ServiceConfig{State: dir, Detection: Detection{OS: "linux"}}, Client{}, EnrollmentWait{
				Clock: clock, Timeout: 36 * time.Second,
				Wait: func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil },
				Observe: func(context.Context) (EnrollmentObservation, error) {
					return EnrollmentObservation{Machine: m, Fresh: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, Policy: HostMaintenance{Mode: "enabled", CPUBudgetCores: &budget, MemoryBudget: &HostMemoryBudget{}}}, nil
				},
				Command: func(context.Context, string, ...string) ([]byte, error) {
					observations++
					pid, restarts := os.Getpid(), 0
					if mode == "previous-process" {
						pid++
					}
					if mode == "start-loop" {
						pid += observations
						restarts = observations
					}
					return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=%d\nResult=success\n", pid, restarts)), nil
				},
			})
			if mode == "start-loop" || mode == "previous-process" {
				if err == nil || result.State == "ready" {
					t.Fatal("restarting or different manager process accepted", result.State, err)
				}
			} else if err != nil || result.State != mode || observations < 31 {
				t.Fatal("stable enrollment did not finish correctly", result.State, observations, err)
			}
		})
	}
}
