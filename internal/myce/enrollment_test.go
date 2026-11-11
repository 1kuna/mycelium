package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func readyEnrollmentMachine() Machine {
	return Machine{ID: "new-hypha", HostID: "new-hypha", Session: "exact-session", GPU: "fake", Devices: []MachineDevice{{ID: "0", Identity: "test-card", Platform: "fake", BudgetGB: 8, TotalMemoryGB: 8}}, HostMemoryMeasured: true, HostAvailableMemoryGB: 16, CapabilityProfile: &CapabilityProfile{OS: "darwin", Arch: "arm64", LogicalCPUs: 8, SystemMemoryBytes: 32 << 30}, Enrollment: &EnrollmentProof{Version: BuildVersion, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, RunnerSmoke: true}}
}
func TestEnrollmentReadinessContract(t *testing.T) {
	m := readyEnrollmentMachine()
	r := EnrollmentReceipt{Session: m.Session, Version: BuildVersion, Devices: m.Devices}
	r.LogicalCPUs, r.SystemMemoryBytes = m.CapabilityProfile.LogicalCPUs, m.CapabilityProfile.SystemMemoryBytes
	budget := 0
	o := EnrollmentObservation{Machine: m, Fresh: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, Policy: HostMaintenance{Mode: "enabled", CPUBudgetCores: &budget, MemoryBudget: &HostMemoryBudget{}}}
	state, err := EvaluateEnrollment(r, o)
	if err != nil || state != "ready" {
		t.Fatal(state, err)
	}
	oldReceipt := r
	oldReceipt.Version = "previous-release"
	oldObservation := o
	oldProof := *m.Enrollment
	oldProof.Version = oldReceipt.Version
	oldObservation.Machine.Enrollment = &oldProof
	if state, err := EvaluateEnrollment(oldReceipt, oldObservation); err == nil || state == "ready" {
		t.Fatal("old release receipt certified replacement service", state, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*EnrollmentObservation)
	}{
		{"wrong-session", func(o *EnrollmentObservation) { o.Machine.Session = "old" }},
		{"protocol", func(o *EnrollmentObservation) { o.AttemptProtocol = "old" }},
		{"stale", func(o *EnrollmentObservation) { o.Fresh = false }},
		{"recovering", func(o *EnrollmentObservation) { o.Machine.Recovering = true }},
		{"missing-device", func(o *EnrollmentObservation) { o.Machine.Devices = nil }},
		{"unavailable-device", func(o *EnrollmentObservation) {
			o.Machine.Devices = append([]MachineDevice{}, m.Devices...)
			o.Machine.Devices[0].Reason = "device_probe_unavailable"
		}},
		{"cpu-missing", func(o *EnrollmentObservation) { o.Machine.CapabilityProfile = nil }},
		{"ram-missing", func(o *EnrollmentObservation) { o.Machine.HostMemoryMeasured = false }},
		{"wrong-cpu-count", func(o *EnrollmentObservation) {
			p := *m.CapabilityProfile
			p.LogicalCPUs--
			o.Machine.CapabilityProfile = &p
		}},
		{"wrong-ram-total", func(o *EnrollmentObservation) {
			p := *m.CapabilityProfile
			p.SystemMemoryBytes--
			o.Machine.CapabilityProfile = &p
		}},
		{"cpu-policy-unacknowledged", func(o *EnrollmentObservation) {
			budget := 2
			o.Policy.CPUBudgetCores = &budget
		}},
		{"no-smoke", func(o *EnrollmentObservation) {
			p := *o.Machine.Enrollment
			p.RunnerSmoke = false
			o.Machine.Enrollment = &p
		}},
		{"ram-policy-unacknowledged", func(o *EnrollmentObservation) { o.Machine.HostMemoryBudgetGB = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := o
			tc.change(&bad)
			if state, err := EvaluateEnrollment(r, bad); err == nil || state == "ready" {
				t.Fatal("false ready", state, err)
			}
		})
	}
	o.Policy.Mode = "disabled"
	if state, err = EvaluateEnrollment(r, o); err != nil || state != "disabled" {
		t.Fatal(state, err)
	}
	o.Policy.Mode = "enabled"
	p := *m.CapabilityProfile
	p.Availability = &Availability{Mode: "daily", Reason: "waiting for 5 idle minutes"}
	o.Machine.CapabilityProfile = &p
	if state, err = EvaluateEnrollment(r, o); err != nil || state != "polite_waiting" {
		t.Fatal(state, err)
	}
}
func TestEnrollmentServiceStartLoopRefused(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}, Service: "/ignored/myce-worker.service"}
	for _, output := range []string{"ActiveState=activating\nSubState=auto-restart\nMainPID=0\nNRestarts=5\nResult=exit-code\n", "ActiveState=active\nSubState=running\nMainPID=123\n"} {
		if _, err := EnrollmentServiceHealth(context.Background(), c, func(context.Context, string, ...string) ([]byte, error) { return []byte(output), nil }); err == nil {
			t.Fatal("unhealthy manager accepted")
		}
	}
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
func TestEnrollmentWaitUnreachableAnd426(t *testing.T) {
	for _, status := range []int{0, 426} {
		t.Run(string(rune('a'+status)), func(t *testing.T) {
			dir := t.TempDir()
			m := readyEnrollmentMachine()
			r := EnrollmentReceipt{Session: m.Session, Version: BuildVersion, Devices: m.Devices}
			data, _ := json.Marshal(r)
			if err := os.WriteFile(filepath.Join(dir, "enrollment.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
			c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: dir, Detection: Detection{OS: "linux"}, Service: "myce-worker.service"}
			_, err := WaitEnrollment(context.Background(), c, Client{}, EnrollmentWait{Clock: clock, Timeout: time.Second, Wait: func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil }, Observe: func(context.Context) (EnrollmentObservation, error) {
				if status == 426 {
					return EnrollmentObservation{}, &HTTPError{Status: 426}
				}
				return EnrollmentObservation{}, errors.New("unreachable")
			}, Command: func(context.Context, string, ...string) ([]byte, error) {
				return []byte("ActiveState=active\nSubState=running\nMainPID=123\nNRestarts=0\nResult=success\n"), nil
			}})
			if err == nil || strings.Contains(err.Error(), "private-smoke-test-token") {
				t.Fatal("false success or token in error", err)
			}
		})
	}
}

func TestEnrollmentWaitRequiresStableManagerAndSupportsPoliteWaiting(t *testing.T) {
	for _, mode := range []string{"ready", "polite_waiting", "start-loop", "previous-process", "interrupted-health", "interrupted-pid", "changed-restarts"} {
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
			var verdicts []bool
			timeout := 36 * time.Second
			if strings.HasPrefix(mode, "interrupted-") || mode == "changed-restarts" {
				timeout = 90 * time.Second
			}
			result, err := WaitEnrollment(context.Background(), ServiceConfig{State: dir, Detection: Detection{OS: "linux"}}, Client{}, EnrollmentWait{
				Clock: clock, Timeout: timeout,
				HealthObserved: func(healthy bool) { verdicts = append(verdicts, healthy) },
				Wait:           func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil },
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
					// Build valid windows before two interruptions; each recovery
					// must earn a fresh 30 seconds instead of retaining prior time.
					if observations == 11 || observations == 26 {
						if mode == "interrupted-health" {
							return []byte(fmt.Sprintf("ActiveState=activating\nSubState=auto-restart\nMainPID=%d\nNRestarts=0\nResult=exit-code\n", pid)), nil
						}
						if mode == "interrupted-pid" {
							pid++
						}
					}
					if mode == "changed-restarts" {
						if observations >= 11 {
							restarts++
						}
						if observations >= 26 {
							restarts++
						}
					}
					return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=%d\nResult=success\n", pid, restarts)), nil
				},
			})
			if mode == "start-loop" || mode == "previous-process" {
				if err == nil || result.State == "ready" {
					t.Fatal("restarting or different manager process accepted", result.State, err)
				}
			} else {
				wantState := "ready"
				if mode == "polite_waiting" {
					wantState = mode
				}
				if err != nil || result.State != wantState || observations < 31 {
					t.Fatal("stable enrollment did not finish correctly", result.State, observations, err)
				}
				if strings.HasPrefix(mode, "interrupted-") && observations != 57 || mode == "changed-restarts" && observations != 56 {
					t.Fatal("interrupted enrollment reused prior stable time", observations)
				}
			}
			if len(verdicts) != observations {
				t.Fatal("health verdict was not published for every observation", len(verdicts), observations)
			}
			for i, healthy := range verdicts {
				want := mode != "start-loop" && mode != "previous-process" && !(strings.HasPrefix(mode, "interrupted-") && (i == 10 || i == 25))
				if healthy != want {
					t.Fatal("health verdict bypassed parser or receipt PID fence", i, healthy, want)
				}
			}
		})
	}
}
