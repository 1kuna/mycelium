package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Successful group delivery is evidence before session cleanup finishes. A
// disappeared or refused group is not evidence that a live payload was stopped.
func TestSignalWorkerGroupDeliveryEvidence(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"delivered", nil, true},
		{"gone", syscall.ESRCH, false},
		{"permission", syscall.EPERM, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivered := false
			err := signalWorkerGroupUsing(42, syscall.SIGTERM, func(int, syscall.Signal) error {
				return tc.err
			}, func(int) ([]int, error) {
				return []int{42}, nil
			}, func() { delivered = true })
			if delivered != tc.want || tc.err == syscall.EPERM && !errors.Is(err, syscall.EPERM) {
				t.Fatalf("delivery=%v error=%v", delivered, err)
			}
		})
	}
}

// Force Wait to return inside kill, before the sender resumes and persists
// delivery. Its exit observation is ordered by the separate observation lock;
// no scheduler timing or sleeps are needed for #743.
func TestNativeStopExitBeforeDeliveryReceipt(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, shutdown := range []bool{false, true} {
			for _, signal := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
				t.Run(fmt.Sprintf("group=%v/shutdown=%v/signal=%v", grouped, shutdown, signal), func(t *testing.T) {
					p := &process{shutdown: shutdown}
					if !shutdown {
						p.priorityStop = &PriorityPreemption{Cause: priorityPreemptionCause, Demand: "demand", Victim: "victim"}
					}
					var saved memberOutcome
					p.saveOutcome = func(facts memberOutcome) error { saved = facts; return nil }
					m := &nativeMember{Name: "main", PID: 42}
					g := &nativeGroup{p: p, dir: t.TempDir(), identity: "attempt", members: []*nativeMember{m}}
					var observed *memberExit
					deliver := func(delivered func()) error {
						return signalWorkerGroupUsing(42, signal, func(int, syscall.Signal) error {
							// The OS wait returns while the sender is descheduled.
							// The observation lock is held until receipt persistence.
							observed = &memberExit{Status: 0, ObservedAt: time.Now().UTC()}
							return nil
						}, func(int) ([]int, error) { return nil, nil }, delivered)
					}
					if grouped {
						g.signalLockedUsing(nil, signal, deliver)
					} else if !p.signalLockedUsing(signal, deliver) {
						t.Fatal("delivery failed")
					}
					if grouped {
						var receipt struct {
							Members []*nativeMember `json:"members"`
						}
						data, err := os.ReadFile(filepath.Join(g.dir, "native-attempt.json"))
						if err != nil {
							t.Fatal(err)
						}
						if err := json.Unmarshal(data, &receipt); err != nil {
							t.Fatal(err)
						}
						saved = receipt.Members[0].memberOutcome
					}
					// Replay the saved facts, as restart recovery does, after the
					// waiter finally obtains the lock and records its observation.
					saved.Exit = observed
					r := classifyAttemptOutcome(Result{Success: true}, attemptCause{PriorityStop: p.priorityStop, memberOutcome: saved})
					if r.Success || !r.Interrupted || (r.PriorityStop != nil) != !shutdown {
						t.Fatalf("stop lost across exit/receipt ordering: %+v; facts=%+v", r, saved)
					}
				})
			}
		}
	}
}

// An independently observed exit must win even while its waiter is unable to
// publish under p.mu and a surviving group can still accept a signal (#743).
func TestNativeStopPreservesPendingExitObservation(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, code := range []int{0, 7} {
			t.Run(fmt.Sprintf("group=%v/exit=%d", grouped, code), func(t *testing.T) {
				cmd := exec.Command("sh", "-c", fmt.Sprintf("exit %d", code))
				if err := cmd.Run(); err != nil && code == 0 {
					t.Fatal(err)
				}
				p := &process{stopGroup: true, priorityStop: &PriorityPreemption{Cause: priorityPreemptionCause}}
				p.saveOutcome = func(memberOutcome) error { return nil }
				m := &nativeMember{Name: "main", PID: 42, exitObservation: new(nativeExitObservation)}
				g := &nativeGroup{p: p, dir: t.TempDir(), identity: "attempt", members: []*nativeMember{m}}
				p.mu.Lock()
				defer p.mu.Unlock()
				observation := &p.exitObservation
				if grouped {
					observation = m.exitObservation
				}
				observation.observe(cmd) // Waiter has not yet obtained p.mu.
				killCalled := false
				deliver := func(delivered func()) error {
					return signalWorkerGroupUsing(42, syscall.SIGTERM, func(int, syscall.Signal) error {
						killCalled = true // Group anchor survives main's exit.
						return nil
					}, func(int) ([]int, error) { return nil, nil }, delivered)
				}
				if grouped {
					g.signalLockedUsing(nil, syscall.SIGTERM, deliver)
				} else if !p.signalLockedUsing(syscall.SIGTERM, deliver) {
					t.Fatal("group cleanup failed")
				}
				facts := p.outcome
				if grouped {
					facts = m.memberOutcome
				}
				r := classifyAttemptOutcome(Result{Success: code == 0}, attemptCause{PriorityStop: p.priorityStop, memberOutcome: facts})
				if !killCalled || len(facts.Signals) != 0 || r.Success != (code == 0) || r.Interrupted || r.PriorityStop != nil {
					t.Fatalf("group cleanup rewrote independent exit: %+v; facts=%+v", r, facts)
				}
			})
		}
	}
}
