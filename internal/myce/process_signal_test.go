package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
			}, func(time.Time) { delivered = true })
			if delivered != tc.want || tc.err == syscall.EPERM && !errors.Is(err, syscall.EPERM) {
				t.Fatalf("delivery=%v error=%v", delivered, err)
			}
		})
	}
}

// Force Wait's observation to happen inside kill, before the sender resumes
// and persists delivery. No scheduler timing or sleeps are needed for #743.
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
					err := signalWorkerGroupUsing(42, signal, func(int, syscall.Signal) error {
						// The waiter observes a cooperative zero exit while the
						// sender is descheduled, before it can acquire p.mu.
						observed = &memberExit{Status: 0, ObservedAt: time.Now().UTC()}
						return nil
					}, func(int) ([]int, error) { return nil, nil }, func(at time.Time) {
						if grouped {
							g.recordSignalLocked(nil, signal, at)
						} else {
							p.recordSignalLocked(signal, at)
						}
					})
					if err != nil {
						t.Fatal(err)
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
