package myce

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestInstallSystemdSlowStopAndRollback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "upgrade", true: "expired-install-rollback"}[fail], func(t *testing.T) {
			c, executable := preserveWorkerFixture(t)
			if err := Install(context.Background(), c, executable, "test", fakeInstallManager); err != nil {
				t.Fatal(err)
			}
			old, _ := os.ReadFile(c.Binary)
			if err := os.WriteFile(executable, []byte("replacement binary"), 0700); err != nil {
				t.Fatal(err)
			}
			synctest.Test(t, func(t *testing.T) {
				active, restarts, inspections := true, 0, 0
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				raw := func(ctx context.Context, name string, args ...string) ([]byte, error) {
					if err := ctx.Err(); err != nil {
						t.Fatalf("manager received an expired context: %v", err)
					}
					call := strings.Join(args, " ")
					switch {
					case strings.Contains(call, "--property=TimeoutStopUSec"):
						time.Sleep(100 * time.Millisecond) // measured manager round trip
						return []byte("TimeoutStopUSec=45s\nTimeoutStartUSec=1min 30s\n"), nil
					case strings.Contains(call, "--property=UnitFileState"):
						inspections++
						if strings.Contains(call, "myce-worker.service") {
							return []byte("enabled"), nil
						}
						return nil, nil
					case strings.Contains(call, "is-active"):
						if active && strings.Contains(call, "myce-worker.service") {
							return nil, nil
						}
						return nil, installTestExit(3)
					case strings.Contains(call, " restart "):
						restarts++
						active = false
						if fail && restarts == 1 {
							// Consume the complete install allowance. Recovery must
							// outlive this expired command and canceled parent.
							<-ctx.Done()
							cancel()
							return nil, ctx.Err()
						}
						select {
						case <-time.After(20 * time.Second):
							active = true
							return nil, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return nil, nil
				}
				began := time.Now()
				err := Install(ctx, c, executable, "", BoundSetupCommand(raw))
				if fail {
					if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "rollback incomplete") {
						t.Fatalf("expected original failure and complete rollback: %v", err)
					}
					got, _ := os.ReadFile(c.Binary)
					if string(got) != string(old) || restarts != 2 {
						t.Fatal("old bytes/service were not restored")
					}
				} else if err != nil || restarts != 1 {
					t.Fatalf("slow upgrade failed: %v", err)
				}
				if !active || inspections == 0 || time.Since(began) < 20*time.Second {
					t.Fatal("service state was not known/active after a slow stop")
				}
			})
		})
	}
}

func TestInstallSystemdEffectiveLifecycleBudgets(t *testing.T) {
	for _, tc := range []struct {
		action []string
		budget time.Duration
	}{{[]string{"stop"}, 45 * time.Second}, {[]string{"start"}, 135 * time.Second}, {[]string{"restart"}, 135 * time.Second}, {[]string{"enable", "--now"}, 135 * time.Second}, {[]string{"disable", "--now"}, 45 * time.Second}} {
		t.Run(strings.Join(tc.action, "-"), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				command := BoundSetupCommand(func(ctx context.Context, _ string, args ...string) ([]byte, error) {
					if contains(args, "show") {
						time.Sleep(time.Second)
						return []byte("TimeoutStopUSec=45s\nTimeoutStartUSec=1min 30s\n"), nil
					}
					deadline, ok := ctx.Deadline()
					if !ok || deadline.Sub(time.Now()) != tc.budget+2*time.Second {
						t.Fatalf("effective manager budget/margin lost: %s", deadline.Sub(time.Now()))
					}
					return nil, nil
				})
				if _, err := command(context.Background(), "systemctl", append(append([]string{"--user"}, tc.action...), "fixture.service")...); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
	if d, err := systemdTimeout("1d 2h 3min 4s 5ms 6us"); err != nil || d != 26*time.Hour+3*time.Minute+4*time.Second+5*time.Millisecond+6*time.Microsecond {
		t.Fatalf("systemd formatted timespan: %s %v", d, err)
	}
}
