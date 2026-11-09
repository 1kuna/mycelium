package myce

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestLaunchdSlowExitInstallAndRollback(t *testing.T) {
	for _, earlyAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "blocking-bootout", true: "async-label-removal"}[earlyAck], func(t *testing.T) {
			for _, rollback := range []bool{false, true} {
				t.Run(map[bool]string{false: "install", true: "canceled-install-rollback"}[rollback], func(t *testing.T) {
					f := newInstallMatrixFixture(t, "darwin", "worker", 2)
					unit := filepath.Base(f.c.Service)
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						var pendingUntil time.Time
						bootouts, bootstraps, inspections := 0, 0, 0
						injected := errors.New("replacement activation failed")
						raw := func(ctx context.Context, name string, args ...string) ([]byte, error) {
							if err := ctx.Err(); err != nil {
								t.Fatalf("manager received expired context: %v", err)
							}
							probe := strings.Contains(args[len(args)-1], "preflight") || filepath.Base(args[len(args)-1]) == "probe.plist"
							if name != "launchctl" || probe {
								return f.manager.run(ctx, name, args...)
							}
							if args[0] == "print" {
								inspections++
								time.Sleep(100 * time.Millisecond)
								if !pendingUntil.IsZero() && !time.Now().Before(pendingUntil) {
									pendingUntil = time.Time{}
									f.manager.states[unit] = installUnitState{}
									delete(f.manager.cached, unit)
								}
								out, err := f.manager.run(ctx, name, args...)
								return append(out, []byte("exit timeout = 20\n")...), err
							}
							if args[0] == "bootout" {
								bootouts++
								if earlyAck {
									pendingUntil = time.Now().Add(20 * time.Second)
									return nil, nil
								}
								select {
								case <-time.After(20 * time.Second):
								case <-ctx.Done():
									return nil, ctx.Err()
								}
							}
							if args[0] == "bootstrap" {
								if !pendingUntil.IsZero() {
									t.Fatal("bootstrap raced old label removal")
								}
								bootstraps++
							}
							out, err := f.manager.run(ctx, name, args...)
							if rollback && args[0] == "bootstrap" && bootstraps == 1 && err == nil {
								cancel()
								return out, injected
							}
							return out, err
						}
						err := installWithFiles(ctx, f.c, f.source, "fixture", BoundSetupCommand(raw), installFiles{})
						if rollback {
							if !errors.Is(err, injected) || strings.Contains(err.Error(), "rollback incomplete") {
								t.Fatalf("expected activation failure and clean rollback: %v", err)
							}
							f.assertRestored(t)
						} else if err != nil {
							t.Fatalf("slow launchd install failed: %v", err)
						}
						if bootouts < 1 || inspections < 1 || !f.manager.states[unit].active {
							t.Fatal("final manager state not known and active")
						}
					})
				})
			}
		})
	}
}

// launchctl bootout can return before the old label disappears. A bootstrap
// during that interval fails with EIO even though the replacement is valid.
func TestLaunchdReplacementWaitsForBootout(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "install", true: "rollback"}[rollback], func(t *testing.T) {
			f := newInstallMatrixFixture(t, "darwin", "worker", 2)
			unit := filepath.Base(f.c.Service)
			pending, inspections, bootstraps := false, 0, 0
			injected := errors.New("replacement activation failed")
			run := func(ctx context.Context, name string, args ...string) ([]byte, error) {
				probe := strings.Contains(args[len(args)-1], "preflight") || filepath.Base(args[len(args)-1]) == "probe.plist"
				if name == "launchctl" && !probe {
					if args[0] == "print" && pending {
						inspections++
						if inspections%3 == 0 {
							pending = false
							f.manager.states[unit] = installUnitState{}
							delete(f.manager.cached, unit)
						}
					}
					if args[0] == "bootstrap" {
						if pending {
							return nil, installTestExit(5)
						}
						bootstraps++
					}
				}
				out, err := f.manager.run(ctx, name, args...)
				if name == "launchctl" && !probe && args[0] == "bootout" && err == nil {
					pending = true
					f.manager.states[unit] = installUnitState{loaded: true}
				}
				if rollback && name == "launchctl" && !probe && args[0] == "bootstrap" && bootstraps == 1 && err == nil {
					return out, injected
				}
				return out, err
			}
			err := installWithFiles(context.Background(), f.c, f.source, "fixture", run, installFiles{})
			if rollback {
				if !errors.Is(err, injected) || strings.Contains(err.Error(), "rollback incomplete") {
					t.Fatalf("rollback did not restore the prior service: %v", err)
				}
				f.assertRestored(t)
			} else if err != nil {
				t.Fatalf("replacement raced the departing service: %v", err)
			}
			if pending || inspections < 3 {
				t.Fatalf("old label removal was not observed: pending=%t inspections=%d", pending, inspections)
			}
		})
	}
}
