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
								return append(out, []byte("exit timeout = 5\n")...), err
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
								select {
								case <-time.After(20 * time.Second):
								case <-ctx.Done():
									return nil, ctx.Err()
								}
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

func TestLaunchdStopWaitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prints := 0
	m := installManager{os: "darwin", command: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "print" {
			prints++
			if prints == 1 {
				cancel()
			}
			return []byte("state = running\npid = 42\nexit timeout = 0\n"), nil
		}
		return nil, nil
	}}
	err := m.stop(ctx, installUnit{label: "local.mycelium.fixture"})
	if !errors.Is(err, context.Canceled) || prints == 0 {
		t.Fatalf("stop did not wait with cancellation: prints=%d err=%v", prints, err)
	}
}

func TestLaunchdInterruptedRecovery(t *testing.T) {
	for _, action := range []string{"bootout", "bootstrap", "kickstart"} {
		for _, after := range []bool{false, true} {
			t.Run(action+map[bool]string{false: "-before-effect", true: "-after-effect"}[after], func(t *testing.T) {
				f := newInstallMatrixFixture(t, "darwin", "worker", 2)
				unit := filepath.Base(f.c.Service)
				primary := errors.New("publication failed")
				recovering, interrupted := false, false
				files := installFiles{before: func(op, path string) error {
					if op == "directory-fsync" && path == f.c.Service {
						recovering = true
						return primary
					}
					return nil
				}}
				raw := func(ctx context.Context, name string, args ...string) ([]byte, error) {
					interrupt := recovering && name == "launchctl" && args[0] == action && !interrupted
					if interrupt {
						interrupted = true
						if !after {
							return nil, installTestExit(-1) // exec.ExitError for a signaled client
						}
					}
					out, err := f.manager.run(ctx, name, args...)
					if recovering && name == "launchctl" && args[0] == "bootstrap" && action == "kickstart" {
						f.manager.states[unit] = installUnitState{loaded: true}
					}
					if interrupt {
						return out, installTestExit(-1)
					}
					return out, err
				}
				err := installWithFiles(context.Background(), f.c, f.source, "fixture", BoundSetupCommand(raw), files)
				if !interrupted || !errors.Is(err, primary) || strings.Contains(err.Error(), "rollback incomplete") {
					t.Fatalf("interrupted recovery abandoned: interrupted=%t err=%v", interrupted, err)
				}
				f.assertRestored(t)
			})
		}
	}
}

func TestLaunchdActivationObserved(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "install", true: "rollback"}[rollback], func(t *testing.T) {
			f := newInstallMatrixFixture(t, "darwin", "worker", 2)
			unit := filepath.Base(f.c.Service)
			synctest.Test(t, func(t *testing.T) {
				var activeAt time.Time
				bootstraps := 0
				primary := errors.New("bootstrap client killed")
				raw := func(ctx context.Context, name string, args ...string) ([]byte, error) {
					probe := strings.Contains(args[len(args)-1], "preflight") || filepath.Base(args[len(args)-1]) == "probe.plist"
					if name != "launchctl" || probe {
						return f.manager.run(ctx, name, args...)
					}
					if args[0] == "print" && !activeAt.IsZero() && !time.Now().Before(activeAt) {
						f.manager.states[unit] = installUnitState{loaded: true, active: true}
						activeAt = time.Time{}
					}
					out, err := f.manager.run(ctx, name, args...)
					switch args[0] {
					case "bootout":
						activeAt = time.Time{}
					case "bootstrap", "kickstart":
						f.manager.states[unit] = installUnitState{loaded: true}
						activeAt = time.Now().Add(20 * time.Second)
						if args[0] == "bootstrap" {
							bootstraps++
							if rollback && bootstraps == 1 {
								return out, primary
							}
						}
					}
					return out, err
				}
				began := time.Now()
				err := installWithFiles(context.Background(), f.c, f.source, "fixture", BoundSetupCommand(raw), installFiles{})
				if rollback {
					if !errors.Is(err, primary) || strings.Contains(err.Error(), "rollback incomplete") {
						t.Fatalf("activation rollback failed: %v", err)
					}
					f.assertRestored(t)
				} else if err != nil {
					t.Fatal(err)
				}
				if !f.manager.states[unit].active || !activeAt.IsZero() || time.Since(began) < 20*time.Second {
					t.Fatal("installer returned before observing asynchronous activation")
				}
			})
		})
	}
}

func TestLaunchdRepeatedRecoveryInterruptionReported(t *testing.T) {
	f := newInstallMatrixFixture(t, "darwin", "worker", 2)
	primary := errors.New("publication failed")
	recovering, stops := false, 0
	files := installFiles{before: func(op, path string) error {
		if op == "directory-fsync" && path == f.c.Service {
			recovering = true
			return primary
		}
		return nil
	}}
	raw := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if recovering && name == "launchctl" && args[0] == "bootout" {
			stops++
			return nil, installTestExit(-1)
		}
		return f.manager.run(ctx, name, args...)
	}
	err := installWithFiles(context.Background(), f.c, f.source, "fixture", BoundSetupCommand(raw), files)
	if !errors.Is(err, primary) || !strings.Contains(err.Error(), "rollback incomplete") || !strings.Contains(err.Error(), "reload:") || !strings.Contains(err.Error(), "keep stopped:") || !strings.Contains(err.Error(), "active") || stops < 2 {
		t.Fatalf("persistent failure not reported precisely: stops=%d err=%v", stops, err)
	}
	f.assertFiles(t)
}

func TestLaunchdExitedActivationRollsBack(t *testing.T) {
	f := newInstallMatrixFixture(t, "darwin", "worker", 2)
	bootstraps := 0
	raw := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		probe := strings.Contains(args[len(args)-1], "preflight") || filepath.Base(args[len(args)-1]) == "probe.plist"
		if name == "launchctl" && !probe {
			if args[0] == "bootstrap" {
				bootstraps++
			}
			if args[0] == "print" && bootstraps == 1 && f.manager.states[filepath.Base(f.c.Service)].loaded {
				return []byte("state = exited\nlast exit code = 1\n"), nil
			}
		}
		return f.manager.run(ctx, name, args...)
	}
	err := installWithFiles(context.Background(), f.c, f.source, "fixture", BoundSetupCommand(raw), installFiles{})
	if err == nil || !strings.Contains(err.Error(), "before activation") || strings.Contains(err.Error(), "rollback incomplete") {
		t.Fatalf("exited service not rolled back: %v", err)
	}
	f.assertRestored(t)
}
