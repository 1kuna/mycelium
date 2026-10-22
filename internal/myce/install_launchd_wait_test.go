package myce

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

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
			cancel()
			return []byte("state = running\npid = 42\n"), nil
		}
		return nil, nil
	}}
	err := m.stop(ctx, installUnit{label: "local.mycelium.fixture"})
	if !errors.Is(err, context.Canceled) || prints != 1 {
		t.Fatalf("stop did not wait with cancellation: prints=%d err=%v", prints, err)
	}
}
