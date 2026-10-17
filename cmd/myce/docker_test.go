package main

import (
	"context"
	"mycelium/internal/myce"
	"strings"
	"testing"
)

func TestResolveDockerCLIRejectsAssertionAndRequiresCustody(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	if err := run([]string{"resolve-docker", "--confirm-gone"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatal("assertion escape remains", err)
	}
	for _, args := range [][]string{{"resolve-docker"}, {"resolve-docker", "--state-dir", t.TempDir(), "--attempt-id", "owned"}} {
		if err := run(args); err == nil {
			t.Fatal("missing custody accepted")
		}
	}
	for _, mode := range []string{"queue", "join"} {
		if err := run([]string{mode, "--simulated"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatal("production exposes simulation flag", mode, err)
		}
	}
}

func TestResolveDockerUsesWorkerMutationExecutor(t *testing.T) {
	r, a, c := localIdentityFixture(t)
	calls := 0
	err := resolveDockerCommand([]string{"--state-dir", c.State, "--attempt-id", "owned", "--worker-user", a.Name, "--config", c.Config}, myce.MachineProbe{}, r, func(_ context.Context, launch myce.HostChildLaunch) ([]byte, error) {
		calls++
		if launch.Request.Operation != "resolve-docker" || launch.Request.SpoolDir != c.State || launch.Request.AttemptID != "owned" || launch.Account.UID != a.UID {
			t.Fatal(launch.Request)
		}
		return []byte(`{"version":1,"operation":"resolve-docker","result":{"revision":1,"operation_id":"close"}}`), nil
	})
	if err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
}
