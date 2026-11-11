package myce

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDockerInspectSlowDrainRetainsStartingCustody(t *testing.T) {
	clock := &dockerTestClock{now: time.Unix(1, 0)}
	d := &dockerAttempt{clock: clock, state: dockerExecution{Identity: "owned", Container: "id", Operation: "start"}}
	initialDrain := d.rpcDrainBudget("inspect")
	reads := 0
	d.command = func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "ps" {
			return []byte("id"), nil
		}
		if args[0] != "inspect" {
			t.Fatalf("observation dispatched mutation: %v", args)
		}
		reads++
		if reads == 1 {
			// A slow local response consumer can outlive many heartbeats.
			clock.advance(time.Minute)
			return nil, exec.ErrWaitDelay
		}
		return []byte(`[{"Id":"id","Config":{"Labels":{"myce.attempt":"owned"}},"State":{"Running":true,"Status":"running"}}]`), nil
	}
	cs, err := d.containers()
	if err != nil || len(cs) != 1 || !dockerLive(cs[0]) {
		t.Fatalf("slow starting observation failed: containers=%v err=%v", cs, err)
	}
	if reads != 2 || d.state.Stopped || d.state.InfrastructureStop || d.state.ObservationLost || d.state.Released || d.state.CleanupCause != nil {
		t.Fatalf("slow read changed custody or retained recovered cause: %+v", d.state)
	}
	drain := d.rpcDrainBudget("inspect")
	if drain <= initialDrain {
		t.Fatalf("slow pipe consumption did not widen its own allowance: %s -> %s", initialDrain, drain)
	}
	d.backoffRPC("inspect", time.Hour)
	if d.rpcDrainBudget("inspect") != drain {
		t.Fatal("daemon outage enlarged pipe cleanup allowance")
	}
}

// Inventory is metadata, not a pull stream. In particular, a large list of
// mounts/env entries must not trigger one host-capacity probe per JSON line.
func TestDockerInspectMetadataAvoidsProgressProbes(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "docker")
	data := "[{\n  \"metadata\": [\n" + strings.Repeat("    \"value\",\n", 600) + "    \"last\"\n  ]\n}]\n"
	for path, b := range map[string][]byte{
		binary:           []byte("#!/bin/sh\ncat \"$0.json\"\n"),
		binary + ".json": []byte(data),
	} {
		if err := os.WriteFile(path, b, 0700); err != nil {
			t.Fatal(err)
		}
	}
	old := queueHostMemorySource
	probes := 0
	queueHostMemorySource = func(float64) (MemorySource, error) {
		probes++
		return func(context.Context) (float64, error) { return 32, nil }, nil
	}
	defer func() { queueHostMemorySource = old }()
	d := &dockerAttempt{binary: binary, dir: root, state: dockerExecution{Endpoint: "fake"}}
	out, err := d.call("inspect", "owned")
	if err != nil || string(out) != data {
		t.Fatalf("inventory output lost: bytes=%d err=%v", len(out), err)
	}
	if probes != 0 {
		t.Fatalf("inventory ran %d pull-progress capacity probes", probes)
	}
}

func TestDockerInspectRecoveryBudgetRetainsSlowEvidence(t *testing.T) {
	d := &dockerAttempt{}
	d.noteRPCLatency("inspect", time.Millisecond)
	// A timed-out read is a lower bound, even if local consumption considerably
	// overshot its old deadline. One succeeding idle read cannot erase it.
	d.backoffRPC("inspect", 40*time.Second)
	slow := d.rpcBudget("inspect")
	d.noteRPCLatency("inspect", time.Millisecond)
	recovered := d.rpcBudget("inspect")
	if recovered <= 40*time.Second || recovered >= slow {
		t.Fatalf("fast response erased slow evidence or failed to adapt: %s -> %s", slow, recovered)
	}
	for range 1000 {
		d.noteRPCLatency("inspect", time.Millisecond)
	}
	if got := d.rpcBudget("inspect"); got >= recovered || got > 2*time.Millisecond {
		t.Fatalf("healthy observations did not retire outage allowance: %s", got)
	}
	if got := d.rpcCadence("inspect"); got != time.Millisecond {
		t.Fatalf("outage slowed healthy cadence: %s", got)
	}
	d.timeout = time.Second
	d.backoffRPC("inspect", time.Hour)
	if got := d.rpcBudget("inspect"); got != time.Second {
		t.Fatalf("explicit timeout changed: %s", got)
	}
}
