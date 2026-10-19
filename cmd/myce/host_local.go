package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"time"

	"mycelium/internal/myce"
)

// This is the S06 invocation adapter. S08 owns the versioned verdict envelope
// and wait/expect UX; a mutation result here is never local-exclusion PASS.
// S05 wires mandatory reconciliation and settlement here so in-process and
// re-executed commands use exactly the same handler construction.
func hostMutationExecutor(probe myce.MachineProbe) myce.HostMutationExecutor {
	return myce.HostMutationExecutor{Probe: probe, Reconcile: func(ctx context.Context, v myce.HostInvocation) error {
		return myce.ReconcileHostLocal(ctx, v.StateDir, myce.RealClock{}, func(endpoint string) (myce.Client, error) {
			workers, err := myce.HostWorkers(v.StateDir)
			if err != nil {
				return myce.Client{}, err
			}
			return myce.HostQueueClient(workers, os.Getenv("MYCE_TOKEN"))(endpoint)
		})
	}}
}

func hostLocalCommand(args []string, probe myce.MachineProbe, out io.Writer) error {
	return hostLocalCommandWithIdentity(args, probe, out, myce.LocalHostIdentityResolver(), nil)
}
func hostLocalCommandWithIdentity(args []string, probe myce.MachineProbe, out io.Writer, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher) error {
	if len(args) > 0 && args[0] == "reconcile" {
		return hostReconcileCommand(args, probe, out, resolver, launch)
	}
	return hostEnvelopeCommand(args, probe, out, resolver, launch)
}

func hostReconcileCommand(args []string, probe myce.MachineProbe, out io.Writer, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher) error {
	if len(args) == 0 {
		return errors.New("usage: myce host close|open|status|reconcile --local --worker-user <user>")
	}
	op := args[0]
	if op != "close" && op != "open" && op != "status" && op != "reconcile" {
		return errors.New("unknown local host command")
	}
	f := flag.NewFlagSet("host "+op, flag.ContinueOnError)
	local := f.Bool("local", false, "control this host")
	user := f.String("worker-user", "", "installed OS worker account (required under root)")
	config := f.String("config", "", "installed worker config (default from worker account home)")
	state := f.String("state-dir", "", "override shared host-control directory only; must be owned by worker and match installed host identity")
	reason := f.String("reason", "disabled by operator", "close reason")
	grace := f.Duration("grace", 10*time.Second, "checkpoint grace (0 to 1m)")
	timeout := f.Duration("timeout", 30*time.Second, "command deadline")
	revision := f.Uint64("expect-revision", 0, "expected host revision")
	operation := f.String("expect-operation", "", "expected operation ID")
	_ = f.Bool("json", false, "structured output")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if !*local || f.NArg() != 0 {
		return errors.New("host command requires --local and flags only")
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	v, e := resolver.Resolve(*user, *config, *state)
	if e != nil {
		return e
	}
	expected := myce.HostRevision{Revision: *revision, Operation: *operation}
	m := myce.HostMutation{Operation: op, Reason: *reason, Grace: *grace, Timeout: *timeout, Expected: expected}
	if op == "close" && *revision != 0 {
		m.CloseExpected = &expected
	}
	if op == "status" {
		r, e := v.Mutate(ctx, m, launch, hostMutationExecutor(probe).Execute)
		if e != nil {
			return e
		}
		if probe.OS == "linux" {
			if e = v.ObservationContext().CheckUserManager(ctx, probe.Command); e != nil {
				return e
			}
		}
		if v.Privileged {
			observation, err := v.InspectExclusion(ctx, launch, hostMutationExecutor(probe).Execute, myce.LocalHostKernelReaders())
			if err != nil {
				return err
			}
			r.Exclusion = &observation
		}
		return json.NewEncoder(out).Encode(r)
	}
	r, e := v.Mutate(ctx, m, launch, hostMutationExecutor(probe).Execute)
	if e != nil {
		if r.Partial {
			if writeErr := json.NewEncoder(out).Encode(r); writeErr != nil {
				return writeErr
			}
		}
		return e
	}
	if v.Privileged && (op == "close" || op == "reconcile") {
		observation, err := v.InspectExclusion(ctx, launch, hostMutationExecutor(probe).Execute, myce.LocalHostKernelReaders())
		if err != nil {
			return err
		}
		r.Exclusion = &observation
		if observation.Verdict != "PASS" {
			r.CleanupConfirmed = false
		}
		if _, err = v.Mutate(ctx, myce.HostMutation{Operation: "record-exclusion", Evidence: &observation}, launch, hostMutationExecutor(probe).Execute); err != nil {
			return err
		}
	}
	return json.NewEncoder(out).Encode(r)
}
