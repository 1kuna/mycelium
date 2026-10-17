package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"time"

	"mycelium/internal/myce"
)

// This is the S06 invocation adapter. S08 owns the versioned verdict envelope
// and wait/expect UX; a mutation result here is never local-exclusion PASS.
// S05 wires mandatory reconciliation and settlement here so in-process and
// re-executed commands use exactly the same handler construction.
func hostMutationExecutor(probe myce.MachineProbe) myce.HostMutationExecutor {
	return myce.HostMutationExecutor{Probe: probe}
}

func hostLocalCommand(args []string, probe myce.MachineProbe, out io.Writer) error {
	return hostLocalCommandWithIdentity(args, probe, out, myce.LocalHostIdentityResolver(), nil)
}
func hostLocalCommandWithIdentity(args []string, probe myce.MachineProbe, out io.Writer, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher) error {
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
	v, e := resolver.Resolve(*user, *config, *state)
	if e != nil {
		return e
	}
	if e = myce.ValidateHostControlTree(v.StateDir, v.Account.UID); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if op == "status" {
		// Deliberately no HostControl, mutation handler, lock or persisted observation.
		if probe.OS == "linux" {
			if e := v.ObservationContext().CheckUserManager(ctx, probe.Command); e != nil {
				return e
			}
		}
		s, e := myce.ReadHostIntent(v.StateDir)
		if e != nil {
			return e
		}
		return json.NewEncoder(out).Encode(struct {
			Desired    string `json:"desired"`
			Revision   uint64 `json:"revision"`
			Privileged bool   `json:"privileged_observation"`
		}{s.Desired, s.Revision, v.Privileged})
	}
	expected := myce.HostRevision{Revision: *revision, Operation: *operation}
	if *revision != 0 && *operation == "" {
		s, e := myce.ReadHostIntent(v.StateDir)
		if e != nil {
			return e
		}
		if s.Revision != *revision {
			return myce.ErrHostRevision
		}
		expected = s.HostRevision
	}
	m := myce.HostMutation{Operation: op, Reason: *reason, Grace: *grace, Timeout: *timeout, Expected: expected}
	if op == "close" && *revision != 0 {
		m.CloseExpected = &expected
	}
	r, e := v.Mutate(ctx, m, launch, hostMutationExecutor(probe).Execute)
	if e != nil {
		return e
	}
	return json.NewEncoder(out).Encode(r)
}
