package myce

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
)

// HostMutationExecutor uses the existing HostControl and command runner. S05
// supplies Reconcile/Settled; S08 can use the same boundary for its envelope.
// Every callback runs in the dropped child (or in-process for the worker/dev).
type HostMutationExecutor struct {
	Probe     MachineProbe
	Clock     Clock
	Reconcile func(context.Context, HostInvocation) error
	Settled   func(HostIntent) error
}

func (x HostMutationExecutor) Execute(ctx context.Context, v HostInvocation, m HostMutation) (HostMutationResult, error) {
	var result HostMutationResult
	if v.Privileged || !validHostMutation(m) {
		return result, ErrHostChild
	}
	if m.Operation == "resolve-docker" {
		allowed := m.SpoolDir != "" && m.SpoolDir == v.Config.State
		workers, e := HostWorkers(v.StateDir)
		if e != nil {
			return result, e
		}
		for _, w := range workers {
			allowed = allowed || m.SpoolDir != "" && m.SpoolDir == w.State
		}
		if !allowed || !filepath.IsAbs(m.SpoolDir) || filepath.Clean(m.SpoolDir) != m.SpoolDir {
			return result, ErrHostTarget
		}
		if e = validateHostTarget(m.SpoolDir, v.Account.UID, true); e != nil {
			return result, e
		}
		if e = ReconcileDockerAttempt(ctx, v.StateDir, m.SpoolDir, m.AttemptID); e != nil {
			return result, e
		}
		state, e := ReadHostIntent(v.StateDir)
		return HostMutationResult{HostRevision: state.HostRevision}, e
	}
	if m.Operation == "record-exclusion" {
		if err := recordHostExclusion(ctx, v, m.Evidence, x.Clock); err != nil {
			return result, err
		}
		if x.Reconcile != nil {
			_ = x.Reconcile(ctx, v)
		}
		return HostMutationResult{HostRevision: m.Evidence.HostRevision, Exclusion: m.Evidence, CleanupConfirmed: m.Evidence.Verdict == "PASS"}, nil
	}
	if m.Operation == "unit-stop" || m.Operation == "unit-kill" || m.Operation == "unit-start" {
		// Only units installed for this host may be mutated.
		units, e := WorkerUnits(v.StateDir, "")
		if e != nil {
			return result, e
		}
		allowed := filepath.Base(v.Config.Service) == m.Unit
		for _, u := range units {
			allowed = allowed || u == m.Unit
		}
		if !allowed {
			return result, ErrHostIdentity
		}
		if e = CheckHostUserManager(ctx, x.Probe.Command); e != nil {
			return result, e
		}
		args := []string{"--user"}
		switch m.Operation {
		case "unit-stop":
			args = append(args, "stop", "--no-block", m.Unit)
		case "unit-start":
			if m.Expected.Revision == 0 {
				return result, ErrHostRevision
			}
			if e = CheckHostRevision(v.StateDir, m.Expected); e != nil {
				return result, e
			}
			args = append(args, "start", "--no-block", m.Unit)
		case "unit-kill":
			args = append(args, "kill", "--kill-whom=all", "--signal=SIGKILL", m.Unit)
		}
		if m.Operation == "unit-start" {
			e = hostDispatch(v.StateDir, m.Expected, func() error { _, err := x.Probe.Command(ctx, "systemctl", args...); return err })
		} else {
			_, e = x.Probe.Command(ctx, "systemctl", args...)
		}
		return result, e
	}
	h := HostControl{ContainerRetention: v.Config.ContainerRetention, DockerEndpoint: v.Config.DockerEndpoint, Clock: x.Clock, Dir: v.StateDir, Docker: true, Timeout: m.Timeout, Expected: m.Expected, CloseExpected: m.CloseExpected, Settled: x.Settled}
	h.Prepare = func() error {
		workers, err := HostWorkers(v.StateDir)
		if err != nil {
			return err
		}
		for _, w := range workers {
			if w.Host != v.Config.HostID {
				return ErrHostIdentity
			}
			if err := validateHostTarget(w.State, v.Account.UID, true); err != nil {
				return err
			}
		}

		if x.Probe.OS == "linux" {
			return CheckHostUserManager(ctx, x.Probe.Command)
		}
		return nil
	}
	h.StopSecondary = func(ctx context.Context) error {
		if x.Probe.OS != "darwin" || v.Config.Service == "" {
			return nil
		}
		if x.Probe.Command == nil {
			return ErrHostChild
		}
		_, e := x.Probe.Command(ctx, "launchctl", "bootout", "gui/"+strconv.FormatUint(uint64(v.Account.UID), 10), v.Config.Service)
		return e
	}
	h.ResolveServices = func() (map[string]HostService, error) {
		services := map[string]HostService{}
		if x.Probe.OS != "linux" {
			return services, nil
		}
		units, e := WorkerUnits(v.StateDir, "")
		if e != nil {
			return nil, e
		}
		if len(units) == 0 && v.Config.Service != "" {
			units = []string{filepath.Base(v.Config.Service)}
		}
		for _, u := range units {
			if !ValidWorkerUnit(u) {
				return nil, ErrHostIdentity
			}
			services[u] = &SystemdHostService{Unit: u, Command: x.Probe.Command, Clock: x.Clock}
		}
		return services, nil
	}
	switch m.Operation {
	case "close":
		r, e := h.Disable(ctx, m.Reason, m.Grace)
		if !r.cleanupComplete && e != nil {
			return result, e
		}
		return HostMutationResult{HostRevision: r.HostRevision, Stopped: r.Stopped, CleanupConfirmed: r.Confirmed, Exclusion: r.Exclusion}, nil
	case "open":
		if e := h.Prepare(); e != nil {
			return result, e
		}
		before, e := ReadHostIntent(v.StateDir)
		if e != nil {
			return result, e
		}
		expectedRevision := m.Expected.Revision
		if before.Desired != "open" {
			expectedRevision++
		}
		if e := h.Enable(ctx); e != nil {
			return result, e
		}
		s, e := ReadHostIntent(v.StateDir)
		if e != nil {
			return result, e
		}
		if s.Revision != expectedRevision || s.Desired != "open" {
			return result, ErrHostRevision
		}
		units, e := WorkerUnits(v.StateDir, "")
		if e != nil {
			return result, e
		}
		if x.Probe.OS == "linux" {
			if len(units) == 0 && v.Config.Service != "" {
				units = []string{filepath.Base(v.Config.Service)}
			}
			for _, u := range units {
				if !ValidWorkerUnit(u) {
					return result, ErrHostIdentity
				}
				if e := hostDispatch(v.StateDir, s.HostRevision, func() error { _, e := x.Probe.Command(ctx, "systemctl", "--user", "start", "--no-block", u); return e }); e != nil {
					return result, e
				}
			}
		}
		if x.Probe.OS == "darwin" && v.Config.Service != "" {
			if x.Probe.Command == nil {
				return result, ErrHostChild
			}
			if e := hostDispatch(v.StateDir, s.HostRevision, func() error {
				_, err := x.Probe.Command(ctx, "launchctl", "bootstrap", "gui/"+strconv.FormatUint(uint64(v.Account.UID), 10), v.Config.Service)
				return err
			}); e != nil {
				return result, e
			}
		}
		return HostMutationResult{HostRevision: s.HostRevision}, nil
	case "reconcile":
		if x.Reconcile == nil {
			return result, ErrHostReconcileUnavailable
		}
		s, e := ReadHostIntent(v.StateDir)
		if e != nil {
			return result, e
		}
		if hostOpenConverged(s) && len(s.Errors) == 0 {
			return HostMutationResult{HostRevision: s.HostRevision, Desired: s.Desired}, nil
		}
		boot, e := HostBootID()
		if e != nil {
			return result, e
		}
		if s.Desired == "closed" && localExclusionConfirmed(s, boot) {
			// The stopped worker cannot perform ordinary retention; the timer uses
			// the same exact-label inventory, without creating a maintenance daemon.
			workers, e := HostWorkers(v.StateDir)
			if e != nil {
				return result, e
			}
			if e = h.maintainDocker(ctx, workers); e != nil {
				return result, e
			}
		}
		var cleanupErr error
		if s.Desired == "closed" && !localExclusionConfirmed(s, boot) {
			h.CloseExpected = &s.HostRevision
			report, e := h.Disable(ctx, s.Reason, 0)
			if !report.cleanupComplete {
				cleanupErr = e
			}
		}
		// A worker timer may relay an already privileged proof, but cleanup
		// performed here cannot manufacture one. Root collects after this child.
		reconcileErr := x.Reconcile(ctx, v)
		current, e := ReadHostIntent(v.StateDir)
		if e != nil {
			return result, e
		}
		var codes []string
		if cleanupErr != nil {
			codes = append(codes, "local_exclusion_unconfirmed")
		}
		if reconcileErr != nil {
			codes = append(codes, "host_reconcile_pending")
		}
		if e = mutateHostIntent(v.StateDir, current.HostRevision, func(s *HostIntent) error { s.Errors = codes; return nil }); e != nil {
			return result, e
		}
		result = HostMutationResult{HostRevision: current.HostRevision, Desired: current.Desired, Errors: codes, Code: hostStatusCode(current)}
		return result, errors.Join(cleanupErr, reconcileErr)

	}
	s, e := ReadHostIntent(v.StateDir)
	if e != nil {
		return result, e
	}
	result.HostRevision = s.HostRevision
	return result, nil
}
