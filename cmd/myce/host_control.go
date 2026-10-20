package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Retired forms have no production dispatch; retained only for a migration diagnostic.
func hostCommand(args []string, probe myce.MachineProbe, out io.Writer) error {
	return errors.New("myce disable/enable is retired; use myce host open --expect-revision or myce host close --local")
}

func hostService(ctx context.Context, p myce.MachineProbe, path string, start bool) error {
	if path == "" {
		return nil
	}
	if p.Command == nil {
		return errors.New("service runner unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var err error
	if p.OS == "linux" {
		action := "stop"
		if start {
			action = "start"
		}
		_, err = p.Command(ctx, "systemctl", "--user", "--no-block", action, filepath.Base(path))
	} else {
		action := "bootout"
		if start {
			action = "bootstrap"
		}
		_, err = p.Command(ctx, "launchctl", action, "gui/"+strconv.Itoa(os.Getuid()), path)
	}
	if err != nil {
		return fmt.Errorf("worker service %s: %w", path, err)
	}
	return nil
}
func hostServiceCommand(system, path string, start bool) string {
	if system == "linux" {
		return shellCommand([]string{"systemctl", "--user", "start", filepath.Base(path)})
	}
	return shellCommand([]string{"launchctl", "bootstrap", "gui/" + strconv.Itoa(os.Getuid()), path})
}
func shellCommand(args []string) string {
	var out []string
	for _, s := range args {
		out = append(out, "'"+strings.ReplaceAll(s, "'", "'\\''")+"'")
	}
	return strings.Join(out, " ")
}
func safeJoinCommand(args []string) []string {
	out := []string{"myce"}
	for i := 0; i < len(args); i++ {
		if args[i] == "--token" || args[i] == "-token" {
			i++
			continue
		}
		if strings.HasPrefix(args[i], "--token=") || strings.HasPrefix(args[i], "-token=") {
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func hostEnvelopeCommand(args []string, probe myce.MachineProbe, out io.Writer, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher) error {
	return hostEnvelopeCommandWithHandler(args, probe, out, resolver, launch, hostMutationExecutor(probe).Execute)
}

func hostEnvelopeCommandWithHandler(args []string, probe myce.MachineProbe, out io.Writer, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher, handler myce.HostMutationHandler) error {
	return hostEnvelopeCommandWithObservation(args, probe, out, resolver, launch, handler, nil)
}

func hostEnvelopeCommandWithObservation(args []string, probe myce.MachineProbe, out io.Writer, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher, handler myce.HostMutationHandler, observe func(context.Context, myce.HostInvocation) (myce.HostExclusionReport, error)) (retErr error) {
	return hostEnvelopeCommandWithTiming(args, probe, out, resolver, launch, handler, observe, myce.RealClock{}, waitPause)
}

// As with waitJobs, Clock decides the deadline and pause only paces polling.
// Context deadlines bound external I/O to the remaining command budget.
func hostEnvelopeCommandWithTiming(args []string, probe myce.MachineProbe, out io.Writer, resolver myce.HostIdentityResolver, launch myce.HostChildLauncher, handler myce.HostMutationHandler, observe func(context.Context, myce.HostInvocation) (myce.HostExclusionReport, error), clock myce.Clock, pause func(context.Context, time.Duration) error) (retErr error) {
	op := "status"
	if len(args) > 0 && (args[0] == "close" || args[0] == "open" || args[0] == "status") {
		op = args[0]
	}
	envelope := myce.UnknownHostEnvelope(op, clock)
	jsonOutput := false
	verdictRequested := false
	expectation := ""
	// Parse errors must still honor --json, even if it follows a bad flag.
	for _, arg := range args {
		if arg == "--json" || arg == "--json=true" {
			jsonOutput = true
		}
	}
	defer func() {
		if retErr != nil {
			if op == "close" && envelope.Accepted != nil {
				envelope.Errors = append(envelope.Errors, myce.HostEnvelopeError(retErr))
				retErr = errors.Join(myce.ErrHostReleaseUnconfirmed, retErr)
			}
			envelope.Errors = append(envelope.Errors, myce.HostEnvelopeError(retErr))
		}
		// A revision fence can fail after durable acceptance. Preserve that
		// acceptance, but reserve revision_mismatch for rejected CAS requests.
		if envelope.Accepted != nil {
			for i := range envelope.Errors {
				failure := &envelope.Errors[i]
				if failure.Component == "host_intent" && failure.Code == "revision_mismatch" {
					failure.Code, failure.Message = "revision_superseded", "revision_superseded"
				}
			}
		}
		if verdictRequested && op == "status" && (expectation == "closed" || expectation == "ready") {
			// Successful/unsatisfied observations already have their verdict.
			// Only early errors need evaluation here, after recording their cause.
			if envelope.HostRestVerdict == nil {
				envelope.RestEvidence.CollectionFailed = envelope.RestEvidence.CollectionFailed || retErr != nil
				envelope.ApplyRestVerdict(expectation, clock.Now())
			}
			if retErr == nil && envelope.Verdict != "PASS" {
				retErr = waitError(1, "host expectation not satisfied")
			}
		}
		if jsonOutput {
			if err := json.NewEncoder(out).Encode(envelope); err != nil {
				retErr = err
			}
		} else {
			state := "unknown"
			if envelope.Desired.State != nil {
				state = *envelope.Desired.State
			}
			_, err := fmt.Fprintf(out, "Host %s: local exclusion %s; settlement %s\n", state, envelope.LocalExclusion, envelope.Settlement)
			if err == nil && envelope.HostRestVerdict != nil {
				_, err = fmt.Fprintf(out, "%s: %s (%s)\n", envelope.Expectation, envelope.Verdict, strings.Join(envelope.Reasons, ", "))
			}
			if err != nil {
				retErr = err
			}
		}
	}()
	if len(args) == 0 || args[0] != op {
		return waitError(2, "usage: myce host close|open|status --local")
	}
	f := flag.NewFlagSet("host "+op, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	local := f.Bool("local", false, "control this host")
	user := f.String("worker-user", "", "installed worker account")
	config := f.String("config", "", "installed worker config")
	state := f.String("state-dir", "", "existing worker-owned host directory")
	reason := f.String("reason", "closed by operator", "close reason")
	grace := f.Duration("grace", 10*time.Second, "checkpoint grace")
	timeout := f.Duration("timeout", 30*time.Second, "total command budget")
	revision := f.Uint64("expect-revision", 0, "durable desired revision")
	waitReleased := f.Bool("wait-released", false, "wait for local exclusion")
	expect := &expectation
	f.StringVar(expect, "expect", "", "closed or ready")
	f.BoolVar(&verdictRequested, "verdict", false, "compute the product rest verdict")
	wait := f.Bool("wait", false, "wait for expectation")
	f.BoolVar(&jsonOutput, "json", jsonOutput, "JSON envelope")
	if err := f.Parse(args[1:]); err != nil {
		return waitError(2, "invalid host command flags")
	}
	hasRevision := false
	f.Visit(func(v *flag.Flag) {
		if v.Name == "expect-revision" {
			hasRevision = true
		}
	})
	if !*local || f.NArg() != 0 || *timeout <= 0 || *grace < 0 || *grace > time.Minute || op == "open" && !hasRevision || op != "close" && *waitReleased || op != "status" && (*wait || *expect != "" || verdictRequested) || (*wait || verdictRequested) && *expect == "" || *expect != "" && *expect != "closed" && *expect != "ready" {
		return waitError(2, "invalid host command arguments")
	}
	// This deadline is created once, before identity/config resolution.
	deadline := clock.Now().Add(*timeout)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	v, err := resolver.Resolve(*user, *config, *state)
	if err != nil {
		return err
	}
	mutate := func(m myce.HostMutation) (myce.HostMutationResult, error) {
		remaining := deadline.Sub(clock.Now())
		if remaining <= 0 || ctx.Err() != nil {
			return myce.HostMutationResult{}, myce.ErrHostDeadline
		}
		callCtx, callCancel := context.WithTimeout(ctx, remaining)
		defer callCancel()
		return v.Mutate(callCtx, m, launch, handler)
	}
	accepted := (*uint64)(nil)
	if op != "status" {
		m := myce.HostMutation{Operation: op, Reason: *reason, Grace: *grace, Timeout: *timeout, Expected: myce.HostRevision{Revision: *revision}}
		if op == "close" {
			m.Expected = myce.HostRevision{}
			if hasRevision {
				m.CloseExpected = &myce.HostRevision{Revision: *revision}
			}
		}
		r, mutationErr := mutate(m)
		if r.Revision > 0 {
			accepted = &r.Revision
		}
		envelope.Accepted = accepted
		// An accepted close may return an error because its dropped child
		// cannot prove privileged exclusion. Collect and record that proof in
		// the parent below; cleanup errors alone cannot decide release.
		if mutationErr != nil && !(op == "close" && accepted != nil && !errors.Is(mutationErr, myce.ErrHostDeadline) && ctx.Err() == nil) {
			// Read current durable state on CAS rejection; this accessor has no writes.
			if ctx.Err() == nil && (errors.Is(mutationErr, myce.ErrHostRevision) || errors.Is(mutationErr, myce.ErrPreviousCloseUnsettled) || accepted != nil) {
				if snapshot, se := mutate(myce.HostMutation{Operation: "status"}); se == nil && snapshot.Envelope != nil {
					envelope = *snapshot.Envelope
					if v.Privileged {
						envelope.ClearExclusion()
					}
				}
			}
			envelope.Command = op
			envelope.Accepted = accepted
			if errors.Is(mutationErr, myce.ErrHostRevision) && accepted == nil {
				envelope.Accepted = nil
				return &exitError{3, mutationErr}
			}
			if accepted != nil && !*waitReleased && !errors.Is(mutationErr, myce.ErrHostDeadline) && ctx.Err() == nil {
				envelope.Errors = append(envelope.Errors, myce.HostEnvelopeError(mutationErr))
				return nil
			}
			return mutationErr
		}
	}
	for {
		remaining := deadline.Sub(clock.Now())
		if remaining <= 0 || ctx.Err() != nil {
			return myce.ErrHostDeadline
		}
		readCtx, readCancel := context.WithTimeout(ctx, min(remaining, myce.HostObservationWindow))
		r, se := v.Mutate(readCtx, myce.HostMutation{Operation: "status"}, launch, handler)
		readCancel()
		if se != nil {
			envelope.Command = op
			envelope.Accepted = accepted
			return se
		}
		if r.Envelope == nil {
			return myce.ErrHostChild
		}
		envelope = *r.Envelope
		envelope.HostRestVerdict = nil
		envelope.Command = op
		envelope.Accepted = accepted
		if op == "close" && accepted != nil && (envelope.Desired.Revision == nil || *envelope.Desired.Revision != *accepted) {
			return myce.ErrHostRevision
		}
		if v.Privileged || observe != nil {
			envelope.ClearExclusion()
			observationBudget := min(deadline.Sub(clock.Now()), envelope.At.Add(myce.HostObservationWindow).Sub(clock.Now()))
			if observationBudget <= 0 {
				return myce.ErrHostDeadline
			}
			observeCtx, observeCancel := context.WithTimeout(ctx, observationBudget)
			var observation myce.HostExclusionReport
			var oe error
			if observe != nil {
				observation, oe = observe(observeCtx, v)
			} else {
				observation, oe = v.InspectExclusion(observeCtx, launch, handler, myce.LocalHostKernelReaders())
			}
			observeCancel()
			if oe != nil {
				if verdictRequested {
					return oe
				}
				envelope.Errors = append(envelope.Errors, myce.HostEnvelopeError(oe))
			} else {
				if v.Privileged && op == "close" && accepted != nil {
					if _, err := mutate(myce.HostMutation{Operation: "record-exclusion", Evidence: &observation}); err != nil {
						return err
					}
					// Recording relays proof to queue reconciliation. Read its
					// resulting settlement before rendering the final envelope.
					updated, err := mutate(myce.HostMutation{Operation: "status"})
					if err != nil {
						return err
					}
					if updated.Envelope == nil {
						return myce.ErrHostChild
					}
					envelope = *updated.Envelope
					envelope.Command, envelope.Accepted = op, accepted
					envelope.ClearExclusion()
				}
				envelope.ApplyExclusion(&observation, clock.Now())
			}
		}
		if v.Privileged || observe != nil {
			fence, err := mutate(myce.HostMutation{Operation: "resolve"})
			if err != nil {
				envelope.ClearExclusion()
				return err
			}
			if envelope.Desired.Revision == nil || fence.Revision != *envelope.Desired.Revision || envelope.Operation == nil || fence.Operation != *envelope.Operation {
				envelope.ClearExclusion()
				return myce.ErrHostRevision
			}
		}
		now := clock.Now()
		if ctx.Err() != nil || !now.Before(deadline) {
			return myce.ErrHostDeadline
		}
		if *expect == "closed" || verdictRequested {
			envelope.ApplyRestVerdict(*expect, now)
		} else {
			envelope.FinishObservation(now)
		}
		satisfied := true
		if op == "close" || *waitReleased {
			satisfied = envelope.LocalExclusion == "PASS"
		}
		if *expect == "ready" {
			satisfied = envelope.Ready != nil && *envelope.Ready
		}
		if *expect == "closed" || verdictRequested {
			satisfied = envelope.Verdict == "PASS"
			if !verdictRequested {
				envelope.HostRestVerdict = nil
			}
		}
		if satisfied {
			return nil
		}
		if op != "close" && !*wait && !*waitReleased {
			return waitError(1, "host expectation not satisfied")
		}
		// A further poll (or an error while waiting) is a new evaluation.
		envelope.HostRestVerdict = nil
		if err := pause(ctx, min(25*time.Millisecond, deadline.Sub(clock.Now()))); err != nil {
			return myce.ErrHostDeadline
		}
	}
}
