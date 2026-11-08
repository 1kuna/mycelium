package myce

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// BoundSetupCommand retains the setup probe allowance for ordinary commands.
// Lifecycle jobs instead belong to the manager: killing its client before the
// service timeout leaves a job still changing manager state during rollback.
func BoundSetupCommand(command func(context.Context, string, ...string) ([]byte, error)) func(context.Context, string, ...string) ([]byte, error) {
	return func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if filepath.Base(name) == "launchctl" && len(args) > 0 && args[0] == "bootout" && ctx.Value(launchdStopWaitKey{}) == true {
			return command(ctx, name, args...)
		}
		stop, start := false, false
		if filepath.Base(name) == "systemctl" {
			for _, arg := range args {
				switch arg {
				case "stop":
					stop = true
				case "start", "restart", "try-restart":
					// A start may queue behind a still-pending stop.
					stop, start = true, true
				case "--now":
					stop = true
					start = !contains(args, "disable")
				}
			}
		}
		// Timers own no process and have no service stop/start properties.
		// Retiring their schedule is an ordinary bounded manager transaction.
		if len(args) > 0 && strings.HasSuffix(args[len(args)-1], ".timer") {
			stop, start = false, false
		}
		if !stop && !start {
			bounded, cancel := SetupTimeout(ctx)
			defer cancel()
			return command(bounded, name, args...)
		}
		unit := args[len(args)-1]
		query, cancel := SetupTimeout(ctx)
		began := time.Now()
		data, err := command(query, name, "--user", "show", unit, "--property=TimeoutStopUSec", "--property=TimeoutStartUSec")
		elapsed := time.Since(began)
		cancel()
		// When the manager cannot describe its deadline, no guessed client
		// deadline is safe: it could expire while systemd still owns the job.
		// Use the same manager-owned wait as an infinite unit timeout, keeping
		// caller cancellation. Do not log manager output (it may be private).
		managerWait := func() ([]byte, error) {
			log.Print("warning: effective systemd service timeout unavailable or invalid; waiting for systemd completion with caller cancellation and no additional client deadline")
			return command(ctx, name, args...)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return managerWait()
		}
		budget := time.Duration(0)
		unbounded := false
		for _, property := range []struct {
			name string
			use  bool
		}{{"TimeoutStopUSec", stop}, {"TimeoutStartUSec", start}} {
			if !property.use {
				continue
			}
			value := ""
			for _, line := range strings.Split(string(data), "\n") {
				if v, ok := strings.CutPrefix(line, property.name+"="); ok {
					value = strings.TrimSpace(v)
				}
			}
			if value == "infinity" {
				unbounded = true
				continue
			}
			// systemctl formats timespans as e.g. "1min 30s" or "45s".
			duration, err := systemdTimeout(value)
			if err != nil || duration < 0 || duration > time.Duration(1<<63-1)-budget {
				return managerWait()
			}
			unbounded = unbounded || duration == 0
			budget += duration
		}
		if unbounded || budget == 0 {
			// systemd's infinity/zero means no manager deadline; explicit
			// operator cancellation still owns this wait.
			return command(ctx, name, args...)
		}
		// Allow two measured manager round trips for job dispatch and completion.
		// This scales with this manager's latency rather than a fleet-tuned grace.
		margin := 2 * elapsed
		if margin > time.Duration(1<<63-1)-budget {
			return managerWait()
		}
		bounded, cancel := context.WithTimeout(ctx, budget+margin)
		defer cancel()
		return command(bounded, name, args...)
	}
}

func systemdTimeout(value string) (time.Duration, error) {
	if value == "" {
		return 0, errors.New("missing service timeout")
	}
	var total time.Duration
	for _, part := range strings.Fields(value) {
		part = strings.ReplaceAll(part, "min", "m")
		// systemd's formatted long units use Gregorian average years/months.
		for _, unit := range []struct {
			suffix string
			hours  float64
		}{{"month", 730.5}, {"y", 8766}, {"w", 168}, {"d", 24}} {
			if number, ok := strings.CutSuffix(part, unit.suffix); ok {
				n, err := strconv.ParseFloat(number, 64)
				if err != nil {
					return 0, err
				}
				part = strconv.FormatFloat(n*unit.hours, 'f', -1, 64) + "h"
				break
			}
		}
		d, err := time.ParseDuration(part)
		if err != nil || d < 0 || d > time.Duration(1<<63-1)-total {
			return 0, errors.New("invalid service timeout")
		}
		total += d
	}
	return total, nil
}

// A bootout may acknowledge before the label disappears. Its command and the
// subsequent observation share the manager's stop budget, not a setup probe.
type launchdStopWaitKey struct{}

const launchdStopPoll = 20 * time.Millisecond

func (m installManager) launchdStopContext(ctx context.Context, u installUnit) (context.Context, context.CancelFunc) {
	query, cancel := SetupTimeout(ctx)
	began := time.Now()
	data, err := m.command(query, "launchctl", "print", u.target())
	elapsed := time.Since(began)
	cancel()
	// The loaded definition is authoritative: the plist may already have been
	// replaced on disk, and an absent ExitTimeOut uses a system-defined default.
	var seconds int64
	found := false
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), " = ")
			if ok && key == "exit timeout" {
				seconds, err = strconv.ParseInt(value, 10, 64)
				found = err == nil && seconds >= 0
				break
			}
		}
	}
	ctx = context.WithValue(ctx, launchdStopWaitKey{}, true)
	// Two measured manager round trips cover dispatch/completion, plus one
	// observation tick to see asynchronous label removal after the exit bound.
	margin := 2*elapsed + launchdStopPoll
	if found && seconds > 0 && seconds <= int64((time.Duration(1<<63-1)-margin)/time.Second) {
		return context.WithTimeout(ctx, time.Duration(seconds)*time.Second+margin)
	}
	if !found || seconds != 0 {
		log.Print("warning: effective launchd exit timeout unavailable or invalid; waiting for launchd completion with caller cancellation and no additional client deadline")
	}
	// ExitTimeOut=0 is infinity. Unknown deadlines also belong to the manager;
	// guessing a client cutoff can abandon an active job and race recovery.
	return context.WithCancel(ctx)
}
