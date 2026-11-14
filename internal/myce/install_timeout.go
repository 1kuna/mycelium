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
// Lifecycle jobs instead belong to the manager: killing its client before
// completion leaves a job still changing manager state during rollback.
func BoundSetupCommand(command func(context.Context, string, ...string) ([]byte, error)) func(context.Context, string, ...string) ([]byte, error) {
	return func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if filepath.Base(name) == "launchctl" && len(args) > 0 {
			// ExitTimeOut bounds the worker's TERM grace, not launchd's
			// transaction, disk I/O, label removal or subsequent activation.
			// launchd exposes no deadline for those operations. Let the manager
			// finish, retaining explicit caller cancellation rather than killing
			// its client on an unrelated setup-probe deadline.
			switch args[0] {
			case "bootout", "bootstrap", "kickstart", "kill":
				return command(ctx, name, args...)
			}
			if ctx.Value(launchdLifecycleKey{}) == true {
				return command(ctx, name, args...)
			}
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

// Lifecycle observations share caller cancellation with their manager operation;
// an ordinary setup-probe timeout must not interrupt label or activation proof.
type launchdLifecycleKey struct{}

const launchdStopPoll = 20 * time.Millisecond
