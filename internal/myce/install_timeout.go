package myce

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// BoundSetupCommand retains the setup probe allowance for ordinary commands.
// Lifecycle jobs instead belong to systemd: killing its client before the unit
// timeout leaves a job still changing manager state during rollback.
func BoundSetupCommand(command func(context.Context, string, ...string) ([]byte, error)) func(context.Context, string, ...string) ([]byte, error) {
	return func(ctx context.Context, name string, args ...string) ([]byte, error) {
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
		if err != nil {
			return nil, fmt.Errorf("cannot read effective service timeouts: %w", err)
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
				return nil, errors.New("invalid effective service timeout")
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
			return nil, errors.New("effective service timeout overflow")
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
