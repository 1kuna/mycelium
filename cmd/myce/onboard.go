package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

type enrollmentCLI struct {
	queue, pin, ticket, version, sha string
	timeout                          time.Duration
	config, root                     string
	forward                          []string
}

func parseEnrollmentCLI(args []string) (enrollmentCLI, error) {
	o := enrollmentCLI{version: myce.BuildVersion, timeout: 2 * time.Minute}
	for i := 2; i < len(args); i++ {
		arg, value, hasValue := strings.Cut(args[i], "=")
		if arg == "--enroll" {
			if hasValue && value != "true" {
				return o, errors.New("--enroll must be true")
			}
			continue
		}
		var target *string
		switch arg {
		case "--pin":
			target = &o.pin
		case "--ticket":
			target = &o.ticket
		case "--release-version":
			target = &o.version
		case "--release-sha":
			target = &o.sha
		case "--queue":
			target = &o.queue
		case "--root":
			target = &o.root
		case "--config":
			target = &o.config
		case "--enrollment-timeout":
			if !hasValue {
				i++
				if i >= len(args) {
					return o, errors.New("missing enrollment timeout")
				}
				value = args[i]
			}
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return o, errors.New("invalid enrollment timeout")
			}
			o.timeout = d
			continue
		}
		if target != nil {
			if !hasValue {
				i++
				if i >= len(args) {
					return o, errors.New("missing enrollment option value")
				}
				value = args[i]
			}
			*target = value
			if arg == "--queue" || arg == "--root" || arg == "--config" {
				o.forward = append(o.forward, arg, value)
			}
		} else {
			o.forward = append(o.forward, args[i])
		}
	}
	return o, nil
}
func enrollmentCommand(args []string, probe myce.MachineProbe, home string, lookup func(string) (myce.WorkerAccount, error)) error {
	o, err := parseEnrollmentCLI(args)
	if err != nil {
		return err
	}
	if hasEnrollmentFlag(o.forward, "--dry-run") || hasEnrollmentFlag(o.forward, "--joining-file") || hasEnrollmentFlag(o.forward, "--environment-file") || hasEnrollmentFlag(o.forward, "--from-worker-config") {
		return errors.New("enrollment owns joining authority; use the normal installer for previews or explicit M4 conversion")
	}
	if o.version != myce.BuildVersion {
		return errors.New("release version mismatch before install")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	actualHash, err := myce.FileSHA256(executable)
	if err != nil {
		return err
	}
	if o.sha != "" && o.sha != actualHash {
		return errors.New("release checksum refused before install")
	}
	config := o.config
	if config == "" {
		base := o.root
		if base == "" {
			base = filepath.Join(home, ".local", "share", "mycelium")
		}
		config = filepath.Join(base, "worker", "config.json")
	}
	config, err = filepath.Abs(config)
	if err != nil {
		return err
	}
	// Serialize onboarding without owning or replacing the worker's spool lock.
	lockDir := filepath.Join(home, ".cache", "mycelium", "bootstrap")
	if err = os.MkdirAll(lockDir, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(lockDir, "enroll.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another enrollment is active; wait for it to finish")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	signals, cancelSignals := signal.NotifyContext(context.Background(), syscall.SIGHUP, os.Interrupt, syscall.SIGTERM)
	defer cancelSignals()
	ctx, cancel := context.WithTimeout(signals, o.timeout)
	defer cancel()
	var old *myce.ServiceConfig
	if saved, e := myce.ReadServiceConfig(config); e == nil {
		old = &saved
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	clock := myce.RealClock{}
	var client myce.Client
	var drain *myce.HostMaintenanceStatus
	if old != nil {
		if old.Role != "worker" || old.Config != config || old.JoiningFile == "" || old.EnvironmentFile != "" || old.Detection.OS != probe.OS || old.Detection.Arch != probe.Arch {
			return errors.New("existing installation requires its saved pinned joining authority and matching platform; use the explicit conversion path")
		}
		authority, e := myce.ReadInvitation(old.JoiningFile)
		if e != nil {
			return e
		}
		if o.pin != "" && o.pin != authority.Fingerprint || o.queue != "" && o.queue != authority.Address {
			return errors.New("enrollment cannot change the saved mycelium identity")
		}
		client, e = authority.Client(authority.Address)
		if e != nil {
			return e
		}
		defer client.HTTP.CloseIdleConnections()
		installedHash, e := myce.FileSHA256(old.Binary)
		if e != nil {
			return e
		}
		if installedHash == actualHash && old.Enrollment {
			result, e := myce.WaitEnrollment(ctx, *old, client, myce.EnrollmentWait{Clock: clock, Timeout: o.timeout, Command: probe.Command})
			if e != nil {
				return e
			}
			fmt.Println(result.Summary(*old))
			return nil
		}
		status, e := myce.PrepareEnrollmentUpgrade(ctx, *old, client, clock, o.timeout, nil)
		if e != nil {
			return e
		}
		drain = &status
		o.forward = withoutEnrollmentSelection(o.forward)
		o.forward = append(o.forward, "--config", config)
	} else {
		// Keep redeemed authority private and durable if setup is interrupted.
		// It is outside the worker state roots and never printed.
		pending := filepath.Join(lockDir, "invitation.json")
		authority, e := myce.ReadInvitation(pending)
		if e == nil {
			if authority.Address != o.queue || authority.Fingerprint != o.pin {
				return errors.New("pending enrollment belongs to another mycelium; preserve its private authority and resolve explicitly")
			}
		} else {
			if !errors.Is(e, os.ErrNotExist) {
				return e
			}
			authority, e = myce.RedeemInvitation(ctx, o.queue, o.pin, o.ticket)
			if e != nil {
				return e
			}
			if e = myce.WriteInvitation(pending, authority); e != nil {
				return e
			}
		}
		client, e = authority.Client(authority.Address)
		if e != nil {
			return e
		}
		defer client.HTTP.CloseIdleConnections()
		o.forward = append(o.forward, "--joining-file", pending)
		if !hasEnrollmentFlag(o.forward, "--name") && !hasEnrollmentFlag(o.forward, "--host-id") {
			identityPath := filepath.Join(lockDir, "hypha-id")
			id, e := os.ReadFile(identityPath)
			if errors.Is(e, os.ErrNotExist) {
				name, _ := os.Hostname()
				if name == "" {
					name = "hypha"
				}
				id = []byte(name + "-" + myce.ID()[:12])
				e = os.WriteFile(identityPath, id, 0600)
			}
			if e != nil {
				return e
			}
			o.forward = append(o.forward, "--name", string(id), "--host-id", string(id))
		} else if hasEnrollmentFlag(o.forward, "--name") != hasEnrollmentFlag(o.forward, "--host-id") {
			return errors.New("explicit enrollment identity requires both --name and --host-id")
		}
	}
	// This dispatch is the existing installer, with B2/B3/B4 options unchanged.
	installArgs := append([]string{"install", "worker"}, o.forward...)
	installArgs = append(installArgs, "--enrollment")
	if err = setupCommandWithAccountLookup(installArgs, probe, home, lookup); err != nil {
		return err
	}
	c, err := myce.ReadServiceConfig(config)
	if err != nil {
		return err
	}
	result, err := myce.WaitEnrollment(ctx, c, client, myce.EnrollmentWait{Clock: clock, Timeout: o.timeout, Command: probe.Command})
	if err != nil {
		return err
	}
	if drain != nil && drain.Policy.Mode == "drained" {
		if err = myce.FinishEnrollmentUpgrade(ctx, c, client, *drain); err != nil {
			return err
		}
		result, err = myce.WaitEnrollment(ctx, c, client, myce.EnrollmentWait{Clock: clock, Timeout: o.timeout, Command: probe.Command})
		if err != nil {
			return err
		}
	}
	fmt.Println(result.Summary(c))
	return nil
}
func hasEnrollmentFlag(args []string, key string) bool {
	for _, arg := range args {
		if arg == key || strings.HasPrefix(arg, key+"=") {
			return true
		}
	}
	return false
}
func withoutEnrollmentSelection(args []string) []string {
	out := []string{}
	for i := 0; i < len(args); i++ {
		key, _, eq := strings.Cut(args[i], "=")
		if key == "--root" || key == "--config" || key == "--queue" {
			if !eq {
				i++
			}
			continue
		}
		out = append(out, args[i])
	}
	return out
}
