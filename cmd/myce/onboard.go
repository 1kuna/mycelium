package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	ticketStdin, allowDowngrade      bool
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
		case "--ticket-stdin", "--allow-downgrade":
			enabled := true
			if hasValue {
				if value != "true" && value != "false" {
					return o, errors.New("invalid enrollment boolean")
				}
				enabled = value == "true"
			}
			if arg == "--ticket-stdin" {
				o.ticketStdin = enabled
			} else {
				o.allowDowngrade = enabled
			}
			continue
		case "--pin":
			target = &o.pin
		case "--ticket":
			return o, errors.New("enrollment ticket requires --ticket-stdin; argv tickets are refused")
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
func enrollmentCommand(args []string, probe myce.MachineProbe, home string, lookup func(string) (myce.WorkerAccount, error)) (resultErr error) {
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
	if o.ticketStdin {
		// Read a bounded private channel, never argv or the environment.
		data, e := io.ReadAll(io.LimitReader(os.Stdin, 66))
		if e != nil || len(data) > 65 {
			return errors.New("invalid enrollment ticket input")
		}
		o.ticket = strings.TrimSpace(string(data))
		if len(o.ticket) != 64 {
			return errors.New("invalid enrollment ticket input")
		}
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
	// Each phase owns its budget. Time spent draining never consumes the
	// install/readiness budget; the existing installer has its own setup timeout.
	ctx := signals
	waitReady := func(c myce.ServiceConfig, client myce.Client) (myce.EnrollmentResult, error) {
		phase, cancel := context.WithTimeout(signals, o.timeout)
		defer cancel()
		return myce.WaitEnrollment(phase, c, client, myce.EnrollmentWait{Clock: myce.RealClock{}, Timeout: o.timeout, Command: probe.Command})
	}
	var old *myce.ServiceConfig
	if saved, e := myce.ReadServiceConfig(config); e == nil {
		old = &saved
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	clock := myce.RealClock{}
	var client myce.Client
	var drain *myce.HostMaintenanceStatus
	var previousHash, previousVersion string
	var rollback func(context.Context) error
	var resumed bool
	defer func() {
		if resultErr == nil || old == nil || drain == nil || drain.Policy.Mode != "drained" || drain.Policy.UpdatedAt.IsZero() {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		recoveryFailed := func(reason error) {
			resultErr = fmt.Errorf("upgrade failed; host could not be resumed: %v; rerun to recover: %w", reason, resultErr)
		}
		// If final verification fails after resume, regain quiescence before rollback.
		if resumed {
			status, e := myce.PrepareEnrollmentUpgrade(cleanup, *old, client, clock, o.timeout, nil)
			if e != nil {
				recoveryFailed(e)
				return
			}
			drain = &status
		}
		if rollback != nil {
			if e := rollback(cleanup); e != nil {
				recoveryFailed(e)
				return
			}
		}
		hash, e := myce.FileSHA256(old.Binary)
		if e != nil || hash != previousHash || strings.Contains(resultErr.Error(), "rollback incomplete") {
			recoveryFailed(errors.New("previous installation could not be restored; preserve the drain and repair the install"))
			return
		}
		if old.Enrollment {
			_, e = myce.WaitEnrollment(cleanup, *old, client, myce.EnrollmentWait{Clock: clock, Timeout: 2 * time.Minute, Command: probe.Command, Version: previousVersion})
			if e != nil {
				recoveryFailed(fmt.Errorf("previous release did not become ready: %w", e))
				return
			}
		}
		if e = myce.FinishEnrollmentUpgrade(cleanup, *old, client, *drain); e != nil {
			recoveryFailed(e)
			return
		}
		resultErr = fmt.Errorf("upgrade failed; host resumed on previous release: %w", resultErr)
	}()
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
		versionCtx, cancelVersion := context.WithTimeout(signals, 10*time.Second)
		installedVersion, e := installedEnrollmentVersion(versionCtx, old.Binary)
		cancelVersion()
		if e != nil {
			return e
		}
		previousHash, previousVersion = installedHash, installedVersion
		if e = myce.CheckReleaseUpgrade(o.version, installedVersion, o.allowDowngrade, installedHash == actualHash); e != nil {
			return e
		}
		if old.State == "" {
			return errors.New("upgrade state directory required")
		}
		pendingDrain, e := myce.ResolveEnrollmentUpgrade(ctx, *old, client)
		if e != nil {
			return e
		}
		if installedHash == actualHash && old.Enrollment {
			// An earlier process may have installed new bytes without leaving a durable
			// installer snapshot. Never claim that this invocation can roll those back.
			if pendingDrain.Policy.Mode == "drained" {
				drain = &pendingDrain
			}
			result, e := waitReady(*old, client)
			if e != nil {
				if drain != nil {
					drain = nil
					return fmt.Errorf("upgrade failed; rollback unavailable for the already-installed binary; host could not be resumed; rerun to recover: %w", e)
				}
				return e
			}
			if pendingDrain.Policy.Mode == "drained" {
				if e = myce.FinishEnrollmentUpgrade(ctx, *old, client, pendingDrain); e != nil {
					return e
				}
				resumed = true
				result, e = waitReady(*old, client)
				if e != nil {
					return e
				}
			}
			if e = cleanupPendingEnrollment(lockDir, *old); e != nil {
				return e
			}
			fmt.Println("Same release: installer flags are not reapplied; use myce install worker --config with the desired flags to change settings.")
			fmt.Println(result.Summary(*old))
			return nil
		}
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
			phase, cancel := context.WithTimeout(signals, o.timeout)
			authority, e = myce.RedeemInvitation(phase, o.queue, o.pin, o.ticket)
			cancel()
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
	install := func(installCtx context.Context, c myce.ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error)) error {
		// The existing installer calls us only after hardware, storage, policy,
		// session and candidate service-environment preflights have succeeded.
		if old != nil {
			drainCtx, cancelDrain := context.WithTimeout(signals, o.timeout)
			status, e := myce.PrepareEnrollmentUpgrade(drainCtx, *old, client, clock, o.timeout, nil)
			cancelDrain()
			if e != nil {
				return e
			}
			drain = &status
		}
		var e error
		rollback, e = myce.InstallWithRollback(installCtx, c, executable, token, command)
		return e
	}
	if err = setupCommandWithInstaller(installArgs, probe, home, lookup, install); err != nil {
		return err
	}
	c, err := myce.ReadServiceConfig(config)
	if err != nil {
		return err
	}
	// The installed joining file is durable now. Drop the bootstrap copy even
	// if later readiness fails, and also on a resumed same-version invocation.
	if err = cleanupPendingEnrollment(lockDir, c); err != nil {
		return err
	}
	result, err := waitReady(c, client)
	if err != nil {
		return err
	}
	if drain != nil && drain.Policy.Mode == "drained" {
		if err = myce.FinishEnrollmentUpgrade(ctx, c, client, *drain); err != nil {
			return err
		}
		resumed = true
		result, err = waitReady(c, client)
		if err != nil {
			return err
		}
	}
	fmt.Println(result.Summary(c))
	return nil
}

func installedEnrollmentVersion(ctx context.Context, binary string) (string, error) {
	cmd := exec.CommandContext(ctx, binary, "version", "--json")
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != "MYCE_TOKEN" {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	data, err := cmd.Output()
	if err != nil {
		return "", errors.New("cannot inspect installed release version")
	}
	var info myce.VersionInfo
	if json.Unmarshal(data, &info) != nil || info.Version == "" {
		return "", errors.New("installed release version unavailable")
	}
	return info.Version, nil
}

func cleanupPendingEnrollment(lockDir string, c myce.ServiceConfig) error {
	path := filepath.Join(lockDir, "invitation.json")
	pending, err := myce.ReadInvitation(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	installed, err := myce.ReadInvitation(c.JoiningFile)
	if err != nil {
		return err
	}
	if c.JoiningFile == path || pending != installed {
		return errors.New("pending joining authority differs from installation; preserve private invitation")
	}
	return os.Remove(path)
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
