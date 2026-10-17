package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func hostCommand(args []string, probe myce.MachineProbe, out io.Writer) error {
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	unit := f.String("unit", "", "target one registered Mycelium worker unit (Linux only)")
	dir := f.String("host-state-dir", "", "shared local host control directory (default ~/.local/share/myce)")
	config := f.String("config", "", "installed worker config (auto-detected by default)")
	state := f.String("state-dir", "", "worker spool to adopt from before this upgrade")
	hostname, _ := os.Hostname()
	host := f.String("host-id", hostname, "physical host identity (for adopted worker state)")
	name := f.String("name", hostname, "worker name (for adopted worker state)")
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue address")
	reason := f.String("reason", "disabled by operator", "operator reason")
	grace := f.Duration("grace", 10*time.Second, "checkpoint grace, from 0 to 1m")
	timeout := f.Duration("timeout", 0, "overall disable deadline (default grace + 20s)")
	now := f.Bool("now", false, "kill owned work immediately")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected host-control arguments")
	}
	if len(args) > 0 && args[0] == "enable" {
		return errors.New("myce enable is retired; use myce host open --expect-revision with the v3 host CLI (S08)")
	}
	if os.Geteuid() == 0 {
		return myce.ErrHostIdentity
	}
	if *now {
		*grace = 0
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	if *dir == "" {
		*dir, err = myce.HostStateDirectory()
		if err != nil {
			return err
		}
	}

	if *config != "" {
		installed, e := myce.ReadServiceConfig(*config)
		// A broken config must not delay disable's durable local fence.
		if e != nil && args[0] != "disable" {
			return e
		}
		explicitDir, explicitHost := false, false
		f.Visit(func(v *flag.Flag) {
			if v.Name == "host-state-dir" {
				explicitDir = true
			}
			if v.Name == "host-id" {
				explicitHost = true
			}
		})
		if !explicitDir && installed.HostStateDir != "" {
			*dir = installed.HostStateDir
		}
		if !explicitHost && installed.HostID != "" {
			*host = installed.HostID
		}
	}
	if *unit != "" {
		return errors.New("unit selection is diagnostic only; control the whole host")
	}
	token := env("MYCE_TOKEN", "")
	var c myce.ServiceConfig
	service := ""
	prepare := func() error {
		if *config == "" {
			candidate := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
			if _, e := os.Stat(candidate); e == nil {
				*config = candidate
			} else if !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
		if *config != "" {
			c, err = myce.ReadServiceConfig(*config)
			if err != nil {
				return err
			}
			if c.Role != "worker" {
				return errors.New("host control requires a worker config")
			}
			if token == "" {
				token, _ = myce.ReadServiceToken(c)
			}
			*state, *name, *queue = c.State, c.Name, c.Queue
			if c.EnvironmentFile != "" {
				_, address, e := myce.ReadFleetEnvironment(c.EnvironmentFile)
				if e != nil {
					return e
				}
				*queue = address
			}
		}
		if *state == "" {
			candidate := filepath.Join(home, ".local", "share", "myce", "worker")
			if _, e := os.Stat(filepath.Join(candidate, "state.json")); e == nil {
				*state = candidate
			}
		}
		if *state != "" && c.State == "" {
			return errors.New("legacy adoption is retired; run myce host convert offline")
		}
		service = c.Service
		if service == "" {
			if probe.OS == "linux" {
				service = filepath.Join(home, ".config", "systemd", "user", "myce-worker.service")
			} else {
				service = filepath.Join(home, "Library", "LaunchAgents", "local.myce.worker.plist")
			}
			if _, e := os.Stat(service); errors.Is(e, os.ErrNotExist) {
				service = ""
			} else if e != nil {
				return e
			}
		}
		return nil
	}
	units := []string{}
	resolveUnits := func() error {
		if probe.OS != "linux" {
			return nil
		}
		var e error
		units, e = myce.WorkerUnits(*dir, *unit)
		if e != nil {
			return e
		}
		if len(units) == 0 && service != "" {
			candidate := filepath.Base(service)
			if !myce.ValidWorkerUnit(candidate) {
				return errors.New("refusing non-Mycelium worker unit")
			}
			units = []string{candidate}
		}
		return nil
	}
	ctx := context.Background()
	if *timeout < 0 {
		return errors.New("timeout must be positive")
	}
	h := myce.HostControl{Docker: true, Timeout: *timeout, Dir: *dir, Prepare: prepare}
	if args[0] == "disable" {
		h.ResolveServices = func() (map[string]myce.HostService, error) {
			if err := resolveUnits(); err != nil {
				return nil, err
			}
			services := map[string]myce.HostService{}
			for _, u := range units {
				services[u] = &myce.SystemdHostService{Unit: u, Command: probe.Command}
			}
			return services, nil
		}
		h.StopSecondary = func(ctx context.Context) error {
			if probe.OS != "linux" {
				return hostService(ctx, probe, service, false)
			}
			return nil
		}
		r, cleanupErr := h.Disable(ctx, *reason, *grace)

		reportedUnits := []string{}
		for u := range r.Units {
			reportedUnits = append(reportedUnits, u)
		}
		slices.Sort(reportedUnits)
		for _, u := range reportedUnits {
			result := r.Units[u]
			if result == "" {
				result = "unconfirmed (overall cleanup did not finish)"
			}
			fmt.Fprintf(out, "Unit %s: %s\n", u, result)
		}
		if r.BestEffort {
			fmt.Fprintln(out, "Not running under the systemd service; ownership is best-effort.")
		}
		if r.Confirmed {
			fmt.Fprintln(out, "Host cleanup confirmed: empty systemd worker cgroup and Mycelium Docker execution gone (or Docker not applicable).")
		}
		if r.Docker != "" {
			fmt.Fprintln(out, "Docker:", r.Docker)
		}
		err = cleanupErr
		confirmation := "none"
		if err != nil {
			confirmation = err.Error()
		}
		fmt.Fprintf(out, "Disabled: stopped %d owned process groups; unconfirmed: %s. State: %s.\n", r.Stopped, confirmation, filepath.Join(*dir, "host-state.json"))
		return err
	}
	if err = prepare(); err != nil {
		return err
	}
	if err = h.Enable(ctx); err != nil {
		return err
	}
	if err = resolveUnits(); err != nil {
		return err
	}
	if probe.OS != "linux" {
		units = []string{service}
	}
	var startErrors []error
	for _, u := range units {
		if e := hostService(ctx, probe, u, true); e != nil {
			startErrors = append(startErrors, e)
		}
	}
	if err = errors.Join(startErrors...); err != nil {
		fmt.Fprintf(out, "Enabled locally. Start worker: %s\n", hostServiceCommand(probe.OS, service, true))
		return err
	}
	if service == "" && len(units) == 0 {
		commands, e := myce.HostResumeCommands(*dir)
		if e != nil {
			return e
		}
		if len(commands) == 0 {
			fmt.Fprintln(out, "Enabled locally. Start worker: myce join (with the original worker options).")
		}
		for _, cmd := range commands {
			fmt.Fprintln(out, "Enabled locally. Start worker:", shellCommand(cmd))
		}
	} else {
		fmt.Fprintln(out, "Enabled: worker service start requested; verify myce status shows joined.")
	}
	return nil
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
