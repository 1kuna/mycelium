package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"mycelium/internal/myce"
)

func setupCommand(args []string) error {
	home, e := os.UserHomeDir()
	if e != nil {
		return e
	}
	return setupCommandWithProbe(args, myce.LocalMachineProbe(), home)
}
func setupCommandWithProbe(args []string, probe myce.MachineProbe, home string) error {
	return setupCommandWithAccountLookup(args, probe, home, myce.LookupWorkerAccount)
}
func setupCommandWithAccountLookup(args []string, probe myce.MachineProbe, home string, lookup func(string) (myce.WorkerAccount, error)) error {
	return setupCommandWithInstaller(args, probe, home, lookup, myce.Install)
}

// Storage validation has per-operation progress bounds. Keep cancellation across
// the install; service waits follow systemd and ordinary probes stay bounded.
func boundedSetupProbe(probe myce.MachineProbe) myce.MachineProbe {
	command, commandEnv := probe.Command, probe.CommandEnv
	if command != nil {
		probe.Command = myce.BoundSetupCommand(command)
	}
	if commandEnv != nil {
		probe.CommandEnv = func(ctx context.Context, env map[string]string, name string, args ...string) ([]byte, error) {
			return myce.BoundSetupCommand(func(ctx context.Context, name string, args ...string) ([]byte, error) {
				return commandEnv(ctx, env, name, args...)
			})(ctx, name, args...)
		}
	}
	return probe
}

func setupCommandWithInstaller(args []string, probe myce.MachineProbe, home string, lookup func(string) (myce.WorkerAccount, error), install func(context.Context, myce.ServiceConfig, string, string, func(context.Context, string, ...string) ([]byte, error)) error) error {
	setupContext := myce.SetupTimeout
	if len(args) > 0 && (args[0] == "install" || args[0] == "uninstall") {
		setupContext = context.WithCancel
		probe = boundedSetupProbe(probe)
	}
	return setupCommandWithInstallerTimeout(args, probe, home, lookup, func(ctx context.Context, c myce.ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error)) error {
		if err := ensureSetupServiceSession(ctx, c, command); err != nil {
			return err
		}
		return install(ctx, c, executable, token, command)
	}, setupContext)
}

func ensureSetupServiceSession(ctx context.Context, c myce.ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	if c.Role != "worker" || c.AllowDeviceless {
		return nil
	}
	_, err := myce.EnsureOnboardingServiceSession(ctx, c, command)
	return err
}

func setupCommandWithInstallerTimeout(args []string, probe myce.MachineProbe, home string, lookup func(string) (myce.WorkerAccount, error), install func(context.Context, myce.ServiceConfig, string, string, func(context.Context, string, ...string) ([]byte, error)) error, setupTimeout func(context.Context) (context.Context, context.CancelFunc)) error {
	if len(args) >= 2 && args[0] == "install" && args[1] == "worker" && hasEnrollmentFlag(args[2:], "--enroll") {
		return enrollmentCommand(args, probe, home, lookup)
	}
	var e error
	if len(args) == 0 || (args[0] != "install" && args[0] != "uninstall" && args[0] != "doctor") {
		return errors.New("usage: myce install|uninstall|doctor")
	}
	action := args[0]
	role := "worker"
	flags := args[1:]
	if action == "install" {
		if len(flags) == 0 {
			return errors.New("usage: myce install worker|queue --nas DIR [--dry-run]")
		}
		role = flags[0]
		flags = flags[1:]
	}
	f := flag.NewFlagSet(action, flag.ContinueOnError)
	if action == "doctor" {
		f.Usage = func() {
			fmt.Fprintln(f.Output(), "Usage of doctor:")
			fmt.Fprintln(f.Output(), "Consumer diagnosis (read-only): myce doctor --project [--adapter FILE] [--entry NAME] [--notify EXECUTABLE] [--json]")
			fmt.Fprintln(f.Output(), "Installation diagnosis: myce doctor [options] (installed config)")
			f.PrintDefaults()
		}
	}
	instance := f.String("instance", "", "Linux worker instance")
	root := f.String("root", "", "installation root")
	dry := f.Bool("dry-run", false, "print plan without installing")
	redetect := f.Bool("redetect", false, "re-detect hardware without changing saved selectors or per-device budgets")
	f.Bool("force", false, "deprecated compatibility option; explicit flags authorize changes")
	f.BoolFunc("local-disable-receipts", "retired; reinstall without this option", func(string) error { return myce.ErrLegacyLocalDisableReceipts })
	// Parsed values are separate from the effective config. These defaults are
	// used only for a fresh plan, after saved-config discovery has failed.
	nasDefault := "/mnt/nas"
	if probe.OS == "darwin" {
		nasDefault = "/Volumes/NAS"
	}
	hostname, _ := os.Hostname()
	values := myce.ServiceConfig{NAS: nasDefault, Queue: env("MYCE_QUEUE", "http://127.0.0.1:7331"), Name: hostname,

		EnvironmentFile: filepath.Join(home, ".config", "myce", "fleet.env"), RequireMountedNAS: probe.OS == "linux",
		Detection: myce.Detection{MemoryMode: "auto"}}
	defaults := values
	myce.RegisterInstallFlags(f, &values)
	config := &values.Config
	if e = f.Parse(flags); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected setup arguments")
	}
	signals, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGHUP, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := setupTimeout(signals)
	defer cancel()
	if action != "install" {
		if *config == "" {
			base := *root
			if base == "" {
				base = filepath.Join(home, ".local", "share", "mycelium")
			}
			*config = filepath.Join(base, "worker", "config.json")
		}
		c, e := myce.ReadServiceConfig(*config)
		if e != nil {
			if action == "doctor" && errors.Is(e, os.ErrNotExist) {
				return fmt.Errorf("no install found; run myce install first (or use --config for an existing installation): %w", e)
			}
			return e
		}
		if action == "uninstall" {
			fmt.Printf("Uninstall %s: stop boot service, move %s and %s to recoverable Trash\n", c.Role, c.Service, filepath.Dir(c.Config))
			return myce.Uninstall(ctx, c, *dry, probe.Command)
		}
		token := os.Getenv("MYCE_TOKEN")
		if token == "" {
			token, e = myce.ReadServiceToken(c)
			if e != nil {
				return e
			}
		}
		failed := false
		for _, check := range myce.Doctor(ctx, c, myce.LocalDoctorProbeWithMachine(token, probe)) {
			state := "OK"
			if !check.OK {
				state = "FAIL"
				failed = true
			}
			fmt.Printf("%s %s: %s\n", state, check.Name, check.Detail)
		}
		if failed {
			return errors.New("doctor found problems")
		}
		return nil
	}
	// Locate the saved configuration before detection or applying fresh-install
	// defaults. Flag presence, rather than its value, defines the upgrade overlay.
	explicit := map[string]bool{}
	f.Visit(func(v *flag.Flag) { explicit[v.Name] = true })
	if explicit["config"] && *config == "" {
		return errors.New("usage: --config must not be empty")
	}
	if explicit["config"] && (explicit["root"] || explicit["instance"]) {
		return errors.New("--config selects the installation; omit --root and --instance")
	}
	configPath := *config
	if configPath == "" {
		base := *root
		if base == "" {
			base = filepath.Join(home, ".local", "share", "mycelium")
		}
		directory := role
		if *instance != "" {
			directory = "worker-" + *instance
		}
		configPath = filepath.Join(base, directory, "config.json")
	}
	configPath, e = filepath.Abs(configPath)
	if e != nil {
		return e
	}
	var installed *myce.ServiceConfig
	if old, err := myce.ReadServiceConfig(configPath); err == nil {
		if old.Role != role || old.Config != configPath || old.Detection.OS != probe.OS || old.Detection.Arch != probe.Arch {
			return errors.New("installed role, config and platform identity cannot change")
		}
		if warning := old.DropRetiredDocker(); warning != "" {
			fmt.Fprintln(os.Stderr, warning)
		}
		installed = &old
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else if explicit["config"] {
		return errors.New("--config must select an existing installation; use --root for a fresh install")
	}
	var c myce.ServiceConfig
	var hardware *myce.HardwarePreflight
	if installed != nil {
		c = *installed
	} else {
		d := myce.Detection{OS: probe.OS, Arch: probe.Arch}
		if role == "worker" {
			if values.Detection.Platform != "" {
				d.Platform, d.MemoryMode, d.MemoryGB, d.ReserveGB = values.Detection.Platform, values.Detection.MemoryMode, values.Detection.MemoryGB, values.Detection.ReserveGB
			} else {
				var detected myce.HardwarePreflight
				detected, e = myce.PreflightHardware(ctx, values, probe)
				d = detected.Detection
				hardware = &detected
				if e != nil {
					hardware = nil
					if !values.AllowDeviceless {
						return e
					}
					d = myce.Detection{OS: probe.OS, Arch: probe.Arch, Platform: "fake", MemoryMode: "fixed", MemoryGB: values.Detection.MemoryGB, ReserveGB: values.Detection.ReserveGB}
					if d.MemoryGB <= 0 {
						return errors.New("deviceless worker requires a positive --memory-gb CPU/test budget")
					}
				}
			}
		}
		var account myce.WorkerAccount
		if role == "worker" {
			if found, err := lookup(""); err == nil {
				account = found
			}
		}
		c, e = myce.PlanInstall(role, home, *root, defaults.NAS, defaults.Queue, defaults.Name, d, account)
		if e != nil {
			return fmt.Errorf("usage: %w", e)
		}
		if hardware != nil && e == nil {
			c = myce.ApplyHardwarePreflight(c, *hardware)
		}
		if *instance != "" {
			if !explicit["name"] {
				c.Name += "-" + *instance
			}
			if e = c.WorkerInstance(*instance); e != nil {
				return e
			}
		}
		c.EnvironmentFile, c.RequireMountedNAS = defaults.EnvironmentFile, defaults.RequireMountedNAS
		if path := os.Getenv("PATH"); path != "" {
			c.Path = path
		}
		if err := myce.FreshInstallDefaults(&c, values); err != nil {
			return err
		}
		if values.JoiningFile != "" {
			if role != "worker" {
				return errors.New("--joining-file is for installing a hypha")
			}
			i, err := myce.ReadInvitation(values.JoiningFile)
			if err != nil {
				return err
			}
			if !*dry {
				address := ""
				if explicit["queue"] {
					address = values.Queue
				}
				client, err := myce.ResolveJoining(ctx, i, address, myce.MDNSDiscovery{})
				if err != nil {
					return err
				}
				i.Address = client.URL
			}
			if i.Address != "" {
				c.Queue = i.Address
			}
			c.EnvironmentFile = ""
		}
	}
	c, e = myce.OverlayInstallFlags(f, c, values)
	if e != nil {
		return e
	}
	if err := myce.ResolveEnvironmentCacheSizing(&c, explicit["environment-cache-bytes"]); err != nil {
		return err
	}
	if role == "worker" && *redetect {
		probeConfig := c
		if !explicit["gpu"] {
			probeConfig.Detection.Platform = ""
		}
		found, err := myce.PreflightHardware(ctx, probeConfig, probe)
		if err != nil {
			return err
		}
		c = myce.ApplyHardwarePreflight(c, found)
	}
	if role == "worker" && !*dry && (installed == nil || c.Enrollment) && hardware == nil && c.Detection.Platform != "fake" {
		found, err := myce.PreflightHardware(ctx, c, probe)
		if err != nil {
			return err
		}
		c = myce.ApplyHardwarePreflight(c, found)
	}
	if role == "worker" {
		c = myce.OverlayHardwareSettings(c, explicit)
	}
	if err := myce.CheckMachineInstallGate(c); err != nil {
		return err
	}
	if err := c.ValidateInstallPolicies(); err != nil {
		return err
	}
	if role == "worker" && c.Detection.Platform != "fake" {
		var identity myce.NASIdentity
		if c.JoiningFile != "" {
			invitation, err := myce.ReadInvitation(c.JoiningFile)
			if err != nil {
				return err
			}
			identity = invitation.NASIdentity
		}
		beforeStorage := c
		c, e = myce.ResolveOnboardingStorage(ctx, c, installed == nil && !explicit["nas"], probe.Command, identity)
		if explicit["checkpoint-root"] {
			c.Checkpoints = beforeStorage.Checkpoints
		}
		if explicit["model-root"] {
			c.ModelRoot = beforeStorage.ModelRoot
		}
		if explicit["environment-root"] {
			c.EnvironmentRoot = beforeStorage.EnvironmentRoot
		}
		if e != nil {
			return e
		}
	}
	d := c.Detection
	if role == "worker" {
		if err := myce.ValidateOnboardingModelSelection(ctx, c, probe.Command); err != nil {
			return err
		}
	}
	if installed != nil {
		if err := myce.ValidateInstallIdentity(*installed, c); err != nil {
			return err
		}
		fmt.Println("Intended configuration changes:\n" + myce.InstallConfigDiff(*installed, c))
		if c.EnvironmentFile != "" && c.EnvironmentFile == installed.EnvironmentFile && c.JoiningFile == installed.JoiningFile {
			fmt.Println("Joining authority: preserve saved EnvironmentFile credential reference (no authority migration).")
		}
	}
	fileCacheDescription := "automatic from disk headroom at worker startup"
	if c.FileCacheBytes > 0 {
		fileCacheDescription = fmt.Sprintf("%d bytes each", c.FileCacheBytes)
	}
	fmt.Printf("%s %s/%s: platform=%s memory=%s reserve=%.0f GiB\nQueue: %s\nNAS: %s\nCheckpoint root: %s\nFile/shared-input cache: %s\nModel cache: %s (%d bytes)\nEnvironment cache: %s (%d bytes)\nConfig: %s\nBoot service: %s\n", role, d.OS, d.Arch, d.Platform, d.MemoryMode, d.ReserveGB, c.Queue, c.NAS, c.Checkpoints, fileCacheDescription, c.ModelCache, c.ModelCacheBytes, c.EnvironmentCache, c.EnvironmentCacheBytes, c.Config, c.Service)
	if d.OS == "linux" && role == "queue" {
		if probe.Command == nil {
			fmt.Println("cannot inspect user lingering: command probe missing; run myce doctor.")
		} else {
			uid := fmt.Sprint(os.Getuid())
			b, err := probe.Command(context.Background(), "loginctl", "show-user", uid, "--property=Linger", "--value")
			switch {
			case err != nil:
				fmt.Printf("cannot inspect user lingering: %v; run myce doctor.\n", err)
			case strings.TrimSpace(string(b)) == "yes":
				fmt.Println("User lingering enabled: starts at boot and survives logout.")
			case strings.TrimSpace(string(b)) == "no":
				fmt.Printf("Starts at login; for boot without login and after logout, an administrator must run loginctl enable-linger %s.\n", uid)
			default:
				fmt.Println("cannot inspect user lingering: unexpected loginctl result; run myce doctor.")
			}
		}
	}
	if role == "worker" && *dry {
		if d.OS == "linux" {
			fmt.Println("Install will enable and verify user lingering, or report the required administrator action.")
		} else {
			fmt.Println("LaunchAgent starts at GUI login; no pre-login or after-logout service. macOS CPU/RAM budgets are admission-only.")
		}
	}
	fmt.Printf("Command: myce %s\n", strings.Join(c.Args(), " "))
	if *dry {
		fmt.Println("Dry run: no files written, packages installed, or services changed.")
		fmt.Print(c.ServiceText())
		return nil
	}
	executable, e := os.Executable()
	if e != nil {
		return e
	}
	if role == "worker" && !c.AllowDeviceless {
		_, e = myce.PreflightOnboardingServiceSession(ctx, c, probe.Command)
		if e != nil {
			return e
		}
	}
	if c.Enrollment {
		if e = myce.PreflightEnrollmentInstall(ctx, c, executable, probe.Command); e != nil {
			return e
		}
	}
	if role == "worker" && c.Detection.Platform != "fake" {
		if c.Enrollment {
			// Preflight already proved NAS I/O. The installer creates its roots
			// after drain succeeds, alongside the other persistent changes.
			c.RequireMountedNAS = true
		} else {
			c, e = myce.PrepareOnboardingStorage(ctx, c, installed == nil && !explicit["nas"], probe.Command)
			if e != nil {
				return e
			}
		}
	}
	if e = install(ctx, c, executable, env("MYCE_TOKEN", ""), probe.Command); e != nil {
		return e
	}
	if role == "worker" {
		fmt.Println("Worker installed; queue maintenance controls admission.")
		statusCtx, cancelStatus := setupTimeout(signals)
		defer cancelStatus()
		if !c.AllowDeviceless {
			if report, err := myce.PreflightOnboardingServiceSession(statusCtx, c, probe.Command); err == nil {
				fmt.Println(report.String())
			}
		}
		fmt.Println(myce.WorkerMemoryDelegationStatus(statusCtx, c, probe.Command))
	}
	fmt.Println("Installed. Run myce doctor to verify prerequisites.")
	return nil
}
func serviceCommand(args []string) error {
	f := flag.NewFlagSet("service", flag.ContinueOnError)
	path := f.String("config", "", "private installed configuration")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *path == "" {
		return errors.New("service requires --config")
	}
	for _, arg := range f.Args() {
		if arg == "--local-disable-receipts" || strings.HasPrefix(arg, "--local-disable-receipts=") {
			return myce.ErrLegacyLocalDisableReceipts
		}
	}
	if e := myce.ProtectServiceCredentials(); e != nil {
		return e
	}
	c, e := myce.ReadServiceConfig(*path)
	if e != nil {
		return e
	}
	if c.LocalDisableReceipts {
		return myce.ErrLegacyLocalDisableReceipts
	}
	if e = myce.EnsureXPUHealthAccess(context.Background(), c, myce.LocalMachineProbe().Command); e != nil {
		return e
	}
	if c.EnvironmentFile != "" {
		_, queue, err := myce.ReadFleetEnvironment(c.EnvironmentFile)
		if err != nil {
			return err
		}
		if err = os.Setenv("MYCE_QUEUE", queue); err != nil {
			return err
		}
	}
	token, e := myce.ReadServiceToken(c)
	if e != nil {
		return e
	}
	// Keep credentials out of argv. run/worker removes MYCE_TOKEN from jobs/builds.
	if c.Path != "" {
		if e = os.Setenv("PATH", c.Path); e != nil {
			return e
		}
	}
	if e = os.Setenv("MYCE_TOKEN", token); e != nil {
		return e
	}
	args = c.Args()
	if f.NArg() > 0 {
		args = f.Args()
	}
	args = append(args, "--service-config", c.Config)
	return run(args)
}
