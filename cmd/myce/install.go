package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	instance := f.String("instance", "", "Linux worker instance")
	root := f.String("root", "", "installation root")
	dry := f.Bool("dry-run", false, "print plan without installing")
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
	ctx, cancel := myce.SetupTimeout(context.Background())
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
		installed = &old
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else if explicit["config"] {
		return errors.New("--config must select an existing installation; use --root for a fresh install")
	}
	var c myce.ServiceConfig
	if installed != nil {
		c = *installed
	} else {
		d := myce.Detection{OS: probe.OS, Arch: probe.Arch}
		if role == "worker" {
			if values.Detection.Platform != "" {
				d.Platform, d.MemoryMode, d.MemoryGB, d.ReserveGB = "fake", values.Detection.MemoryMode, values.Detection.MemoryGB, values.Detection.ReserveGB
			} else {
				d, e = probe.DetectDevice(ctx, values.Device)
				if e != nil {
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
		if *instance != "" {
			if !explicit["name"] {
				c.Name += "-" + *instance
			}
			if e = c.WorkerInstance(*instance); e != nil {
				return e
			}
		}
		c.DockerEndpoint, c.EnvironmentFile, c.RequireMountedNAS = defaults.DockerEndpoint, defaults.EnvironmentFile, defaults.RequireMountedNAS
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
				if err = myce.WriteInvitation(values.JoiningFile, i); err != nil {
					return err
				}
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
	if err := c.ValidateInstallPolicies(); err != nil {
		return err
	}
	d := c.Detection
	if installed != nil {
		if err := myce.ValidateInstallIdentity(*installed, c); err != nil {
			return err
		}
		fmt.Println("Intended configuration changes:\n" + myce.InstallConfigDiff(*installed, c))
	}
	fmt.Printf("%s %s/%s: platform=%s memory=%s reserve=%.0f GiB\nQueue: %s\nNAS: %s\nCheckpoint root: %s\nModel cache: %s (%d bytes)\nEnvironment cache: %s (%d bytes)\nConfig: %s\nBoot service: %s\n", role, d.OS, d.Arch, d.Platform, d.MemoryMode, d.ReserveGB, c.Queue, c.NAS, c.Checkpoints, c.ModelCache, c.ModelCacheBytes, c.EnvironmentCache, c.EnvironmentCacheBytes, c.Config, c.Service)
	if d.OS == "linux" {
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
	fmt.Printf("Command: myce %s\n", strings.Join(c.Args(), " "))
	if c.Role == "worker" {
		fmt.Println("Worker installed; queue maintenance controls admission.")
	}
	if *dry {
		fmt.Println("Dry run: no files written, packages installed, or services changed.")
		fmt.Print(c.ServiceText())
		return nil
	}
	executable, e := os.Executable()
	if e != nil {
		return e
	}
	if e = myce.Install(ctx, c, executable, env("MYCE_TOKEN", ""), probe.Command); e != nil {
		return e
	}
	if role == "worker" {
		fmt.Println(myce.WorkerMemoryDelegationStatus(ctx, c, probe.Command))
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
	c, e := myce.ReadServiceConfig(*path)
	if e != nil {
		return e
	}
	if c.LocalDisableReceipts {
		return myce.ErrLegacyLocalDisableReceipts
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
	return run(args)
}
