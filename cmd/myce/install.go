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
	allowDeviceless := f.Bool("allow-deviceless", false, "allow CPU/fake test worker without service GPU validation")
	instance := f.String("instance", "", "Linux worker instance (e.g. gpu1)")
	device := f.String("device", "", "physical GPU selector (one device per worker)")
	hostDir := f.String("host-state-dir", "", "shared local host control directory")
	root := f.String("root", "", "installation root (default ~/.local/share/mycelium)")

	config := f.String("config", "", "installed config path (doctor/uninstall)")
	nasDefault := "/mnt/nas"
	if probe.OS == "darwin" {
		nasDefault = "/Volumes/NAS"
	}
	nas := f.String("nas", nasDefault, "local NAS mount root")
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue address")
	listen := f.String("listen", "127.0.0.1:7331", "queue bind address")
	hostname, _ := os.Hostname()
	name := f.String("name", hostname, "stable worker name")
	dry := f.Bool("dry-run", false, "print plan without writing files or installing services")
	gpu := f.String("gpu", "", "explicit GPU platform override")
	mode := f.String("memory-mode", "auto", "explicit memory mode with --gpu")
	memory := f.Float64("memory-gb", 0, "explicit GPU budget")
	hostBudget := f.Float64("host-memory-budget-gb", 0, "aggregate declared RAM ceiling budget per physical host")
	memorySlack := f.Float64("memory-ceiling-slack-gb", .125, "extra GiB per declared RAM ceiling")
	memorySwap := f.Int64("memory-swap-max-bytes", 0, "swap allowance per ceiling in bytes")
	reserve := f.Float64("reserve-memory-gb", 0, "explicit memory reserve")
	models := f.String("model-config", "", "NAS model catalog")
	environments := f.String("environment-config", "", "NAS environment catalog")
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
	d := myce.Detection{OS: probe.OS, Arch: probe.Arch}
	if role == "worker" {
		if *gpu != "" {
			d.Platform = *gpu
			d.MemoryMode = *mode
			d.MemoryGB = *memory
			d.ReserveGB = *reserve
			if _, e = myce.NewMemorySourceWithMode(*gpu, *memory, *reserve, *mode); e != nil {
				return e
			}
		} else {
			d, e = probe.DetectDevice(ctx, *device)
			if e != nil {
				if !*allowDeviceless {
					return e
				}
				d = myce.Detection{OS: probe.OS, Arch: probe.Arch, Platform: "fake", MemoryMode: "fixed", MemoryGB: *memory, ReserveGB: *reserve}
				if d.MemoryGB <= 0 {
					return errors.New("deviceless worker requires a positive --memory-gb CPU/test budget")
				}
			}
		}
	}
	c, e := myce.PlanInstall(role, home, *root, *nas, *queue, *name, d)
	if e != nil {
		return e
	}
	if *instance != "" {
		nameSet := false
		f.Visit(func(v *flag.Flag) {
			if v.Name == "name" {
				nameSet = true
			}
		})
		if !nameSet {
			c.Name += "-" + *instance
		}
		if e = c.WorkerInstance(*instance); e != nil {
			return e
		}
	}
	if _, err := myce.MemoryCeilingConfig(*hostBudget, *memorySlack, *memorySwap); err != nil {
		return err
	}
	c.HostMemoryBudgetGB, c.MemoryCeilingSlackGB, c.MemorySwapMaxBytes = *hostBudget, *memorySlack, *memorySwap
	c.AllowDeviceless = *allowDeviceless
	c.Device = *device
	c.HostStateDir = *hostDir
	if c.HostStateDir == "" {
		c.HostStateDir = os.Getenv("MYCE_HOST_STATE_DIR")
	}
	if c.HostStateDir != "" {
		c.HostStateDir, e = filepath.Abs(c.HostStateDir)
		if e != nil {
			return e
		}
	}
	c.Listen = *listen
	// Services start in a different working directory from the install command.
	for _, path := range []*string{models, environments} {
		if *path != "" {
			*path, e = filepath.Abs(*path)
			if e != nil {
				return e
			}
		}
	}
	c.ModelCatalog = *models
	c.EnvironmentCatalog = *environments
	if *environments != "" {
		c.EnvironmentRoot = filepath.Dir(*environments)
	}
	if p := os.Getenv("PATH"); p != "" {
		c.Path = p
	}
	fmt.Printf("%s %s/%s: platform=%s memory=%s reserve=%.0f GiB\nQueue: %s\nNAS: %s\nCheckpoint root: %s\nModel cache: %s (%d bytes)\nEnvironment cache: %s (%d bytes)\nConfig: %s\nBoot service: %s\n", role, d.OS, d.Arch, d.Platform, d.MemoryMode, d.ReserveGB, c.Queue, c.NAS, c.Checkpoints, c.ModelCache, c.ModelCacheBytes, c.EnvironmentCache, c.EnvironmentCacheBytes, c.Config, c.Service)
	if d.OS == "linux" {
		fmt.Printf("Starts at login; for boot without login and after logout, an administrator must run loginctl enable-linger %d. Run myce doctor to check lingering.\n", os.Getuid())
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
	c, e := myce.ReadServiceConfig(*path)
	if e != nil {
		return e
	}
	if c.Role == "worker" {
		dir := c.HostStateDir
		unit, err := myce.CurrentWorkerUnit()
		if err != nil {
			return err
		}
		if unit != "" {
			dir, err = myce.WorkerUnitDirectory(dir, unit)
			if err != nil {
				return err
			}
		}
		reason, err := myce.HostDisabled(dir)
		if err != nil {
			return err
		}
		if reason != "" {
			fmt.Println("Worker disabled by operator:", reason)
			return nil
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
	return run(c.Args())
}
