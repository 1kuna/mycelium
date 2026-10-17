package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	f.BoolFunc("local-disable-receipts", "retired; reinstall the queue without this option", func(string) error { return myce.ErrLegacyLocalDisableReceipts })
	hostDir := f.String("host-state-dir", "", "shared local host control directory")
	joiningFile := f.String("joining-file", "", "private invitation; discovers queue and provisions trust")
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
	hostID := f.String("host-id", hostname, "physical host identity shared by per-card workers")
	hostReserve := f.Float64("reserve-host-memory-gb", 4, "host RAM reserve in GiB")
	stateDir := f.String("state-dir", "", "local worker spool (default role/state)")
	checkpointRoot := f.String("checkpoint-root", "", "worker NAS checkpoint root")
	modelRoot := f.String("model-root", "", "worker NAS model root")
	environmentRoot := f.String("environment-root", "", "worker NAS environment root")
	modelCache := f.String("model-cache", "", "local model cache")
	modelCacheBytes := f.Int64("model-cache-bytes", 100<<30, "local model cache bytes")
	environmentCache := f.String("environment-cache", "", "local environment cache")
	environmentCacheBytes := f.Int64("environment-cache-bytes", 20<<30, "local environment cache bytes")
	heartbeat := f.Duration("heartbeat", time.Second, "worker check-in interval")
	startDisabled := f.Bool("start-disabled", false, "install with a durable per-unit disable gate; enable explicitly later")
	requireCeilings := f.Bool("require-memory-ceilings", false, "refuse startup without tested cgroup v2 memory ceilings")
	requireMounted := f.Bool("require-mounted-nas", probe.OS == "linux", "require NAS root to be a mount point in /proc/mounts")
	force := f.Bool("force", false, "accept displayed installed configuration changes")
	db := f.String("db", "", "queue SQLite path (default state/myce.db)")
	projectConfig := f.String("project-config", "", "project policy YAML")
	hostConfig := f.String("host-config", "", "host policy YAML")
	containerRetention := f.Duration("container-retention", myce.DefaultContainerRetention, "acknowledged stopped container retention (0 removes immediately)")
	logRetention := f.Duration("log-retention", myce.DefaultLogRetention, "completed log retention")
	uploadSpoolDir := f.String("upload-spool-dir", "", "local upload spool (default state/uploads)")
	uploadSpoolBytes := f.Int64("upload-spool-bytes", myce.DefaultUploadSpoolBytes, "total upload spool bytes")
	restartGrace := f.Duration("restart-grace", myce.RestartGrace, "queue restart recovery grace")
	productionLoss := f.Duration("production-loss-timeout", myce.ProductionLossAfter, "production quiet timeout")
	deadlineLoss := f.Duration("deadline-loss-timeout", myce.DeadlineLossAfter, "deadline quiet timeout")
	experimentLoss := f.Duration("experiment-loss-timeout", myce.LossAfter, "experiment quiet timeout")
	checkpoints := f.String("checkpoints", "", "queue NAS checkpoint root")
	environmentFile := f.String("environment-file", filepath.Join(home, ".config", "myce", "fleet.env"), "private fleet MYCE_TOKEN and MYCE_QUEUE EnvironmentFile")
	privateDevices := f.Bool("private-devices", false, "queue private device namespace (unsupported for Linux user units)")
	noNewPrivileges := f.Bool("no-new-privileges", true, "queue disallows privilege escalation")
	nice := f.Int("nice", 10, "queue scheduling priority")
	cpuQuota := f.String("cpu-quota", "25%", "queue systemd CPUQuota")
	memoryMax := f.String("memory-max", "512M", "queue systemd MemoryMax")
	tasksMax := f.String("tasks-max", "64", "queue systemd TasksMax")
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
	if *joiningFile != "" {
		if role != "worker" {
			return errors.New("--joining-file is for installing a hypha")
		}
		path, err := filepath.Abs(*joiningFile)
		if err != nil {
			return err
		}
		*joiningFile = path
		i, err := myce.ReadInvitation(path)
		if err != nil {
			return err
		}
		if !*dry {
			explicit := ""
			f.Visit(func(v *flag.Flag) {
				if v.Name == "queue" {
					explicit = *queue
				}
			})
			client, err := myce.ResolveJoining(ctx, i, explicit, myce.MDNSDiscovery{})
			if err != nil {
				return err
			}
			i.Address = client.URL
			if err = myce.WriteInvitation(path, i); err != nil {
				return err
			}
		}
		if i.Address != "" {
			*queue = i.Address
		}
		*environmentFile = ""
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
	if role == "worker" {
		f.Visit(func(v *flag.Flag) {
			switch v.Name {
			case "reserve-memory-gb":
				d.ReserveGB = *reserve
			case "memory-gb":
				d.MemoryGB = *memory
			case "memory-mode":
				d.MemoryMode = *mode
			}
		})
		if _, err := myce.NewMemorySourceWithMode(d.Platform, d.MemoryGB, d.ReserveGB, d.MemoryMode); err != nil {
			return err
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
	if *hostDir != "" {
		c.HostStateDir = *hostDir
	} else if v := os.Getenv("MYCE_HOST_STATE_DIR"); v != "" {
		c.HostStateDir = v
	}
	if c.HostStateDir != "" {
		c.HostStateDir, e = filepath.Abs(c.HostStateDir)
		if e != nil {
			return e
		}
	}
	c.HostID, c.ReserveHostMemoryGB, c.Heartbeat = *hostID, *hostReserve, *heartbeat
	c.StartDisabled, c.RequireMemoryCeilings, c.RequireMountedNAS, c.Force = *startDisabled, *requireCeilings, *requireMounted, *force
	c.ModelCacheBytes, c.EnvironmentCacheBytes = *modelCacheBytes, *environmentCacheBytes
	if *stateDir != "" {
		c.State = *stateDir
		c.ModelCache = filepath.Join(c.State, "model-cache")
		c.EnvironmentCache = filepath.Join(c.State, "environment-cache")
	}
	for _, v := range []struct {
		dst   *string
		value string
	}{{&c.Checkpoints, *checkpointRoot}, {&c.ModelRoot, *modelRoot}, {&c.EnvironmentRoot, *environmentRoot}, {&c.ModelCache, *modelCache}, {&c.EnvironmentCache, *environmentCache}} {
		if v.value != "" {
			*v.dst = v.value
		}
	}
	c.Listen = *listen
	c.EnvironmentFile = *environmentFile
	c.JoiningFile = *joiningFile
	c.ProjectConfig, c.HostConfig = *projectConfig, *hostConfig
	if role == "worker" {
		f.Visit(func(v *flag.Flag) {
			if v.Name == "container-retention" {
				c.ContainerRetention = containerRetention
			}
		})
	}
	c.LogRetention, c.UploadSpoolBytes, c.RestartGrace = *logRetention, *uploadSpoolBytes, *restartGrace
	c.ProductionLossTimeout, c.DeadlineLossTimeout, c.ExperimentLossTimeout = *productionLoss, *deadlineLoss, *experimentLoss
	c.PrivateDevices, c.NoNewPrivileges, c.Nice, c.CPUQuota, c.MemoryMax, c.TasksMax = *privateDevices, *noNewPrivileges, *nice, *cpuQuota, *memoryMax, *tasksMax
	for _, v := range []struct {
		dst   *string
		value string
	}{{&c.DB, *db}, {&c.UploadSpoolDir, *uploadSpoolDir}, {&c.Checkpoints, *checkpoints}} {
		if v.value != "" {
			*v.dst = v.value
		}
	}
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
	if *environments != "" && *environmentRoot == "" {
		c.EnvironmentRoot = filepath.Dir(*environments)
	}
	// Resolve caller-relative paths before the service changes working directory.
	for _, p := range []*string{&c.State, &c.Checkpoints, &c.ModelRoot, &c.EnvironmentRoot, &c.ModelCache, &c.EnvironmentCache, &c.DB, &c.UploadSpoolDir, &c.ProjectConfig, &c.HostConfig, &c.EnvironmentFile} {
		if *p != "" {
			*p, e = filepath.Abs(*p)
			if e != nil {
				return e
			}
		}
	}
	if e = c.ValidateInstall(); e != nil {
		return e
	}
	if p := os.Getenv("PATH"); p != "" {
		if strings.ContainsAny(p, "\x00\n\r\"\\") {
			return errors.New("PATH cannot contain newlines, quotes or backslashes for service capture")
		}
		c.Path = p
	}
	if !*dry && c.EnvironmentFile != "" {
		_, address, err := myce.ReadFleetEnvironment(c.EnvironmentFile)
		if err != nil {
			return err
		}
		c.Queue = address
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
	if c.StartDisabled {
		fmt.Println("Worker will remain disabled until myce enable.")
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
	if c.Role == "worker" {
		dir := c.HostStateDir
		unit, err := myce.CurrentWorkerUnit()
		if err != nil {
			return err
		}
		if unit == "" && myce.ValidWorkerUnit(filepath.Base(c.Service)) {
			unit = filepath.Base(c.Service)
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
