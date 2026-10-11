package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(exitCode(err))
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func run(args []string) error {
	return runWithProbe(args, myce.LocalMachineProbe())
}
func runWithProbe(args []string, probe myce.MachineProbe) error {
	if len(args) == 0 {
		return errors.New("usage: myce queue|join|leave|submit|status|logs|wait|cancel|dev")
	}
	if args[0] == "install" || args[0] == "uninstall" || args[0] == "doctor" {
		return setupCommand(args)
	}
	if args[0] == "service" {
		return serviceCommand(args[1:])
	}
	if args[0] == "wait" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return waitCommand(ctx, args[1:], os.Stdout, os.Stderr)
	}
	if args[0] == "measure" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runMeasure(ctx, args[1:], os.Stdout, os.Stderr)
	}
	if args[0] == "model" {
		return modelCommand(args[1:])
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	since := f.String("since", "7d", "usage lookback, up to 90d")
	by := f.String("by", "project", "usage grouping: project or machine")
	projectConfig := f.String("project-config", "", "project weights and experiment caps YAML")
	url := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue address")
	token := f.String("token", env("MYCE_TOKEN", ""), "shared fleet token (prefer MYCE_TOKEN)")
	address := f.String("listen", "127.0.0.1:7331", "queue listen address")
	db := f.String("db", "myce.db", "SQLite database file")
	uploadSpoolDir := f.String("upload-spool-dir", "", "queue model-upload directory (default OS temporary directory)")
	uploadSpoolBytes := f.Int64("upload-spool-bytes", myce.DefaultUploadSpoolBytes, "total bytes held in the queue model-upload spool")
	checkpoint := f.String("checkpoints", "./checkpoints", "queue NAS checkpoint root")
	workerCheckpoint := f.String("checkpoint-root", env("MYCE_CHECKPOINT_ROOT", ""), "worker-local NAS root (may differ from queue mount path)")
	cacheRoot := f.String("model-cache", "", "worker local model cache directory")
	modelRoot := f.String("model-root", "", "worker NAS model root for relative model paths")
	copyParallel := f.Int("model-copy-parallel", 1, "maximum concurrent NAS copies per worker")
	cacheLimit := f.Int64("model-cache-bytes", 100*1024*1024*1024, "maximum active cache bytes")
	productionGrace := f.Duration("production-stop-grace", 10*time.Second, "production checkpoint grace")
	heartbeat := f.Duration("heartbeat", time.Second, "worker check-in interval")
	memoryMode := f.String("memory-mode", "auto", "memory source: auto, unified, dedicated, or fixed")
	hostReserve := f.Float64("reserve-host-memory-gb", 4, "host RAM reserve in GiB (unified uses the larger GPU/host reserve once)")
	reserve := f.Float64("reserve-memory-gb", 0, "GPU memory to keep outside Mycelium")
	productionLoss := f.Duration("production-loss-timeout", myce.ProductionLossAfter, "quiet timeout for production work")
	deadlineLoss := f.Duration("deadline-loss-timeout", myce.DeadlineLossAfter, "quiet timeout for deadline work")
	experimentLoss := f.Duration("experiment-loss-timeout", myce.LossAfter, "quiet timeout for experiment work")
	restartGrace := f.Duration("restart-grace", myce.RestartGrace, "worker recovery grace after queue restart")
	stateDir := f.String("state-dir", "", "durable local worker spool directory")
	hostname, _ := os.Hostname()
	name := f.String("name", hostname, "unique machine name")
	hostID := f.String("host-id", hostname, "physical host identity shared by every per-card worker")
	device := f.String("device", "", "physical GPU selector override (one device per worker)")
	gpu := f.String("gpu", "", "GPU override: cuda, xpu, metal, or fake (join auto-detects when omitted)")
	memory := f.Float64("memory-gb", 0, "memory cap in GiB (required for fixed/XPU/fake budgets)")
	environments := f.String("environments", "", "comma-separated ready environment names or lock-file paths")
	models := f.String("models", "", "comma-separated already loaded model names")
	environmentFile := f.String("environment-config", "", "shared NAS environment catalog")
	environmentSourceRoot := f.String("environment-root", "", "worker-local NAS environment catalog directory")
	environmentRoot := f.String("environment-cache", "", "local environment cache directory")
	environmentLimit := f.Int64("environment-cache-bytes", 20*1024*1024*1024, "active environment cache limit")
	modelFile := f.String("model-config", "", "model YAML config shared by queue and workers")
	modelIdle := f.Duration("model-idle", 0, "override model idle timeout")
	modelWait := f.Duration("model-wait", 0, "override maximum model wait")
	modelListen := f.String("model-listen", "127.0.0.1:0", "worker model proxy listen address")
	modelAdvertise := f.String("model-advertise", "", "worker model proxy URL reachable by queue")
	workers := f.Int("workers", 2, "fake workers in dev mode")
	now := f.Bool("now", false, "stop every job on leave")
	full := f.Bool("full", false, "show full job IDs in status")
	follow := f.Bool("follow", false, "follow logs until the job is terminal")
	// Accept conventional 'logs ID --follow' and 'leave --now'.
	flags := args[1:]
	if (args[0] == "logs" || args[0] == "submit" || args[0] == "cancel") && len(flags) > 0 && !strings.HasPrefix(flags[0], "-") {
		flags = append(append([]string{}, flags[1:]...), flags[0])
	}
	if err := f.Parse(flags); err != nil {
		return err
	}
	if args[0] == "dev" && *token == "" {
		*token = "dev-token"
	}
	if *token == "" {
		return errors.New("set MYCE_TOKEN to the shared fleet token (dev defaults to dev-token)")
	}
	client := myce.Client{URL: *url, Token: *token}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	mc, err := myce.ReadModels(*modelFile)
	if err != nil {
		return err
	}
	if *modelIdle > 0 {
		mc.Idle = *modelIdle
	}
	if *modelWait > 0 {
		mc.Wait = *modelWait
	}
	ec, err := myce.ReadEnvironments(*environmentFile)
	if err != nil {
		return err
	}
	switch args[0] {
	case "dev":
		if *memory == 0 {
			*memory = 64
		}
		policies, e := myce.ReadProjectPolicies(*projectConfig)
		if e != nil {
			return e
		}
		d, err := myce.StartDev(myce.DevConfig{ProjectPolicies: policies, EnvironmentConfig: ec, ModelConfig: mc, Address: *address, Database: *db, Checkpoints: *checkpoint, Token: *token, GPU: *gpu, Environments: split(*environments), Models: split(*models), Workers: *workers, MemoryGB: *memory})
		if err != nil {
			return err
		}
		fmt.Printf("Mycelium dev: %s · %d fake workers · token dev-token unless overridden\n", d.URL, *workers)
		select {
		case <-ctx.Done():
			return d.Close()
		case err := <-d.Errors():
			return errors.Join(err, d.Close())
		}
	case "queue":
		q, err := myce.Open(*db, *checkpoint, myce.RealClock{})
		if err != nil {
			return err
		}
		defer q.Close()
		if err = q.ConfigureUploadSpool(*uploadSpoolDir, *uploadSpoolBytes); err != nil {
			return err
		}
		if err = q.ConfigureEnvironments(ec); err != nil {
			return err
		}
		if err = q.ConfigureModels(mc); err != nil {
			return err
		}
		if *projectConfig != "" {
			policies, e := myce.ReadProjectPolicies(*projectConfig)
			if e != nil {
				return e
			}
			if e = q.ConfigureProjects(policies); e != nil {
				return e
			}
		}
		q.SetRestartGrace(*restartGrace)
		if err = q.SetProductionGrace(*productionGrace); err != nil {
			return err
		}
		if err = q.SetLossTimeouts(*productionLoss, *deadlineLoss, *experimentLoss); err != nil {
			return err
		}
		listener, err := net.Listen("tcp", *address)
		if err != nil {
			return err
		}
		server := &http.Server{Handler: myce.API{Queue: q, Token: *token}.Handler(), ReadHeaderTimeout: 5 * time.Second}
		errs := make(chan error, 2)
		go func() { errs <- server.Serve(listener) }()
		go func() { errs <- q.Run(ctx) }()
		fmt.Printf("Mycelium queue: http://%s\n", listener.Addr())
		select {
		case <-ctx.Done():
		case err = <-errs:
		}
		stop()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return errors.Join(err, server.Shutdown(shutdown))
	case "join":
		options := joinGPUOptions{*gpu, *device, *memoryMode, *memory, *reserve}
		explicit := map[string]bool{}
		f.Visit(func(v *flag.Flag) { explicit[v.Name] = true })
		options, err = detectJoinGPU(ctx, probe, options, explicit)
		if err != nil {
			return err
		}
		*gpu, *device, *memoryMode, *memory, *reserve = options.platform, options.device, options.mode, options.memory, options.reserve
		binding := *device
		if *gpu == "cuda" && *device != "" {
			binding, err = myce.ResolveCUDADevice(ctx, *device)
			if err != nil {
				return err
			}
		}
		source, err := myce.NewDeviceMemorySource(*gpu, binding, *memory, *reserve, *memoryMode)
		if err != nil {
			return err
		}
		if *heartbeat <= 0 {
			return errors.New("heartbeat must be positive")
		}
		if *copyParallel < 1 {
			return errors.New("model-copy-parallel must be positive")
		}
		unified, err := myce.DetectUnifiedMemory(ctx, *gpu, binding, *memoryMode)
		if err != nil {
			return err
		}
		ramReserve := *hostReserve
		if unified {
			ramReserve = max(ramReserve, *reserve)
		}
		hostSource, err := myce.NewHostMemorySource(ramReserve)
		if err != nil {
			return err
		}
		w := myce.Worker{HostMemorySource: hostSource, DeviceBinding: binding, EnvironmentSourceRoot: *environmentSourceRoot, EnvironmentRoot: *environmentRoot, EnvironmentLimit: *environmentLimit, CopyParallel: *copyParallel, CacheRoot: *cacheRoot, ModelRoot: *modelRoot, CacheLimit: *cacheLimit, MemorySource: source, MemoryMeasured: *memoryMode != "fixed" && (*gpu == "cuda" || *gpu == "metal" || *memoryMode == "unified" && *gpu == "xpu"), CheckpointRoot: *workerCheckpoint, Interval: *heartbeat, ModelConfig: mc, ModelListen: *modelListen, ModelAdvertise: *modelAdvertise, StateDir: *stateDir, Client: client, Machine: myce.Machine{HostID: *hostID, UnifiedMemory: unified, HostMemoryMeasured: true, ID: *name, GPU: *gpu, Device: *device, FreeMemoryGB: *memory, Environments: split(*environments), Models: split(*models)}}
		fmt.Printf("Joining as %s\n", w)
		return w.Run(ctx)
	case "submit":
		if f.NArg() != 1 {
			return errors.New("usage: myce submit job.yaml")
		}
		file, err := os.Open(f.Arg(0))
		if err != nil {
			return err
		}
		defer file.Close()
		d := yaml.NewDecoder(io.LimitReader(file, 1<<20))
		d.KnownFields(true)
		var s myce.Spec
		if err := d.Decode(&s); err != nil {
			return err
		}
		var extra any
		if d.Decode(&extra) != io.EOF {
			return errors.New("expected one YAML job")
		}
		var j myce.Job
		if err := client.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			return err
		}
		fmt.Println(j.ID)
		return nil
	case "usage":
		duration, e := usageDuration(*since)
		if e != nil {
			return e
		}
		if *by != "project" && *by != "machine" {
			return errors.New("by must be project or machine")
		}
		var report myce.UsageReport
		if e = client.Call(ctx, "GET", "/api/usage?since="+duration.String(), nil, &report); e != nil {
			return e
		}
		return printUsage(os.Stdout, report, *by)
	case "status":
		s, err := status(ctx, client)
		if err != nil {
			return err
		}
		printStatus(s, *full)
		return nil
	case "cancel":
		if f.NArg() != 1 {
			return errors.New("usage: myce cancel <id>")
		}
		return client.Call(ctx, "POST", "/api/jobs/"+f.Arg(0)+"/cancel", nil, nil)
	case "leave":
		s, err := status(ctx, client)
		if err != nil {
			return err
		}
		nonresumable := false
		for _, j := range s.Jobs {
			if j.Machine == *name && j.State == "running" && !j.Spec.Resumable {
				nonresumable = true
			}
		}
		if !*now && nonresumable { // Mark leaving before asking, so no new work is assigned.
			if err := client.Call(ctx, "POST", "/api/workers/"+*name+"/leave", map[string]bool{"now": false}, nil); err != nil {
				return err
			}
			fmt.Print("A non-resumable job is running. Wait for it to finish, or stop and restart it? [wait/stop]: ")
			answer, err := bufio.NewReader(os.Stdin).ReadString('\n')
			if err != nil {
				return fmt.Errorf("machine is leaving and waiting for its job; use --now to stop it: %w", err)
			}
			answer = strings.TrimSpace(answer)
			switch answer {
			case "stop":
				*now = true
			case "wait", "":
			default:
				return errors.New("expected wait or stop; machine remains leaving and waiting")
			}
		}
		return client.Call(ctx, "POST", "/api/workers/"+*name+"/leave", map[string]bool{"now": *now}, nil)
	case "logs":
		if f.NArg() != 1 {
			return errors.New("usage: myce logs <id> [--follow]")
		}
		return logs(ctx, client, f.Arg(0), *follow)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func split(v string) []string {
	out := []string{}
	for _, v := range strings.Split(v, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
func status(ctx context.Context, c myce.Client) (myce.Snapshot, error) {
	var s myce.Snapshot
	err := c.Call(ctx, "GET", "/api/status", nil, &s)
	return s, err
}
func printStatus(s myce.Snapshot, full bool) {
	if s.Warning != "" {
		fmt.Println(s.Warning)
	}
	for _, m := range s.Machines {
		state := "joined"
		if m.Gone {
			state = "gone"
		} else if m.Leaving {
			state = "leaving"
		}
		if m.HostMemoryError != "" {
			state += "; host RAM unavailable: " + m.HostMemoryError
		}
		if m.HostMemoryMeasured {
			state += fmt.Sprintf("; host %s RAM %.1f GiB available for admission", m.HostID, m.FreeHostMemoryGB)
		}
		if m.MemoryError != "" {
			state += "; memory unavailable: " + m.MemoryError
		}
		fmt.Printf("environments %s: ready=%v build seconds=%v\n", m.ID, m.Environments, m.EnvironmentBuildSeconds)
		for _, r := range m.EnvironmentReports {
			fmt.Printf("environment %s/%s: %s %s\n", m.ID, r.Key, r.State, r.Error)
		}
		for _, r := range m.CopyReports {
			fmt.Printf("copy %s/%s: %s %d/%d bytes %s\n", m.ID, r.Model, r.State, r.Bytes, r.Total, r.Error)
		}
		fmt.Printf("copy speed %s: %.0f bytes/s; estimated NAS copy seconds %v\n", m.ID, m.CopyBytesPerSecond, m.ModelCopySeconds)
		fmt.Printf("cache %s: %v, %d bytes free %s\n", m.ID, m.CachedModels, m.CacheFreeBytes, m.CacheError)
		fmt.Printf("machine %s: %s, %.1f GB free, %s\n", m.ID, m.GPU, m.FreeMemoryGB, state)
	}
	for _, r := range s.SyncRequests {
		fmt.Printf("sync %s to %s cancelled=%t\n", r.Model, r.To, r.Cancel)
	}
	for _, p := range s.Pins {
		fmt.Printf("pin %s/%s: machine=%s platform=%s assigned=%s window=%s-%s %s\n", p.Project, p.Model, p.Machine, p.Platform, p.Assigned, p.Start, p.End, p.Timezone)
	}
	for _, m := range s.LoadedModels {
		fmt.Printf("model %s on %s: %s, idle %.1fs, calls in flight %d %s\n", m.Model, m.Machine, m.State, m.IdleSeconds, m.InFlight, m.Error)
	}
	for _, j := range s.Jobs {
		id := myce.ShortID(j.ID)
		if full {
			id = j.ID
		}
		if j.StartedAt != nil && j.Spec.Priority == "production" {
			fmt.Printf("production start latency %s: %.3fs\n", id, j.StartLatencySeconds)
		}
		fmt.Printf("%s %s [%s] %s machine=%s %s %s\n", id, j.Spec.Name, j.Spec.Priority, myce.JobState(j), j.Machine, j.Reason, myce.JobAge(j, s.Now))
	}
	if len(s.Jobs) == 0 {
		fmt.Println("No jobs yet.")
	}
}
func logs(ctx context.Context, c myce.Client, id string, follow bool) error {
	var after int64
	for {
		var chunks []myce.Log
		if err := c.Call(ctx, "GET", fmt.Sprintf("/api/jobs/%s/logs?after=%d", id, after), nil, &chunks); err != nil {
			return err
		}
		for _, l := range chunks {
			fmt.Print(l.Text)
			after = l.Seq
		}
		if len(chunks) == 256 {
			continue
		}
		if !follow {
			return nil
		}
		s, err := status(ctx, c)
		if err != nil {
			return err
		}
		for _, j := range s.Jobs {
			if j.ID == id && (j.State == "succeeded" || j.State == "failed" || j.State == "cancelled") { // Drain logs once more after observing completion.
				follow = false
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(300 * time.Millisecond):
		}
	}
}
