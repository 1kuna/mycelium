package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func main() {
	if err := myce.GuardRootStartup(); err != nil {
		log.Print(err)
		os.Exit(exitCode(err))
	}
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
	if err := myce.GuardRootStartup(); err != nil {
		return err
	}
	if len(args) > 0 && args[0] == "_command-exec" {
		return myce.CommandExec(args[1:])
	}

	if len(args) == 0 {
		return errors.New("usage: myce queue|join|leave|host|submit|policy|status|logs|wait|cancel|resolve-docker|dev")
	}
	if args[0] == "dev" && len(args) > 1 && args[1] == "clock" {
		return devClockCommand(args[2:], os.Stdout)
	}
	if args[0] == "invite" {
		return inviteCommand(args[1:])
	}
	if args[0] == "file" {
		return fileCommand(args[1:])
	}
	if args[0] == "benchmark" {
		return benchmarkCommand(args[1:])
	}
	if args[0] == "policy" {
		return policyCommand(args[1:], os.Stdout)
	}
	if args[0] == "extension" {
		return extensionCommand(args[1:])
	}
	if args[0] == "host" {
		return hostMaintenanceCommand(context.Background(), args[1:], os.Stdout)
	}
	if args[0] == "resolve-docker" {
		return resolveDockerCommand(args[1:])
	}
	if args[0] == "disable" || args[0] == "enable" {
		return errors.New("retired host control; use myce host drain|disable|resume HOST")
	}
	if args[0] == "sim" {
		return simCommand(args[1:], os.Stdout)
	}
	if args[0] == "install" || args[0] == "uninstall" || args[0] == "doctor" {
		return setupCommand(args)
	}
	if args[0] == "detect" {
		f := flag.NewFlagSet("detect", flag.ContinueOnError)
		path := f.String("config", "", "installed worker config (no credentials read)")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *path == "" {
			return errors.New("detect requires --config")
		}
		c, err := myce.ReadServiceConfig(*path)
		if err != nil {
			return err
		}
		ctx, cancel := myce.SetupTimeout(context.Background())
		defer cancel()
		d, err := myce.DetectService(ctx, c, probe, myce.LocalGPUAccessProbe())
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(d)
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
	joiningFile := f.String("joining-file", env("MYCE_JOINING_FILE", ""), "privately supplied invitation (discovery plus saved address)")
	discovery := f.Bool("discovery", true, "advertise a pinned TLS joining endpoint over DNS-SD")
	discoveryListen := f.String("discovery-listen", "", "TLS joining listen address (default same interface as queue, automatic port)")
	identityDir := f.String("identity-dir", "", "durable joining identity directory (default DB.joining)")
	hostConfig := f.String("host-config", "", "physical-host production hold policy YAML")
	projectConfig := f.String("project-config", "", "project weights and experiment caps YAML")
	url := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue address")
	token := f.String("token", env("MYCE_TOKEN", ""), "shared fleet token (prefer MYCE_TOKEN)")
	// DefValue is only help text; keep the actual environment default private.
	f.Lookup("token").DefValue = ""
	address := f.String("listen", "127.0.0.1:7331", "queue listen address")
	db := f.String("db", "myce-state/myce.db", "SQLite database file")
	uploadSpoolDir := f.String("upload-spool-dir", "", "queue model-upload directory (default private directory beside DB)")
	dockerHost := f.String("docker-host", "", "opt in to managed Docker at this endpoint (empty disables Docker)")
	containerRetention := f.Duration("container-retention", myce.DefaultContainerRetention, "acknowledged stopped Docker container retention (0 removes immediately)")
	logRetention := f.Duration("log-retention", myce.DefaultLogRetention, "retention of completed batch and inactive model-server logs")
	uploadSpoolBytes := f.Int64("upload-spool-bytes", myce.DefaultUploadSpoolBytes, "total bytes held in the queue model-upload spool")
	fileStore := f.String("file-store", "", "authoritative file storage folder (default beside database)")
	fileStoreLimit := f.Int64("file-store-bytes", 100<<30, "authoritative files plus upload reservations limit")
	fileCache := f.String("file-cache", "", "local reproducible file cache folder")
	fileCacheLimit := f.Int64("file-cache-bytes", 10<<30, "maximum local file cache bytes")
	filesFrom := f.String("files-from", "", "upload explicitly mapped inputs from this local folder")
	checkpoint := f.String("checkpoints", "./checkpoints", "queue NAS checkpoint root")
	workerCheckpoint := f.String("checkpoint-root", env("MYCE_CHECKPOINT_ROOT", ""), "worker-local NAS root (may differ from queue mount path)")
	extensionRoot := f.String("extension-root", "", "local extension library/cache (opt in)")
	jobFileRoot := f.String("job-file-root", "", "local shared-storage root for extension job inputs/outputs")
	cacheRoot := f.String("model-cache", "", "worker local model cache directory")
	secretRoot := f.String("secret-root", "", "worker-local directory of mode-0600 named secrets")
	modelApprovalRoot := f.String("model-approval-root", "", "worker-local approved model manifests")
	modelRoot := f.String("model-root", "", "worker NAS model root for relative model paths")
	copyParallel := f.Int("model-copy-parallel", 1, "maximum concurrent NAS copies per worker")
	cacheLimit := f.Int64("model-cache-bytes", 100*1024*1024*1024, "maximum active cache bytes")
	productionGrace := f.Duration("preemption-stop-grace", 10*time.Second, "higher-tier checkpoint grace and repeated-preemption minimum runtime")
	f.DurationVar(productionGrace, "production-stop-grace", 10*time.Second, "legacy alias for preemption-stop-grace")
	heartbeat := f.Duration("heartbeat", time.Second, "worker check-in interval")
	memoryMode := f.String("memory-mode", "auto", "memory source: auto, unified, dedicated, or fixed")
	cpuReserve := f.Int("reserve-cpu-cores", 1, "logical cores reserved for the owner")
	cpuBudget := f.Int("cpu-budget-cores", 0, "initial host CPU allowance; zero uses logical cores minus reserve")
	hostBudget := f.Float64("host-memory-budget-gb", 0, "initial host RAM allowance in GiB; change with myce host budget while drained or disabled")
	memorySlack := f.Float64("memory-ceiling-slack-gb", .125, "extra GiB per declared RAM ceiling (counts against host budget)")
	requireCeilings := f.Bool("require-memory-ceilings", false, "refuse startup unless per-attempt cgroup v2 memory ceilings are enforceable; require host_memory_gb")
	memorySwap := f.Int64("memory-swap-max-bytes", 0, "swap allowance per memory ceiling in bytes")
	hostReserve := f.Float64("reserve-host-memory-gb", 4, "host RAM reserve in GiB (unified uses the larger GPU/host reserve once)")
	reserve := f.Float64("reserve-memory-gb", 0, "GPU memory to keep outside Mycelium")
	productionRetries := f.Int("production-infrastructure-retries", myce.DefaultProductionInfrastructureRetries, "maximum infrastructure requeues per production job")
	productionLoss := f.Duration("production-loss-timeout", myce.ProductionLossAfter, "quiet timeout for production work")
	deadlineLoss := f.Duration("deadline-loss-timeout", myce.DeadlineLossAfter, "quiet timeout for deadline work")
	experimentLoss := f.Duration("experiment-loss-timeout", myce.LossAfter, "quiet timeout for experiment work")
	restartGrace := f.Duration("restart-grace", myce.RestartGrace, "worker recovery grace after queue restart")
	stateDir := f.String("state-dir", "", "durable local worker spool directory")
	hostname, _ := os.Hostname()
	name := f.String("name", hostname, "unique machine name")
	hostID := f.String("host-id", hostname, "physical host identity shared by every per-card worker")
	device := f.String("device", "", "comma-separated cards offered by this machine worker (omitted offers all)")
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
	onboardingBenchmark := f.Bool("onboarding-benchmark", false, "measure throughput before admission (may take nine minutes)")
	dailyUse := f.Bool("daily-use", false, "take work only after 5 idle minutes, on external power, with known safe heat signals")
	laptop := f.Bool("laptop", false, "require external power even in dedicated mode")
	desktop := f.Bool("desktop", false, "declare a desktop; allow GPU-only thermal coverage on external power")
	maxJobs := f.Int("max-jobs", 0, "optional maximum simultaneous jobs/calls per physical host (0 means unlimited)")
	workers := f.Int("workers", 2, "fake workers in dev mode")
	fakeGPUs := f.Int("fake-gpus", 1, "GPU components per fake machine in dev mode (use --workers 1)")
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
	if *desktop && *laptop {
		return errors.New("--desktop and --laptop are mutually exclusive")
	}
	if args[0] == "dev" && *token == "" {
		*token = "dev-token"
	}
	workerUnit := ""
	if args[0] == "join" {
		var e error
		workerUnit, e = myce.CurrentWorkerUnit()
		if e != nil {
			return e
		}
		if workerUnit != "" {
			if !myce.ValidWorkerUnit(workerUnit) {
				return errors.New("invalid worker unit")
			}
		}

	}
	var invitation *myce.Invitation
	if *joiningFile != "" {
		i, e := myce.ReadInvitation(*joiningFile)
		if e != nil {
			return e
		}
		invitation = &i
		*token = i.Token
	}
	if *token == "" {
		return errors.New("set MYCE_TOKEN to the shared fleet token (dev defaults to dev-token)")
	}
	client := myce.Client{URL: *url, Token: *token}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if invitation != nil && args[0] != "join" {
		explicit := ""
		f.Visit(func(v *flag.Flag) {
			if v.Name == "queue" {
				explicit = *url
			}
		})
		if explicit == "" {
			explicit = invitation.Address
		}
		var e error
		client, e = invitation.Client(explicit)
		if e != nil {
			return e
		}
	}
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
	hosts, err := myce.ReadHostPolicies(*hostConfig)
	if err != nil {
		return err
	}
	ec, err := myce.ReadEnvironments(*environmentFile)
	if err != nil {
		return err
	}
	switch args[0] {
	case "dev":
		if f.NArg() != 0 {
			return errors.New("unexpected dev arguments; use myce dev clock advance <duration>")
		}
		if *memory == 0 {
			*memory = 64
		}
		policies, e := myce.ReadProjectPolicies(*projectConfig)
		if e != nil {
			return e
		}
		d, err := myce.StartDev(myce.DevConfig{Devices: *fakeGPUs, ProductionInfrastructureRetries: productionRetries, FileStore: *fileStore, FileStoreLimit: *fileStoreLimit, ExtensionRoot: *extensionRoot, JobFileRoot: *jobFileRoot, HostPolicies: hosts, HostID: *hostID, ProjectPolicies: policies, EnvironmentConfig: ec, ModelConfig: mc, Address: *address, Database: *db, Checkpoints: *checkpoint, Token: *token, GPU: *gpu, Environments: split(*environments), Models: split(*models), Workers: *workers, CPUReserveCores: *cpuReserve, CPUBudgetCores: *cpuBudget, MemoryGB: *memory})
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
		if f.NArg() != 0 {
			return errors.New("queue accepts flags only")
		}
		if err := myce.CheckQueueGPUDescriptors(); err != nil {
			return err
		}

		if err := myce.PrepareStateRoots(myce.QueueStateRoots(*db, *uploadSpoolDir, *fileStore, *identityDir, *discovery)...); err != nil {
			return err
		}

		q, err := myce.Open(*db, *checkpoint, myce.RealClock{})
		if err != nil {
			return err
		}
		defer q.Close()

		root := *fileStore
		if root == "" {
			root = *db + ".files"
		}
		nasConfigured := false
		f.Visit(func(value *flag.Flag) {
			if value.Name == "checkpoints" {
				nasConfigured = true
			}
		})
		if !nasConfigured {
			if err = os.MkdirAll(*checkpoint, 0700); err != nil {
				return err
			}
		}
		if err = q.ConfigureFileStore(root, *fileStoreLimit, !nasConfigured); err != nil {
			return err
		}
		if *hostConfig != "" {
			if err = q.ConfigureHosts(hosts); err != nil {
				return err
			}
		}
		if err = q.ConfigureLogRetention(*logRetention); err != nil {
			return err
		}
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
		if err = q.SetProductionInfrastructureRetries(*productionRetries); err != nil {
			return err
		}
		q.SetRestartGrace(*restartGrace)
		if err = q.SetProductionGrace(*productionGrace); err != nil {
			return err
		}
		if err = q.SetLossTimeouts(*productionLoss, *deadlineLoss, *experimentLoss); err != nil {
			return err
		}
		listener, err := listenWithRetry(ctx, *address, net.Listen, myce.RealClock{}, waitForBind, log.Printf)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if err = q.Tick(); err != nil {
			listener.Close()
			return err
		}
		defer listener.Close()
		if *discovery {
			dir := *identityDir
			if dir == "" {
				dir = *db + ".joining"
			}
			identity, e := myce.LoadJoiningIdentity(dir)
			if e != nil {
				return e
			}
			bind := *discoveryListen
			if bind == "" {
				host, _, e := net.SplitHostPort(listener.Addr().String())
				if e != nil {
					return e
				}
				bind = net.JoinHostPort(host, "0")
			}
			tlsListener, e := listenWithRetry(ctx, bind, net.Listen, myce.RealClock{}, waitForBind, log.Printf)
			if e != nil {
				if errors.Is(e, context.Canceled) {
					return nil
				}
				return e
			}
			defer tlsListener.Close()
			joiningServer, _, e := myce.ServeJoining(tlsListener, myce.API{Queue: q, Token: *token}.Handler(), identity)
			if e != nil {
				return e
			}
			defer joiningServer.Close()
			withdraw, e := myce.AdvertiseJoining(tlsListener.Addr(), identity.Fingerprint())
			if e != nil {
				fmt.Fprintln(os.Stderr, "Discovery unavailable; use a saved HTTPS joining address")
			} else {
				defer withdraw()
			}
			fmt.Printf("Mycelium joining: https://%s\n", tlsListener.Addr())
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
		if *cpuReserve < 0 || *cpuBudget < 0 {
			return errors.New("CPU reserve and allowance must be nonnegative")
		}
		if *maxJobs < 0 {
			return errors.New("max-jobs must be nonnegative")
		}
		options := joinGPUOptions{*gpu, *device, *memoryMode, *memory, *reserve}
		explicit := map[string]bool{}
		f.Visit(func(v *flag.Flag) { explicit[v.Name] = true })
		options, err = detectJoinGPU(ctx, probe, options, explicit)
		if err != nil {
			return err
		}
		*gpu, *device, *memoryMode, *memory, *reserve = options.platform, options.device, options.mode, options.memory, options.reserve
		binding := strings.TrimSpace(strings.Split(*device, ",")[0])
		source, err := myce.NewDeviceMemorySource(*gpu, binding, *memory, *reserve, *memoryMode)
		if err != nil {
			return err
		}
		// Keep the host ledger unified even when auto was explicitly selected.
		// Inventory reports probe failures; they must not prevent CPU startup.
		unified, _ := myce.DetectUnifiedMemory(ctx, *gpu, binding, *memoryMode)
		if _, err := myce.MemoryCeilingConfig(*hostBudget, *memorySlack, *memorySwap); err != nil {
			return err
		}
		if *heartbeat <= 0 {
			return errors.New("heartbeat must be positive")
		}
		if *copyParallel < 1 {
			return errors.New("model-copy-parallel must be positive")
		}
		ramReserve := *hostReserve
		if unified {
			ramReserve = max(ramReserve, *reserve)
		}
		hostSource, err := myce.NewHostMemorySource(ramReserve)
		if err != nil {
			return err
		}
		if invitation != nil {
			explicit := ""
			f.Visit(func(v *flag.Flag) {
				if v.Name == "queue" {
					explicit = *url
				}
			})
			resolveCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			client, err = myce.ResolveJoining(resolveCtx, *invitation, explicit, myce.MDNSDiscovery{})
			cancel()
			if err != nil {
				return err
			}
			invitation.Address = client.URL
			if err = myce.WriteInvitation(*joiningFile, *invitation); err != nil {
				return err
			}
		}
		w := myce.Worker{OnboardingBenchmark: *onboardingBenchmark, Benchmark: myce.RunOnboardingBenchmark, FileCacheRoot: *fileCache, FileCacheLimit: *fileCacheLimit, DailyUse: *dailyUse, Laptop: *laptop, Desktop: *desktop, ExtensionRoot: *extensionRoot, JobFileRoot: *jobFileRoot, DockerEndpoint: *dockerHost, ContainerRetention: containerRetention, RequireMemoryCeilings: *requireCeilings, MemorySwapMaxBytes: *memorySwap, LogRetention: *logRetention, Unit: workerUnit, SuperviseProcess: true, HostMemorySource: hostSource, DeviceBinding: binding, EnvironmentSourceRoot: *environmentSourceRoot, EnvironmentRoot: *environmentRoot, EnvironmentLimit: *environmentLimit, SecretRoot: *secretRoot, ModelApprovalRoot: *modelApprovalRoot, CopyParallel: *copyParallel, CacheRoot: *cacheRoot, ModelRoot: *modelRoot, CacheLimit: *cacheLimit, MemorySource: source, FixedAcceleratorMemory: *memoryMode == "fixed", MemoryMeasured: *memoryMode != "fixed" && (*gpu == "cuda" || *gpu == "metal" || *memoryMode == "unified" && *gpu == "xpu"), CheckpointRoot: *workerCheckpoint, Interval: *heartbeat, ModelConfig: mc, ModelListen: *modelListen, ModelAdvertise: *modelAdvertise, StateDir: *stateDir, Client: client, Machine: myce.Machine{CPUReserveCores: *cpuReserve, CPUBudgetCores: *cpuBudget, MaxJobs: *maxJobs, HostMemoryBudgetGB: *hostBudget, MemoryCeilingSlackGB: *memorySlack, HostID: *hostID, UnifiedMemory: unified, HostMemoryMeasured: true, ID: *name, GPU: *gpu, Device: *device, FreeMemoryGB: *memory, ReservedMemoryGB: *reserve, Environments: split(*environments), Models: split(*models)}}
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
		if *filesFrom != "" {
			if err := uploadJobInputs(ctx, client, &s, *filesFrom); err != nil {
				return err
			}
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
		ack, err := client.Cancel(ctx, f.Arg(0))
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(ack)
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
	for _, hold := range s.HostHolds {
		fmt.Println(hold)
	}
	if s.Warning != "" {
		fmt.Println(s.Warning)
	}
	for _, m := range s.Machines {
		state := myce.HyphaAvailability(m)
		if m.MaxJobs > 0 {
			state += fmt.Sprintf("; maximum simultaneous jobs %d", m.MaxJobs)
		}
		if m.DockerCleanup != "" {
			state += "; Docker " + m.DockerCleanup
		}
		if m.HostMemoryError != "" {
			state += "; host RAM unavailable: " + m.HostMemoryError
		}
		if m.MemoryEnforcement != "" {
			state += "; " + m.MemoryEnforcement
		}
		if m.EffectiveHostMemoryBudgetGB > 0 {
			state += fmt.Sprintf("; host allowance (last reported) %d bytes (UNQUALIFIED)", m.HostMemoryAllowanceBytes)
		} else if m.HostMemoryBudgetGB > 0 {
			state += fmt.Sprintf("; host ceiling budget (last reported) %.3g GiB", m.HostMemoryBudgetGB)
		}
		if over := m.HostMemoryBudgetStatus(); over != "" {
			state += "; " + over
		}
		if m.CapabilityProfile != nil {
			state += fmt.Sprintf("; CPU %d/%d cores free (reserve %d)", m.FreeCPUCores, m.EffectiveCPUCores, m.CPUReserveCores)
		}
		if m.CPUCapacityReason != "" {
			state += "; " + m.CPUCapacityReason
		}
		if m.HostMemoryMeasured {
			state += fmt.Sprintf("; host %s RAM %.1f GiB available for admission", m.HostID, m.FreeHostMemoryGB)
		}
		if m.InventoryReason != "" {
			state += "; device inventory: " + m.InventoryReason
		}
		if m.MemoryError != "" {
			state += "; memory unavailable: " + m.MemoryError
		}
		profileLabel := "capability profile"
		if m.CapabilityProfile != nil {
			profileLabel = fmt.Sprintf("capability profile v%d", m.CapabilityProfile.Version)
		}
		for _, line := range myce.CapabilityProfileLines(m) {
			fmt.Printf("hypha %s %s: %s\n", m.ID, profileLabel, line)
		}
		for _, r := range m.ExtensionReports {
			fmt.Printf("extension %s: %s\n", m.ID, myce.ExtensionStatus(r))
		}
		fmt.Printf("hypha %s: %s, %.1f GiB free, %s\n", m.ID, m.GPU, m.FreeMemoryGB, state)
		for _, d := range m.Devices {
			fmt.Printf("  device %s: %s, %.1f/%.1f GiB free, %d batch attempts, %d engines, %d outstanding releases %s\n", d.ID, d.Platform, d.FreeMemoryGB, d.BudgetGB, len(d.Attempts), len(d.Models), len(d.Releases), d.Reason)
			for _, r := range d.ExtensionReports {
				fmt.Printf("  extension %s/%s: %s\n", m.ID, d.ID, myce.ExtensionStatus(r))
			}
		}
		if m.Replacement != nil {
			fmt.Printf("  historical: replaced by %s device %s\n", m.Replacement.Machine, m.Replacement.Device)
		}
		if len(m.Environments) > 0 {
			fmt.Printf("environments %s: ready=%s\n", m.ID, strings.Join(m.Environments, ", "))
		}
		for _, name := range sortedKeys(m.EnvironmentBuildSeconds) {
			fmt.Printf("environment %s/%s: build_seconds=%.3f\n", m.ID, name, m.EnvironmentBuildSeconds[name])
		}
		for _, r := range m.EnvironmentReports {
			label := "environment"
			if r.Kind == "extension-setup" {
				label = "extension preparation"
			}
			fmt.Printf("%s %s/%s: %s %s\n", label, m.ID, r.Key, r.State, r.Error)
		}
		for _, r := range m.CopyReports {
			fmt.Printf("copy %s/%s: %s %d/%d bytes %s\n", m.ID, r.Model, r.State, r.Bytes, r.Total, r.Error)
		}
		if m.CopyBytesPerSecond > 0 {
			fmt.Printf("copy speed %s: bytes_per_second=%.0f\n", m.ID, m.CopyBytesPerSecond)
		}
		for _, name := range sortedKeys(m.ModelCopySeconds) {
			if m.ModelCopySeconds[name] > 0 {
				fmt.Printf("copy estimate %s/%s: seconds=%.3f\n", m.ID, name, m.ModelCopySeconds[name])
			}
		}
		if m.FileTransferError != "" {
			fmt.Printf("files %s: %s\n", m.ID, m.FileTransferError)
		}
		fmt.Printf("cache %s: %v, %d bytes free %s\n", m.ID, m.CachedModels, m.CacheFreeBytes, m.CacheError)
	}
	for _, r := range s.SyncRequests {
		fmt.Printf("sync %s to %s cancelled=%t\n", r.Model, r.To, r.Cancel)
	}
	for _, p := range s.Pins {
		fmt.Printf("pin %s/%s: machine=%s platform=%s assigned=%s window=%s-%s %s\n", p.Project, p.Model, p.Machine, p.Platform, p.Assigned, p.Start, p.End, p.Timezone)
	}
	for _, m := range s.LoadedModels {
		if m.CrashCount > 0 {
			fmt.Printf("engine recovery %s on %s: attempt=%s crashes=%d retry_at=%v\n", m.Model, m.Machine, m.Attempt, m.CrashCount, m.RetryAt)
		}
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
		policy := j.Spec.Logs
		if policy == "" {
			policy = "keep"
		}
		fmt.Printf("tier=%s selector=%s policy_revision=%d ", j.PriorityPolicy.Tier, j.PriorityPolicy.Selector, j.PriorityPolicy.Revision)
		fmt.Printf("%s %s [%s/%s] %s hypha=%s logs=%s %s %s; %s\n", id, j.Spec.Name, j.Spec.Project, j.Spec.Class, myce.JobState(j), j.Machine, policy, myce.JobReason(j), myce.JobAge(j, s.Now), myce.JobWork(j))
	}
	if len(s.Jobs) == 0 {
		fmt.Println("No jobs yet.")
	}
}
func logs(ctx context.Context, c myce.Client, id string, follow bool) error {
	s, err := status(ctx, c)
	if err != nil {
		return err
	}
	resolved, err := myce.FindJob(s.Jobs, id)
	if err != nil {
		return err
	}
	id = resolved.ID
	for _, j := range s.Jobs {
		if j.ID == id && j.Spec.Logs == "none" {
			fmt.Println("logs are disabled for this job (logs: none)")
			return nil
		}
	}
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

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
