package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/optimizer"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
)

func runPeer(ctx context.Context, args []string) error {
	if ctx.Err() != nil {
		return nil
	}
	addr, handler, err := buildPeerGateway(ctx, args)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: addr, Handler: handler}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return nil
}

func buildPeerGateway(ctx context.Context, args []string) (string, http.Handler, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	listen := fs.String("listen", "", "peer listen address override")
	compute := fs.Bool("compute", false, "enable local compute runtime")
	backendListen := fs.String("backend-listen", "", "local backend inference server listen address")
	id := fs.String("id", "", "local compute peer id")
	name := fs.String("name", "", "local compute peer name")
	backend := fs.String("backend", "", "local backend engine (llamacpp, mlx, vllm)")
	backendBinary := fs.String("backend-binary", "", "local backend server binary override")
	llamaServer := fs.String("llama-server", "", "llama.cpp server binary")
	ggufParser := fs.String("gguf-parser", "", "local GGUF parser binary")
	maxUtil := fs.Float64("max-util", 0, "maximum accelerator utilization")
	vramMB := fs.Int("vram-mb", 0, "local allocatable memory in MB")
	if err := fs.Parse(args); err != nil {
		return "", nil, err
	}
	cfg, err := loadPeerConfig(*configPath)
	if err != nil {
		return "", nil, err
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *compute {
		cfg.Compute = true
	}
	overrideString(backendListen, &cfg.ComputeConfig.BackendListen)
	overrideString(id, &cfg.ComputeConfig.ID)
	overrideString(name, &cfg.ComputeConfig.Name)
	if *backend != "" {
		cfg.ComputeConfig.Backend = domain.Backend(*backend)
	}
	overrideString(backendBinary, &cfg.ComputeConfig.BackendBinary)
	overrideString(llamaServer, &cfg.ComputeConfig.LlamaServer)
	overrideString(ggufParser, &cfg.ComputeConfig.GGUFParser)
	if *maxUtil != 0 {
		cfg.ComputeConfig.MaxUtil = *maxUtil
	}
	if *vramMB != 0 {
		cfg.ComputeConfig.VRAMMB = *vramMB
	}
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		return "", nil, err
	}
	if err := seedControlStore(ctx, store, cfg); err != nil {
		_ = store.Close()
		return "", nil, err
	}

	var fleet gateway.FleetSource
	var nodes gateway.NodeResolver
	var mux *http.ServeMux
	if cfg.JoinToken != "" {
		tokens, err := membership.NewPersistentTokenManager(ctx, cfg.JoinToken, store)
		if err != nil {
			return "", nil, err
		}
		registry, err := membership.NewPersistentRegistry(ctx, tokens, membership.NewLANTunnel(), store)
		if err != nil {
			return "", nil, err
		}
		fleet = registry
		nodes = registry
		mux = http.NewServeMux()
		mux.Handle("/join", registry)
		mux.Handle("/nodes", registry)
	}
	agents := map[string]ports.NodeAgent{}
	if cfg.Compute {
		local, err := buildComputeRuntime(ctx, cfg, store)
		if err != nil {
			return "", nil, err
		}
		if err := store.SaveNode(ctx, local.node); err != nil {
			return "", nil, err
		}
		agents[local.node.ID] = local.agent
		if mux == nil {
			mux = http.NewServeMux()
		}
		mountNodeHTTP(mux, local.handler)
	}
	for _, nodeURL := range cfg.NodeURLs {
		client := nodeagent.NewHTTPClient(nodeURL)
		snap, err := client.Snapshot(ctx)
		if err != nil {
			return "", nil, fmt.Errorf("snapshot node %s: %w", nodeURL, err)
		}
		if err := store.SaveNode(ctx, snap.Node); err != nil {
			return "", nil, err
		}
		for _, inst := range snap.Instances {
			if err := store.SaveInstance(ctx, inst); err != nil {
				return "", nil, err
			}
		}
		agents[snap.Node.ID] = client
	}
	if len(agents) > 0 {
		directory := gateway.NodeDirectory{Agents: agents}
		if fleet == nil {
			fleet = directory
			nodes = directory
		} else {
			fleet = combinedFleet{left: fleet, right: directory}
			nodes = combinedNodes{left: nodes, right: directory}
		}
	}
	if fleet == nil || nodes == nil {
		return "", nil, fmt.Errorf("peer config must enable compute or provide join_token/node_urls")
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return "", nil, err
	}
	if len(presets) == 0 {
		return "", nil, fmt.Errorf("peer config/store has no presets")
	}
	projects, err := store.ListProjects(ctx)
	if err != nil {
		return "", nil, err
	}
	reservations, err := store.ListReservations(ctx)
	if err != nil {
		return "", nil, err
	}
	allocator := allocatorFromReservations(reservations, presetMap(presets))
	estimator := peerEstimator(cfg, agents)
	placer := scheduler.NewPlacer(estimator, allocator, clock.System{}, presets...)
	queue := scheduler.NewQueue(clock.System{})
	if err := restoreQueuedJobs(ctx, store, queue); err != nil {
		return "", nil, err
	}
	runtime := &scheduler.Service{
		Placer:  placer,
		Fleet:   fleet,
		Nodes:   nodes,
		Queue:   queue,
		Store:   store,
		Clock:   clock.System{},
		Presets: presetMap(presets),
	}
	if _, err := runtime.ExpireLeases(ctx); err != nil {
		return "", nil, err
	}
	startQueueDrainer(ctx, runtime, clock.System{}, time.Duration(cfg.QueueDrainMS)*time.Millisecond, cfg.QueueDrainLimit)
	startOptimizerEvaluator(ctx, store, clock.System{}, time.Duration(cfg.OptimizerEvalMS)*time.Millisecond)
	handler := gateway.Server{Router: &gateway.Router{
		Placer:         placer,
		Fleet:          fleet,
		Nodes:          nodes,
		Presets:        gateway.NewPresetRegistry(presets...),
		Runtime:        runtime,
		Telemetry:      store,
		Reporter:       gateway.InstanceFailureReporter{Store: store, Nodes: nodes},
		Clock:          clock.System{},
		Sticky:         gateway.NewStickyTable(clock.System{}, 10*time.Minute),
		Projects:       projectMap(projects),
		DefaultProject: cfg.DefaultProject,
	}}
	if mux != nil {
		mux.Handle("/", handler)
		return cfg.Listen, mux, nil
	}
	return cfg.Listen, handler, nil
}

func mountNodeHTTP(mux *http.ServeMux, handler http.Handler) {
	for _, path := range []string{
		"/snapshot",
		"/load",
		"/unload",
		"/inspect",
		"/begin-request",
		"/end-request",
		"/admission/offer",
		"/admission/commit",
		"/admission/release",
		"/admission/preempt",
	} {
		mux.Handle(path, handler)
	}
}

type combinedFleet struct {
	left  gateway.FleetSource
	right gateway.FleetSource
}

func (f combinedFleet) Snapshot(ctx context.Context) (domain.FleetSnapshot, error) {
	left, err := f.left.Snapshot(ctx)
	if err != nil {
		return domain.FleetSnapshot{}, err
	}
	right, err := f.right.Snapshot(ctx)
	if err != nil {
		return domain.FleetSnapshot{}, err
	}
	left.Nodes = append(left.Nodes, right.Nodes...)
	left.Instances = append(left.Instances, right.Instances...)
	return left, nil
}

type combinedNodes struct {
	left  gateway.NodeResolver
	right gateway.NodeResolver
}

func (n combinedNodes) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent, err := n.left.NodeAgent(nodeID)
	if err == nil {
		return agent, nil
	}
	return n.right.NodeAgent(nodeID)
}

type jobLister interface {
	ListJobs(ctx context.Context) ([]domain.Job, error)
}

func restoreQueuedJobs(ctx context.Context, store jobLister, queue *scheduler.Queue) error {
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.Status == domain.JobQueued {
			queue.Enqueue(job)
		}
	}
	return nil
}

func startQueueDrainer(ctx context.Context, runtime *scheduler.Service, clk ports.Clock, interval time.Duration, limit int) {
	if runtime == nil || clk == nil || interval <= 0 || limit <= 0 {
		return
	}
	go func() {
		for {
			timer := clk.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			if _, err := runtime.ExpireLeases(ctx); err != nil {
				log.Printf("mycelium queue lease expiry failed: %v", err)
			}
			if _, err := runtime.Drain(ctx, limit); err != nil {
				log.Printf("mycelium queue drain failed: %v", err)
			}
		}
	}()
}

type optimizerRuntimeStore interface {
	optimizer.RuntimeStore
	optimizer.SpeedCalibrationStore
	ListProjects(ctx context.Context) ([]domain.Project, error)
}

func startOptimizerEvaluator(ctx context.Context, store optimizerRuntimeStore, clk ports.Clock, interval time.Duration) {
	if store == nil || clk == nil || interval <= 0 {
		return
	}
	go func() {
		for {
			timer := clk.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			if err := runOptimizerEvaluation(ctx, store, clk); err != nil {
				log.Printf("mycelium optimizer evaluation failed: %v", err)
			}
		}
	}()
}

func runOptimizerEvaluation(ctx context.Context, store optimizerRuntimeStore, clk ports.Clock) error {
	if store == nil {
		return fmt.Errorf("optimizer store is not configured")
	}
	if clk == nil {
		return fmt.Errorf("optimizer clock is not configured")
	}
	projects, err := store.ListProjects(ctx)
	if err != nil {
		return err
	}
	service := optimizer.RecommendationService{Store: store, Clock: clk}
	for _, project := range projects {
		if _, err := service.EvaluateProject(ctx, project); err != nil {
			return err
		}
	}
	_, err = optimizer.CalibrateSpeedClasses(ctx, store, clk)
	return err
}

func seedControlStore(ctx context.Context, store *storesqlite.Store, cfg PeerConfig) error {
	for _, project := range cfg.Projects {
		if err := store.SaveProject(ctx, project); err != nil {
			return err
		}
	}
	if len(cfg.Projects) == 0 {
		if err := store.SaveProject(ctx, domain.Project{
			ID:         "default",
			Priority:   domain.PriorityInteractive,
			SpeedPref:  domain.SpeedThroughput,
			Preemption: domain.PreemptSoft,
		}); err != nil {
			return err
		}
	}
	for _, preset := range cfg.Presets {
		if err := store.SavePreset(ctx, preset); err != nil {
			return err
		}
	}
	for _, reservation := range cfg.Reservations {
		if err := store.SaveReservation(ctx, reservation); err != nil {
			return err
		}
	}
	return nil
}

func presetMap(presets []domain.Preset) map[string]domain.Preset {
	out := map[string]domain.Preset{}
	for _, preset := range presets {
		out[preset.ID] = preset
		for _, model := range append([]string{preset.ModelRef}, preset.Aliases...) {
			if model == "" {
				continue
			}
			if _, exists := out[model]; !exists {
				out[model] = preset
			}
		}
	}
	return out
}

func projectMap(projects []domain.Project) map[string]domain.Project {
	out := map[string]domain.Project{}
	for _, project := range projects {
		out[project.ID] = project
	}
	return out
}

func peerEstimator(cfg PeerConfig, agents map[string]ports.NodeAgent) ports.ResourceEstimator {
	if cfg.GGUFParser != "" {
		return estimate.NewGGUF(estimate.NewCommandParser(cfg.GGUFParser, nil), agents)
	}
	return estimate.NewInMemory()
}

func allocatorFromReservations(reservations []domain.Reservation, presets map[string]domain.Preset) *lease.Allocator {
	var opts []lease.Option
	for _, reservation := range reservations {
		claim := reservation.Headroom
		if reservation.Kind == domain.ReservationPinned && reservation.PresetID != "" {
			if preset, ok := presets[reservation.PresetID]; ok {
				claim = presetClaim(preset)
			}
		}
		if claim != (domain.Claim{}) {
			opts = append(opts, lease.WithReservedHeadroom(reservation.NodeID, claim))
		}
	}
	return lease.NewAllocator(opts...)
}

func presetClaim(preset domain.Preset) domain.Claim {
	return domain.Claim{
		WeightsMB:    preset.EstWeightsMB,
		KVReservedMB: int(math.Ceil(float64(preset.ContextLength) * preset.KVPerTokenMB)),
	}
}
