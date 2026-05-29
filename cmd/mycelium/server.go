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

func runServer(ctx context.Context, args []string) error {
	if ctx.Err() != nil {
		return nil
	}
	addr, handler, err := buildGatewayServer(ctx, args)
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

func buildGatewayServer(ctx context.Context, args []string) (string, http.Handler, error) {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	configPath := fs.String("config", "", "server config JSON path")
	listen := fs.String("listen", "", "gateway listen address override")
	if err := fs.Parse(args); err != nil {
		return "", nil, err
	}
	cfg, err := loadServerConfig(*configPath)
	if err != nil {
		return "", nil, err
	}
	if *listen != "" {
		cfg.Listen = *listen
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
		}
	}
	if fleet == nil || nodes == nil {
		return "", nil, fmt.Errorf("server config must provide join_token or node_urls")
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return "", nil, err
	}
	if len(presets) == 0 {
		return "", nil, fmt.Errorf("server config/store has no presets")
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
	estimator := serverEstimator(cfg, agents)
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

func seedControlStore(ctx context.Context, store *storesqlite.Store, cfg ServerConfig) error {
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

func serverEstimator(cfg ServerConfig, agents map[string]ports.NodeAgent) ports.ResourceEstimator {
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
