package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	"mycelium/internal/optimizer"
	peercoord "mycelium/internal/peer"
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
	joinRaw := fs.String("join", "", "join token URI or raw join token")
	listen := fs.String("listen", "", "peer listen address override")
	discoveryListen := fs.String("discovery-listen", "", "peer discovery listen address override")
	discoveryAddr := fs.String("discovery-addr", "", "peer discovery broadcast address override")
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
	overrideString(discoveryListen, &cfg.DiscoveryListen)
	overrideString(discoveryAddr, &cfg.DiscoveryAddr)
	if *joinRaw != "" {
		token, err := parseJoinFlag(*joinRaw)
		if err != nil {
			return "", nil, err
		}
		cfg.JoinToken = token
	}
	if *compute {
		cfg.Compute = true
	}
	overrideString(backendListen, &cfg.ComputeConfig.BackendListen)
	overrideString(id, &cfg.ComputeConfig.ID)
	overrideString(id, &cfg.ID)
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
	mux := http.NewServeMux()
	var discovery ports.PeerDiscovery
	if cfg.JoinToken != "" {
		if _, err := membership.NewPersistentTokenManager(ctx, cfg.JoinToken, store); err != nil {
			return "", nil, err
		}
		lan := membership.NewPeerLANDiscovery(cfg.DiscoveryListen, cfg.DiscoveryAddr)
		lan.Token = cfg.JoinToken
		lan.ScanDuration = time.Duration(cfg.DiscoveryScanMS) * time.Millisecond
		scan := time.Duration(cfg.DiscoveryScanMS) * time.Millisecond
		cached := membership.NewCachedPeerDiscovery(lan, clock.System{}, peerDiscoveryTTL(scan))
		if err := cached.Start(ctx, scan); err != nil {
			return "", nil, err
		}
		discovery = cached
		directory := &gateway.PeerDirectory{Discovery: discovery, Store: store, SelfID: cfg.ID}
		fleet = directory
		nodes = directory
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
		inspector, ok := local.admission.(ports.LeaseInspector)
		if !ok {
			return "", nil, fmt.Errorf("local admission controller does not expose lease inspection")
		}
		agents[local.node.ID] = localPeerAgent{NodeAgent: local.agent, AdmissionController: local.admission, LeaseInspector: inspector}
		mountNodeHTTP(mux, local.handler)
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
		return "", nil, fmt.Errorf("peer config must enable compute or provide join_token")
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
	jobLog := peercoord.NewJobLog()
	self := domain.Peer{ID: cfg.ID, Addresses: []string{cfg.Listen}, Compute: cfg.Compute, LastSeen: clock.System{}.Now(), Version: "dev"}
	if discovery != nil {
		startPeerAdvertiser(ctx, discovery, self, clock.System{}, time.Duration(cfg.DiscoveryAdvertiseMS)*time.Millisecond)
	}
	coordinator := peercoord.NewCoordinator(self, jobLog, store, placer, fleet, admissionResolver(nodes), clock.System{})
	runtime := &scheduler.Service{
		Placer:      placer,
		Fleet:       fleet,
		Nodes:       nodes,
		Owners:      admissionResolver(nodes),
		Coordinator: coordinator,
		JobLog:      jobLog,
		Queue:       queue,
		Store:       store,
		Clock:       clock.System{},
		Presets:     presetMap(presets),
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
	mountPeerHTTP(mux, self)
	mux.Handle("/", handler)
	return cfg.Listen, mux, nil
}

func mountPeerHTTP(mux *http.ServeMux, self domain.Peer) {
	mux.HandleFunc("/peer/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(self); err != nil {
			panic(err)
		}
	})
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
		"/admission/lease",
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

type localPeerAgent struct {
	ports.NodeAgent
	ports.AdmissionController
	ports.LeaseInspector
}

func (n combinedNodes) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent, err := n.left.NodeAgent(nodeID)
	if err == nil {
		return agent, nil
	}
	return n.right.NodeAgent(nodeID)
}

func (n combinedNodes) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	if left, ok := n.left.(scheduler.AdmissionResolver); ok {
		admission, err := left.AdmissionController(nodeID)
		if err == nil {
			return admission, nil
		}
	}
	right, ok := n.right.(scheduler.AdmissionResolver)
	if !ok {
		return nil, fmt.Errorf("node resolver does not expose admission")
	}
	return right.AdmissionController(nodeID)
}

func (n combinedNodes) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	if left, ok := n.left.(interface {
		LeaseInspector(string) (ports.LeaseInspector, error)
	}); ok {
		inspector, err := left.LeaseInspector(nodeID)
		if err == nil {
			return inspector, nil
		}
	}
	right, ok := n.right.(interface {
		LeaseInspector(string) (ports.LeaseInspector, error)
	})
	if !ok {
		return nil, fmt.Errorf("node resolver does not expose lease inspection")
	}
	return right.LeaseInspector(nodeID)
}

func admissionResolver(nodes gateway.NodeResolver) scheduler.AdmissionResolver {
	admissions, ok := nodes.(scheduler.AdmissionResolver)
	if !ok {
		return nil
	}
	return admissions
}

func peerDiscoveryTTL(scan time.Duration) time.Duration {
	if scan <= 0 {
		scan = 250 * time.Millisecond
	}
	ttl := 5 * scan
	if ttl < 5*time.Second {
		return 5 * time.Second
	}
	return ttl
}

func startPeerAdvertiser(ctx context.Context, discovery ports.PeerDiscovery, self domain.Peer, clk ports.Clock, interval time.Duration) {
	if discovery == nil || clk == nil || self.ID == "" || interval <= 0 {
		return
	}
	self.LastSeen = clk.Now()
	if err := discovery.Advertise(ctx, self); err != nil {
		log.Printf("mycelium peer advertise failed: %v", err)
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
			self.LastSeen = clk.Now()
			if err := discovery.Advertise(ctx, self); err != nil {
				log.Printf("mycelium peer advertise failed: %v", err)
			}
		}
	}()
}

func parseJoinFlag(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("join token is required")
	}
	if strings.HasPrefix(raw, "mycjoin://") {
		info, err := membership.ParseJoinToken(raw)
		if err != nil {
			return "", err
		}
		return info.Token, nil
	}
	return raw, nil
}

func probePeerHealth(ctx context.Context, peer domain.Peer) error {
	if peer.ID == "" {
		return fmt.Errorf("peer id is required")
	}
	if len(peer.Addresses) == 0 {
		return fmt.Errorf("peer %q has no reachable address", peer.ID)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, peerHTTPBaseURL(peer.Addresses[0])+"/peer/health", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: peer health returned %s: %s", domain.ErrUnreachable, resp.Status, strings.TrimSpace(string(body)))
	}
	var got domain.Peer
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		return err
	}
	if got.ID != peer.ID {
		return fmt.Errorf("peer health returned %q for %q", got.ID, peer.ID)
	}
	return nil
}

func peerHTTPBaseURL(address string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return strings.TrimRight(address, "/")
	}
	return "http://" + address
}

func markDeadPeer(store interface {
	SaveNode(ctx context.Context, node domain.Node) error
}) peercoord.DeadPeerFunc {
	return func(ctx context.Context, peer domain.Peer) error {
		if store == nil || !peer.Compute {
			return nil
		}
		node := domain.Node{ID: peer.ID, Name: peer.ID, Status: domain.NodeUnreachable}
		if len(peer.Addresses) > 0 {
			node.Address = peer.Addresses[0]
		}
		return store.SaveNode(ctx, node)
	}
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
