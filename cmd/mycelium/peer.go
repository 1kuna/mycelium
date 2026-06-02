package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
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
	"mycelium/internal/telemetry"
)

func runPeer(ctx context.Context, args []string) error {
	if ctx.Err() != nil {
		return nil
	}
	addr, handler, cleanup, err := buildPeerGateway(ctx, args)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	server := &http.Server{Addr: addr, Handler: handler}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = server.Shutdown(shutdownCtx)
		cancel()
		if cleanup != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := cleanup(cleanupCtx); err != nil {
				log.Printf("mycelium peer cleanup failed: %v", err)
			}
		}
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		if ctx.Err() != nil {
			<-shutdownDone
			return nil
		}
		return err
	}
	if ctx.Err() != nil {
		<-shutdownDone
	}
	return nil
}

func buildPeerGateway(ctx context.Context, args []string) (string, http.Handler, func(context.Context) error, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	joinRaw := fs.String("join", "", "join token URI or raw join token")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
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
	diskMinFreeRatio := fs.Float64("disk-min-free-ratio", 0, "minimum free disk ratio required for placement")
	vramMB := fs.Int("vram-mb", 0, "local allocatable memory in MB")
	if err := fs.Parse(args); err != nil {
		return "", nil, nil, err
	}
	cfg, resolvedConfigPath, bootstrappedConfig, err := loadOrBootstrapPeerConfig(*configPath, *joinRaw != "")
	if err != nil {
		return "", nil, nil, err
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	overrideString(discoveryListen, &cfg.DiscoveryListen)
	overrideString(discoveryAddr, &cfg.DiscoveryAddr)
	if *joinRaw != "" {
		join, err := parseJoinFlag(*joinRaw)
		if err != nil {
			return "", nil, nil, err
		}
		cfg.JoinToken = join.Token
		if join.RPCToken != "" {
			cfg.RPCToken = join.RPCToken
		}
		cfg.SeedPeers = appendSeedPeer(cfg.SeedPeers, join.Address)
	}
	overrideString(rpcToken, &cfg.RPCToken)
	if cfg.JoinToken != "" && cfg.RPCToken == "" {
		return "", nil, nil, fmt.Errorf("rpc_token is required when join_token is configured")
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
	if *diskMinFreeRatio != 0 {
		cfg.ComputeConfig.DiskMinFreeRatio = *diskMinFreeRatio
	}
	if *vramMB != 0 {
		cfg.ComputeConfig.VRAMMB = *vramMB
	}
	if bootstrappedConfig {
		if err := savePeerConfig(resolvedConfigPath, cfg); err != nil {
			return "", nil, nil, err
		}
	}
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		return "", nil, nil, err
	}
	if err := seedControlStore(ctx, store, cfg); err != nil {
		_ = store.Close()
		return "", nil, nil, err
	}
	privateKey, err := privateStorageKey(cfg.PrivateStorageKey)
	if err != nil {
		_ = store.Close()
		return "", nil, nil, err
	}

	var fleet gateway.FleetSource
	var nodes gateway.NodeResolver
	var peerDirectory *gateway.PeerDirectory
	mux := http.NewServeMux()
	var discovery ports.PeerDiscovery
	var shutdowns []func(context.Context) error
	var joinTokens *membership.TokenManager
	if cfg.JoinToken != "" {
		joinTokens, err = membership.NewPersistentTokenManager(ctx, cfg.JoinToken, store)
		if err != nil {
			return "", nil, nil, err
		}
		var tunnel ports.Tunnel
		if cfg.Overlay {
			backend, err := membership.NewLibp2pOverlayBackend(ctx, membership.Libp2pOverlayConfig{
				ListenAddrs:    cfg.OverlayListenAddrs,
				BootstrapPeers: cfg.OverlayBootstrap,
				LocalTarget:    cfg.Listen,
				TokenManager:   joinTokens,
			})
			if err != nil {
				return "", nil, nil, err
			}
			shutdowns = append(shutdowns, func(context.Context) error { return backend.CloseHost() })
			discovery = membership.NewOverlayDiscovery(backend)
			tunnel = membership.NewOverlayTunnel(backend)
		} else {
			lan := membership.NewPeerLANDiscovery(cfg.DiscoveryListen, cfg.DiscoveryAddr)
			lan.TokenManager = joinTokens
			lan.ScanDuration = time.Duration(cfg.DiscoveryScanMS) * time.Millisecond
			scan := time.Duration(cfg.DiscoveryScanMS) * time.Millisecond
			cached := membership.NewCachedPeerDiscovery(lan, clock.System{}, peerDiscoveryTTL(scan))
			if err := cached.Start(ctx, scan); err != nil {
				return "", nil, nil, err
			}
			startSeedPeerProber(ctx, cached, cfg.SeedPeers, cfg.JoinToken, joinTokens, clock.System{}, scan)
			discovery = cached
			lanTunnel := membership.NewLANTunnel()
			lanTunnel.AuthToken = cfg.RPCToken
			tunnel = lanTunnel
		}
		peerDirectory = &gateway.PeerDirectory{Discovery: discovery, Tunnel: tunnel, Store: store, SelfID: cfg.ID, AuthToken: cfg.RPCToken}
		fleet = peerDirectory
		nodes = peerDirectory
	}
	agents := map[string]ports.NodeAgent{}
	if cfg.Compute {
		local, err := buildComputeRuntime(ctx, cfg, store)
		if err != nil {
			return "", nil, nil, err
		}
		if err := store.SaveNode(ctx, local.node); err != nil {
			return "", nil, nil, err
		}
		agent, err := newLocalPeerAgent(local.agent, local.admission)
		if err != nil {
			return "", nil, nil, err
		}
		agents[local.node.ID] = agent
		if local.shutdown != nil {
			shutdowns = append(shutdowns, local.shutdown)
		}
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
		return "", nil, nil, fmt.Errorf("peer config must enable compute or provide join_token")
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return "", nil, nil, err
	}
	projects, err := store.ListProjects(ctx)
	if err != nil {
		return "", nil, nil, err
	}
	reservations, err := store.ListReservations(ctx)
	if err != nil {
		return "", nil, nil, err
	}
	allocator := allocatorFromReservations(reservations, presetMap(presets))
	estimator := peerEstimator(cfg, agents, nodes)
	placer := scheduler.NewPlacer(estimator, allocator, clock.System{}, presets...)
	queue := scheduler.NewQueue(clock.System{})
	if err := restoreQueuedJobs(ctx, store, queue); err != nil {
		return "", nil, nil, err
	}
	jobLog := peercoord.NewJobLog()
	self := domain.Peer{ID: cfg.ID, Addresses: []string{cfg.Listen}, Compute: cfg.Compute, LastSeen: clock.System{}.Now(), Version: "dev"}
	var coordinatorOpts []peercoord.CoordinatorOption
	if len(privateKey) > 0 {
		coordinatorOpts = append(coordinatorOpts, peercoord.WithPrivatePayloadKey(privateKey))
	}
	coordinator := peercoord.NewCoordinator(self, jobLog, store, placer, fleet, admissionResolver(nodes), clock.System{}, coordinatorOpts...)
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
		return "", nil, nil, err
	}
	if discovery != nil {
		startPeerAdvertiser(ctx, discovery, self, clock.System{}, time.Duration(cfg.DiscoveryAdvertiseMS)*time.Millisecond)
		startRegistryReplication(ctx, store, discovery, cfg.ID, cfg.RPCToken, clock.System{}, time.Duration(cfg.RegistrySyncMS)*time.Millisecond)
		startPeerHeartbeat(ctx, self, discovery, nodes, runtime, store, cfg.JoinToken, joinTokens, clock.System{})
	}
	startQueueDrainer(ctx, runtime, clock.System{}, time.Duration(cfg.QueueDrainMS)*time.Millisecond, cfg.QueueDrainLimit)
	startOptimizerEvaluator(ctx, store, fleet, cfg.ID, cfg.Compute, clock.System{}, time.Duration(cfg.OptimizerEvalMS)*time.Millisecond, telemetrySyncConfig{
		SelfID:   cfg.ID,
		Peers:    discovery,
		Client:   telemetryHTTPClient{AuthToken: cfg.RPCToken},
		Interval: time.Duration(cfg.OptimizerEvalMS) * time.Millisecond,
	})
	handler := gateway.Server{Router: &gateway.Router{
		Placer:              placer,
		Fleet:               fleet,
		Nodes:               nodes,
		Presets:             gateway.NewPresetRegistry(presets...),
		Runtime:             runtime,
		Telemetry:           store,
		TelemetryPeers:      peerDirectory,
		TelemetryPeerClient: telemetryHTTPClient{AuthToken: cfg.RPCToken},
		SelfNodeID:          privateLocalNodeID(cfg),
		Reporter:            gateway.InstanceFailureReporter{Store: store, Nodes: nodes},
		Clock:               clock.System{},
		Sticky:              gateway.NewStickyTable(clock.System{}, 10*time.Minute),
		Projects:            projectMap(projects),
		DefaultProject:      cfg.DefaultProject,
		PrivateStorage:      len(privateKey) > 0,
		PrivateLocalNodeID:  privateLocalNodeID(cfg),
	}}
	mountPeerHTTP(mux, self, joinTokens)
	mountRegistryHTTP(mux, store, cfg.RPCToken)
	mountTelemetryHTTP(mux, store, cfg.RPCToken)
	mux.Handle("/", handler)
	return cfg.Listen, mux, combineShutdowns(shutdowns), nil
}

func combineShutdowns(shutdowns []func(context.Context) error) func(context.Context) error {
	if len(shutdowns) == 0 {
		return nil
	}
	return func(ctx context.Context) error {
		errs := make([]error, 0, len(shutdowns))
		for _, shutdown := range shutdowns {
			if err := shutdown(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
}

func privateStorageKey(raw string) ([]byte, error) {
	if raw == "" {
		return nil, nil
	}
	key := []byte(raw)
	if len(key) != 32 {
		return nil, fmt.Errorf("private_storage_key must be 32 bytes")
	}
	return key, nil
}

func privateLocalNodeID(cfg PeerConfig) string {
	if !cfg.Compute {
		return ""
	}
	return cfg.ComputeConfig.ID
}

func mountPeerHTTP(mux *http.ServeMux, self domain.Peer, joinTokens *membership.TokenManager) {
	mux.HandleFunc("/peer/health", func(w http.ResponseWriter, r *http.Request) {
		if !peerJoinAuthorized(r, joinTokens) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "join token required"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(self); err != nil {
			panic(err)
		}
	})
}

func mountRegistryHTTP(mux *http.ServeMux, registry ports.JobRegistry, rpcToken string) {
	mux.HandleFunc("/registry/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		records, err := registry.Snapshot(r.Context())
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(records); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/registry/records", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var records []domain.JobRecord
		if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		for _, rec := range records {
			if err := registry.Put(r.Context(), rec); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

type telemetryRPCStore interface {
	ports.TelemetryStore
	SaveRecommendation(ctx context.Context, rec domain.RecommendationRecord) error
	ListRecommendations(ctx context.Context, projectID string) ([]domain.RecommendationRecord, error)
}

func mountTelemetryHTTP(mux *http.ServeMux, store telemetryRPCStore, rpcToken string) {
	mux.HandleFunc("/telemetry/metrics", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		switch r.Method {
		case http.MethodGet:
			metrics, err := store.Metrics(r.Context(), "")
			if err != nil {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(metrics); err != nil {
				panic(err)
			}
		case http.MethodPost:
			var metrics []domain.RunMetric
			if err := json.NewDecoder(r.Body).Decode(&metrics); err != nil {
				writePeerRPCError(w, http.StatusBadRequest, err)
				return
			}
			for _, metric := range metrics {
				if err := store.Record(r.Context(), metric); err != nil {
					writePeerRPCError(w, http.StatusInternalServerError, err)
					return
				}
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/telemetry/recommendations", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		switch r.Method {
		case http.MethodGet:
			recs, err := store.ListRecommendations(r.Context(), "")
			if err != nil {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(recs); err != nil {
				panic(err)
			}
		case http.MethodPost:
			var recs []domain.RecommendationRecord
			if err := json.NewDecoder(r.Body).Decode(&recs); err != nil {
				writePeerRPCError(w, http.StatusBadRequest, err)
				return
			}
			for _, rec := range recs {
				if err := store.SaveRecommendation(r.Context(), rec); err != nil {
					writePeerRPCError(w, http.StatusInternalServerError, err)
					return
				}
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
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
		"/admission/bind-instance",
		"/admission/lease",
		"/admission/lease-by-instance",
		"/instances/",
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
	ports.LeaseBinder
}

func newLocalPeerAgent(agent ports.NodeAgent, admission ports.AdmissionController) (localPeerAgent, error) {
	inspector, ok := admission.(ports.LeaseInspector)
	if !ok {
		return localPeerAgent{}, fmt.Errorf("local admission controller does not expose lease inspection")
	}
	binder, ok := admission.(ports.LeaseBinder)
	if !ok {
		return localPeerAgent{}, fmt.Errorf("local admission controller does not expose lease binding")
	}
	return localPeerAgent{NodeAgent: agent, AdmissionController: admission, LeaseInspector: inspector, LeaseBinder: binder}, nil
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
	lastErr := ""
	advertise := func() {
		self.LastSeen = clk.Now()
		if err := discovery.Advertise(ctx, self); err != nil {
			if msg := err.Error(); msg != lastErr {
				log.Printf("mycelium peer advertise failed: %v", err)
				lastErr = msg
			}
			return
		}
		if lastErr != "" {
			log.Printf("mycelium peer advertise recovered")
			lastErr = ""
		}
	}
	advertise()
	go func() {
		for {
			timer := clk.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			advertise()
		}
	}()
}

func parseJoinFlag(raw string) (membership.JoinInfo, error) {
	if raw == "" {
		return membership.JoinInfo{}, fmt.Errorf("join token is required")
	}
	if strings.HasPrefix(raw, "mycjoin://") {
		info, err := membership.ParseJoinToken(raw)
		if err != nil {
			return membership.JoinInfo{}, err
		}
		return info, nil
	}
	return membership.JoinInfo{Token: raw}, nil
}

func appendSeedPeer(seeds []string, seed string) []string {
	seed = strings.TrimSpace(seed)
	if seed == "" {
		return seeds
	}
	for _, existing := range seeds {
		if existing == seed {
			return seeds
		}
	}
	return append(seeds, seed)
}

func startSeedPeerProber(ctx context.Context, cache *membership.CachedPeerDiscovery, seeds []string, joinToken string, joinTokens *membership.TokenManager, clk ports.Clock, interval time.Duration) {
	startSeedPeerProberWithClient(ctx, cache, seeds, joinToken, joinTokens, clk, interval, peerControlHTTPClient())
}

func startSeedPeerProberWithClient(ctx context.Context, cache *membership.CachedPeerDiscovery, seeds []string, joinToken string, joinTokens *membership.TokenManager, clk ports.Clock, interval time.Duration, client *http.Client) {
	if cache == nil || clk == nil || len(seeds) == 0 {
		return
	}
	interval = seedPeerProbeInterval(interval)
	probe := func() {
		probeSeedPeersWithClient(ctx, cache, seeds, joinToken, joinTokens, client)
	}
	probe()
	go func() {
		for {
			timer := clk.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			probe()
		}
	}()
}

func seedPeerProbeInterval(interval time.Duration) time.Duration {
	if interval < 5*time.Second {
		return 5 * time.Second
	}
	return interval
}

func probeSeedPeers(ctx context.Context, cache *membership.CachedPeerDiscovery, seeds []string, joinToken string, joinTokens *membership.TokenManager) {
	probeSeedPeersWithClient(ctx, cache, seeds, joinToken, joinTokens, peerControlHTTPClient())
}

func probeSeedPeersWithClient(ctx context.Context, cache *membership.CachedPeerDiscovery, seeds []string, joinToken string, joinTokens *membership.TokenManager, client *http.Client) {
	token, err := authorizedOutboundJoinToken(joinToken, joinTokens)
	if err != nil {
		log.Printf("mycelium peer seed probe skipped: %v", err)
		return
	}
	for _, seed := range seeds {
		peer, err := fetchPeerHealthWithClient(ctx, seed, token, client)
		if err != nil {
			log.Printf("mycelium peer seed probe failed: seed=%s error=%v", seed, err)
			continue
		}
		if err := cache.Remember(peer); err != nil {
			log.Printf("mycelium peer seed cache failed: seed=%s error=%v", seed, err)
		}
	}
}

func probePeerHealth(ctx context.Context, peer domain.Peer) error {
	return probePeerHealthWithClient(ctx, peer, "", peerControlHTTPClient())
}

func probePeerHealthWithToken(ctx context.Context, peer domain.Peer, joinToken string) error {
	return probePeerHealthWithClient(ctx, peer, joinToken, peerControlHTTPClient())
}

func probePeerHealthWithClient(ctx context.Context, peer domain.Peer, joinToken string, client *http.Client) error {
	if peer.ID == "" {
		return fmt.Errorf("peer id is required")
	}
	if len(peer.Addresses) == 0 {
		return fmt.Errorf("peer %q has no reachable address", peer.ID)
	}
	got, err := fetchPeerHealthWithClient(ctx, peer.Addresses[0], joinToken, client)
	if err != nil {
		return err
	}
	if got.ID != peer.ID {
		return fmt.Errorf("peer health returned %q for %q", got.ID, peer.ID)
	}
	return nil
}

func probePeerHealthWithTokenManager(ctx context.Context, peer domain.Peer, joinToken string, joinTokens *membership.TokenManager) error {
	return probePeerHealthWithTokenManagerAndClient(ctx, peer, joinToken, joinTokens, peerControlHTTPClient())
}

func probePeerHealthWithTokenManagerAndClient(ctx context.Context, peer domain.Peer, joinToken string, joinTokens *membership.TokenManager, client *http.Client) error {
	token, err := authorizedOutboundJoinToken(joinToken, joinTokens)
	if err != nil {
		return err
	}
	return probePeerHealthWithClient(ctx, peer, token, client)
}

func authorizedOutboundJoinToken(joinToken string, joinTokens *membership.TokenManager) (string, error) {
	if joinTokens == nil {
		return joinToken, nil
	}
	if err := joinTokens.Validate(joinToken); err != nil {
		return "", err
	}
	return joinToken, nil
}

func fetchPeerHealth(ctx context.Context, address, joinToken string) (domain.Peer, error) {
	return fetchPeerHealthWithClient(ctx, address, joinToken, peerControlHTTPClient())
}

func fetchPeerHealthWithClient(ctx context.Context, address, joinToken string, client *http.Client) (domain.Peer, error) {
	reachable, err := reachablePeerAddress(address)
	if err != nil {
		return domain.Peer{}, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, peerHTTPBaseURL(address)+"/peer/health", nil)
	if err != nil {
		return domain.Peer{}, err
	}
	if joinToken != "" {
		req.Header.Set("X-Myc-Join-Token", joinToken)
	}
	if client == nil {
		client = peerControlHTTPClient()
	}
	resp, err := client.Do(req)
	if err != nil {
		return domain.Peer{}, fmt.Errorf("%w: %v", domain.ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return domain.Peer{}, fmt.Errorf("%w: peer health returned %s: %s", domain.ErrUnreachable, resp.Status, strings.TrimSpace(string(body)))
	}
	var got domain.Peer
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		return domain.Peer{}, err
	}
	if got.ID == "" {
		return domain.Peer{}, fmt.Errorf("peer health returned missing id")
	}
	got.Addresses = prependReachableAddress(got.Addresses, reachable)
	return got, nil
}

func peerHTTPBaseURL(address string) string {
	address = strings.TrimRight(strings.TrimSpace(address), "/")
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return address
	}
	return "http://" + address
}

func reachablePeerAddress(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", fmt.Errorf("peer address is required")
	}
	if strings.Contains(address, "://") {
		parsed, err := url.Parse(address)
		if err != nil {
			return "", err
		}
		if parsed.Host == "" {
			return "", fmt.Errorf("peer address %q is missing host", address)
		}
		return parsed.Host, nil
	}
	return address, nil
}

func prependReachableAddress(addresses []string, reachable string) []string {
	out := []string{reachable}
	for _, address := range addresses {
		if address == reachable {
			continue
		}
		out = append(out, address)
	}
	return out
}

func peerJoinAuthorized(r *http.Request, joinTokens *membership.TokenManager) bool {
	if joinTokens == nil {
		return true
	}
	got := r.Header.Get("X-Myc-Join-Token")
	return joinTokens.Validate(got) == nil
}

func peerRPCAuthorized(r *http.Request, rpcToken string) bool {
	if rpcToken == "" {
		return true
	}
	const prefix = "Bearer "
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	got := strings.TrimPrefix(value, prefix)
	return subtle.ConstantTimeCompare([]byte(got), []byte(rpcToken)) == 1
}

func writePeerAuthError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "rpc token required"})
}

func writePeerRPCError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

type registryHTTPClient struct {
	AuthToken string
	Client    *http.Client
}

func (c registryHTTPClient) Snapshot(ctx context.Context, peer domain.Peer) ([]domain.JobRecord, error) {
	var records []domain.JobRecord
	err := c.do(ctx, peer, http.MethodGet, "/registry/snapshot", nil, &records)
	return records, err
}

func (c registryHTTPClient) Push(ctx context.Context, peer domain.Peer, records []domain.JobRecord) error {
	return c.do(ctx, peer, http.MethodPost, "/registry/records", records, nil)
}

var _ peercoord.RegistryClient = registryHTTPClient{}

func (c registryHTTPClient) do(ctx context.Context, peer domain.Peer, method, path string, in, out any) error {
	return doPeerRPC(ctx, c.Client, c.AuthToken, peer, method, path, in, out)
}

type telemetryHTTPClient struct {
	AuthToken string
	Client    *http.Client
}

func (c telemetryHTTPClient) Metrics(ctx context.Context, peer domain.Peer) ([]domain.RunMetric, error) {
	var metrics []domain.RunMetric
	err := doPeerRPC(ctx, c.Client, c.AuthToken, peer, http.MethodGet, "/telemetry/metrics", nil, &metrics)
	return metrics, err
}

func (c telemetryHTTPClient) PushMetrics(ctx context.Context, peer domain.Peer, metrics []domain.RunMetric) error {
	return doPeerRPC(ctx, c.Client, c.AuthToken, peer, http.MethodPost, "/telemetry/metrics", metrics, nil)
}

func (c telemetryHTTPClient) Recommendations(ctx context.Context, peer domain.Peer) ([]domain.RecommendationRecord, error) {
	var recs []domain.RecommendationRecord
	err := doPeerRPC(ctx, c.Client, c.AuthToken, peer, http.MethodGet, "/telemetry/recommendations", nil, &recs)
	return recs, err
}

func (c telemetryHTTPClient) PushRecommendations(ctx context.Context, peer domain.Peer, recs []domain.RecommendationRecord) error {
	return doPeerRPC(ctx, c.Client, c.AuthToken, peer, http.MethodPost, "/telemetry/recommendations", recs, nil)
}

var _ ports.TelemetryPeerClient = telemetryHTTPClient{}

func doPeerRPC(ctx context.Context, client *http.Client, authToken string, peer domain.Peer, method, path string, in, out any) error {
	if len(peer.Addresses) == 0 {
		return fmt.Errorf("peer %q has no reachable address", peer.ID)
	}
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, peerHTTPBaseURL(peer.Addresses[0])+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	if client == nil {
		client = peerControlHTTPClient()
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("peer rpc %s %s: %s", method, path, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func peerControlHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}}
}

func startRegistryReplication(ctx context.Context, registry ports.JobRegistry, discovery ports.PeerDiscovery, selfID, rpcToken string, clk ports.Clock, interval time.Duration) {
	if registry == nil || discovery == nil || selfID == "" || clk == nil || interval <= 0 {
		return
	}
	replicator := peercoord.RegistryReplicator{
		Local:  registry,
		Peers:  discovery,
		Client: registryHTTPClient{AuthToken: rpcToken},
		SelfID: selfID,
	}
	pushWatch, err := registry.Watch(ctx, "")
	if err != nil {
		log.Printf("mycelium registry watch failed: %v", err)
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case rec, ok := <-pushWatch:
				if !ok {
					return
				}
				if err := replicator.PushRecord(ctx, rec); err != nil {
					log.Printf("mycelium registry push failed: %v", err)
				}
			}
		}
	}()
	go func() {
		for {
			if err := replicator.SyncOnce(ctx); err != nil {
				log.Printf("mycelium registry sync failed: %v", err)
			}
			timer := clk.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
		}
	}()
}

type peerLeaseInspectorResolver interface {
	LeaseInspector(string) (ports.LeaseInspector, error)
}

type peerRuntimeStore interface {
	ports.JobRegistry
	SaveNode(ctx context.Context, node domain.Node) error
}

type rescueRuntime interface {
	SubmitWithPayload(ctx context.Context, job domain.Job, payload []byte, hooks ...scheduler.SubmitHooks) (scheduler.Result, error)
}

func startPeerHeartbeat(ctx context.Context, self domain.Peer, discovery ports.PeerDiscovery, nodes gateway.NodeResolver, runtime rescueRuntime, registry peerRuntimeStore, joinToken string, joinTokens *membership.TokenManager, clk ports.Clock) {
	if discovery == nil || runtime == nil || registry == nil || clk == nil || self.ID == "" {
		return
	}
	owners, _ := nodes.(peerLeaseInspectorResolver)
	recovery := peercoord.Recovery{Registry: registry, Owners: owners, Rescue: rescueRecoveredJob(runtime), Clock: clk}
	heartbeat := &peercoord.Heartbeat{
		Self:      self,
		Discovery: discovery,
		Clock:     clk,
		Probe: func(ctx context.Context, peer domain.Peer) error {
			return probePeerHealthWithTokenManager(ctx, peer, joinToken, joinTokens)
		},
		OnDead: func(ctx context.Context, dead domain.Peer) error {
			if err := markDeadPeer(registry)(ctx, dead); err != nil {
				return err
			}
			rescued, err := recovery.RecoverPeer(ctx, dead.ID)
			if err != nil {
				return err
			}
			if rescued > 0 {
				log.Printf("mycelium recovered %d unfinished jobs from dead peer %s", rescued, dead.ID)
			}
			return nil
		},
	}
	go func() {
		for {
			if _, err := heartbeat.Tick(ctx); err != nil {
				log.Printf("mycelium peer heartbeat failed: %v", err)
			}
			timer := clk.NewTimer(peercoord.DefaultHeartbeatInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
		}
	}()
}

func rescueRecoveredJob(runtime rescueRuntime) peercoord.RescueFunc {
	return func(ctx context.Context, rec domain.JobRecord) error {
		job, payload, err := peercoord.DecodeRescuePayload(rec.Request)
		if err != nil {
			return err
		}
		if job.ID != rec.JobID {
			return fmt.Errorf("rescue payload job %q does not match registry job %q", job.ID, rec.JobID)
		}
		_, err = runtime.SubmitWithPayload(ctx, job, payload)
		return err
	}
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
	ListRecommendations(ctx context.Context, projectID string) ([]domain.RecommendationRecord, error)
}

type telemetrySyncConfig struct {
	SelfID   string
	Peers    ports.PeerDiscovery
	Client   ports.TelemetryPeerClient
	Interval time.Duration
}

type telemetrySyncResult struct {
	SlotID                  string
	ImportedMetrics         int
	ImportedRecommendations int
	PushedRecommendations   int
	SkippedPeers            []string
}

func startOptimizerEvaluator(ctx context.Context, store optimizerRuntimeStore, fleet gateway.FleetSource, selfID string, compute bool, clk ports.Clock, interval time.Duration, syncCfg telemetrySyncConfig) {
	if store == nil || fleet == nil || selfID == "" || !compute || clk == nil || interval <= 0 {
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
			ok, err := shouldRunGroupOptimizer(ctx, fleet, selfID, clk, interval)
			if err != nil {
				log.Printf("mycelium optimizer group selection failed: %v", err)
				continue
			}
			if !ok {
				continue
			}
			result, err := runOptimizerEvaluation(ctx, store, clk, syncCfg)
			if len(result.SkippedPeers) > 0 {
				log.Printf("mycelium optimizer telemetry skipped peers: %s", strings.Join(result.SkippedPeers, "; "))
			}
			if err != nil {
				log.Printf("mycelium optimizer evaluation failed: %v", err)
			}
		}
	}()
}

func shouldRunGroupOptimizer(ctx context.Context, fleet gateway.FleetSource, selfID string, clk ports.Clock, interval time.Duration) (bool, error) {
	if fleet == nil {
		return false, fmt.Errorf("optimizer fleet source is not configured")
	}
	if selfID == "" {
		return false, fmt.Errorf("optimizer self id is required")
	}
	if clk == nil {
		return false, fmt.Errorf("optimizer clock is not configured")
	}
	snap, err := fleet.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	selected, ok := telemetry.SelectGroupAnalysisNode(snap.Nodes, clk.Now(), interval)
	return ok && selected.ID == selfID, nil
}

func runOptimizerEvaluation(ctx context.Context, store optimizerRuntimeStore, clk ports.Clock, syncCfg telemetrySyncConfig) (telemetrySyncResult, error) {
	if store == nil {
		return telemetrySyncResult{}, fmt.Errorf("optimizer store is not configured")
	}
	if clk == nil {
		return telemetrySyncResult{}, fmt.Errorf("optimizer clock is not configured")
	}
	slotID := telemetry.AnalysisSlotID(clk.Now(), syncCfg.Interval)
	syncResult, reachablePeers, err := pullFleetTelemetry(ctx, store, syncCfg)
	syncResult.SlotID = slotID
	if err != nil {
		return syncResult, err
	}
	projects, err := store.ListProjects(ctx)
	if err != nil {
		return syncResult, err
	}
	service := optimizer.RecommendationService{Store: store, Clock: clk}
	for _, project := range projects {
		records, err := service.EvaluateProject(ctx, project)
		if err != nil {
			return syncResult, err
		}
		for _, rec := range records {
			if rec.SlotID == "" {
				rec.SlotID = slotID
				if err := store.SaveRecommendation(ctx, rec); err != nil {
					return syncResult, err
				}
			}
		}
	}
	if _, err := optimizer.CalibrateSpeedClasses(ctx, store, clk); err != nil {
		return syncResult, err
	}
	if err := pushFleetRecommendations(ctx, store, syncCfg, reachablePeers, slotID, &syncResult); err != nil {
		return syncResult, err
	}
	return syncResult, nil
}

func pullFleetTelemetry(ctx context.Context, store optimizerRuntimeStore, cfg telemetrySyncConfig) (telemetrySyncResult, []domain.Peer, error) {
	if cfg.Peers == nil || cfg.Client == nil {
		return telemetrySyncResult{}, nil, nil
	}
	peers, err := cfg.Peers.Peers(ctx)
	if err != nil {
		return telemetrySyncResult{}, nil, err
	}
	result := telemetrySyncResult{}
	reachable := make([]domain.Peer, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == "" || peer.ID == cfg.SelfID {
			continue
		}
		metrics, err := cfg.Client.Metrics(ctx, peer)
		if err != nil {
			result.SkippedPeers = append(result.SkippedPeers, fmt.Sprintf("%s metrics: %v", peer.ID, err))
			continue
		}
		reachable = append(reachable, peer)
		for _, metric := range metrics {
			if err := store.Record(ctx, metric); err != nil {
				return result, reachable, fmt.Errorf("import telemetry metric %q from peer %q: %w", metric.JobID, peer.ID, err)
			}
			result.ImportedMetrics++
		}
		recs, err := cfg.Client.Recommendations(ctx, peer)
		if err != nil {
			result.SkippedPeers = append(result.SkippedPeers, fmt.Sprintf("%s recommendations: %v", peer.ID, err))
			continue
		}
		for _, rec := range recs {
			if err := store.SaveRecommendation(ctx, rec); err != nil {
				return result, reachable, fmt.Errorf("import recommendation %q from peer %q: %w", rec.ID, peer.ID, err)
			}
			result.ImportedRecommendations++
		}
	}
	sort.Strings(result.SkippedPeers)
	return result, reachable, nil
}

func pushFleetRecommendations(ctx context.Context, store optimizerRuntimeStore, cfg telemetrySyncConfig, peers []domain.Peer, slotID string, result *telemetrySyncResult) error {
	if cfg.Client == nil || len(peers) == 0 {
		return nil
	}
	recs, err := store.ListRecommendations(ctx, "")
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		return nil
	}
	recs = recommendationsForSlot(recs, slotID)
	if len(recs) == 0 {
		return nil
	}
	for _, peer := range peers {
		if err := cfg.Client.PushRecommendations(ctx, peer, recs); err != nil {
			result.SkippedPeers = append(result.SkippedPeers, fmt.Sprintf("%s push-recommendations: %v", peer.ID, err))
			continue
		}
		result.PushedRecommendations += len(recs)
	}
	sort.Strings(result.SkippedPeers)
	return nil
}

func recommendationsForSlot(recs []domain.RecommendationRecord, slotID string) []domain.RecommendationRecord {
	seen := map[string]struct{}{}
	out := make([]domain.RecommendationRecord, 0, len(recs))
	for _, rec := range recs {
		if slotID != "" && rec.SlotID != slotID {
			continue
		}
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		seen[rec.ID] = struct{}{}
		out = append(out, rec)
	}
	return out
}

func seedControlStore(ctx context.Context, store *storesqlite.Store, cfg PeerConfig) error {
	for _, project := range cfg.Projects {
		if err := store.SaveProject(ctx, project); err != nil {
			return err
		}
	}
	if len(cfg.Projects) == 0 {
		if err := store.SaveProject(ctx, domain.Project{
			ID:                  "default",
			Priority:            domain.PriorityInteractive,
			SpeedPref:           domain.SpeedThroughput,
			ExpectedConcurrency: 1,
			Preemption:          domain.PreemptSoft,
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

func peerEstimator(cfg PeerConfig, agents map[string]ports.NodeAgent, resolver estimate.NodeAgentResolver) ports.ResourceEstimator {
	explicit := estimate.NewInMemory()
	parser := cfg.GGUFParser
	if parser == "" {
		parser = "gguf-parser"
	}
	return estimate.NewBackendAware(estimate.NewGGUFWithResolver(estimate.NewCommandParser(parser, nil), agents, resolver), explicit)
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
