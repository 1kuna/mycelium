package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/gateway/translate"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	"mycelium/internal/optimizer"
	peercoord "mycelium/internal/peer"
	"mycelium/internal/ports"
	projectvalidation "mycelium/internal/project"
	"mycelium/internal/reconcile"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/internal/telemetry"
	"mycelium/internal/workload"
)

const maxPeerRPCJSONBodyBytes = 16 << 20
const maxWorkloadResultBodyBytes = 16 << 20

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
	server := &http.Server{Addr: addr, Handler: handler, IdleTimeout: 5 * time.Second, ReadHeaderTimeout: 5 * time.Second}
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
	gatewayToken := fs.String("gateway-token", "", "gateway /v1 bearer token")
	listen := fs.String("listen", "", "peer listen address override")
	discoveryListen := fs.String("discovery-listen", "", "peer discovery listen address override")
	discoveryAddr := fs.String("discovery-addr", "", "peer discovery broadcast address override")
	var compute computeOverrideFlag
	fs.Var(&compute, "compute", "local compute runtime override (on/off)")
	backendListen := fs.String("backend-listen", "", "local backend inference server listen address")
	id := fs.String("id", "", "local compute peer id")
	name := fs.String("name", "", "local compute peer name")
	backend := fs.String("backend", "", "local backend engine (llamacpp, mlx, vllm, openvino, custom)")
	backendBinary := fs.String("backend-binary", "", "local backend server binary override")
	llamaServer := fs.String("llama-server", "", "llama.cpp server binary")
	ggufParser := fs.String("gguf-parser", "", "local GGUF parser binary")
	maxUtil := fs.Float64("max-util", 0, "maximum accelerator utilization")
	diskMinFreeRatio := fs.Float64("disk-min-free-ratio", 0, "minimum free disk ratio required for placement")
	loadTimeoutMS := fs.Int("load-timeout-ms", 0, "backend load timeout in milliseconds")
	var vramMB optionalIntFlag
	fs.Var(&vramMB, "vram-mb", "local allocatable memory in MB; set 0 to clear config override")
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
		cfg.SeedPeers = appendSeedPeer(cfg.SeedPeers, join.Address)
	}
	if bootstrappedConfig && *joinRaw != "" {
		peerID, err := prefixedRandomID("peer", randomHex)
		if err != nil {
			return "", nil, nil, err
		}
		cfg.ID = peerID
		cfg.ComputeConfig.ID = peerID
		if cfg.ComputeConfig.Name == "" || cfg.ComputeConfig.Name == "local-peer" || cfg.ComputeConfig.Name == "peer_local" {
			cfg.ComputeConfig.Name = peerID
		}
	}
	overrideString(rpcToken, &cfg.RPCToken)
	overrideString(gatewayToken, &cfg.GatewayToken)
	if cfg.JoinToken != "" && cfg.RPCToken == "" {
		return "", nil, nil, fmt.Errorf("rpc_token is required when join_token is configured")
	}
	if compute.set {
		cfg.Compute = compute.value
	}
	overrideString(backendListen, &cfg.ComputeConfig.BackendListen)
	overrideString(id, &cfg.ComputeConfig.ID)
	overrideString(id, &cfg.ID)
	overrideString(name, &cfg.ComputeConfig.Name)
	if *backend != "" {
		normalized, err := normalizeBackend(*backend)
		if err != nil {
			return "", nil, nil, err
		}
		cfg.ComputeConfig.Backend = normalized
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
	if *loadTimeoutMS != 0 {
		cfg.ComputeConfig.LoadTimeoutMS = *loadTimeoutMS
	}
	if vramMB.set {
		cfg.ComputeConfig.VRAMMB = vramMB.value
	}
	cfg = applyPeerConfigDefaults(cfg)
	if err := validatePeerConfig(cfg); err != nil {
		return "", nil, nil, err
	}
	if bootstrappedConfig {
		if err := savePeerConfig(resolvedConfigPath, cfg); err != nil {
			return "", nil, nil, err
		}
	} else if *joinRaw != "" {
		if err := savePeerJoinConfig(resolvedConfigPath, cfg); err != nil {
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
		lan := membership.NewPeerLANDiscovery(cfg.DiscoveryListen, cfg.DiscoveryAddr)
		lan.TokenManager = joinTokens
		lan.ScanDuration = time.Duration(cfg.DiscoveryScanMS) * time.Millisecond
		scan := time.Duration(cfg.DiscoveryScanMS) * time.Millisecond
		cached := membership.NewCachedPeerDiscovery(lan, clock.System{}, peerDiscoveryTTL(scan))
		if err := cached.Start(ctx, scan); err != nil {
			return "", nil, nil, err
		}
		startSeedPeerProber(ctx, cached, cfg.SeedPeers, cfg.JoinToken, joinTokens, clock.System{}, scan)
		discovery = seedRefreshingDiscovery{
			cache:      cached,
			seeds:      cfg.SeedPeers,
			joinToken:  cfg.JoinToken,
			joinTokens: joinTokens,
			client:     peerControlHTTPClient(),
		}
		lanTunnel := membership.NewLANTunnel()
		lanTunnel.AuthToken = cfg.RPCToken
		tunnel = lanTunnel
		peerDirectory = &gateway.PeerDirectory{Discovery: discovery, Tunnel: tunnel, Store: store, SelfID: cfg.ID, AuthToken: cfg.RPCToken}
		fleet = peerDirectory
		nodes = peerDirectory
		mountAdminHTTP(mux, &cfg, resolvedConfigPath, joinTokens, store, cfg.RPCToken)
	}
	agents := map[string]ports.NodeAgent{}
	var localRuntime *computeRuntime
	if cfg.Compute {
		local, err := buildComputeRuntime(ctx, cfg, store)
		if err != nil {
			return "", nil, nil, err
		}
		localRuntime = &local
		if err := store.SaveNode(ctx, local.node); err != nil {
			return "", nil, nil, err
		}
		agent, err := newLocalPeerAgent(local.agent, local.admission, store)
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
	var presetReport runtimePresetLoadReport
	if localRuntime != nil {
		presetReport, err = loadRuntimePresets(ctx, store, localRuntime.node)
	} else {
		presetReport, err = loadRuntimePresets(ctx, store)
	}
	if err != nil {
		return "", nil, nil, err
	}
	logRuntimePresetLoadReport(presetReport)
	presets := presetReport.Presets
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
	jobLog := peercoord.NewRescueJobLog(peercoord.NewJobLog(), store, privateKey)
	self := domain.Peer{ID: cfg.ID, Addresses: []string{cfg.Listen}, Compute: cfg.Compute, LastSeen: clock.System{}.Now(), Version: "dev"}
	controlClient := peerControlHTTPClient()
	var coordinatorOpts []peercoord.CoordinatorOption
	if len(privateKey) > 0 {
		coordinatorOpts = append(coordinatorOpts, peercoord.WithPrivatePayloadKey(privateKey))
	}
	if discovery != nil {
		coordinatorOpts = append(coordinatorOpts, peercoord.WithRegistryPusher(peercoord.RegistryReplicator{
			Local:  store,
			Peers:  discovery,
			Client: registryHTTPClient{AuthToken: cfg.RPCToken, Client: controlClient},
			SelfID: cfg.ID,
		}))
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
		startRegistryReplicationWithClient(ctx, store, discovery, cfg.ID, cfg.RPCToken, clock.System{}, time.Duration(cfg.RegistrySyncMS)*time.Millisecond, controlClient)
		startPeerHeartbeatWithClient(ctx, self, discovery, nodes, runtime, store, cfg.JoinToken, joinTokens, clock.System{}, controlClient)
	}
	startQueueDrainer(ctx, runtime, clock.System{}, time.Duration(cfg.QueueDrainMS)*time.Millisecond, cfg.QueueDrainLimit)
	optimizerNodeID := cfg.ID
	if cfg.Compute {
		optimizerNodeID = privateLocalNodeID(cfg)
	}
	startOptimizerEvaluator(ctx, store, fleet, optimizerNodeID, cfg.Compute, clock.System{}, time.Duration(cfg.OptimizerEvalMS)*time.Millisecond, telemetrySyncConfig{
		SelfID:   cfg.ID,
		Peers:    discovery,
		Client:   telemetryHTTPClient{AuthToken: cfg.RPCToken, Client: peerControlHTTPClient()},
		Interval: time.Duration(cfg.OptimizerEvalMS) * time.Millisecond,
	})
	authRequired := gatewayAuthConfigured(cfg) || peerListenRequiresAuth(cfg.Listen)
	router := &gateway.Router{
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
		Projects:            projectMap(projects),
		DefaultProject:      cfg.DefaultProject,
	}
	handler := gateway.Server{
		Router:              router,
		RequireAuth:         authRequired,
		AuthToken:           cfg.GatewayToken,
		AuthTokenProjects:   gatewayProjectTokenMap(cfg.GatewayProjectTokens),
		TrustControlHeaders: false,
	}
	mountPeerHTTPWithDiagnostics(mux, self, joinTokens, cfg.SeedPeers, cfg.JoinToken, cfg.RPCToken, peerControlHTTPClient())
	mountRegistryHTTP(mux, store, cfg.RPCToken)
	mountTelemetryHTTP(mux, store, cfg.RPCToken)
	mountCatalogHTTP(mux, cfg, store, cfg.RPCToken, clock.System{})
	mountWorkloadHTTP(mux, runtime, store, cfg.RPCToken, cfg.ID, router)
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

type peerDiagnosticsReport struct {
	Peer  domain.Peer          `json:"peer"`
	Seeds []peerSeedDiagnostic `json:"seeds"`
	Ready bool                 `json:"ready"`
}

type peerSeedDiagnostic struct {
	Address string `json:"address"`
	PeerID  string `json:"peer_id,omitempty"`
	Compute bool   `json:"compute,omitempty"`
	Ready   bool   `json:"ready"`
	Error   string `json:"error,omitempty"`
}

func mountPeerHTTP(mux *http.ServeMux, self domain.Peer, joinTokens *membership.TokenManager) {
	mountPeerHTTPWithDiagnostics(mux, self, joinTokens, nil, "", "", nil)
}

func mountPeerHTTPWithDiagnostics(mux *http.ServeMux, self domain.Peer, joinTokens *membership.TokenManager, seeds []string, joinToken string, rpcToken string, client *http.Client) {
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
	mux.HandleFunc("/peer/diagnostics", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		report := buildPeerDiagnostics(r.Context(), self, seeds, joinToken, joinTokens, client)
		w.Header().Set("Content-Type", "application/json")
		if !report.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		if err := json.NewEncoder(w).Encode(report); err != nil {
			panic(err)
		}
	})
}

func buildPeerDiagnostics(ctx context.Context, self domain.Peer, seeds []string, joinToken string, joinTokens *membership.TokenManager, client *http.Client) peerDiagnosticsReport {
	report := peerDiagnosticsReport{Peer: self, Ready: true}
	if len(seeds) == 0 {
		return report
	}
	token, err := authorizedOutboundJoinToken(joinToken, joinTokens)
	if err != nil {
		report.Ready = false
		for _, seed := range seeds {
			report.Seeds = append(report.Seeds, peerSeedDiagnostic{Address: seed, Ready: false, Error: err.Error()})
		}
		return report
	}
	for _, seed := range seeds {
		check := peerSeedDiagnostic{Address: seed}
		peer, err := fetchPeerHealthWithClient(ctx, seed, token, client)
		if err != nil {
			check.Error = err.Error()
			report.Ready = false
		} else {
			check.Ready = true
			check.PeerID = peer.ID
			check.Compute = peer.Compute
		}
		report.Seeds = append(report.Seeds, check)
	}
	return report
}

type adminTokenStore interface {
	ListJoinTokens(ctx context.Context) ([]domain.JoinTokenRecord, error)
}

type adminInviteResponse struct {
	Join string `json:"join"`
}

type adminTokenRequest struct {
	Token string `json:"token,omitempty"`
}

type catalogStageRequest struct {
	Preset domain.Preset `json:"preset"`
	Source string        `json:"source,omitempty"`
}

type catalogStageResponse struct {
	Preset   domain.Preset        `json:"preset"`
	Locality domain.ModelLocality `json:"locality"`
	JobID    string               `json:"job_id"`
}

type catalogEvictRequest struct {
	PresetID string `json:"preset_id"`
	NodeID   string `json:"node_id"`
}

type catalogEvictResponse struct {
	Locality domain.ModelLocality `json:"locality"`
}

type workloadRuntime interface {
	SubmitWithPayload(ctx context.Context, job domain.Job, payload []byte, hooks ...scheduler.SubmitHooks) (scheduler.Result, error)
	Drain(ctx context.Context, limit int) ([]scheduler.Result, error)
	FinishJob(ctx context.Context, job domain.Job, lease domain.Lease, cause error) error
}

type workloadProviderExecutor interface {
	ExecutePlacedProvider(ctx context.Context, req translate.IngressRequest, job domain.Job, decision domain.PlacementDecision, inst domain.ModelInstance, lease domain.Lease, attempt int, cold bool, loadMS int) (gateway.RouteResponse, error)
}

type workloadStore interface {
	Job(ctx context.Context, id string) (domain.Job, error)
	SaveJob(ctx context.Context, job domain.Job) error
	SaveWorkloadResultBody(ctx context.Context, result domain.WorkloadResultBody) error
	WorkloadResultBody(ctx context.Context, jobID string) (domain.WorkloadResultBody, error)
}

type workloadReconciliationStore interface {
	ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error)
	SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error
}

type workloadRegistryStore interface {
	Snapshot(ctx context.Context) ([]domain.JobRecord, error)
}

type workloadReplayStore interface {
	workloadRegistryStore
	Put(ctx context.Context, rec domain.JobRecord) error
}

type catalogStageStore interface {
	SaveJob(ctx context.Context, job domain.Job) error
	SavePreset(ctx context.Context, preset domain.Preset) error
	SaveModelLocality(ctx context.Context, locality domain.ModelLocality) error
	ListModelLocalities(ctx context.Context) ([]domain.ModelLocality, error)
	ListInstances(ctx context.Context) ([]domain.ModelInstance, error)
	ListReservations(ctx context.Context) ([]domain.Reservation, error)
	DeleteModelLocality(ctx context.Context, id string) error
}

func mountWorkloadHTTP(mux *http.ServeMux, runtime workloadRuntime, store workloadStore, rpcToken string, coordinatorID string, executor workloadProviderExecutor) {
	mux.HandleFunc("/workloads/drain", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerRPCError(w, http.StatusUnauthorized, fmt.Errorf("unauthorized"))
			return
		}
		if r.Method != http.MethodPost {
			writePeerRPCError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
			return
		}
		limit := 1
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				writePeerRPCError(w, http.StatusBadRequest, fmt.Errorf("limit must be positive"))
				return
			}
			limit = n
		}
		execute := false
		if raw := r.URL.Query().Get("execute"); raw != "" {
			v, err := strconv.ParseBool(raw)
			if err != nil {
				writePeerRPCError(w, http.StatusBadRequest, fmt.Errorf("execute must be true or false"))
				return
			}
			execute = v
		}
		if execute && executor == nil {
			writePeerRPCError(w, http.StatusConflict, fmt.Errorf("queued workload provider execution is not configured"))
			return
		}
		results, err := runtime.Drain(r.Context(), limit)
		if err != nil {
			writePeerRPCError(w, http.StatusConflict, err)
			return
		}
		resp := workload.DrainResponse{Results: make([]workload.AdmissionResponse, 0, len(results))}
		for _, result := range results {
			job := domain.Job{ID: result.Decision.JobID}
			if latest, readErr := store.Job(r.Context(), result.Decision.JobID); readErr == nil {
				job = latest
			}
			if err := persistWorkloadReconciliationTasks(r.Context(), store, job, result); err != nil {
				writePeerRPCError(w, http.StatusConflict, err)
				return
			}
			if execute {
				if err := executeDrainedWorkload(r.Context(), executor, runtime, store, job, result); err != nil {
					writePeerRPCError(w, http.StatusConflict, err)
					return
				}
				if latest, readErr := store.Job(r.Context(), result.Decision.JobID); readErr == nil {
					job = latest
				}
			}
			resp.Results = append(resp.Results, workloadResponse(r.Context(), store, job, result, 0))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/workloads", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req workload.AdmissionRequest
		if err := decodePeerRPCJSON(r, &req); err != nil {
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		job, err := normalizeWorkloadJob(req.Job)
		if err != nil {
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		var hooks []scheduler.SubmitHooks
		if req.QueueOnly {
			hooks = append(hooks, scheduler.SubmitHooks{QueueOnly: true})
		}
		result, err := runtime.SubmitWithPayload(r.Context(), job, []byte(req.Payload), hooks...)
		if err != nil {
			writePeerRPCError(w, http.StatusConflict, err)
			return
		}
		if latest, readErr := store.Job(r.Context(), job.ID); readErr == nil {
			job = latest
		}
		if err := persistWorkloadReplayPayload(r.Context(), store, coordinatorID, job, []byte(req.Payload)); err != nil {
			writePeerRPCError(w, http.StatusConflict, err)
			return
		}
		if err := persistWorkloadReconciliationTasks(r.Context(), store, job, result); err != nil {
			writePeerRPCError(w, http.StatusConflict, err)
			return
		}
		resp := workloadResponse(r.Context(), store, job, result, len(req.Payload))
		w.Header().Set("Content-Type", "application/json")
		if resp.Queued {
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/workloads/", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/workloads/")
		finish := strings.HasSuffix(id, "/finish")
		if finish {
			id = strings.TrimSuffix(id, "/finish")
		}
		cancelWorkload := strings.HasSuffix(id, "/cancel")
		if cancelWorkload {
			id = strings.TrimSuffix(id, "/cancel")
		}
		execute := strings.HasSuffix(id, "/execute")
		if execute {
			id = strings.TrimSuffix(id, "/execute")
		}
		resultBody := strings.HasSuffix(id, "/result")
		if resultBody {
			id = strings.TrimSuffix(id, "/result")
		}
		if id == "" || strings.Contains(id, "/") {
			writePeerRPCError(w, http.StatusBadRequest, fmt.Errorf("workload id is required"))
			return
		}
		if !finish && !cancelWorkload && !execute && !resultBody && r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		job, err := store.Job(r.Context(), id)
		if err != nil {
			writePeerRPCError(w, http.StatusNotFound, err)
			return
		}
		if resultBody {
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			body, err := workloadResultBody(r.Context(), store, job)
			if err != nil {
				writePeerRPCError(w, http.StatusNotFound, err)
				return
			}
			contentType := body.MediaType
			if contentType == "" {
				contentType = "application/octet-stream"
			}
			w.Header().Set("Content-Type", contentType)
			w.Header().Set("Content-Length", strconv.Itoa(len(body.Body)))
			w.Header().Set("X-Myc-Job", job.ID)
			w.Header().Set("X-Myc-Result-SHA256", body.SHA256)
			if _, err := w.Write(body.Body); err != nil {
				panic(err)
			}
			return
		}
		if execute {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if executor == nil {
				writePeerRPCError(w, http.StatusConflict, fmt.Errorf("queued workload provider execution is not configured"))
				return
			}
			result, err := workloadSchedulerResult(job)
			if err != nil {
				writePeerRPCError(w, http.StatusConflict, err)
				return
			}
			if err := executeDrainedWorkload(r.Context(), executor, runtime, store, job, result); err != nil {
				writePeerRPCError(w, http.StatusConflict, err)
				return
			}
			latest, readErr := store.Job(r.Context(), id)
			if readErr == nil {
				job = latest
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(workload.FinishResponse{Job: job, Result: job.Result, Execution: workloadExecutionEvidence(r.Context(), store, job)}); err != nil {
				panic(err)
			}
			return
		}
		if finish {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var req workload.FinishRequest
			if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 {
				if err := decodePeerRPCJSON(r, &req); err != nil {
					writePeerRPCError(w, http.StatusBadRequest, err)
					return
				}
			}
			lease, err := workloadLease(job)
			if err != nil {
				writePeerRPCError(w, http.StatusConflict, err)
				return
			}
			if err := attachWorkloadResultArtifact(job.Result, req); err != nil {
				writePeerRPCError(w, http.StatusBadRequest, err)
				return
			}
			if err := validateWorkloadResultBodyRequest(req); err != nil {
				writePeerRPCError(w, http.StatusBadRequest, err)
				return
			}
			var cause error
			if strings.TrimSpace(req.Error) != "" {
				cause = fmt.Errorf("%s", strings.TrimSpace(req.Error))
			}
			if err := runtime.FinishJob(r.Context(), job, lease, cause); err != nil {
				writePeerRPCError(w, http.StatusConflict, err)
				return
			}
			if len(req.ResultBody) > 0 {
				if err := store.SaveWorkloadResultBody(r.Context(), domain.WorkloadResultBody{
					JobID:     job.ID,
					MediaType: strings.TrimSpace(req.ResultMediaType),
					Bytes:     req.ResultBytes,
					SHA256:    strings.ToLower(strings.TrimSpace(req.ResultSHA256)),
					Body:      append([]byte(nil), req.ResultBody...),
				}); err != nil {
					writePeerRPCError(w, http.StatusConflict, err)
					return
				}
			}
			latest, readErr := store.Job(r.Context(), id)
			if readErr == nil {
				job = latest
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(workload.FinishResponse{Job: job, Result: job.Result, Execution: workloadExecutionEvidence(r.Context(), store, job)}); err != nil {
				panic(err)
			}
			return
		}
		if cancelWorkload {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var req workload.CancelRequest
			if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 {
				if err := decodePeerRPCJSON(r, &req); err != nil {
					writePeerRPCError(w, http.StatusBadRequest, err)
					return
				}
			}
			job, err = cancelWorkloadJob(r.Context(), runtime, store, job, req)
			if err != nil {
				writePeerRPCError(w, http.StatusConflict, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(workload.FinishResponse{Job: job, Result: job.Result, Execution: workloadExecutionEvidence(r.Context(), store, job)}); err != nil {
				panic(err)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workload.StatusResponse{Job: job, Result: job.Result, Payload: workloadPayloadEvidence(r.Context(), store, job), Execution: workloadExecutionEvidence(r.Context(), store, job)}); err != nil {
			panic(err)
		}
	})
}

func persistWorkloadReconciliationTasks(ctx context.Context, store workloadStore, job domain.Job, result scheduler.Result) error {
	reconStore, ok := store.(workloadReconciliationStore)
	if !ok {
		return nil
	}
	decision := result.Decision
	if decision.JobID == "" {
		decision.JobID = job.ID
	}
	if decision.JobID == "" || (decision.Action == "" && len(decision.Trace) == 0) {
		return nil
	}
	if decision.Preset.ID == "" && job.PresetID != "" {
		decision.Preset.ID = job.PresetID
	}
	if decision.Preset.ModelRef == "" {
		decision.Preset.ModelRef = job.Model
	}
	plan := reconcile.PlanPlacement(decision, reconcile.DefaultStandingPolicy())
	records := reconcile.RecordsFromPlan(plan, time.Now().UTC())
	if len(records) == 0 {
		return nil
	}
	existing, err := reconStore.ListReconciliationTasks(ctx, decision.JobID)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(existing))
	for _, task := range existing {
		seen[task.ID] = struct{}{}
	}
	for _, task := range records {
		if _, ok := seen[task.ID]; ok {
			continue
		}
		if err := reconStore.SaveReconciliationTask(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

func cancelWorkloadJob(ctx context.Context, runtime workloadRuntime, store workloadStore, job domain.Job, req workload.CancelRequest) (domain.Job, error) {
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "canceled by operator"
	}
	if lease, err := workloadLease(job); err == nil {
		if err := runtime.FinishJob(ctx, job, lease, fmt.Errorf("%s", reason)); err != nil {
			return domain.Job{}, err
		}
		if latest, readErr := store.Job(ctx, job.ID); readErr == nil {
			return latest, nil
		}
		return job, nil
	}
	job.Status = domain.JobFailed
	job.Error = reason
	if job.Result == nil {
		job.Result = &domain.JobResult{}
	}
	job.Result.Source = "operator_cancel"
	job.Result.ResultRedacted = true
	job.Result.Terminal = true
	if err := store.SaveJob(ctx, job); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func attachWorkloadResultArtifact(result *domain.JobResult, req workload.FinishRequest) error {
	artifact := strings.TrimSpace(req.ResultArtifact)
	mediaType := strings.TrimSpace(req.ResultMediaType)
	digest := strings.ToLower(strings.TrimSpace(req.ResultSHA256))
	if req.ResultBytes < 0 {
		return fmt.Errorf("result_bytes must be non-negative")
	}
	if digest != "" && !validSHA256Hex(digest) {
		return fmt.Errorf("result_sha256 must be 64 hexadecimal characters")
	}
	if mediaType != "" && strings.ContainsAny(mediaType, "\r\n\t") {
		return fmt.Errorf("result_media_type must not contain control characters")
	}
	if artifact == "" && mediaType == "" && req.ResultBytes == 0 && digest == "" {
		return nil
	}
	if artifact == "" {
		return fmt.Errorf("result_artifact is required when result metadata is supplied")
	}
	result.ResultArtifact = artifact
	result.ResultMediaType = mediaType
	result.ResultBytes = req.ResultBytes
	result.ResultSHA256 = digest
	result.ResultRedacted = true
	return nil
}

func validateWorkloadResultBodyRequest(req workload.FinishRequest) error {
	if len(req.ResultBody) == 0 {
		return nil
	}
	if len(req.ResultBody) > maxWorkloadResultBodyBytes {
		return fmt.Errorf("result_body exceeds %d bytes", maxWorkloadResultBodyBytes)
	}
	if strings.TrimSpace(req.ResultArtifact) == "" {
		return fmt.Errorf("result_artifact is required when result_body is supplied")
	}
	if req.ResultBytes != 0 && req.ResultBytes != len(req.ResultBody) {
		return fmt.Errorf("result_bytes does not match result_body")
	}
	if digest := strings.TrimSpace(req.ResultSHA256); digest != "" {
		sum := sha256.Sum256(req.ResultBody)
		if strings.ToLower(digest) != hex.EncodeToString(sum[:]) {
			return fmt.Errorf("result_sha256 does not match result_body")
		}
	}
	return nil
}

func workloadResultBody(ctx context.Context, store workloadStore, job domain.Job) (domain.WorkloadResultBody, error) {
	if job.Result == nil || !job.Result.Terminal {
		return domain.WorkloadResultBody{}, fmt.Errorf("workload result is not terminal")
	}
	if job.Result.ResultArtifact == "" {
		return domain.WorkloadResultBody{}, fmt.Errorf("workload has no result artifact")
	}
	body, err := store.WorkloadResultBody(ctx, job.ID)
	if err != nil {
		return domain.WorkloadResultBody{}, err
	}
	if job.Result.ResultBytes > 0 && job.Result.ResultBytes != body.Bytes {
		return domain.WorkloadResultBody{}, fmt.Errorf("stored result body does not match result manifest bytes")
	}
	if job.Result.ResultSHA256 != "" && job.Result.ResultSHA256 != body.SHA256 {
		return domain.WorkloadResultBody{}, fmt.Errorf("stored result body does not match result manifest digest")
	}
	return body, nil
}

func executeDrainedWorkload(ctx context.Context, executor workloadProviderExecutor, runtime workloadRuntime, store workloadStore, job domain.Job, result scheduler.Result) error {
	req, err := executableWorkloadIngressRequest(ctx, store, job)
	if err != nil {
		return err
	}
	req.Project = job.Project
	req.Priority = job.Priority
	req.SpeedPref = job.SpeedPref
	req.ContextRequest = job.ContextRequest
	req.Preemption = job.Preemption
	req.Handling = job.Handling
	req.Submitter = job.Submitter
	if job.Model != "" {
		req, err = translate.WithModel(req, job.Model)
		if err != nil {
			return err
		}
	}
	if req.Stream {
		return fmt.Errorf("queued workload provider execution does not support streaming payloads")
	}
	cold := result.Decision.InstanceID == ""
	resp, err := executor.ExecutePlacedProvider(ctx, req, job, result.Decision, result.Instance, result.Lease, 1, cold, 0)
	if err != nil {
		return err
	}
	if resp.Status >= 400 {
		return fmt.Errorf("queued workload provider execution returned %d", resp.Status)
	}
	if job.Result == nil {
		return fmt.Errorf("workload %q has no scheduler result evidence", job.ID)
	}
	mediaType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if mediaType == "" {
		mediaType = "application/json"
	}
	sum := sha256.Sum256(resp.Body)
	digest := hex.EncodeToString(sum[:])
	finish := workload.FinishRequest{
		ResultArtifact:  "stored://workload/" + job.ID,
		ResultMediaType: mediaType,
		ResultBytes:     len(resp.Body),
		ResultSHA256:    digest,
		ResultBody:      append([]byte(nil), resp.Body...),
	}
	if err := attachWorkloadResultArtifact(job.Result, finish); err != nil {
		return err
	}
	job.Result.ProviderCapture = true
	if err := validateWorkloadResultBodyRequest(finish); err != nil {
		return err
	}
	if err := runtime.FinishJob(ctx, job, result.Lease, nil); err != nil {
		return err
	}
	return store.SaveWorkloadResultBody(ctx, domain.WorkloadResultBody{
		JobID:     job.ID,
		MediaType: mediaType,
		Bytes:     len(resp.Body),
		SHA256:    digest,
		Body:      append([]byte(nil), resp.Body...),
	})
}

func executableWorkloadIngressRequest(ctx context.Context, store workloadStore, job domain.Job) (translate.IngressRequest, error) {
	registry, ok := store.(workloadRegistryStore)
	if !ok {
		return translate.IngressRequest{}, fmt.Errorf("workload payload registry is not available")
	}
	records, err := registry.Snapshot(ctx)
	if err != nil {
		return translate.IngressRequest{}, err
	}
	for _, rec := range records {
		if rec.JobID != job.ID {
			continue
		}
		if rec.PayloadRedacted || rec.Handling == domain.HandlingPrivate {
			return translate.IngressRequest{}, fmt.Errorf("workload payload is redacted")
		}
		payloadJob, payload, err := peercoord.DecodeRescuePayload(rec.Request)
		if err != nil {
			return translate.IngressRequest{}, err
		}
		if payloadJob.ID != job.ID {
			return translate.IngressRequest{}, fmt.Errorf("payload job %q does not match %q", payloadJob.ID, job.ID)
		}
		executable, err := workload.DecodeExecutablePayload(payload)
		if err != nil {
			return translate.IngressRequest{}, err
		}
		switch executable.Kind {
		case workload.ExecutablePayloadKindOpenAIChat:
			return translate.ParseOpenAIChat(executable.Body)
		case workload.ExecutablePayloadKindOpenAICompletion:
			return translate.ParseOpenAICompletion(executable.Body)
		case workload.ExecutablePayloadKindAnthropicMessages:
			return translate.ParseAnthropicMessages(executable.Body)
		default:
			return translate.IngressRequest{}, fmt.Errorf("unsupported executable payload kind %q", executable.Kind)
		}
	}
	return translate.IngressRequest{}, fmt.Errorf("workload payload is not registry-replayable")
}

func persistWorkloadReplayPayload(ctx context.Context, store workloadStore, coordinatorID string, job domain.Job, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	registry, ok := store.(workloadReplayStore)
	if !ok {
		return nil
	}
	request, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		return err
	}
	if coordinatorID == "" {
		coordinatorID = "workload-rpc"
	}
	assignedNode := ""
	if job.Result != nil {
		assignedNode = job.Result.NodeID
	}
	return registry.Put(ctx, domain.JobRecord{
		JobID:        job.ID,
		Coordinator:  coordinatorID,
		AssignedNode: assignedNode,
		Status:       job.Status,
		Request:      request,
		Handling:     job.Handling,
		UpdatedAt:    time.Now().UTC(),
	})
}

func validSHA256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}

func workloadLease(job domain.Job) (domain.Lease, error) {
	if job.Result == nil {
		return domain.Lease{}, fmt.Errorf("workload %q has no scheduler result evidence", job.ID)
	}
	if job.Result.LeaseID == "" {
		return domain.Lease{}, fmt.Errorf("workload %q has no active lease evidence", job.ID)
	}
	lease := domain.Lease{
		ID:     job.Result.LeaseID,
		JobID:  job.ID,
		NodeID: job.Result.NodeID,
	}
	if job.Result.Instance.ID != "" {
		lease.InstanceID = job.Result.Instance.ID
	}
	return lease, nil
}

func workloadSchedulerResult(job domain.Job) (scheduler.Result, error) {
	if job.Status != domain.JobRunning {
		return scheduler.Result{}, fmt.Errorf("workload %q is %s; execute requires running workload", job.ID, job.Status)
	}
	if job.Result == nil {
		return scheduler.Result{}, fmt.Errorf("workload %q has no scheduler result evidence", job.ID)
	}
	lease, err := workloadLease(job)
	if err != nil {
		return scheduler.Result{}, err
	}
	decision := job.Result.Decision
	if decision.JobID == "" {
		decision = domain.PlacementDecision{
			JobID:  job.ID,
			Preset: domain.Preset{ID: job.Result.PresetID},
			NodeID: job.Result.NodeID,
			Action: job.Result.Action,
		}
	}
	inst := job.Result.Instance
	if inst.ID == "" {
		inst = domain.ModelInstance{
			ID:       lease.InstanceID,
			PresetID: job.Result.PresetID,
			NodeID:   job.Result.NodeID,
		}
	}
	return scheduler.Result{Decision: decision, Instance: inst, Lease: lease}, nil
}

func workloadResponse(ctx context.Context, store workloadStore, job domain.Job, result scheduler.Result, payloadBytes int) workload.AdmissionResponse {
	if payloadBytes == 0 && job.Result != nil {
		payloadBytes = job.Result.PayloadBytes
	}
	payloadStored := payloadBytes > 0
	if job.Result != nil {
		payloadStored = job.Result.PayloadStored
	}
	return workload.AdmissionResponse{
		Job:           job,
		Decision:      result.Decision,
		Instance:      result.Instance,
		Lease:         result.Lease,
		Result:        job.Result,
		Queued:        job.Status == domain.JobQueued || result.Decision.Action == domain.ActionQueued,
		PayloadBytes:  payloadBytes,
		PayloadStored: payloadStored,
		Payload:       workloadPayloadEvidence(ctx, store, job),
		Execution:     workloadExecutionEvidence(ctx, store, job),
	}
}

func workloadExecutionEvidence(ctx context.Context, store workloadStore, job domain.Job) workload.ExecutionEvidence {
	out := workload.ExecutionEvidence{AutomaticProviderOutputCapture: false}
	out.PayloadContract, out.PayloadKind, out.PayloadExecutable = workloadExecutablePayloadEvidence(ctx, store, job)
	if job.Result != nil {
		out.AutomaticProviderOutputCapture = job.Result.ProviderCapture
		out.ResultArtifact = job.Result.ResultArtifact != ""
		out.ResultBytes = job.Result.ResultBytes
		out.ResultSHA256 = job.Result.ResultSHA256 != ""
	}
	switch job.Status {
	case domain.JobQueued:
		out.State = "queued_not_executed"
	case domain.JobRunning:
		out.State = "running_needs_finish"
	case domain.JobDone, domain.JobFailed:
		out.State = "terminal_no_result"
		if job.Result != nil && job.Result.ResultArtifact != "" {
			out.State = "terminal_result_manifest_only"
			body, err := workloadResultBody(ctx, store, job)
			if err == nil {
				out.State = "terminal_result_replayable"
				out.ResultReplayable = true
				out.ResultBodyStored = true
				out.ResultBytes = body.Bytes
				out.ResultSHA256 = body.SHA256 != ""
			} else {
				out.Error = "result_body_unavailable"
			}
		}
	default:
		out.State = "job_status_" + string(job.Status)
	}
	return out
}

func workloadExecutablePayloadEvidence(ctx context.Context, store workloadStore, job domain.Job) (string, string, bool) {
	registry, ok := store.(workloadRegistryStore)
	if !ok {
		if job.Result != nil && job.Result.PayloadStored {
			return "runtime_payload_not_inspectable", "", false
		}
		return "no_payload", "", false
	}
	records, err := registry.Snapshot(ctx)
	if err != nil {
		return "registry_payload_unavailable", "", false
	}
	for _, rec := range records {
		if rec.JobID != job.ID {
			continue
		}
		if rec.PayloadRedacted || rec.Handling == domain.HandlingPrivate {
			return "registry_payload_redacted", "", false
		}
		if len(rec.Request) == 0 {
			return "registry_payload_missing", "", false
		}
		payloadJob, payload, err := peercoord.DecodeRescuePayload(rec.Request)
		if err != nil || payloadJob.ID != job.ID {
			return "registry_payload_unreadable", "", false
		}
		executable, err := workload.DecodeExecutablePayload(payload)
		if err != nil {
			return "registry_payload_not_executable", "", false
		}
		return "registry_executable_gateway_request", executable.Kind, true
	}
	if job.Result != nil && job.Result.PayloadStored {
		return "runtime_payload_not_inspectable", "", false
	}
	return "no_payload", "", false
}

func workloadPayloadEvidence(ctx context.Context, store workloadStore, job domain.Job) workload.PayloadEvidence {
	bytes := 0
	if job.Result != nil {
		bytes = job.Result.PayloadBytes
	}
	base := workload.PayloadEvidence{Bytes: bytes}
	registry, ok := store.(workloadRegistryStore)
	if !ok {
		if job.Result != nil && job.Result.PayloadStored {
			base.State = "runtime_payload_stored"
		}
		return base
	}
	records, err := registry.Snapshot(ctx)
	if err != nil {
		base.State = "registry_unavailable"
		base.Error = err.Error()
		return base
	}
	for _, rec := range records {
		if rec.JobID != job.ID {
			continue
		}
		if rec.PayloadRedacted || rec.Handling == domain.HandlingPrivate {
			base.State = "registry_payload_redacted"
			base.Redacted = true
			return base
		}
		if len(rec.Request) == 0 {
			base.State = "registry_payload_missing"
			return base
		}
		payloadJob, payload, err := peercoord.DecodeRescuePayload(rec.Request)
		if err != nil {
			base.State = "registry_payload_unreadable"
			base.Error = err.Error()
			return base
		}
		if payloadJob.ID != job.ID {
			base.State = "registry_payload_mismatch"
			base.Error = fmt.Sprintf("payload job %q does not match %q", payloadJob.ID, job.ID)
			return base
		}
		base.State = "registry_payload_replayable"
		base.Replayable = true
		base.Bytes = len(payload)
		return base
	}
	if job.Result != nil && job.Result.PayloadStored {
		base.State = "runtime_payload_stored"
	}
	return base
}

func normalizeWorkloadJob(job domain.Job) (domain.Job, error) {
	if job.ID == "" {
		return domain.Job{}, fmt.Errorf("job.id is required")
	}
	if job.TaskType == "" {
		job.TaskType = "chat"
	}
	if job.Model == "" && job.PresetID == "" {
		return domain.Job{}, fmt.Errorf("job.model or job.preset is required")
	}
	if job.Project == "" {
		return domain.Job{}, fmt.Errorf("job.project is required")
	}
	if job.Priority == "" {
		job.Priority = domain.PriorityNormal
	}
	if job.SpeedPref == "" {
		job.SpeedPref = domain.SpeedThroughput
	}
	if job.Preemption == "" {
		job.Preemption = domain.PreemptInherit
	}
	if err := validateWorkloadJobEnums(job); err != nil {
		return domain.Job{}, err
	}
	job.Status = domain.JobQueued
	return job, nil
}

func validateWorkloadJobEnums(job domain.Job) error {
	switch job.Priority {
	case domain.PriorityInteractive, domain.PriorityNormal, domain.PriorityBackground:
	default:
		return fmt.Errorf("job.priority %q is invalid", job.Priority)
	}
	switch job.SpeedPref {
	case domain.SpeedThroughput, domain.SpeedLatency, domain.SpeedAuto:
	default:
		return fmt.Errorf("job.speed_pref %q is invalid", job.SpeedPref)
	}
	switch job.Preemption {
	case domain.PreemptInherit, domain.PreemptSoft, domain.PreemptHardForInteractive, domain.PreemptHard:
	default:
		return fmt.Errorf("job.preemption %q is invalid", job.Preemption)
	}
	switch job.Handling {
	case domain.HandlingStandard:
	case domain.HandlingPrivate:
		return fmt.Errorf("private workload handling is not enabled")
	default:
		return fmt.Errorf("job.handling %q is invalid", job.Handling)
	}
	if job.ContextRequest < 0 {
		return fmt.Errorf("job.context_request must be non-negative")
	}
	if job.ExpectedConcurrency < 0 {
		return fmt.Errorf("job.expected_concurrency must be non-negative")
	}
	if job.DeadlineMS < 0 {
		return fmt.Errorf("job.deadline_ms must be non-negative")
	}
	return nil
}

func mountCatalogHTTP(mux *http.ServeMux, cfg PeerConfig, store catalogStageStore, rpcToken string, clk ports.Clock) {
	mux.HandleFunc("/catalog/stage", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req catalogStageRequest
		if err := decodePeerRPCJSON(r, &req); err != nil {
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		if req.Preset.ID == "" {
			writePeerRPCError(w, http.StatusBadRequest, fmt.Errorf("preset id is required"))
			return
		}
		source := req.Source
		if source == "" {
			source = req.Preset.ModelRef
		}
		if source == "" {
			writePeerRPCError(w, http.StatusBadRequest, fmt.Errorf("preset %q has no source model ref", req.Preset.ID))
			return
		}
		nodeID := cfg.ID
		if cfg.ComputeConfig.ID != "" {
			nodeID = cfg.ComputeConfig.ID
		}
		if req.Preset.NodeID != "" && req.Preset.NodeID != nodeID {
			writePeerRPCError(w, http.StatusConflict, fmt.Errorf("preset %q is declared local to node %q, not %q", req.Preset.ID, req.Preset.NodeID, nodeID))
			return
		}
		jobID, err := catalog.InstallJobID(catalog.InstallRequest{Source: source, ID: req.Preset.ID})
		if err != nil {
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		job := domain.Job{ID: jobID, TaskType: "catalog_stage", Model: source, PresetID: req.Preset.ID, Status: domain.JobQueued}
		if err := store.SaveJob(r.Context(), job); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		adoptSource, metadata, adopt, err := verifyRuntimeSourceAdoption(r.Context(), cfg, req.Preset, source, nodeID)
		if err != nil {
			job.Status = domain.JobFailed
			job.Error = err.Error()
			_ = store.SaveJob(r.Context(), job)
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		if adopt {
			staged := req.Preset
			staged.NodeID = nodeID
			staged.ModelRef = adoptSource
			staged.ArtifactSizeMB = metadata.WeightsMB
			staged.EstWeightsMB = metadata.WeightsMB
			staged.KVPerTokenMB = metadata.KVPerTokenMB
			staged.LayerCount = metadata.LayerCount
			if metadata.ContextLength > 0 && staged.ContextLength == 0 {
				staged.ContextLength = metadata.ContextLength
			}
			if err := store.SavePreset(r.Context(), staged); err != nil {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
			locality := domain.ModelLocality{
				ID:             modelLocalityID(nodeID, staged.ID),
				PresetID:       staged.ID,
				NodeID:         nodeID,
				State:          domain.ModelLocalityReady,
				ModelRef:       staged.ModelRef,
				Source:         adoptSource,
				ArtifactSizeMB: staged.ArtifactSizeMB,
				Managed:        false,
				Reason:         "runtime source adopted",
				UpdatedAt:      clk.Now().UTC(),
			}
			if err := store.SaveModelLocality(r.Context(), locality); err != nil {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
			job.Status = domain.JobDone
			job.PresetID = staged.ID
			if err := store.SaveJob(r.Context(), job); err != nil {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(catalogStageResponse{Preset: staged, Locality: locality, JobID: job.ID}); err != nil {
				panic(err)
			}
			return
		}
		result, err := catalog.NewInstaller(cfg.CatalogDir).InstallWithProgress(r.Context(), catalog.InstallRequest{
			Source:        source,
			ID:            req.Preset.ID,
			Model:         firstPresetModelAlias(req.Preset),
			ContextLength: req.Preset.ContextLength,
			Quant:         req.Preset.Quant,
			Backend:       req.Preset.Backend,
		}, func(event catalog.ProgressEvent, _ catalog.InstallState) error {
			job.Status = domain.JobRunning
			job.Progress = append(job.Progress, domain.JobProgress{Stage: event.Stage, Message: event.Message, At: event.At})
			return store.SaveJob(r.Context(), job)
		})
		if err != nil {
			job.Status = domain.JobFailed
			job.Error = err.Error()
			_ = store.SaveJob(r.Context(), job)
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		staged := result.Preset
		staged.NodeID = nodeID
		if err := store.SavePreset(r.Context(), staged); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		locality := domain.ModelLocality{
			ID:             modelLocalityID(nodeID, staged.ID),
			PresetID:       staged.ID,
			NodeID:         nodeID,
			State:          domain.ModelLocalityReady,
			ModelRef:       staged.ModelRef,
			Source:         source,
			ArtifactSizeMB: staged.ArtifactSizeMB,
			Managed:        true,
			Reason:         "catalog stage committed",
			UpdatedAt:      clk.Now().UTC(),
		}
		if err := store.SaveModelLocality(r.Context(), locality); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		job.Status = domain.JobDone
		job.PresetID = staged.ID
		if err := store.SaveJob(r.Context(), job); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(catalogStageResponse{Preset: staged, Locality: locality, JobID: job.ID}); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/catalog/evict", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req catalogEvictRequest
		if err := decodePeerRPCJSON(r, &req); err != nil {
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		if req.PresetID == "" || req.NodeID == "" {
			writePeerRPCError(w, http.StatusBadRequest, fmt.Errorf("preset_id and node_id are required"))
			return
		}
		locality, err := findModelLocality(r.Context(), store, modelLocalityID(req.NodeID, req.PresetID))
		if err != nil {
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		if err := validateManagedEviction(r.Context(), store, locality); err != nil {
			writePeerRPCError(w, http.StatusConflict, err)
			return
		}
		if locality.ModelRef != "" {
			if !isManagedCatalogModel(cfg.CatalogDir, locality.ModelRef) {
				writePeerRPCError(w, http.StatusConflict, fmt.Errorf("locality %s is not under managed catalog models", locality.ID))
				return
			}
			if err := os.Remove(locality.ModelRef); err != nil && !os.IsNotExist(err) {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
		}
		if err := store.DeleteModelLocality(r.Context(), locality.ID); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		locality.State = domain.ModelLocalityEvicted
		locality.UpdatedAt = clk.Now().UTC()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(catalogEvictResponse{Locality: locality}); err != nil {
			panic(err)
		}
	})
}

func firstPresetModelAlias(preset domain.Preset) string {
	if len(preset.Aliases) > 0 {
		return preset.Aliases[0]
	}
	if preset.ID != "" {
		return preset.ID
	}
	return preset.ModelRef
}

func shouldAdoptRuntimeSource(preset domain.Preset, source, nodeID string) bool {
	if preset.NodeID == "" || preset.NodeID != nodeID {
		return false
	}
	if strings.HasPrefix(source, "hf://") || strings.HasPrefix(source, "oci://") {
		return false
	}
	localSource := strings.TrimPrefix(source, "file://")
	info, err := os.Stat(localSource)
	return err == nil && !info.IsDir()
}

func verifyRuntimeSourceAdoption(ctx context.Context, cfg PeerConfig, preset domain.Preset, source, nodeID string) (string, domain.ModelMetadata, bool, error) {
	if !shouldAdoptRuntimeSource(preset, source, nodeID) {
		return "", domain.ModelMetadata{}, false, nil
	}
	if preset.Backend != "" && preset.Backend != domain.BackendLlamaCpp {
		return "", domain.ModelMetadata{}, false, fmt.Errorf("runtime source adoption for backend %s requires owner inspection; catalog stage cannot mark it ready", preset.Backend)
	}
	parser := cfg.GGUFParser
	if parser == "" {
		parser = cfg.ComputeConfig.GGUFParser
	}
	if parser == "" {
		return "", domain.ModelMetadata{}, false, fmt.Errorf("runtime source adoption for preset %q requires a configured gguf parser", preset.ID)
	}
	localSource := strings.TrimPrefix(source, "file://")
	metadata, err := estimate.NewCommandParser(parser, nil).Parse(ctx, localSource)
	if err != nil {
		return "", domain.ModelMetadata{}, false, err
	}
	if metadata.WeightsMB <= 0 {
		return "", domain.ModelMetadata{}, false, fmt.Errorf("runtime source %q reported invalid weights: %dMB", localSource, metadata.WeightsMB)
	}
	return localSource, metadata, true, nil
}

func modelLocalityID(nodeID, presetID string) string {
	return nodeID + ":" + presetID
}

func findModelLocality(ctx context.Context, store catalogStageStore, id string) (domain.ModelLocality, error) {
	localities, err := store.ListModelLocalities(ctx)
	if err != nil {
		return domain.ModelLocality{}, err
	}
	for _, locality := range localities {
		if locality.ID == id {
			return locality, nil
		}
	}
	return domain.ModelLocality{}, fmt.Errorf("model locality %q was not found", id)
}

func validateManagedEviction(ctx context.Context, store catalogStageStore, locality domain.ModelLocality) error {
	if !locality.Managed {
		return fmt.Errorf("locality %s is not managed by Mycelium", locality.ID)
	}
	if locality.Pinned || locality.Warm {
		return fmt.Errorf("locality %s is warm or pinned", locality.ID)
	}
	instances, err := store.ListInstances(ctx)
	if err != nil {
		return err
	}
	for _, inst := range instances {
		if inst.NodeID == locality.NodeID && inst.PresetID == locality.PresetID {
			return fmt.Errorf("locality %s has a live instance", locality.ID)
		}
	}
	reservations, err := store.ListReservations(ctx)
	if err != nil {
		return err
	}
	for _, reservation := range reservations {
		if reservation.NodeID == locality.NodeID && reservation.PresetID == locality.PresetID {
			return fmt.Errorf("locality %s has a reservation", locality.ID)
		}
	}
	return nil
}

func isManagedCatalogModel(catalogDir, modelRef string) bool {
	modelsDir := filepath.Clean(filepath.Join(catalogDir, "models"))
	ref := filepath.Clean(modelRef)
	rel, err := filepath.Rel(modelsDir, ref)
	return err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." && !filepath.IsAbs(rel)
}

func mountAdminHTTP(mux *http.ServeMux, cfg *PeerConfig, configPath string, joinTokens *membership.TokenManager, tokenStore adminTokenStore, rpcToken string) {
	mux.HandleFunc("/admin/invite", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		join, err := adminJoinURI(*cfg, r.Host)
		if err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(adminInviteResponse{Join: join}); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/admin/tokens", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		tokens, err := tokenStore.ListJoinTokens(r.Context())
		if err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(publicJoinTokenRecords(tokens)); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/admin/tokens/rotate", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req adminTokenRequest
		if r.Body != nil && r.ContentLength != 0 {
			if err := decodePeerRPCJSON(r, &req); err != nil {
				writePeerRPCError(w, http.StatusBadRequest, err)
				return
			}
		}
		if req.Token == "" {
			token, err := randomHex(32)
			if err != nil {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
			req.Token = token
		}
		if err := joinTokens.Rotate(req.Token); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		cfg.JoinToken = req.Token
		if err := savePeerConfig(configPath, *cfg); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		join, err := adminJoinURI(*cfg, r.Host)
		if err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(adminInviteResponse{Join: join}); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/admin/tokens/revoke", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req adminTokenRequest
		if err := decodePeerRPCJSON(r, &req); err != nil {
			writePeerRPCError(w, http.StatusBadRequest, err)
			return
		}
		if req.Token == "" {
			writePeerRPCError(w, http.StatusBadRequest, fmt.Errorf("token is required"))
			return
		}
		if err := joinTokens.Revoke(req.Token); err != nil {
			writePeerRPCError(w, http.StatusInternalServerError, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func publicJoinTokenRecords(tokens []domain.JoinTokenRecord) []domain.JoinTokenRecord {
	out := append([]domain.JoinTokenRecord(nil), tokens...)
	for i := range out {
		out[i].Secret = ""
	}
	return out
}

func adminJoinURI(cfg PeerConfig, requestHost string) (string, error) {
	return membership.BuildJoinTokenForPeer(adminJoinAddress(cfg.Listen, requestHost), cfg.JoinToken)
}

func adminJoinAddress(listen, requestHost string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		if requestHost != "" {
			return requestHost
		}
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
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
		if err := json.NewEncoder(w).Encode(peercoord.RescueSnapshot(records)); err != nil {
			log.Printf("mycelium registry snapshot write failed: %v", err)
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
		if err := decodePeerRPCJSON(r, &records); err != nil {
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
			if err := decodePeerRPCJSON(r, &metrics); err != nil {
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
	mux.HandleFunc("/telemetry/samples", func(w http.ResponseWriter, r *http.Request) {
		if !peerRPCAuthorized(r, rpcToken) {
			writePeerAuthError(w)
			return
		}
		switch r.Method {
		case http.MethodGet:
			query, err := parseSessionMetricQuery(r.URL.Query())
			if err != nil {
				writePeerRPCError(w, http.StatusBadRequest, err)
				return
			}
			samples, err := store.Samples(r.Context(), query)
			if err != nil {
				writePeerRPCError(w, http.StatusInternalServerError, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(samples); err != nil {
				panic(err)
			}
		case http.MethodPost:
			var samples []domain.SessionMetric
			if err := decodePeerRPCJSON(r, &samples); err != nil {
				writePeerRPCError(w, http.StatusBadRequest, err)
				return
			}
			for _, sample := range samples {
				if err := store.RecordSample(r.Context(), sample); err != nil {
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
			if err := decodePeerRPCJSON(r, &recs); err != nil {
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

func parseSessionMetricQuery(values url.Values) (domain.SessionMetricQuery, error) {
	var query domain.SessionMetricQuery
	query.SessionID = values.Get("session_id")
	query.Project = values.Get("project")
	query.NodeID = values.Get("node_id")
	if raw := values.Get("since"); raw != "" {
		at, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return domain.SessionMetricQuery{}, fmt.Errorf("invalid since: %w", err)
		}
		query.Since = at
	}
	if raw := values.Get("until"); raw != "" {
		at, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return domain.SessionMetricQuery{}, fmt.Errorf("invalid until: %w", err)
		}
		query.Until = at
	}
	if raw := values.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 0 {
			return domain.SessionMetricQuery{}, fmt.Errorf("invalid limit %q", raw)
		}
		query.Limit = limit
	}
	return query, nil
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
		"/admission/job-status",
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
	leftErr := err
	right, err := f.right.Snapshot(ctx)
	rightErr := err
	if leftErr != nil && rightErr != nil {
		return domain.FleetSnapshot{}, errors.Join(leftErr, rightErr)
	}
	if leftErr != nil {
		return right, nil
	}
	if rightErr != nil {
		return left, nil
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
	statusReader jobStatusReader
}

type jobStatusReader interface {
	Job(ctx context.Context, id string) (domain.Job, error)
}

func newLocalPeerAgent(agent ports.NodeAgent, admission ports.AdmissionController, readers ...jobStatusReader) (localPeerAgent, error) {
	inspector, ok := admission.(ports.LeaseInspector)
	if !ok {
		return localPeerAgent{}, fmt.Errorf("local admission controller does not expose lease inspection")
	}
	binder, ok := admission.(ports.LeaseBinder)
	if !ok {
		return localPeerAgent{}, fmt.Errorf("local admission controller does not expose lease binding")
	}
	var reader jobStatusReader
	if len(readers) > 0 {
		reader = readers[0]
	}
	return localPeerAgent{NodeAgent: agent, AdmissionController: admission, LeaseInspector: inspector, LeaseBinder: binder, statusReader: reader}, nil
}

func (a localPeerAgent) JobStatus(ctx context.Context, jobID string) (domain.JobStatus, bool, error) {
	if a.statusReader == nil {
		return "", false, nil
	}
	job, err := a.statusReader.Job(ctx, jobID)
	if err != nil {
		return "", false, err
	}
	return job.Status, true, nil
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

func (n combinedNodes) JobStatusInspector(nodeID string) (ports.JobStatusInspector, error) {
	if left, ok := n.left.(interface {
		JobStatusInspector(string) (ports.JobStatusInspector, error)
	}); ok {
		inspector, err := left.JobStatusInspector(nodeID)
		if err == nil {
			return inspector, nil
		}
	}
	right, ok := n.right.(interface {
		JobStatusInspector(string) (ports.JobStatusInspector, error)
	})
	if !ok {
		return nil, fmt.Errorf("node resolver does not expose job status inspection")
	}
	return right.JobStatusInspector(nodeID)
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

type computeOverrideFlag struct {
	set   bool
	value bool
}

func (f *computeOverrideFlag) Set(raw string) error {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "on", "yes":
		f.set = true
		f.value = true
		return nil
	case "0", "false", "off", "no":
		f.set = true
		f.value = false
		return nil
	default:
		return fmt.Errorf("compute must be on or off, got %q", raw)
	}
}

func (f *computeOverrideFlag) String() string {
	if !f.set {
		return ""
	}
	if f.value {
		return "on"
	}
	return "off"
}

func (f *computeOverrideFlag) IsBoolFlag() bool {
	return true
}

type optionalIntFlag struct {
	set   bool
	value int
}

func (f *optionalIntFlag) Set(raw string) error {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return err
	}
	f.set = true
	f.value = value
	return nil
}

func (f *optionalIntFlag) String() string {
	return strconv.Itoa(f.value)
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

type seedRefreshingDiscovery struct {
	cache      *membership.CachedPeerDiscovery
	seeds      []string
	joinToken  string
	joinTokens *membership.TokenManager
	client     *http.Client
}

func (d seedRefreshingDiscovery) Advertise(ctx context.Context, self domain.Peer) error {
	if d.cache == nil {
		return fmt.Errorf("seed refreshing discovery is not configured")
	}
	return d.cache.Advertise(ctx, self)
}

func (d seedRefreshingDiscovery) Peers(ctx context.Context) ([]domain.Peer, error) {
	if d.cache == nil {
		return nil, fmt.Errorf("seed refreshing discovery is not configured")
	}
	probeSeedPeersWithClient(ctx, d.cache, d.seeds, d.joinToken, d.joinTokens, d.client)
	return d.cache.Peers(ctx)
}

func (d seedRefreshingDiscovery) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if d.cache == nil {
		return nil, fmt.Errorf("seed refreshing discovery is not configured")
	}
	return d.cache.WatchPeers(ctx)
}

var _ ports.PeerDiscovery = seedRefreshingDiscovery{}

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
	transientClient := false
	if client == nil {
		client = peerControlHTTPClient()
		transientClient = true
	}
	if transientClient {
		defer client.CloseIdleConnections()
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
	resp, err := client.Do(req)
	if err != nil {
		return domain.Peer{}, fmt.Errorf("%w: %v", domain.ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := readLimitedPeerRPCBody(resp.Body)
		return domain.Peer{}, fmt.Errorf("%w: peer health returned %s: %s", domain.ErrUnreachable, resp.Status, strings.TrimSpace(string(body)))
	}
	var got domain.Peer
	if err := decodePeerRPCResponse(resp.Body, &got); err != nil {
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

func decodePeerRPCJSON(r *http.Request, out any) error {
	data, err := readLimitedPeerRPCBody(r.Body)
	if err != nil {
		return err
	}
	return decodePeerRPCJSONBytes(data, out)
}

func decodePeerRPCJSONBytes(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(out); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("peer rpc request body contains multiple JSON values")
	}
	return nil
}

func readLimitedPeerRPCBody(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	n, err := buf.ReadFrom(io.LimitReader(r, maxPeerRPCJSONBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if n > maxPeerRPCJSONBodyBytes {
		return nil, fmt.Errorf("peer rpc body exceeds %d bytes", maxPeerRPCJSONBodyBytes)
	}
	return buf.Bytes(), nil
}

func decodePeerRPCResponse(r io.Reader, out any) error {
	data, err := readLimitedPeerRPCBody(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func decodePeerRPCResponseStrict(r io.Reader, out any) error {
	data, err := readLimitedPeerRPCBody(r)
	if err != nil {
		return err
	}
	return decodePeerRPCJSONBytes(data, out)
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

func (c telemetryHTTPClient) Samples(ctx context.Context, peer domain.Peer, query domain.SessionMetricQuery) ([]domain.SessionMetric, error) {
	var samples []domain.SessionMetric
	err := doPeerRPC(ctx, c.Client, c.AuthToken, peer, http.MethodGet, telemetrySamplesPath(query), nil, &samples)
	return samples, err
}

func (c telemetryHTTPClient) PushSamples(ctx context.Context, peer domain.Peer, samples []domain.SessionMetric) error {
	return doPeerRPC(ctx, c.Client, c.AuthToken, peer, http.MethodPost, "/telemetry/samples", samples, nil)
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

func telemetrySamplesPath(query domain.SessionMetricQuery) string {
	values := url.Values{}
	if query.SessionID != "" {
		values.Set("session_id", query.SessionID)
	}
	if query.Project != "" {
		values.Set("project", query.Project)
	}
	if query.NodeID != "" {
		values.Set("node_id", query.NodeID)
	}
	if !query.Since.IsZero() {
		values.Set("since", query.Since.UTC().Format(time.RFC3339Nano))
	}
	if !query.Until.IsZero() {
		values.Set("until", query.Until.UTC().Format(time.RFC3339Nano))
	}
	if query.Limit > 0 {
		values.Set("limit", strconv.Itoa(query.Limit))
	}
	if len(values) == 0 {
		return "/telemetry/samples"
	}
	return "/telemetry/samples?" + values.Encode()
}

func doPeerRPC(ctx context.Context, client *http.Client, authToken string, peer domain.Peer, method, path string, in, out any) error {
	if len(peer.Addresses) == 0 {
		return fmt.Errorf("peer %q has no reachable address", peer.ID)
	}
	transientClient := false
	if client == nil {
		client = peerControlHTTPClient()
		transientClient = true
	}
	if transientClient {
		defer client.CloseIdleConnections()
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
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := readLimitedPeerRPCBody(resp.Body)
		return fmt.Errorf("peer rpc %s %s: %s", method, path, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	return decodePeerRPCResponse(resp.Body, out)
}

func peerControlHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy:             nil,
		DisableKeepAlives: true,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		IdleConnTimeout:       5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
	}}
}

func startRegistryReplication(ctx context.Context, registry ports.JobRegistry, discovery ports.PeerDiscovery, selfID, rpcToken string, clk ports.Clock, interval time.Duration) {
	startRegistryReplicationWithClient(ctx, registry, discovery, selfID, rpcToken, clk, interval, peerControlHTTPClient())
}

func startRegistryReplicationWithClient(ctx context.Context, registry ports.JobRegistry, discovery ports.PeerDiscovery, selfID, rpcToken string, clk ports.Clock, interval time.Duration, client *http.Client) {
	if registry == nil || discovery == nil || selfID == "" || clk == nil || interval <= 0 {
		return
	}
	replicator := peercoord.RegistryReplicator{
		Local:  registry,
		Peers:  discovery,
		Client: registryHTTPClient{AuthToken: rpcToken, Client: client},
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

type peerOwnerInspectorResolver interface {
	peerLeaseInspectorResolver
	JobStatusInspector(string) (ports.JobStatusInspector, error)
}

type peerRuntimeStore interface {
	ports.JobRegistry
	SaveNode(ctx context.Context, node domain.Node) error
}

type rescueRuntime interface {
	SubmitWithPayload(ctx context.Context, job domain.Job, payload []byte, hooks ...scheduler.SubmitHooks) (scheduler.Result, error)
}

func startPeerHeartbeat(ctx context.Context, self domain.Peer, discovery ports.PeerDiscovery, nodes gateway.NodeResolver, runtime rescueRuntime, registry peerRuntimeStore, joinToken string, joinTokens *membership.TokenManager, clk ports.Clock) {
	startPeerHeartbeatWithClient(ctx, self, discovery, nodes, runtime, registry, joinToken, joinTokens, clk, peerControlHTTPClient())
}

func startPeerHeartbeatWithClient(ctx context.Context, self domain.Peer, discovery ports.PeerDiscovery, nodes gateway.NodeResolver, runtime rescueRuntime, registry peerRuntimeStore, joinToken string, joinTokens *membership.TokenManager, clk ports.Clock, client *http.Client) {
	if discovery == nil || runtime == nil || registry == nil || clk == nil || self.ID == "" {
		return
	}
	owners, _ := nodes.(peerOwnerInspectorResolver)
	recovery := peercoord.Recovery{Registry: registry, Owners: owners, Rescue: rescueRecoveredJob(runtime), Clock: clk}
	heartbeat := &peercoord.Heartbeat{
		Self:      self,
		Discovery: discovery,
		Clock:     clk,
		Probe: func(ctx context.Context, peer domain.Peer) error {
			return probePeerHealthWithTokenManagerAndClient(ctx, peer, joinToken, joinTokens, client)
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

type jobRegistrySnapshotter interface {
	Snapshot(ctx context.Context) ([]domain.JobRecord, error)
}

func restoreQueuedJobs(ctx context.Context, store jobLister, queue *scheduler.Queue) error {
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return err
	}
	payloads := map[string][]byte{}
	if registry, ok := store.(jobRegistrySnapshotter); ok {
		records, err := registry.Snapshot(ctx)
		if err != nil {
			return err
		}
		for _, rec := range records {
			if rec.Status != domain.JobQueued || len(rec.Request) == 0 || rec.Handling == domain.HandlingPrivate || rec.PayloadRedacted {
				continue
			}
			_, payload, err := peercoord.DecodeRescuePayload(rec.Request)
			if err != nil {
				return fmt.Errorf("decode queued rescue payload for job %q: %w", rec.JobID, err)
			}
			payloads[rec.JobID] = payload
		}
	}
	for _, job := range jobs {
		if job.Status == domain.JobQueued {
			if payload, ok := payloads[job.ID]; ok {
				queue.EnqueueWithPayload(job, payload)
			} else {
				queue.Enqueue(job)
			}
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
	ImportedSamples         int
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
	hasSlot, err := hasRecommendationsForSlot(ctx, store, slotID)
	if err != nil {
		return syncResult, err
	}
	if hasSlot {
		if err := pushFleetRecommendations(ctx, store, syncCfg, reachablePeers, slotID, &syncResult); err != nil {
			return syncResult, err
		}
		return syncResult, nil
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

func hasRecommendationsForSlot(ctx context.Context, store optimizerRuntimeStore, slotID string) (bool, error) {
	recs, err := store.ListRecommendations(ctx, "")
	if err != nil {
		return false, err
	}
	for _, rec := range recs {
		if rec.SlotID == slotID {
			return true, nil
		}
	}
	return false, nil
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
		samples, err := cfg.Client.Samples(ctx, peer, domain.SessionMetricQuery{})
		if err != nil {
			result.SkippedPeers = append(result.SkippedPeers, fmt.Sprintf("%s samples: %v", peer.ID, err))
			continue
		}
		for _, sample := range samples {
			if err := store.RecordSample(ctx, sample); err != nil {
				return result, reachable, fmt.Errorf("import telemetry sample %q/%d from peer %q: %w", sample.SessionID, sample.Sequence, peer.ID, err)
			}
			result.ImportedSamples++
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
	if err := projectvalidation.ValidateSet(cfg.Projects, cfg.DefaultProject); err != nil {
		return err
	}
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

func gatewayProjectTokenMap(tokens []GatewayProjectToken) map[string]string {
	if len(tokens) == 0 {
		return nil
	}
	out := make(map[string]string, len(tokens))
	for _, token := range tokens {
		out[token.Token] = token.Project
	}
	return out
}

func peerEstimator(cfg PeerConfig, agents map[string]ports.NodeAgent, resolver estimate.NodeAgentResolver) ports.ResourceEstimator {
	explicit := estimate.NewInMemory()
	parser := cfg.GGUFParser
	if parser == "" {
		parser = cfg.ComputeConfig.GGUFParser
	}
	var metadataParser estimate.MetadataParser
	if parser != "" {
		metadataParser = estimate.NewCommandParser(parser, nil)
	}
	return estimate.NewBackendAware(estimate.NewGGUFWithResolver(metadataParser, agents, resolver), explicit)
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
