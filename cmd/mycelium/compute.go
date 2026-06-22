package main

import (
	"context"
	"errors"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/backends/mlx"
	"mycelium/internal/backends/openvino"
	"mycelium/internal/backends/processadapter"
	"mycelium/internal/backends/vllm"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/engine"
	"mycelium/internal/estimate"
	"mycelium/internal/hardware"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/ports"
	storesqlite "mycelium/internal/store/sqlite"
)

type computeRuntime struct {
	handler   http.Handler
	node      domain.Node
	agent     ports.NodeAgent
	admission ports.AdmissionController
	shutdown  func(context.Context) error
}

func buildComputeRuntime(ctx context.Context, cfg PeerConfig, store *storesqlite.Store) (computeRuntime, error) {
	compute := cfg.ComputeConfig
	seed := domain.Node{
		ID:               compute.ID,
		Name:             compute.Name,
		Address:          cfg.Listen,
		MaxUtil:          compute.MaxUtil,
		DiskTotalMB:      compute.DiskTotalMB,
		DiskFreeMB:       compute.DiskFreeMB,
		DiskMinFreeRatio: compute.DiskMinFreeRatio,
		Status:           domain.NodeReady,
		SpeedClass:       domain.SpeedClass{TokensPerSecRef: 1, Source: "class-default"},
	}
	detector := hardware.NewDetector()
	if compute.DiskPath != "" {
		detector.DiskPath = compute.DiskPath
	}
	node, err := detector.Detect(ctx, seed)
	if err != nil {
		return computeRuntime{}, err
	}
	node.Arch = runtime.GOARCH
	if compute.VRAMMB > 0 {
		node, err = applyExplicitVRAM(node, compute.VRAMMB)
		if err != nil {
			return computeRuntime{}, err
		}
	}
	if len(node.Accelerators) == 0 {
		return computeRuntime{}, errors.New("compute=true requires at least one detected accelerator")
	}
	runtimes := computeBackendRuntimes(compute)
	node.Labels = withPeerBackendLabels(node.Labels, runtimeBackends(runtimes))
	if err := validateRuntimeComputeSafety(compute, node); err != nil {
		return computeRuntime{}, err
	}

	registry := nodeagent.StoreProcessRegistry{Store: store, NodeID: node.ID}
	adapter, err := computeBackendAdapter(compute, registry)
	if err != nil {
		return computeRuntime{}, err
	}
	if refs, err := store.ProcessRefs(ctx, node.ID); err == nil && len(refs) > 0 {
		reaper := nodeagent.NewReaperFromRefs(refs, nodeagent.BackendProcessKiller{Backend: adapter})
		if _, err := reaper.Reap(ctx); err != nil {
			return computeRuntime{}, err
		}
		if err := store.DeleteProcessRefs(ctx, node.ID); err != nil {
			return computeRuntime{}, err
		}
	} else if err != nil {
		return computeRuntime{}, err
	}

	allocator, pinnedReservations, err := computeAdmissionAllocator(ctx, store, node.ID)
	if err != nil {
		return computeRuntime{}, err
	}
	opts := []nodeagent.Option{
		nodeagent.WithListenAddr(compute.BackendListen),
		nodeagent.WithAllocator(allocator),
		nodeagent.WithEngineReadinessChecker(engine.NewReadinessChecker(store, domain.EngineReadinessLegacyAllow)),
	}
	if compute.LoadTimeoutMS > 0 {
		opts = append(opts, nodeagent.WithLoadTimeout(time.Duration(compute.LoadTimeoutMS)*time.Millisecond))
	}
	if compute.GGUFParser != "" {
		opts = append(opts, nodeagent.WithModelInspector(nodeagent.ParserInspector{Parser: estimate.NewCommandParser(compute.GGUFParser, nil)}))
	}
	agent := nodeagent.NewAgent(node, adapter, clock.System{}, opts...)
	admissionOpts := []nodeagent.AdmissionOption{
		nodeagent.WithAdmissionInstances(agent.Instances),
		nodeagent.WithAdmissionStateStore(store),
		nodeagent.WithPinnedReservations(pinnedReservations...),
	}
	admission := nodeagent.NewAdmission(node, allocator, clock.System{}, admissionOpts...)
	if err := loadPinnedReservations(ctx, agent, store, node.ID); err != nil {
		return computeRuntime{}, err
	}
	if _, err := nodeagent.ReconcileAdmissionState(ctx, store, node.ID, agent.Instances(), clock.System{}.Now()); err != nil {
		return computeRuntime{}, err
	}
	return computeRuntime{
		handler:   nodeagent.HTTPServer{Agent: agent, Admission: admission, Jobs: store, AuthToken: cfg.RPCToken},
		node:      node,
		agent:     agent,
		admission: admission,
		shutdown:  agent.Shutdown,
	}, nil
}

func loadPinnedReservations(ctx context.Context, agent *nodeagent.Agent, store *storesqlite.Store, nodeID string) error {
	reservations, err := store.ListReservations(ctx)
	if err != nil {
		return err
	}
	for _, reservation := range reservations {
		if reservation.Kind != domain.ReservationPinned || reservation.NodeID != nodeID {
			continue
		}
		if reservation.PresetID == "" {
			return errors.New("pinned reservation missing preset id")
		}
		preset, err := store.Preset(ctx, reservation.PresetID)
		if err != nil {
			return err
		}
		inst, err := agent.Load(ctx, domain.LoadRequest{
			Preset:         preset,
			Claim:          domain.Claim{WeightsMB: preset.EstWeightsMB, KVReservedMB: int(math.Ceil(float64(preset.ContextLength) * preset.KVPerTokenMB))},
			AcceleratorSet: []int{0},
			ReservationID:  reservation.ID,
		})
		if err != nil {
			return err
		}
		if err := agent.ProtectInstance(inst.ID, reservation.ID); err != nil {
			return err
		}
	}
	return nil
}

func computeAdmissionAllocator(ctx context.Context, store *storesqlite.Store, nodeID string) (ports.Allocator, []string, error) {
	reservations, err := store.ListReservations(ctx)
	if err != nil {
		return nil, nil, err
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return nil, nil, err
	}
	return allocatorFromReservations(reservations, presetMap(presets)), pinnedReservationIDs(reservations, nodeID), nil
}

func pinnedReservationIDs(reservations []domain.Reservation, nodeID string) []string {
	var ids []string
	for _, reservation := range reservations {
		if reservation.Kind == domain.ReservationPinned && reservation.NodeID == nodeID {
			ids = append(ids, reservation.ID)
		}
	}
	return ids
}

const LabelPeerBackend = domain.LabelPeerBackend

func computeBackendAdapter(cfg ComputeConfig, registry nodeagent.StoreProcessRegistry) (ports.BackendAdapter, error) {
	return computeBackendAdapterWithProcessRunner(cfg, registry, nil)
}

func computeBackendAdapterWithProcessRunner(cfg ComputeConfig, registry nodeagent.StoreProcessRegistry, runner processadapter.ProcessRunner) (ports.BackendAdapter, error) {
	runtimes := computeBackendRuntimes(cfg)
	if len(runtimes) > 1 {
		adapters := map[domain.Backend]ports.BackendAdapter{}
		for _, runtime := range runtimes {
			adapter, err := computeBackendRuntimeAdapter(runtime, registry, runner)
			if err != nil {
				return nil, err
			}
			adapters[runtime.Backend] = adapter
		}
		return newBackendRouter(adapters), nil
	}
	return computeBackendRuntimeAdapter(runtimes[0], registry, runner)
}

func computeBackendRuntimeAdapter(cfg BackendRuntimeConfig, registry nodeagent.StoreProcessRegistry, runner processadapter.ProcessRunner) (ports.BackendAdapter, error) {
	backend := cfg.Backend
	if backend == "" {
		backend = domain.BackendLlamaCpp
	}
	switch backend {
	case domain.BackendLlamaCpp:
		args := append([]string(nil), llamacpp.DefaultConfig().Args...)
		args = append(args, cfg.CustomArgs...)
		return llamacpp.NewAdapter(llamacpp.Config{
			BinaryPath:      computeRuntimeBackendBinary(cfg, "llama-server"),
			Args:            args,
			HealthPath:      cfg.HealthPath,
			StopGracePeriod: time.Duration(cfg.StopGraceMS) * time.Millisecond,
			ProcessRegistry: registry,
		}), nil
	case domain.BackendMLX:
		return mlx.NewAdapterWithConfig(mlx.Config{BinaryPath: computeRuntimeBackendBinary(cfg, "mlx_lm.server"), Args: append([]string(nil), cfg.CustomArgs...), ProcessRegistry: registry, ProcessRunner: runner}), nil
	case domain.BackendVLLM:
		return vllm.NewAdapterWithConfig(vllm.Config{BinaryPath: computeRuntimeBackendBinary(cfg, "vllm"), Args: append([]string(nil), cfg.CustomArgs...), ProcessRegistry: registry, ProcessRunner: runner}), nil
	case domain.BackendOpenVINO:
		return openvino.NewAdapterWithConfig(openvino.Config{BinaryPath: computeRuntimeBackendBinary(cfg, "openvino-genai-openai"), Args: append([]string(nil), cfg.CustomArgs...), ProcessRegistry: registry, ProcessRunner: runner}), nil
	case domain.BackendCustom:
		binary := computeRuntimeBackendBinary(cfg, "")
		if binary == "" {
			return nil, errors.New("custom compute backend binary path is required")
		}
		return processadapter.New(processadapter.Config{
			Name:            "custom",
			BinaryPath:      binary,
			Args:            append([]string(nil), cfg.CustomArgs...),
			HealthPath:      cfg.HealthPath,
			StopGracePeriod: time.Duration(cfg.StopGraceMS) * time.Millisecond,
			ProcessRegistry: registry,
			ProcessRunner:   runner,
		}), nil
	default:
		return nil, errors.New("unknown compute backend " + string(backend))
	}
}

func validateRuntimeComputeSafety(cfg ComputeConfig, node domain.Node) error {
	if node.OOMSeverity != domain.OOMCatastrophic {
		return nil
	}
	for _, runtime := range computeBackendRuntimes(cfg) {
		if runtime.Backend != domain.BackendVLLM {
			continue
		}
		value, ok, err := vllmGPUUtilization(runtime.CustomArgs)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("catastrophic vllm host requires --gpu-memory-utilization <= 0.85")
		}
		if value > sparkSafeVLLMGPUUtil {
			return errors.New("catastrophic vllm host requires --gpu-memory-utilization <= 0.85")
		}
	}
	return nil
}

func computeBackendBinary(cfg ComputeConfig, fallback string) string {
	return computeRuntimeBackendBinary(legacyBackendRuntime(cfg), fallback)
}

func computeRuntimeBackendBinary(cfg BackendRuntimeConfig, fallback string) string {
	if cfg.BackendBinary != "" {
		return cfg.BackendBinary
	}
	if cfg.Backend == "" || cfg.Backend == domain.BackendLlamaCpp {
		if cfg.LlamaServer != "" {
			return cfg.LlamaServer
		}
	}
	return fallback
}

func withPeerBackendLabel(labels map[string]string, backend domain.Backend) map[string]string {
	return withPeerBackendLabels(labels, []domain.Backend{backend})
}

func withPeerBackendLabels(labels map[string]string, backends []domain.Backend) map[string]string {
	out := map[string]string{}
	for key, value := range labels {
		out[key] = value
	}
	normalized := normalizeBackendSet(backends)
	if len(normalized) == 0 {
		normalized = []domain.Backend{domain.BackendLlamaCpp}
	}
	if len(normalized) == 1 {
		out[LabelPeerBackend] = string(normalized[0])
		delete(out, domain.LabelPeerBackends)
		return out
	}
	delete(out, LabelPeerBackend)
	parts := make([]string, 0, len(normalized))
	for _, backend := range normalized {
		parts = append(parts, string(backend))
	}
	out[domain.LabelPeerBackends] = strings.Join(parts, ",")
	return out
}

func normalizeBackendSet(backends []domain.Backend) []domain.Backend {
	seen := map[domain.Backend]struct{}{}
	for _, backend := range backends {
		if backend == "" {
			backend = domain.BackendLlamaCpp
		}
		seen[backend] = struct{}{}
	}
	normalized := make([]domain.Backend, 0, len(seen))
	for backend := range seen {
		normalized = append(normalized, backend)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i] < normalized[j] })
	return normalized
}

func runtimeBackends(runtimes []BackendRuntimeConfig) []domain.Backend {
	backends := make([]domain.Backend, 0, len(runtimes))
	for _, runtime := range runtimes {
		backends = append(backends, runtime.Backend)
	}
	return backends
}

func computeBackendRuntimes(cfg ComputeConfig) []BackendRuntimeConfig {
	if len(cfg.Backends) == 0 {
		return []BackendRuntimeConfig{legacyBackendRuntime(cfg)}
	}
	runtimes := make([]BackendRuntimeConfig, len(cfg.Backends))
	for i, runtime := range cfg.Backends {
		runtimes[i] = defaultedBackendRuntimeConfig(runtime)
	}
	return runtimes
}

func legacyBackendRuntime(cfg ComputeConfig) BackendRuntimeConfig {
	return defaultedBackendRuntimeConfig(BackendRuntimeConfig{
		Backend:       cfg.Backend,
		BackendBinary: cfg.BackendBinary,
		CustomArgs:    append([]string(nil), cfg.CustomArgs...),
		HealthPath:    cfg.HealthPath,
		StopGraceMS:   cfg.StopGraceMS,
		LlamaServer:   cfg.LlamaServer,
	})
}

func overrideString(flagValue *string, target *string) {
	if flagValue != nil && *flagValue != "" {
		*target = *flagValue
	}
}

func applyExplicitVRAM(node domain.Node, vramMB int) (domain.Node, error) {
	if vramMB <= 0 {
		return node, nil
	}
	if len(node.Accelerators) == 0 {
		return domain.Node{}, errors.New("compute vram_mb cannot override a host with no detected accelerator")
	}
	node.Accelerators = append([]domain.Accelerator(nil), node.Accelerators...)
	node.Accelerators[0].VRAMTotalMB = vramMB
	return node, nil
}
