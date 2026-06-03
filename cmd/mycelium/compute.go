package main

import (
	"context"
	"errors"
	"math"
	"net/http"
	"time"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/backends/mlx"
	"mycelium/internal/backends/processadapter"
	"mycelium/internal/backends/vllm"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
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
	node := seed
	if compute.VRAMMB > 0 {
		node = explicitMemoryNode(seed, compute.VRAMMB)
		withDisk, err := detector.AddDiskStats(node)
		if err != nil {
			return computeRuntime{}, err
		}
		node = withDisk
	} else {
		detected, err := detector.Detect(ctx, seed)
		if err != nil {
			return computeRuntime{}, err
		}
		node = detected
	}
	node.Labels = withPeerBackendLabel(node.Labels, compute.Backend)

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
	}
	if compute.LoadTimeoutMS > 0 {
		opts = append(opts, nodeagent.WithLoadTimeout(time.Duration(compute.LoadTimeoutMS)*time.Millisecond))
	}
	if compute.GGUFParser != "" {
		opts = append(opts, nodeagent.WithModelInspector(nodeagent.ParserInspector{Parser: estimate.NewCommandParser(compute.GGUFParser, []string{"{model}"})}))
	}
	agent := nodeagent.NewAgent(node, adapter, clock.System{}, opts...)
	admissionOpts := []nodeagent.AdmissionOption{
		nodeagent.WithAdmissionInstances(agent.Instances),
		nodeagent.WithAdmissionStateStore(store),
		nodeagent.WithPinnedReservations(pinnedReservations...),
	}
	if policy := submitterPolicyFromConfig(cfg.SubmitterPolicy); len(policy.Rules) > 0 {
		admissionOpts = append(admissionOpts, nodeagent.WithSubmitterPolicy(policy))
	}
	admission := nodeagent.NewAdmission(node, allocator, clock.System{}, admissionOpts...)
	if err := loadPinnedReservations(ctx, agent, store, node.ID); err != nil {
		return computeRuntime{}, err
	}
	if _, err := nodeagent.ReconcileAdmissionState(ctx, store, node.ID, agent.Instances(), clock.System{}.Now()); err != nil {
		return computeRuntime{}, err
	}
	return computeRuntime{
		handler:   nodeagent.HTTPServer{Agent: agent, Admission: admission, AuthToken: cfg.RPCToken},
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

func submitterPolicyFromConfig(config map[string]SubmitterPolicyRule) nodeagent.SubmitterPolicy {
	policy := nodeagent.SubmitterPolicy{Rules: map[string]nodeagent.SubmitterRule{}}
	for submitter, rule := range config {
		if submitter == "" {
			continue
		}
		policy.Rules[submitter] = nodeagent.SubmitterRule{MaxPriority: rule.MaxPriority, AllowPrivate: rule.AllowPrivate}
	}
	if len(policy.Rules) == 0 {
		return nodeagent.SubmitterPolicy{}
	}
	return policy
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
	backend := cfg.Backend
	if backend == "" {
		backend = domain.BackendLlamaCpp
	}
	switch backend {
	case domain.BackendLlamaCpp:
		return llamacpp.NewAdapter(llamacpp.Config{BinaryPath: computeBackendBinary(cfg, "llama-server"), ProcessRegistry: registry}), nil
	case domain.BackendMLX:
		return mlx.NewAdapterWithConfig(mlx.Config{BinaryPath: computeBackendBinary(cfg, "mlx_lm.server"), ProcessRegistry: registry}), nil
	case domain.BackendVLLM:
		return vllm.NewAdapterWithConfig(vllm.Config{BinaryPath: computeBackendBinary(cfg, "vllm"), ProcessRegistry: registry}), nil
	case domain.BackendCustom:
		binary := computeBackendBinary(cfg, "")
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

func computeBackendBinary(cfg ComputeConfig, fallback string) string {
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
	out := map[string]string{}
	for key, value := range labels {
		out[key] = value
	}
	if backend == "" {
		backend = domain.BackendLlamaCpp
	}
	out[LabelPeerBackend] = string(backend)
	return out
}

func overrideString(flagValue *string, target *string) {
	if flagValue != nil && *flagValue != "" {
		*target = *flagValue
	}
}

func explicitMemoryNode(seed domain.Node, vramMB int) domain.Node {
	node := seed
	node.OS = "darwin"
	node.Labels = map[string]string{"gpu.vendor": "apple", "memory.class": "unified"}
	node.OOMSeverity = domain.OOMSoft
	node.UnifiedMemory = true
	node.Accelerators = []domain.Accelerator{{
		Index:         0,
		Vendor:        "apple",
		Kind:          "unified",
		VRAMTotalMB:   vramMB,
		UnifiedMemory: true,
	}}
	return node
}
