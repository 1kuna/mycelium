package main

import (
	"context"
	"errors"
	"net/http"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/backends/mlx"
	"mycelium/internal/backends/vllm"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/hardware"
	"mycelium/internal/lease"
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
		ID:         compute.ID,
		Name:       compute.Name,
		Address:    cfg.Listen,
		MaxUtil:    compute.MaxUtil,
		Status:     domain.NodeReady,
		SpeedClass: domain.SpeedClass{TokensPerSecRef: 1, Source: "class-default"},
	}
	node := seed
	if compute.VRAMMB > 0 {
		node = explicitMemoryNode(seed, compute.VRAMMB)
	} else {
		detected, err := hardware.NewDetector().Detect(ctx, seed)
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

	opts := []nodeagent.Option{
		nodeagent.WithListenAddr(compute.BackendListen),
		nodeagent.WithAllocator(lease.NewAllocator()),
	}
	if compute.GGUFParser != "" {
		opts = append(opts, nodeagent.WithModelInspector(nodeagent.ParserInspector{Parser: estimate.NewCommandParser(compute.GGUFParser, []string{"{model}"})}))
	}
	agent := nodeagent.NewAgent(node, adapter, clock.System{}, opts...)
	admission := nodeagent.NewAdmission(node, lease.NewAllocator(), clock.System{}, nodeagent.WithAdmissionInstances(agent.Instances))
	return computeRuntime{
		handler:   nodeagent.HTTPServer{Agent: agent, Admission: admission, AuthToken: cfg.RPCToken},
		node:      node,
		agent:     agent,
		admission: admission,
		shutdown:  agent.Shutdown,
	}, nil
}

const LabelPeerBackend = "mycelium.peer.backend"

func computeBackendAdapter(cfg ComputeConfig, registry nodeagent.StoreProcessRegistry) (ports.BackendAdapter, error) {
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
