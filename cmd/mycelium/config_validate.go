package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"mycelium/internal/domain"
	projectvalidation "mycelium/internal/project"
)

func validatePeerConfig(cfg PeerConfig) error {
	listen := cfg.Listen
	if listen == "" {
		listen = "127.0.0.1:51846"
	}
	if cfg.JoinToken != "" && cfg.RPCToken == "" {
		return fmt.Errorf("rpc_token is required when join_token is configured")
	}
	if peerListenRequiresAuth(listen) && cfg.RPCToken == "" {
		return fmt.Errorf("rpc_token is required when listen is not loopback")
	}
	if peerListenRequiresAuth(listen) && !gatewayAuthConfigured(cfg) {
		return fmt.Errorf("gateway_token or gateway_project_tokens is required when listen is not loopback")
	}
	if cfg.Overlay || len(cfg.OverlayListenAddrs) > 0 || len(cfg.OverlayBootstrap) > 0 {
		return fmt.Errorf("overlay membership is disabled for the MVP; use LAN peer discovery")
	}
	if cfg.PrivateStorageKey != "" {
		return fmt.Errorf("private_storage_key is disabled until private job recovery is implemented")
	}
	if len(cfg.SubmitterPolicy) > 0 {
		return fmt.Errorf("submitter_policy is disabled until authenticated submitter policy is implemented")
	}
	if cfg.QueueDrainMS <= 0 {
		return fmt.Errorf("queue_drain_ms must be positive")
	}
	if cfg.QueueDrainLimit <= 0 {
		return fmt.Errorf("queue_drain_limit must be positive")
	}
	if cfg.OptimizerEvalMS <= 0 {
		return fmt.Errorf("optimizer_eval_ms must be positive")
	}
	if cfg.RegistrySyncMS <= 0 {
		return fmt.Errorf("registry_sync_ms must be positive")
	}
	if cfg.DiscoveryScanMS <= 0 {
		return fmt.Errorf("discovery_scan_ms must be positive")
	}
	if cfg.DiscoveryAdvertiseMS <= 0 {
		return fmt.Errorf("discovery_advertise_ms must be positive")
	}
	if err := projectvalidation.ValidateSet(cfg.Projects, cfg.DefaultProject); err != nil {
		return err
	}
	projects := map[string]struct{}{}
	for _, project := range cfg.Projects {
		projects[project.ID] = struct{}{}
	}
	seenGatewayTokens := map[string]struct{}{}
	if cfg.GatewayToken != "" {
		seenGatewayTokens[cfg.GatewayToken] = struct{}{}
	}
	for _, token := range cfg.GatewayProjectTokens {
		if token.Token == "" {
			return fmt.Errorf("gateway_project_tokens token is required")
		}
		if token.Project == "" {
			return fmt.Errorf("gateway_project_tokens project is required")
		}
		if _, ok := projects[token.Project]; !ok {
			return fmt.Errorf("gateway_project_tokens project %q is not configured", token.Project)
		}
		if _, ok := seenGatewayTokens[token.Token]; ok {
			return fmt.Errorf("duplicate gateway token")
		}
		seenGatewayTokens[token.Token] = struct{}{}
	}
	compute := defaultedComputeConfig(cfg.ComputeConfig)
	if compute.MaxUtil <= 0 || compute.MaxUtil > 1 {
		return fmt.Errorf("compute_config.max_util must be in (0,1]")
	}
	if compute.DiskMinFreeRatio <= 0 || compute.DiskMinFreeRatio >= 1 {
		return fmt.Errorf("compute_config.disk_min_free_ratio must be in (0,1)")
	}
	if compute.LoadTimeoutMS <= 0 {
		return fmt.Errorf("compute_config.load_timeout_ms must be positive")
	}
	if compute.VRAMMB < 0 {
		return fmt.Errorf("compute_config.vram_mb must be non-negative")
	}
	if compute.StopGraceMS < 0 {
		return fmt.Errorf("compute_config.stop_grace_ms must be non-negative")
	}
	switch compute.Backend {
	case "", domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendVLLM, domain.BackendOpenVINO, domain.BackendCustom:
	default:
		return fmt.Errorf("unknown compute backend %q", compute.Backend)
	}
	seenRuntime := map[domain.Backend]struct{}{}
	for _, runtime := range computeBackendRuntimes(compute) {
		switch runtime.Backend {
		case domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendVLLM, domain.BackendOpenVINO, domain.BackendCustom:
		default:
			return fmt.Errorf("unknown compute backend %q", runtime.Backend)
		}
		if _, ok := seenRuntime[runtime.Backend]; ok {
			return fmt.Errorf("duplicate compute backend %q", runtime.Backend)
		}
		seenRuntime[runtime.Backend] = struct{}{}
		if runtime.StopGraceMS < 0 {
			return fmt.Errorf("compute_config.backends stop_grace_ms must be non-negative")
		}
		if runtime.Backend == domain.BackendVLLM {
			_, _, err := vllmGPUUtilization(runtime.CustomArgs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func gatewayAuthConfigured(cfg PeerConfig) bool {
	return cfg.GatewayToken != "" || len(cfg.GatewayProjectTokens) > 0
}

func peerListenRequiresAuth(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return true
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return true
	}
	return !ip.IsLoopback()
}

func hasVLLMGPUUtilization(args []string) bool {
	_, ok, _ := vllmGPUUtilization(args)
	return ok
}

func vllmGPUUtilization(args []string) (float64, bool, error) {
	for i, arg := range args {
		if strings.HasPrefix(arg, "--gpu-memory-utilization=") {
			return parseGPUUtilization(strings.TrimPrefix(arg, "--gpu-memory-utilization="))
		}
		if arg == "--gpu-memory-utilization" {
			if i+1 >= len(args) {
				return 0, false, fmt.Errorf("vllm --gpu-memory-utilization requires a value")
			}
			return parseGPUUtilization(args[i+1])
		}
	}
	return 0, false, nil
}

func parseGPUUtilization(raw string) (float64, bool, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse vllm --gpu-memory-utilization %q: %w", raw, err)
	}
	if value <= 0 || value > 1 {
		return 0, false, fmt.Errorf("vllm --gpu-memory-utilization must be in (0,1]")
	}
	return value, true, nil
}
