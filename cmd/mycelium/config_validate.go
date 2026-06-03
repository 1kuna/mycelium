package main

import (
	"fmt"
	"strconv"
	"strings"

	"mycelium/internal/domain"
)

func validatePeerConfig(cfg PeerConfig) error {
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
	switch compute.Backend {
	case "", domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendVLLM, domain.BackendCustom:
	default:
		return fmt.Errorf("unknown compute backend %q", compute.Backend)
	}
	if compute.Backend == domain.BackendVLLM {
		value, ok, err := vllmGPUUtilization(compute.CustomArgs)
		if err != nil {
			return err
		}
		if ok && value >= unsafeVLLMGPUUtilization {
			return fmt.Errorf("vllm --gpu-memory-utilization %.2f is unsafe; use <= %.2f for catastrophic hosts", value, sparkSafeVLLMGPUUtil)
		}
	}
	return nil
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
