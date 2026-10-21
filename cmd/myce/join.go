package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"mycelium/internal/myce"
)

type joinGPUOptions struct {
	platform, device, mode string
	memory, reserve        float64
}

func detectJoinGPU(ctx context.Context, probe myce.MachineProbe, options joinGPUOptions, explicit map[string]bool) (joinGPUOptions, error) {
	if options.platform != "" {
		return options, nil
	}
	ctx, cancel := myce.SetupTimeout(ctx)
	defer cancel()
	checked := []string{"OS/architecture compatibility"}
	if command := probe.Command; command != nil {
		probe.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			checked = append(checked, name)
			return command(ctx, name, args...)
		}
	}
	if command := probe.CommandEnv; command != nil {
		probe.CommandEnv = func(ctx context.Context, env map[string]string, name string, args ...string) ([]byte, error) {
			checked = append(checked, name)
			return command(ctx, env, name, args...)
		}
	}
	selector := strings.TrimSpace(strings.Split(options.device, ",")[0])
	d, err := probe.DetectDevice(ctx, selector)
	if err != nil && err.Error() == "automatic setup requires exactly one GPU" {
		// Detect a representative card without narrowing the offered inventory.
		var devices []myce.MachineDevice
		devices, err = probe.Accelerators(ctx)
		if err == nil && len(devices) > 0 {
			d, err = probe.DetectDevice(ctx, devices[0].Selector)
		}
	}
	if errors.Is(err, myce.ErrNoGPU) {
		fmt.Fprintln(os.Stderr, cpuFallbackNotice)
		options.platform, options.mode, options.memory, options.reserve = "cpu", "fixed", 0, 0
		return options, nil
	}
	if err != nil {
		return options, fmt.Errorf("GPU auto-detection on %s/%s checked %s: %w; specify --gpu (and --device on multi-GPU hosts)", probe.OS, probe.Arch, strings.Join(checked, ", "), err)
	}
	options.platform = d.Platform
	// Like install worker, an omitted selector uses the sole detected CUDA
	// device or the platform's default device. Explicit selectors stay intact.
	if !explicit["memory-mode"] {
		options.mode = d.MemoryMode
	}
	if !explicit["memory-gb"] {
		options.memory = d.MemoryGB
	}
	if !explicit["reserve-memory-gb"] {
		options.reserve = d.ReserveGB
	}
	return options, nil
}

const cpuFallbackNotice = "No supported GPU hardware is visible; this hypha joins with CPU and RAM components."
