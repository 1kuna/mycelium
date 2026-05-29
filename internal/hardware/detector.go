package hardware

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Detector struct {
	GOOS    string
	Command func(ctx context.Context, name string, args ...string) ([]byte, error)
	Clock   ports.Clock
}

func NewDetector() Detector {
	return Detector{GOOS: runtime.GOOS, Command: runCommand, Clock: clock.System{}}
}

func (d Detector) Detect(ctx context.Context, seed domain.Node) (domain.Node, error) {
	goos := d.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	switch goos {
	case "darwin":
		return d.detectDarwin(ctx, seed)
	case "linux":
		return d.detectLinux(ctx, seed)
	default:
		return domain.Node{}, fmt.Errorf("unsupported hardware discovery OS %q", goos)
	}
}

func (d Detector) detectDarwin(ctx context.Context, seed domain.Node) (domain.Node, error) {
	command := d.Command
	if command == nil {
		command = runCommand
	}
	out, err := command(ctx, "sysctl", "-n", "hw.memsize")
	if err != nil {
		return domain.Node{}, err
	}
	bytes, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return domain.Node{}, err
	}
	node := seed
	node.OS = "darwin"
	node.Labels = mergeLabels(node.Labels, map[string]string{"gpu.vendor": "apple", "memory.class": "unified"})
	node.OOMSeverity = domain.OOMSoft
	node.Status = domain.NodeReady
	node.UnifiedMemory = true
	node.Accelerators = []domain.Accelerator{{
		Index:         0,
		Vendor:        "apple",
		Kind:          "unified",
		VRAMTotalMB:   int(bytes / 1024 / 1024),
		UnifiedMemory: true,
	}}
	if node.SpeedClass.TokensPerSecRef == 0 {
		clk := d.Clock
		if clk == nil {
			clk = clock.System{}
		}
		node.SpeedClass = domain.SpeedClass{TokensPerSecRef: 1, Source: "detected-default", ProbedAt: clk.Now().UTC()}
	}
	return node, nil
}

func (d Detector) detectLinux(ctx context.Context, seed domain.Node) (domain.Node, error) {
	command := d.Command
	if command == nil {
		command = runCommand
	}
	out, err := command(ctx, "nvidia-smi", "--query-gpu=index,name,memory.total,compute_cap", "--format=csv,noheader,nounits")
	if err != nil {
		return domain.Node{}, err
	}
	accelerators, err := parseNVIDIASMI(out)
	if err != nil {
		return domain.Node{}, err
	}
	node := seed
	node.OS = "linux"
	node.Labels = mergeLabels(node.Labels, map[string]string{"gpu.vendor": "nvidia", "memory.class": "discrete"})
	node.OOMSeverity = domain.OOMSoft
	node.Status = domain.NodeReady
	node.UnifiedMemory = false
	node.Accelerators = accelerators
	if node.SpeedClass.TokensPerSecRef == 0 {
		clk := d.Clock
		if clk == nil {
			clk = clock.System{}
		}
		node.SpeedClass = domain.SpeedClass{TokensPerSecRef: 1, Source: "detected-default", ProbedAt: clk.Now().UTC()}
	}
	return node, nil
}

func parseNVIDIASMI(out []byte) ([]domain.Accelerator, error) {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	accelerators := make([]domain.Accelerator, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) != 4 {
			return nil, fmt.Errorf("unexpected nvidia-smi row %q", line)
		}
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		index, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("parse nvidia gpu index %q: %w", parts[0], err)
		}
		vramMB, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("parse nvidia memory %q: %w", parts[2], err)
		}
		accelerators = append(accelerators, domain.Accelerator{
			Index:             index,
			Vendor:            "nvidia",
			Kind:              "cuda",
			VRAMTotalMB:       vramMB,
			ComputeCapability: parts[3],
			ArchFamily:        parts[1],
		})
	}
	if len(accelerators) == 0 {
		return nil, fmt.Errorf("nvidia-smi returned no GPUs")
	}
	return accelerators, nil
}

func mergeLabels(base, add map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range add {
		out[k] = v
	}
	return out
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).Output()
}

var _ ports.HardwareDetector = Detector{}
