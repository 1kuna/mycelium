package hardware

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Detector struct {
	GOOS    string
	Command func(ctx context.Context, name string, args ...string) ([]byte, error)
}

func NewDetector() Detector {
	return Detector{GOOS: runtime.GOOS, Command: runCommand}
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
		return domain.Node{}, fmt.Errorf("linux hardware discovery requires an explicit --vram-mb until NVIDIA probing is enabled")
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
		node.SpeedClass = domain.SpeedClass{TokensPerSecRef: 1, Source: "detected-default", ProbedAt: time.Now().UTC()}
	}
	return node, nil
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
