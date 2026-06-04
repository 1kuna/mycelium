package hardware

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Detector struct {
	GOOS     string
	Command  func(ctx context.Context, name string, args ...string) ([]byte, error)
	Clock    ports.Clock
	DiskPath string
	StatDisk func(path string) (DiskStats, error)
	ReadFile func(path string) ([]byte, error)
}

type DiskStats struct {
	TotalMB int
	FreeMB  int
}

func NewDetector() Detector {
	return Detector{GOOS: runtime.GOOS, Command: runCommand, Clock: clock.System{}, DiskPath: "/"}
}

func (d Detector) Detect(ctx context.Context, seed domain.Node) (domain.Node, error) {
	goos := d.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	var (
		node domain.Node
		err  error
	)
	switch goos {
	case "darwin":
		node, err = d.detectDarwin(ctx, seed)
	case "linux":
		node, err = d.detectLinux(ctx, seed)
	default:
		return domain.Node{}, fmt.Errorf("unsupported hardware discovery OS %q", goos)
	}
	if err != nil {
		return domain.Node{}, err
	}
	return d.AddDiskStats(node)
}

func (d Detector) AddDiskStats(node domain.Node) (domain.Node, error) {
	if node.DiskMinFreeRatio == 0 {
		node.DiskMinFreeRatio = domain.DefaultDiskMinFreeRatio
	}
	if node.DiskTotalMB > 0 {
		return node, nil
	}
	path := d.DiskPath
	if path == "" {
		path = "/"
	}
	stat := d.StatDisk
	if stat == nil {
		stat = statDisk
	}
	stats, err := stat(path)
	if err != nil {
		return domain.Node{}, fmt.Errorf("detect disk capacity %s: %w", path, err)
	}
	if stats.TotalMB <= 0 || stats.FreeMB < 0 {
		return domain.Node{}, fmt.Errorf("invalid disk capacity total_mb=%d free_mb=%d", stats.TotalMB, stats.FreeMB)
	}
	node.DiskTotalMB = stats.TotalMB
	node.DiskFreeMB = stats.FreeMB
	return node, nil
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
	appleSilicon, err := darwinAppleSilicon(ctx, command)
	if err != nil {
		return domain.Node{}, err
	}
	node := seed
	node.OS = "darwin"
	node.OOMSeverity = domain.OOMSoft
	node.Status = domain.NodeReady
	if appleSilicon {
		usedMB, err := darwinMemoryUsedMB(ctx, command, bytes)
		if err != nil {
			return domain.Node{}, err
		}
		node.Labels = mergeLabels(node.Labels, map[string]string{"gpu.vendor": "apple", "memory.class": "unified"})
		node.UnifiedMemory = true
		node.Accelerators = []domain.Accelerator{{
			Index:         0,
			Vendor:        "apple",
			Kind:          "unified",
			VRAMTotalMB:   int(bytes / 1024 / 1024),
			VRAMUsedMB:    usedMB,
			UnifiedMemory: true,
		}}
	} else {
		node.Labels = mergeLabels(node.Labels, map[string]string{"memory.class": "system"})
		node.UnifiedMemory = false
		node.Accelerators = nil
	}
	if node.SpeedClass.TokensPerSecRef == 0 {
		clk := d.Clock
		if clk == nil {
			clk = clock.System{}
		}
		node.SpeedClass = domain.SpeedClass{TokensPerSecRef: 1, Source: "class-default", ProbedAt: clk.Now().UTC()}
	}
	return node, nil
}

func darwinMemoryUsedMB(ctx context.Context, command func(context.Context, string, ...string) ([]byte, error), totalBytes int64) (int, error) {
	pageSizeOut, err := command(ctx, "sysctl", "-n", "hw.pagesize")
	if err != nil {
		return 0, err
	}
	pageSize, err := strconv.ParseInt(strings.TrimSpace(string(pageSizeOut)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse hw.pagesize: %w", err)
	}
	if pageSize <= 0 {
		return 0, fmt.Errorf("invalid hw.pagesize %d", pageSize)
	}
	vmOut, err := command(ctx, "vm_stat")
	if err != nil {
		return 0, err
	}
	freePages, err := parseDarwinAvailablePages(vmOut)
	if err != nil {
		return 0, err
	}
	freeBytes := freePages * pageSize
	if freeBytes < 0 || freeBytes > totalBytes {
		return 0, fmt.Errorf("invalid darwin memory pressure free_bytes=%d total_bytes=%d", freeBytes, totalBytes)
	}
	return int((totalBytes - freeBytes) / 1024 / 1024), nil
}

func parseDarwinAvailablePages(out []byte) (int64, error) {
	var freePages int64
	var saw bool
	for _, raw := range strings.Split(string(out), "\n") {
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "Pages free:") && !strings.HasPrefix(line, "Pages inactive:") && !strings.HasPrefix(line, "Pages speculative:") {
			continue
		}
		_, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSuffix(strings.TrimSpace(value), ".")
		pages, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse vm_stat %q: %w", line, err)
		}
		freePages += pages
		saw = true
	}
	if !saw {
		return 0, errors.New("vm_stat did not report free memory pages")
	}
	return freePages, nil
}

func darwinAppleSilicon(ctx context.Context, command func(context.Context, string, ...string) ([]byte, error)) (bool, error) {
	out, err := command(ctx, "sysctl", "-n", "hw.optional.arm64")
	if err != nil {
		return false, err
	}
	value := strings.TrimSpace(string(out))
	switch value {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return false, fmt.Errorf("parse hw.optional.arm64 %q", value)
	}
}

func (d Detector) detectLinux(ctx context.Context, seed domain.Node) (domain.Node, error) {
	command := d.Command
	if command == nil {
		command = runCommand
	}
	out, err := command(ctx, "nvidia-smi", "--query-gpu=index,name,memory.total,memory.used,compute_cap", "--format=csv,noheader,nounits")
	if err == nil {
		accelerators, parseErr := parseNVIDIASMI(out, linuxUnifiedMemoryFallback{})
		if parseErr != nil && nvidiaSMINeedsSystemMemory(out) {
			totalMB, memErr := linuxSystemMemoryMB(ctx, command)
			if memErr != nil {
				return domain.Node{}, fmt.Errorf("%w; system memory fallback: %w", parseErr, memErr)
			}
			usedMB, memErr := d.linuxMemoryUsedMB()
			if memErr != nil {
				return domain.Node{}, fmt.Errorf("%w; system memory pressure: %w", parseErr, memErr)
			}
			accelerators, parseErr = parseNVIDIASMI(out, linuxUnifiedMemoryFallback{TotalMB: totalMB, UsedMB: usedMB})
		}
		if parseErr != nil {
			return domain.Node{}, parseErr
		}
		return linuxNode(seed, "nvidia", accelerators, d.Clock), nil
	}

	nvidiaErr := err
	out, err = command(ctx, "clinfo")
	if err != nil {
		return domain.Node{}, fmt.Errorf("linux hardware discovery failed: nvidia-smi: %w; clinfo: %w", nvidiaErr, err)
	}
	accelerators, err := parseIntelCLInfo(out)
	if err != nil {
		return domain.Node{}, fmt.Errorf("linux hardware discovery failed: nvidia-smi: %w; clinfo: %w", nvidiaErr, err)
	}
	return linuxNode(seed, "intel", accelerators, d.Clock), nil
}

func linuxNode(seed domain.Node, vendor string, accelerators []domain.Accelerator, clk ports.Clock) domain.Node {
	node := seed
	node.OS = "linux"
	labels := map[string]string{"gpu.vendor": vendor, "memory.class": "discrete"}
	for _, acc := range accelerators {
		if acc.Kind != "" {
			labels["gpu.kind"] = acc.Kind
		}
		if acc.UnifiedMemory {
			labels["memory.class"] = "unified"
			node.UnifiedMemory = true
		}
	}
	node.Labels = mergeLabels(node.Labels, labels)
	node.OOMSeverity = domain.OOMSoft
	for _, acc := range accelerators {
		if acc.Kind == "gb10" {
			node.OOMSeverity = domain.OOMCatastrophic
			break
		}
	}
	node.Status = domain.NodeReady
	node.Accelerators = accelerators
	if node.SpeedClass.TokensPerSecRef == 0 {
		if clk == nil {
			clk = clock.System{}
		}
		node.SpeedClass = domain.SpeedClass{TokensPerSecRef: 1, Source: "class-default", ProbedAt: clk.Now().UTC()}
	}
	return node
}

type linuxUnifiedMemoryFallback struct {
	TotalMB int
	UsedMB  int
}

func parseNVIDIASMI(out []byte, unifiedMemoryFallback linuxUnifiedMemoryFallback) ([]domain.Accelerator, error) {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	accelerators := make([]domain.Accelerator, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) != 5 {
			return nil, fmt.Errorf("unexpected nvidia-smi row %q", line)
		}
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		index, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("parse nvidia gpu index %q: %w", parts[0], err)
		}
		kind := nvidiaKind(parts[1])
		unified := kind == "gb10"
		vramMB, err := strconv.Atoi(parts[2])
		if err != nil {
			if !unified || !strings.EqualFold(parts[2], "[N/A]") || unifiedMemoryFallback.TotalMB <= 0 {
				return nil, fmt.Errorf("parse nvidia memory %q: %w", parts[2], err)
			}
			vramMB = unifiedMemoryFallback.TotalMB
		}
		usedMB, err := strconv.Atoi(parts[3])
		if err != nil {
			if !unified || !strings.EqualFold(parts[3], "[N/A]") || unifiedMemoryFallback.UsedMB < 0 {
				return nil, fmt.Errorf("parse nvidia used memory %q: %w", parts[3], err)
			}
			usedMB = unifiedMemoryFallback.UsedMB
		}
		accelerators = append(accelerators, domain.Accelerator{
			Index:             index,
			Vendor:            "nvidia",
			Kind:              kind,
			VRAMTotalMB:       vramMB,
			VRAMUsedMB:        usedMB,
			UnifiedMemory:     unified,
			ComputeCapability: parts[4],
			ArchFamily:        parts[1],
		})
	}
	if len(accelerators) == 0 {
		return nil, fmt.Errorf("nvidia-smi returned no GPUs")
	}
	return accelerators, nil
}

func nvidiaKind(name string) string {
	lower := strings.ToLower(name)
	if strings.Contains(lower, "gb10") {
		return "gb10"
	}
	return "cuda"
}

func nvidiaSMINeedsSystemMemory(out []byte) bool {
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		parts := strings.Split(line, ",")
		if len(parts) != 5 {
			continue
		}
		if nvidiaKind(strings.TrimSpace(parts[1])) == "gb10" && (strings.EqualFold(strings.TrimSpace(parts[2]), "[N/A]") || strings.EqualFold(strings.TrimSpace(parts[3]), "[N/A]")) {
			return true
		}
	}
	return false
}

func (d Detector) linuxMemoryUsedMB() (int, error) {
	readFile := d.ReadFile
	if readFile == nil {
		readFile = os.ReadFile
	}
	data, err := readFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseLinuxMemoryUsedMB(data)
}

func parseLinuxMemoryUsedMB(data []byte) (int, error) {
	var totalKB, availableKB int64
	for _, line := range strings.Split(string(data), "\n") {
		key, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", key, err)
		}
		switch key {
		case "MemTotal":
			totalKB = value
		case "MemAvailable":
			availableKB = value
		}
	}
	if totalKB <= 0 || availableKB < 0 || availableKB > totalKB {
		return 0, fmt.Errorf("invalid linux memory pressure total_kb=%d available_kb=%d", totalKB, availableKB)
	}
	return int((totalKB - availableKB) / 1024), nil
}

func linuxSystemMemoryMB(ctx context.Context, command func(context.Context, string, ...string) ([]byte, error)) (int, error) {
	pagesOut, err := command(ctx, "getconf", "_PHYS_PAGES")
	if err != nil {
		return 0, err
	}
	sizeOut, err := command(ctx, "getconf", "PAGE_SIZE")
	if err != nil {
		return 0, err
	}
	pages, err := strconv.ParseInt(strings.TrimSpace(string(pagesOut)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse _PHYS_PAGES: %w", err)
	}
	pageSize, err := strconv.ParseInt(strings.TrimSpace(string(sizeOut)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse PAGE_SIZE: %w", err)
	}
	if pages <= 0 || pageSize <= 0 {
		return 0, fmt.Errorf("invalid system memory pages=%d page_size=%d", pages, pageSize)
	}
	return int((pages * pageSize) / 1024 / 1024), nil
}

func parseIntelCLInfo(out []byte) ([]domain.Accelerator, error) {
	lines := strings.Split(string(out), "\n")
	accelerators := []domain.Accelerator{}
	var name string
	var memoryBytes int64
	appendCurrent := func() error {
		if name == "" {
			return nil
		}
		lower := strings.ToLower(name)
		if !strings.Contains(lower, "intel") || !strings.Contains(lower, "arc") {
			name = ""
			memoryBytes = 0
			return nil
		}
		if memoryBytes <= 0 {
			return fmt.Errorf("intel accelerator %q missing global memory size", name)
		}
		accelerators = append(accelerators, domain.Accelerator{
			Index:       len(accelerators),
			Vendor:      "intel",
			Kind:        intelKind(name),
			VRAMTotalMB: int(memoryBytes / 1024 / 1024),
			ArchFamily:  name,
		})
		name = ""
		memoryBytes = 0
		return nil
	}
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "NULL platform behavior" {
			break
		}
		switch {
		case strings.HasPrefix(line, "Device Name"):
			if err := appendCurrent(); err != nil {
				return nil, err
			}
			name = strings.TrimSpace(strings.TrimPrefix(line, "Device Name"))
		case name != "" && strings.HasPrefix(line, "Global memory size"):
			fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "Global memory size")))
			if len(fields) == 0 {
				return nil, fmt.Errorf("intel accelerator %q has empty global memory size", name)
			}
			parsed, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse intel global memory %q: %w", fields[0], err)
			}
			memoryBytes = parsed
		}
	}
	if err := appendCurrent(); err != nil {
		return nil, err
	}
	if len(accelerators) == 0 {
		return nil, errors.New("clinfo returned no Intel Arc GPUs")
	}
	return accelerators, nil
}

func intelKind(name string) string {
	normalized := strings.NewReplacer("(r)", "", "(tm)", "", "  ", " ").Replace(strings.ToLower(name))
	if strings.Contains(normalized, "arc pro b70") {
		return "arc-pro-b70"
	}
	return "arc"
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

func statDisk(path string) (DiskStats, error) {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		return DiskStats{}, err
	}
	blockSize := int64(stats.Bsize)
	if blockSize <= 0 {
		return DiskStats{}, fmt.Errorf("invalid filesystem block size %d", blockSize)
	}
	totalBytes := int64(stats.Blocks) * blockSize
	freeBytes := int64(stats.Bavail) * blockSize
	return DiskStats{
		TotalMB: int(totalBytes / 1024 / 1024),
		FreeMB:  int(freeBytes / 1024 / 1024),
	}, nil
}

var _ ports.HardwareDetector = Detector{}
