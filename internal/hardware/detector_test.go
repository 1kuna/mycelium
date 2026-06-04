package hardware

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/mocks"
)

func TestDarwinDetectorBuildsUnifiedMemoryNode(t *testing.T) {
	detector := Detector{
		GOOS:     "darwin",
		Clock:    mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		StatDisk: fakeDiskStats,
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name == "vm_stat" {
				return []byte("Mach Virtual Memory Statistics:\nPages free: 1048576.\nPages inactive: 524288.\nPages speculative: 0.\n"), nil
			}
			if name != "sysctl" || len(args) != 2 || args[0] != "-n" {
				t.Fatalf("command = %s %+v", name, args)
			}
			switch args[1] {
			case "hw.memsize":
				return []byte("68719476736\n"), nil
			case "hw.optional.arm64":
				return []byte("1\n"), nil
			case "hw.pagesize":
				return []byte("4096\n"), nil
			default:
				t.Fatalf("unexpected sysctl %q", args[1])
				return nil, nil
			}
		},
	}
	node, err := detector.Detect(context.Background(), domain.Node{
		ID:      "node-a",
		Name:    "Node A",
		Address: "127.0.0.1:1",
		MaxUtil: 0.9,
	})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if node.OS != "darwin" || !node.UnifiedMemory || node.Accelerators[0].VRAMTotalMB != 65536 || node.Accelerators[0].VRAMUsedMB != 59392 {
		t.Fatalf("node = %+v", node)
	}
	if node.Labels["gpu.vendor"] != "apple" || node.SpeedClass.Source != "class-default" {
		t.Fatalf("labels/speed = %+v %+v", node.Labels, node.SpeedClass)
	}
	if node.DiskTotalMB != 1000 || node.DiskFreeMB != 700 || node.DiskMinFreeRatio != domain.DefaultDiskMinFreeRatio {
		t.Fatalf("disk = %+v", node)
	}
	if !node.SpeedClass.ProbedAt.Equal(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("probed_at = %s", node.SpeedClass.ProbedAt)
	}
}

func TestDarwinDetectorDoesNotInventAppleAcceleratorOnIntel(t *testing.T) {
	detector := Detector{
		GOOS:     "darwin",
		StatDisk: fakeDiskStats,
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			switch args[1] {
			case "hw.memsize":
				return []byte("17179869184\n"), nil
			case "hw.optional.arm64":
				return []byte("0\n"), nil
			default:
				t.Fatalf("unexpected sysctl %q", args[1])
				return nil, nil
			}
		},
	}
	node, err := detector.Detect(context.Background(), domain.Node{ID: "intel-mac"})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if node.OS != "darwin" || node.UnifiedMemory || len(node.Accelerators) != 0 {
		t.Fatalf("node = %+v", node)
	}
	if node.Labels["gpu.vendor"] == "apple" || node.Labels["memory.class"] != "system" {
		t.Fatalf("labels = %+v", node.Labels)
	}
}

func TestLinuxDetectorBuildsNVIDIANode(t *testing.T) {
	detector := Detector{
		GOOS:     "linux",
		Clock:    mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		StatDisk: fakeDiskStats,
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name != "nvidia-smi" || len(args) != 2 || !strings.Contains(args[0], "memory.total") || !strings.Contains(args[0], "memory.used") {
				t.Fatalf("command = %s %+v", name, args)
			}
			return []byte("0, NVIDIA GeForce RTX 4090, 24564, 4096, 8.9\n1, NVIDIA GeForce RTX 4070 Ti, 12282, 1024, 8.9\n"), nil
		},
	}
	node, err := detector.Detect(context.Background(), domain.Node{ID: "cuda-a", MaxUtil: 0.9})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if node.OS != "linux" || node.UnifiedMemory || len(node.Accelerators) != 2 {
		t.Fatalf("node = %+v", node)
	}
	if node.Accelerators[0].Vendor != "nvidia" || node.Accelerators[0].Kind != "cuda" || node.Accelerators[0].VRAMTotalMB != 24564 || node.Accelerators[0].VRAMUsedMB != 4096 || node.Accelerators[0].ComputeCapability != "8.9" {
		t.Fatalf("accelerator = %+v", node.Accelerators[0])
	}
	if node.Labels["gpu.vendor"] != "nvidia" || node.OOMSeverity != domain.OOMSoft || node.SpeedClass.Source != "class-default" {
		t.Fatalf("labels/speed = %+v %+v", node.Labels, node.SpeedClass)
	}
}

func TestLinuxDetectorBuildsSparkGB10Node(t *testing.T) {
	detector := Detector{
		GOOS:     "linux",
		Clock:    mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)),
		StatDisk: fakeDiskStats,
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			switch name {
			case "nvidia-smi":
				return []byte("0, NVIDIA GB10, [N/A], [N/A], 12.1\n"), nil
			case "getconf":
				if len(args) != 1 {
					t.Fatalf("getconf args = %+v", args)
				}
				switch args[0] {
				case "_PHYS_PAGES":
					return []byte("31900187\n"), nil
				case "PAGE_SIZE":
					return []byte("4096\n"), nil
				default:
					t.Fatalf("unexpected getconf arg %q", args[0])
				}
			default:
				t.Fatalf("unexpected command %s", name)
			}
			return nil, nil
		},
		ReadFile: func(path string) ([]byte, error) {
			if path != "/proc/meminfo" {
				t.Fatalf("read path = %s", path)
			}
			return []byte("MemTotal:       127631000 kB\nMemAvailable:   100000000 kB\n"), nil
		},
	}
	node, err := detector.Detect(context.Background(), domain.Node{ID: "spark", MaxUtil: 0.55})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if node.OS != "linux" || !node.UnifiedMemory || node.OOMSeverity != domain.OOMCatastrophic {
		t.Fatalf("node = %+v", node)
	}
	if node.Labels["gpu.vendor"] != "nvidia" || node.Labels["gpu.kind"] != "gb10" || node.Labels["memory.class"] != "unified" {
		t.Fatalf("labels = %+v", node.Labels)
	}
	acc := node.Accelerators[0]
	if acc.Vendor != "nvidia" || acc.Kind != "gb10" || !acc.UnifiedMemory || acc.VRAMTotalMB != 124610 || acc.VRAMUsedMB != 26983 || acc.ComputeCapability != "12.1" {
		t.Fatalf("accelerator = %+v", acc)
	}
}

func TestLinuxDetectorBuildsIntelArcB70Node(t *testing.T) {
	detector := Detector{
		GOOS:     "linux",
		Clock:    mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)),
		StatDisk: fakeDiskStats,
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			switch name {
			case "nvidia-smi":
				return nil, errors.New("no nvidia")
			case "clinfo":
				if len(args) != 0 {
					t.Fatalf("clinfo args = %+v", args)
				}
				return []byte(`Number of platforms                               1
  Platform Name                                   Intel(R) OpenCL Graphics
Number of devices                                 1
  Device Name                                     Intel(R) Arc(TM) Pro B70 Graphics
  Device Vendor                                   Intel(R) Corporation
  Global memory size                              32530182144 (30.3GiB)
NULL platform behavior
    Device Name                                   Intel(R) Arc(TM) Pro B70 Graphics
`), nil
			default:
				t.Fatalf("unexpected command %s", name)
			}
			return nil, nil
		},
	}
	node, err := detector.Detect(context.Background(), domain.Node{ID: "b70-a", MaxUtil: 0.85})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if node.OS != "linux" || node.UnifiedMemory || len(node.Accelerators) != 1 {
		t.Fatalf("node = %+v", node)
	}
	acc := node.Accelerators[0]
	if acc.Vendor != "intel" || acc.Kind != "arc-pro-b70" || acc.VRAMTotalMB != 31023 || !strings.Contains(acc.ArchFamily, "B70") {
		t.Fatalf("accelerator = %+v", acc)
	}
	if node.Labels["gpu.vendor"] != "intel" || node.OOMSeverity != domain.OOMSoft || node.SpeedClass.Source != "class-default" {
		t.Fatalf("labels/speed = %+v %+v", node.Labels, node.SpeedClass)
	}
}

func TestDetectorErrorPathsAndLabelMerge(t *testing.T) {
	if _, err := (Detector{GOOS: "plan9"}).Detect(context.Background(), domain.Node{}); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported err = %v", err)
	}
	_, err := (Detector{
		GOOS: "darwin",
		Command: func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("sysctl")
		},
	}).Detect(context.Background(), domain.Node{})
	if err == nil || !strings.Contains(err.Error(), "sysctl") {
		t.Fatalf("command err = %v", err)
	}
	_, err = (Detector{
		GOOS: "darwin",
		Command: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("not-a-number"), nil
		},
	}).Detect(context.Background(), domain.Node{})
	if err == nil {
		t.Fatal("invalid sysctl output accepted")
	}
	got := mergeLabels(map[string]string{"keep": "yes", "gpu.vendor": "old"}, map[string]string{"gpu.vendor": "apple"})
	if got["keep"] != "yes" || got["gpu.vendor"] != "apple" {
		t.Fatalf("labels = %+v", got)
	}
}

func TestLinuxDetectorErrorPaths(t *testing.T) {
	_, err := (Detector{
		GOOS: "linux",
		Command: func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("nvidia-smi")
		},
	}).Detect(context.Background(), domain.Node{})
	if err == nil || !strings.Contains(err.Error(), "nvidia-smi") || !strings.Contains(err.Error(), "clinfo") {
		t.Fatalf("command err = %v", err)
	}
	for _, raw := range [][]byte{
		[]byte(""),
		[]byte("bad,row"),
		[]byte("x, NVIDIA, 1, 1, 8.9"),
		[]byte("0, NVIDIA, nope, 1, 8.9"),
		[]byte("0, NVIDIA, 1, nope, 8.9"),
	} {
		if _, err := parseNVIDIASMI(raw, linuxUnifiedMemoryFallback{}); err == nil {
			t.Fatalf("parse accepted %q", raw)
		}
	}
	if _, err := parseNVIDIASMI([]byte("0, NVIDIA GB10, [N/A], [N/A], 12.1"), linuxUnifiedMemoryFallback{}); err == nil {
		t.Fatal("GB10 without memory fallback accepted")
	}
	if _, err := linuxSystemMemoryMB(context.Background(), func(_ context.Context, name string, args ...string) ([]byte, error) {
		return []byte("bad"), nil
	}); err == nil {
		t.Fatal("invalid system memory accepted")
	}
	if _, err := parseLinuxMemoryUsedMB([]byte("MemTotal: bad kB\n")); err == nil {
		t.Fatal("invalid meminfo accepted")
	}
	if _, err := parseLinuxMemoryUsedMB([]byte("MemTotal: 10 kB\nMemAvailable: 11 kB\n")); err == nil {
		t.Fatal("invalid memory pressure accepted")
	}
	if !nvidiaSMINeedsSystemMemory([]byte("0, NVIDIA GB10, [N/A], [N/A], 12.1\n")) || nvidiaSMINeedsSystemMemory([]byte("0, NVIDIA GeForce RTX 4090, 24564, 4096, 8.9\n")) {
		t.Fatal("GB10 memory fallback detection mismatch")
	}
	for _, raw := range [][]byte{
		[]byte(""),
		[]byte("  Device Name Intel(R) Arc(TM) Pro B70 Graphics\n"),
		[]byte("  Device Name Intel(R) Arc(TM) Pro B70 Graphics\n  Global memory size nope\n"),
	} {
		if _, err := parseIntelCLInfo(raw); err == nil {
			t.Fatalf("parse accepted %q", raw)
		}
	}
	if kind := intelKind("Intel(R) Arc(TM) Graphics"); kind != "arc" {
		t.Fatalf("generic arc kind = %q", kind)
	}
}

func TestLinuxDetectorGB10SystemMemoryFallbackFailure(t *testing.T) {
	getconfErr := errors.New("getconf failed")
	_, err := (Detector{
		GOOS: "linux",
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			switch name {
			case "nvidia-smi":
				return []byte("0, NVIDIA GB10, [N/A], [N/A], 12.1\n"), nil
			case "getconf":
				return nil, getconfErr
			default:
				t.Fatalf("unexpected command %s %+v", name, args)
				return nil, nil
			}
		},
	}).Detect(context.Background(), domain.Node{})
	if !errors.Is(err, getconfErr) || !strings.Contains(err.Error(), "parse nvidia memory") || !strings.Contains(err.Error(), "system memory fallback") {
		t.Fatalf("fallback err = %v", err)
	}
}

func TestDetectorDiskErrorPaths(t *testing.T) {
	_, err := (Detector{
		GOOS: "darwin",
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name == "vm_stat" {
				return []byte("Pages free: 0.\n"), nil
			}
			if name != "sysctl" {
				t.Fatalf("unexpected command %s", name)
			}
			switch args[1] {
			case "hw.memsize":
				return []byte("1024\n"), nil
			case "hw.optional.arm64":
				return []byte("1\n"), nil
			case "hw.pagesize":
				return []byte("4096\n"), nil
			default:
				t.Fatalf("unexpected sysctl %q", args[1])
				return nil, nil
			}
		},
		StatDisk: func(string) (DiskStats, error) {
			return DiskStats{}, errors.New("statfs")
		},
	}).Detect(context.Background(), domain.Node{})
	if err == nil || !strings.Contains(err.Error(), "statfs") {
		t.Fatalf("disk err = %v", err)
	}

	_, err = (Detector{StatDisk: func(string) (DiskStats, error) {
		return DiskStats{}, nil
	}}).AddDiskStats(domain.Node{})
	if err == nil || !strings.Contains(err.Error(), "invalid disk capacity") {
		t.Fatalf("invalid disk err = %v", err)
	}

	seed := domain.Node{DiskTotalMB: 100, DiskFreeMB: 30}
	node, err := (Detector{StatDisk: func(string) (DiskStats, error) {
		return DiskStats{}, errors.New("should not stat")
	}}).AddDiskStats(seed)
	if err != nil || node.DiskTotalMB != 100 || node.DiskMinFreeRatio != domain.DefaultDiskMinFreeRatio {
		t.Fatalf("preserved disk = %+v err=%v", node, err)
	}
}

func TestNewDetectorAndRunCommand(t *testing.T) {
	detector := NewDetector()
	if detector.GOOS != runtime.GOOS || detector.Command == nil {
		t.Fatalf("detector = %+v", detector)
	}
	out, err := runCommand(context.Background(), "printf", "ok")
	if err != nil || string(out) != "ok" {
		t.Fatalf("runCommand = %q %v", out, err)
	}
}

func TestDetectorSatisfiesPort(t *testing.T) {
	var _ ports.HardwareDetector = Detector{}
}

func fakeDiskStats(string) (DiskStats, error) {
	return DiskStats{TotalMB: 1000, FreeMB: 700}, nil
}
