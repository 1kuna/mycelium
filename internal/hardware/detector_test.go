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
		GOOS:  "darwin",
		Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		Command: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("68719476736\n"), nil
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
	if node.OS != "darwin" || !node.UnifiedMemory || node.Accelerators[0].VRAMTotalMB != 65536 {
		t.Fatalf("node = %+v", node)
	}
	if node.Labels["gpu.vendor"] != "apple" || node.SpeedClass.Source != "class-default" {
		t.Fatalf("labels/speed = %+v %+v", node.Labels, node.SpeedClass)
	}
	if !node.SpeedClass.ProbedAt.Equal(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("probed_at = %s", node.SpeedClass.ProbedAt)
	}
}

func TestLinuxDetectorBuildsNVIDIANode(t *testing.T) {
	detector := Detector{
		GOOS:  "linux",
		Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name != "nvidia-smi" || len(args) != 2 || !strings.Contains(args[0], "memory.total") {
				t.Fatalf("command = %s %+v", name, args)
			}
			return []byte("0, NVIDIA GeForce RTX 4090, 24564, 8.9\n1, NVIDIA GeForce RTX 4070 Ti, 12282, 8.9\n"), nil
		},
	}
	node, err := detector.Detect(context.Background(), domain.Node{ID: "cuda-a", MaxUtil: 0.9})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if node.OS != "linux" || node.UnifiedMemory || len(node.Accelerators) != 2 {
		t.Fatalf("node = %+v", node)
	}
	if node.Accelerators[0].Vendor != "nvidia" || node.Accelerators[0].Kind != "cuda" || node.Accelerators[0].VRAMTotalMB != 24564 || node.Accelerators[0].ComputeCapability != "8.9" {
		t.Fatalf("accelerator = %+v", node.Accelerators[0])
	}
	if node.Labels["gpu.vendor"] != "nvidia" || node.OOMSeverity != domain.OOMSoft || node.SpeedClass.Source != "class-default" {
		t.Fatalf("labels/speed = %+v %+v", node.Labels, node.SpeedClass)
	}
}

func TestLinuxDetectorBuildsSparkGB10Node(t *testing.T) {
	detector := Detector{
		GOOS:  "linux",
		Clock: mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)),
		Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			switch name {
			case "nvidia-smi":
				return []byte("0, NVIDIA GB10, [N/A], 12.1\n"), nil
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
	if acc.Vendor != "nvidia" || acc.Kind != "gb10" || !acc.UnifiedMemory || acc.VRAMTotalMB != 124610 || acc.ComputeCapability != "12.1" {
		t.Fatalf("accelerator = %+v", acc)
	}
}

func TestLinuxDetectorBuildsIntelArcB70Node(t *testing.T) {
	detector := Detector{
		GOOS:  "linux",
		Clock: mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)),
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
		[]byte("x, NVIDIA, 1, 8.9"),
		[]byte("0, NVIDIA, nope, 8.9"),
	} {
		if _, err := parseNVIDIASMI(raw, 0); err == nil {
			t.Fatalf("parse accepted %q", raw)
		}
	}
	if _, err := parseNVIDIASMI([]byte("0, NVIDIA GB10, [N/A], 12.1"), 0); err == nil {
		t.Fatal("GB10 without memory fallback accepted")
	}
	if _, err := linuxSystemMemoryMB(context.Background(), func(_ context.Context, name string, args ...string) ([]byte, error) {
		return []byte("bad"), nil
	}); err == nil {
		t.Fatal("invalid system memory accepted")
	}
	if !nvidiaSMINeedsSystemMemory([]byte("0, NVIDIA GB10, [N/A], 12.1\n")) || nvidiaSMINeedsSystemMemory([]byte("0, NVIDIA GeForce RTX 4090, 24564, 8.9\n")) {
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
				return []byte("0, NVIDIA GB10, [N/A], 12.1\n"), nil
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
