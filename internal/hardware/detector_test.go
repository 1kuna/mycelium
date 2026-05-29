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
	if node.Labels["gpu.vendor"] != "apple" || node.SpeedClass.Source != "detected-default" {
		t.Fatalf("labels/speed = %+v %+v", node.Labels, node.SpeedClass)
	}
	if !node.SpeedClass.ProbedAt.Equal(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("probed_at = %s", node.SpeedClass.ProbedAt)
	}
}

func TestLinuxDetectorFailsUntilNVIDIAProbeExists(t *testing.T) {
	_, err := (Detector{GOOS: "linux"}).Detect(context.Background(), domain.Node{})
	if err == nil || !strings.Contains(err.Error(), "explicit --vram-mb") {
		t.Fatalf("err = %v", err)
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
