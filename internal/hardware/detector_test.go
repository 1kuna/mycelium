package hardware

import (
	"context"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func TestDarwinDetectorBuildsUnifiedMemoryNode(t *testing.T) {
	detector := Detector{
		GOOS: "darwin",
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
}

func TestLinuxDetectorFailsUntilNVIDIAProbeExists(t *testing.T) {
	_, err := (Detector{GOOS: "linux"}).Detect(context.Background(), domain.Node{})
	if err == nil || !strings.Contains(err.Error(), "explicit --vram-mb") {
		t.Fatalf("err = %v", err)
	}
}

func TestDetectorSatisfiesPort(t *testing.T) {
	var _ ports.HardwareDetector = Detector{}
}
