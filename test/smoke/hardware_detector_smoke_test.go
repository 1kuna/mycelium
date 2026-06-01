//go:build smoke

package smoke

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/hardware"
)

func TestLinuxIntelArcB70HardwareDiscovery(t *testing.T) {
	if os.Getenv("MYCELIUM_EXPECT_INTEL_ARC_B70") == "" {
		t.Skip("set MYCELIUM_EXPECT_INTEL_ARC_B70=1 on an Intel Arc Pro B70 host")
	}
	if runtime.GOOS != "linux" {
		t.Skip("Intel Arc B70 discovery smoke must run on the Linux B70 host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	node, err := hardware.NewDetector().Detect(ctx, domain.Node{ID: "b70-smoke", MaxUtil: 0.85})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if node.Labels["gpu.vendor"] != "intel" || node.UnifiedMemory || len(node.Accelerators) == 0 {
		t.Fatalf("node = %+v", node)
	}
	for _, acc := range node.Accelerators {
		if acc.Vendor == "intel" && acc.Kind == "arc-pro-b70" && acc.VRAMTotalMB >= 30000 {
			return
		}
	}
	t.Fatalf("missing Arc Pro B70 accelerator in %+v", node.Accelerators)
}
