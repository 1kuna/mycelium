package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunHardwareDetectorConformance(t *testing.T, name string, newDetector func() ports.HardwareDetector, seed domain.Node) {
	t.Run(name+"/detect_preserves_identity_and_reports_capacity", func(t *testing.T) {
		detector := newDetector()
		node, err := detector.Detect(context.Background(), seed)
		assert.NoError(t, "Detect", err)
		assert.Equal(t, seed.ID, node.ID, "node id")
		assert.True(t, node.UnifiedMemory || len(node.Accelerators) > 0, "detected node lacks memory capacity: %+v", node)
		assert.True(t, node.DiskTotalMB > 0 && node.DiskFreeMB >= 0, "detected node lacks disk capacity: %+v", node)
		assert.Equal(t, domain.DefaultDiskMinFreeRatio, node.DiskMinFreeRatio, "disk floor")
	})
}
