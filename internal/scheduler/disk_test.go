package scheduler

import (
	"testing"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
)

func TestNodeDiskDropReason(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(20), fixtures.WithArtifactSize(30))
	cases := []struct {
		name   string
		node   domain.Node
		preset domain.Preset
		fleet  domain.FleetSnapshot
		reason string
		drop   bool
	}{
		{
			name:   "unknown",
			node:   fixtures.MakeNode(fixtures.WithDisk(0, 0)),
			preset: preset,
			reason: "disk.unknown",
			drop:   true,
		},
		{
			name:   "bad-limit",
			node:   fixtures.MakeNode(fixtures.WithDisk(1000, 900), fixtures.WithDiskMinFreeRatio(1)),
			preset: preset,
			reason: "disk.limit",
			drop:   true,
		},
		{
			name:   "already-at-floor",
			node:   fixtures.MakeNode(fixtures.WithDisk(1000, 250)),
			preset: preset,
			reason: "disk.free",
			drop:   true,
		},
		{
			name:   "negative-required",
			node:   fixtures.MakeNode(fixtures.WithDisk(1000, 900)),
			preset: fixtures.MakePreset(fixtures.WithWeights(-1), fixtures.WithArtifactSize(0)),
			reason: "disk.required",
			drop:   true,
		},
		{
			name:   "would-cross-floor",
			node:   fixtures.MakeNode(fixtures.WithDisk(1000, 270)),
			preset: preset,
			reason: "disk.free_after_model",
			drop:   true,
		},
		{
			name:   "fits",
			node:   fixtures.MakeNode(fixtures.WithDisk(1000, 300)),
			preset: preset,
		},
		{
			name:   "default-floor",
			node:   fixtures.MakeNode(fixtures.WithDisk(1000, 300), fixtures.WithDiskMinFreeRatio(0)),
			preset: preset,
		},
		{
			name:   "preset-local",
			node:   fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithDisk(1000, 260)),
			preset: fixtures.MakePreset(fixtures.WithPresetNode("node-a"), fixtures.WithArtifactSize(100)),
		},
		{
			name:   "instance-present",
			node:   fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithDisk(1000, 260)),
			preset: fixtures.MakePreset(fixtures.WithPresetID("preset-local"), fixtures.WithArtifactSize(100)),
			fleet: domain.FleetSnapshot{Instances: []domain.ModelInstance{
				fixtures.MakeInstance(fixtures.WithInstancePreset("preset-local"), fixtures.OnNode("node-a")),
			}},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			reason, drop := nodeDiskDropReason(tt.preset, tt.node, tt.fleet)
			if reason != tt.reason || drop != tt.drop {
				t.Fatalf("nodeDiskDropReason = %q %v, want %q %v", reason, drop, tt.reason, tt.drop)
			}
		})
	}
}
