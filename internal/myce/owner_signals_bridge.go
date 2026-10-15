package myce

import (
	"context"
	"os"

	"mycelium/internal/hostbridge"
)

// The Windows installer creates a private config. A WSL worker explicitly opts
// in using its readable /mnt/... path; credentials are never argv or output.
func configuredHostOwnerSignals(ctx context.Context) OwnerSignals {
	s := hostbridge.Unknown()
	path := os.Getenv("MYCE_HOST_BRIDGE_CONFIG")
	if path != "" {
		if c, err := hostbridge.ReadConfig(path); err == nil {
			if host, err := hostbridge.Fetch(ctx, c); err == nil {
				s = host
			}
		}
	}
	return OwnerSignals{IdleKnown: s.IdleKnown, IdleSeconds: s.IdleSeconds, Laptop: s.Laptop, Power: s.Power, Heat: s.Heat, GPUHeat: s.GPUHeat, HeatCoverage: s.HeatCoverage}
}
