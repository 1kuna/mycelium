//go:build darwin && cgo

package myce

/*
#cgo LDFLAGS: -framework CoreGraphics -framework IOKit -framework Foundation
void myce_owner_signals(double *idle, int *power, int *laptop, int *heat);
*/
import "C"
import "context"
import "mycelium/internal/probeguard"

const NativeOwnerSignals = true

func LocalOwnerSignals(context.Context) OwnerSignals {
	probeguard.Check()
	var idle C.double
	var power, laptop, heat C.int
	C.myce_owner_signals(&idle, &power, &laptop, &heat)
	s := OwnerSignals{IdleSeconds: float64(idle), IdleKnown: idle >= 0, Laptop: laptop != 0, Power: "unknown", Heat: "unknown"}
	switch power {
	case 0:
		s.Power = "desktop"
	case 1:
		s.Power = "ac"
	case 2:
		s.Power = "battery"
	}
	switch heat {
	case 0:
		s.Heat = "nominal"
	case 1:
		s.Heat = "fair"
	case 2:
		s.Heat = "serious"
	case 3:
		s.Heat = "critical"
	}
	return s
}
