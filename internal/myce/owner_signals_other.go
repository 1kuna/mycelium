//go:build !linux && (!darwin || !cgo)

package myce

import "context"

// No guessed readings on unsupported targets or macOS builds without cgo.
func LocalOwnerSignals(context.Context) OwnerSignals {
	return OwnerSignals{Power: "unknown", Heat: "unknown"}
}
