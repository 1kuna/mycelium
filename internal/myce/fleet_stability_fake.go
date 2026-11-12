//go:build myce_test

package myce

import "time"

// Synthetic end-to-end release builds keep the stability check, shortened so
// a real-process rollout of three services fits the quick gate.
const fleetStableFor = 2 * time.Second
