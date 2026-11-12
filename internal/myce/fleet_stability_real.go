//go:build !myce_test

package myce

import "time"

// The enrollment stability window: it catches a service that crash-loops
// shortly after start. Its cost is 30 seconds per updated service.
const fleetStableFor = 30 * time.Second
