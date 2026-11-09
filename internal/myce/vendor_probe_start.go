package myce

import (
	"context"
	"errors"
	"sync"
)

var errVendorProbeDeferred = errors.New("vendor probe deferred by accelerator custody")

// Custody acquisition and vendor process creation share this lock. Factory
// lookup, command preparation and query completion never hold it: a launch
// waits only for a cmd.Start already in progress, not for the query to finish.
type vendorProbeStart struct {
	mu   sync.Mutex
	busy bool
}

func (g *vendorProbeStart) setBusy(busy bool) {
	g.mu.Lock()
	g.busy = busy
	g.mu.Unlock()
}

func (g *vendorProbeStart) blocked() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.busy
}

type vendorProbeStartKey struct{}

func withVendorProbeStart(ctx context.Context, gate *vendorProbeStart) context.Context {
	return context.WithValue(ctx, vendorProbeStartKey{}, gate)
}

func startProbeCommand(ctx context.Context, start func() error) error {
	if gate, _ := ctx.Value(vendorProbeStartKey{}).(*vendorProbeStart); gate != nil {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		if gate.busy {
			return errVendorProbeDeferred
		}
	}
	return start()
}
