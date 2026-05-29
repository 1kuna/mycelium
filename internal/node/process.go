package node

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func (a *Agent) launchAndWait(ctx context.Context, p domain.Preset, inst domain.ModelInstance) (domain.ModelInstance, ports.Handle, error) {
	loadCtx, cancel := a.withLoadTimeout(ctx)
	defer cancel()

	handle, err := a.backend.Launch(loadCtx, p, a.listenAddr)
	if err != nil {
		return domain.ModelInstance{}, ports.Handle{}, err
	}
	if err := a.backend.WaitReady(loadCtx, handle.Addr); err != nil {
		_ = a.backend.Stop(context.Background(), handle)
		return domain.ModelInstance{}, ports.Handle{}, err
	}
	inst.State = domain.InstReady
	inst.Loading = false
	inst.Addr = handle.Addr
	return inst, handle, nil
}

func (a *Agent) withLoadTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if a.loadTimeout <= 0 {
		return context.WithCancel(parent)
	}
	ctx, cancel := context.WithCancel(parent)
	timer := a.clock.NewTimer(a.loadTimeout)
	go func() {
		defer timer.Stop()
		select {
		case <-parent.Done():
			cancel()
		case <-timer.C():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
