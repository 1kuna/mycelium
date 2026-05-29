package node

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func (a *Agent) launchAndWait(ctx context.Context, p domain.Preset, inst domain.ModelInstance) (domain.ModelInstance, ports.Handle, error) {
	handle, err := a.backend.Launch(ctx, p, a.listenAddr)
	if err != nil {
		return domain.ModelInstance{}, ports.Handle{}, err
	}
	if err := a.backend.WaitReady(ctx, handle.Addr); err != nil {
		_ = a.backend.Stop(context.Background(), handle)
		return domain.ModelInstance{}, ports.Handle{}, err
	}
	inst.State = domain.InstReady
	inst.Loading = false
	inst.Addr = handle.Addr
	return inst, handle, nil
}
