package node

import (
	"context"
	"fmt"
	"net"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func (a *Agent) launchAndWait(ctx context.Context, p domain.Preset, inst domain.ModelInstance) (domain.ModelInstance, ports.Handle, error) {
	loadCtx, cancel := a.withLoadTimeout(ctx)
	defer cancel()

	addr, err := resolveListenAddr(a.listenAddr)
	if err != nil {
		return domain.ModelInstance{}, ports.Handle{}, err
	}
	handle, err := a.backend.Launch(loadCtx, p, addr)
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

func resolveListenAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port != "0" {
		return addr, nil
	}
	if host == "" {
		host = "127.0.0.1"
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return "", fmt.Errorf("allocate backend listen address: %w", err)
	}
	defer listener.Close()
	return listener.Addr().String(), nil
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
