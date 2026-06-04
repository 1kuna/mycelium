package node

import (
	"context"
	"errors"
	"fmt"
	"net"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func (a *Agent) launchAndWait(ctx context.Context, p domain.Preset, inst domain.ModelInstance) (domain.ModelInstance, ports.Handle, error) {
	loadCtx, cancel := a.withLoadTimeout(ctx)
	defer cancel()

	handle, err := a.launchBackend(loadCtx, p)
	if err != nil {
		return domain.ModelInstance{}, ports.Handle{}, err
	}
	if err := a.backend.WaitReady(loadCtx, handle.Addr); err != nil {
		if stopErr := a.backend.Stop(context.Background(), handle); stopErr != nil {
			return domain.ModelInstance{}, ports.Handle{}, errors.Join(err, stopErr)
		}
		return domain.ModelInstance{}, ports.Handle{}, err
	}
	inst.State = domain.InstReady
	inst.Loading = false
	inst.Addr = handle.Addr
	return inst, handle, nil
}

func (a *Agent) launchBackend(ctx context.Context, p domain.Preset) (ports.Handle, error) {
	addr := a.listenAddr
	zeroPort, err := listenAddrUsesZeroPort(addr)
	if err != nil {
		return ports.Handle{}, err
	}
	if !zeroPort {
		return a.backend.Launch(ctx, p, addr)
	}
	dynamic, ok := a.backend.(ports.DynamicBackendAdapter)
	if !ok {
		return ports.Handle{}, fmt.Errorf("backend listen address %q requires a backend that reports dynamic ports", addr)
	}
	launchAddr, err := normalizeDynamicListenAddr(addr)
	if err != nil {
		return ports.Handle{}, err
	}
	handle, err := dynamic.LaunchDynamic(ctx, p, launchAddr)
	if err != nil {
		return ports.Handle{}, err
	}
	if !concreteBackendAddr(handle.Addr) {
		_ = a.backend.Stop(context.Background(), handle)
		return ports.Handle{}, fmt.Errorf("dynamic backend returned non-concrete address %q", handle.Addr)
	}
	return handle, nil
}

func listenAddrUsesZeroPort(addr string) (bool, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false, nil
	}
	_ = host
	return port == "0", nil
}

func normalizeDynamicListenAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), nil
}

func concreteBackendAddr(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	return err == nil && host != "" && port != "" && port != "0"
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
