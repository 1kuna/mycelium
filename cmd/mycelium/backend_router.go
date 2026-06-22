package main

import (
	"context"
	"fmt"
	"sync"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type backendRouter struct {
	mu             sync.Mutex
	byKind         map[domain.Backend]ports.BackendAdapter
	byAddr         map[string]domain.Backend
	byPID          map[int]domain.Backend
	defaultBackend domain.Backend
}

func newBackendRouter(adapters map[domain.Backend]ports.BackendAdapter) ports.BackendAdapter {
	router := &backendRouter{
		byKind: adapters,
		byAddr: map[string]domain.Backend{},
		byPID:  map[int]domain.Backend{},
	}
	if len(adapters) == 1 {
		for backend := range adapters {
			router.defaultBackend = backend
		}
	}
	return router
}

func (r *backendRouter) Name() string {
	return "multi"
}

func (r *backendRouter) Launch(ctx context.Context, preset domain.Preset, addr string) (ports.Handle, error) {
	backend, adapter, err := r.adapterForPreset(preset)
	if err != nil {
		return ports.Handle{}, err
	}
	handle, err := adapter.Launch(ctx, preset, addr)
	if err != nil {
		return ports.Handle{}, err
	}
	r.remember(handle, backend)
	return handle, nil
}

func (r *backendRouter) LaunchDynamic(ctx context.Context, preset domain.Preset, addr string) (ports.Handle, error) {
	backend, adapter, err := r.adapterForPreset(preset)
	if err != nil {
		return ports.Handle{}, err
	}
	dynamic, ok := adapter.(ports.DynamicBackendAdapter)
	if !ok {
		return ports.Handle{}, fmt.Errorf("backend %q does not support dynamic listen addresses", backend)
	}
	handle, err := dynamic.LaunchDynamic(ctx, preset, addr)
	if err != nil {
		return ports.Handle{}, err
	}
	r.remember(handle, backend)
	return handle, nil
}

func (r *backendRouter) WaitReady(ctx context.Context, addr string) error {
	adapter, err := r.adapterForAddr(addr)
	if err != nil {
		return err
	}
	return adapter.WaitReady(ctx, addr)
}

func (r *backendRouter) Stop(ctx context.Context, handle ports.Handle) error {
	adapter, err := r.adapterForHandle(handle)
	if err != nil {
		return err
	}
	err = adapter.Stop(ctx, handle)
	if err == nil {
		r.forget(handle)
	}
	return err
}

func (r *backendRouter) adapterForPreset(preset domain.Preset) (domain.Backend, ports.BackendAdapter, error) {
	backend := preset.Backend
	if backend == "" {
		backend = r.defaultBackend
	}
	if backend == "" {
		return "", nil, fmt.Errorf("preset %q requires a backend when compute has multiple runtimes", preset.ID)
	}
	adapter, ok := r.byKind[backend]
	if !ok {
		return "", nil, fmt.Errorf("compute backend %q is not configured for preset %q", backend, preset.ID)
	}
	return backend, adapter, nil
}

func (r *backendRouter) adapterForAddr(addr string) (ports.BackendAdapter, error) {
	r.mu.Lock()
	backend := r.byAddr[addr]
	r.mu.Unlock()
	if backend == "" {
		return nil, fmt.Errorf("no backend is tracking ready address %q", addr)
	}
	adapter, ok := r.byKind[backend]
	if !ok {
		return nil, fmt.Errorf("backend %q is no longer configured", backend)
	}
	return adapter, nil
}

func (r *backendRouter) adapterForHandle(handle ports.Handle) (ports.BackendAdapter, error) {
	backend := domain.Backend(handle.Kind)
	if backend == "" && handle.PID != 0 {
		r.mu.Lock()
		backend = r.byPID[handle.PID]
		r.mu.Unlock()
	}
	if backend == "" && handle.Addr != "" {
		r.mu.Lock()
		backend = r.byAddr[handle.Addr]
		r.mu.Unlock()
	}
	if backend == "" {
		return nil, fmt.Errorf("backend handle has no runtime identity")
	}
	adapter, ok := r.byKind[backend]
	if !ok {
		return nil, fmt.Errorf("backend %q is not configured for handle", backend)
	}
	return adapter, nil
}

func (r *backendRouter) remember(handle ports.Handle, backend domain.Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if handle.Addr != "" {
		r.byAddr[handle.Addr] = backend
	}
	if handle.PID != 0 {
		r.byPID[handle.PID] = backend
	}
}

func (r *backendRouter) forget(handle ports.Handle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if handle.Addr != "" {
		delete(r.byAddr, handle.Addr)
	}
	if handle.PID != 0 {
		delete(r.byPID, handle.PID)
	}
}

var _ ports.BackendAdapter = (*backendRouter)(nil)
var _ ports.DynamicBackendAdapter = (*backendRouter)(nil)
