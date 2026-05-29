package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunBackendAdapterConformance(t *testing.T, name string, newAdapter func() ports.BackendAdapter, p domain.Preset) {
	RunBackendAdapterConformanceAt(t, name, newAdapter, p, "127.0.0.1:0")
}

func RunBackendAdapterConformanceAt(t *testing.T, name string, newAdapter func() ports.BackendAdapter, p domain.Preset, addr string) {
	t.Run(name+"/launch_wait_stop_happy_path", func(t *testing.T) {
		adapter := newAdapter()
		handle, err := adapter.Launch(context.Background(), p, addr)
		assert.NoError(t, "Launch", err)
		assert.NoError(t, "WaitReady", adapter.WaitReady(context.Background(), handle.Addr))
		assert.NoError(t, "Stop", adapter.Stop(context.Background(), handle))
	})

	t.Run(name+"/stop_is_idempotent", func(t *testing.T) {
		adapter := newAdapter()
		handle, err := adapter.Launch(context.Background(), p, addr)
		assert.NoError(t, "Launch", err)
		_ = adapter.WaitReady(context.Background(), handle.Addr)
		_ = adapter.Stop(context.Background(), handle)
		assert.NoError(t, "second Stop should be a no-op", adapter.Stop(context.Background(), handle))
	})

	t.Run(name+"/waitready_respects_context_cancel", func(t *testing.T) {
		adapter := newAdapter()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.Error(t, "WaitReady should error on cancelled context", adapter.WaitReady(ctx, "127.0.0.1:1"))
	})
}
