package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func RunBackendAdapterConformance(t *testing.T, name string, newAdapter func() ports.BackendAdapter, p domain.Preset) {
	t.Run(name+"/launch_wait_stop_happy_path", func(t *testing.T) {
		adapter := newAdapter()
		handle, err := adapter.Launch(context.Background(), p, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Launch: %v", err)
		}
		if err := adapter.WaitReady(context.Background(), handle.Addr); err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
		if err := adapter.Stop(context.Background(), handle); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run(name+"/stop_is_idempotent", func(t *testing.T) {
		adapter := newAdapter()
		handle, err := adapter.Launch(context.Background(), p, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Launch: %v", err)
		}
		_ = adapter.WaitReady(context.Background(), handle.Addr)
		_ = adapter.Stop(context.Background(), handle)
		if err := adapter.Stop(context.Background(), handle); err != nil {
			t.Fatalf("second Stop should be a no-op, got %v", err)
		}
	})

	t.Run(name+"/waitready_respects_context_cancel", func(t *testing.T) {
		adapter := newAdapter()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := adapter.WaitReady(ctx, "127.0.0.1:1"); err == nil {
			t.Fatal("WaitReady should error on cancelled context")
		}
	})
}
