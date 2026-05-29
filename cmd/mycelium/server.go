package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/lease"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
)

func runServer(ctx context.Context, args []string) error {
	addr, handler, err := buildGatewayServer(ctx, args)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: addr, Handler: handler}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func buildGatewayServer(ctx context.Context, args []string) (string, http.Handler, error) {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:51846", "gateway listen address")
	nodeAddr := fs.String("node", "", "node agent base URL, for example http://192.0.2.63:51847")
	model := fs.String("model", "", "model name exposed by the gateway")
	contextLen := fs.Int("context", 2048, "preset context length")
	weightsMB := fs.Int("weights-mb", 1, "estimated model weights in MB")
	kvPerToken := fs.Float64("kv-per-token-mb", 0.01, "estimated KV cache MB per token")
	if err := fs.Parse(args); err != nil {
		return "", nil, err
	}
	if *nodeAddr == "" {
		return "", nil, fmt.Errorf("--node is required")
	}
	if *model == "" {
		return "", nil, fmt.Errorf("--model is required")
	}

	client := nodeagent.NewHTTPClient(*nodeAddr)
	snap, err := client.Snapshot(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("snapshot node %s: %w", *nodeAddr, err)
	}
	preset := domain.Preset{
		ID:            *model,
		ModelRef:      *model,
		Backend:       domain.BackendLlamaCpp,
		ContextLength: *contextLen,
		Capabilities:  []domain.Capability{domain.CapabilityChat},
		EstWeightsMB:  *weightsMB,
		KVPerTokenMB:  *kvPerToken,
		NodeID:        snap.Node.ID,
	}
	directory := gateway.NodeDirectory{Agents: map[string]ports.NodeAgent{snap.Node.ID: client}}
	placer := scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clock.System{}, preset)
	return *listen, gateway.Server{Router: &gateway.Router{
		Placer:  placer,
		Fleet:   directory,
		Nodes:   directory,
		Presets: gateway.NewPresetRegistry(preset),
	}}, nil
}
