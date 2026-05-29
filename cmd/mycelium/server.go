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
	"mycelium/internal/membership"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
)

func runServer(ctx context.Context, args []string) error {
	if ctx.Err() != nil {
		return nil
	}
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
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return nil
}

func buildGatewayServer(ctx context.Context, args []string) (string, http.Handler, error) {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	configPath := fs.String("config", "", "server config JSON path")
	listen := fs.String("listen", "", "gateway listen address override")
	if err := fs.Parse(args); err != nil {
		return "", nil, err
	}
	cfg, err := loadServerConfig(*configPath)
	if err != nil {
		return "", nil, err
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		return "", nil, err
	}
	if err := seedControlStore(ctx, store, cfg); err != nil {
		_ = store.Close()
		return "", nil, err
	}

	var fleet gateway.FleetSource
	var nodes gateway.NodeResolver
	var mux *http.ServeMux
	if cfg.JoinToken != "" {
		tokens, err := membership.NewTokenManager(cfg.JoinToken)
		if err != nil {
			return "", nil, err
		}
		registry := membership.NewRegistry(tokens, membership.NewLANTunnel())
		fleet = registry
		nodes = registry
		mux = http.NewServeMux()
		mux.Handle("/join", registry)
		mux.Handle("/nodes", registry)
	}
	agents := map[string]ports.NodeAgent{}
	for _, nodeURL := range cfg.NodeURLs {
		client := nodeagent.NewHTTPClient(nodeURL)
		snap, err := client.Snapshot(ctx)
		if err != nil {
			return "", nil, fmt.Errorf("snapshot node %s: %w", nodeURL, err)
		}
		if err := store.SaveNode(ctx, snap.Node); err != nil {
			return "", nil, err
		}
		for _, inst := range snap.Instances {
			if err := store.SaveInstance(ctx, inst); err != nil {
				return "", nil, err
			}
		}
		agents[snap.Node.ID] = client
	}
	if len(agents) > 0 {
		directory := gateway.NodeDirectory{Agents: agents}
		if fleet == nil {
			fleet = directory
			nodes = directory
		}
	}
	if fleet == nil || nodes == nil {
		return "", nil, fmt.Errorf("server config must provide join_token or node_urls")
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return "", nil, err
	}
	if len(presets) == 0 {
		return "", nil, fmt.Errorf("server config/store has no presets")
	}
	placer := scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clock.System{}, presets...)
	handler := gateway.Server{Router: &gateway.Router{
		Placer:  placer,
		Fleet:   fleet,
		Nodes:   nodes,
		Presets: gateway.NewPresetRegistry(presets...),
	}}
	if mux != nil {
		mux.Handle("/", handler)
		return cfg.Listen, mux, nil
	}
	return cfg.Listen, handler, nil
}

func seedControlStore(ctx context.Context, store *storesqlite.Store, cfg ServerConfig) error {
	for _, project := range cfg.Projects {
		if err := store.SaveProject(ctx, project); err != nil {
			return err
		}
	}
	if len(cfg.Projects) == 0 {
		if err := store.SaveProject(ctx, domain.Project{
			ID:         "default",
			Priority:   domain.PriorityInteractive,
			SpeedPref:  domain.SpeedThroughput,
			Preemption: domain.PreemptSoft,
		}); err != nil {
			return err
		}
	}
	for _, preset := range cfg.Presets {
		if err := store.SavePreset(ctx, preset); err != nil {
			return err
		}
	}
	return nil
}
