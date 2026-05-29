package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"time"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/lease"
	nodeagent "mycelium/internal/node"
)

func runNode(ctx context.Context, args []string) error {
	addr, handler, err := buildNodeServer(args)
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

func buildNodeServer(args []string) (string, http.Handler, error) {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:51847", "node agent listen address")
	id := fs.String("id", "node_local", "node id")
	name := fs.String("name", "local-node", "node name")
	llamaServer := fs.String("llama-server", "llama-server", "llama.cpp server binary")
	maxUtil := fs.Float64("max-util", 0.90, "maximum accelerator utilization")
	vramMB := fs.Int("vram-mb", 65536, "local allocatable memory in MB")
	if err := fs.Parse(args); err != nil {
		return "", nil, err
	}

	node := domain.Node{
		ID:          *id,
		Name:        *name,
		Address:     *listen,
		OS:          "darwin",
		Labels:      map[string]string{"gpu.vendor": "apple", "memory.class": "unified"},
		MaxUtil:     *maxUtil,
		OOMSeverity: domain.OOMSoft,
		Status:      domain.NodeReady,
		Accelerators: []domain.Accelerator{{
			Index:         0,
			Vendor:        "apple",
			Kind:          "unified",
			VRAMTotalMB:   *vramMB,
			UnifiedMemory: true,
		}},
		UnifiedMemory: true,
		SpeedClass:    domain.SpeedClass{TokensPerSecRef: 1, Source: "class-default"},
	}
	adapter := llamacpp.NewAdapter(llamacpp.Config{BinaryPath: *llamaServer})
	agent := nodeagent.NewAgent(node, adapter, clock.System{}, nodeagent.WithAllocator(lease.NewAllocator()))
	return *listen, nodeagent.HTTPServer{Agent: agent}, nil
}
