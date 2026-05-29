package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"net/http"
	"time"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	nodeagent "mycelium/internal/node"
)

func runNode(ctx context.Context, args []string) error {
	spec, err := buildNodeServerSpec(args)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", spec.addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: spec.handler}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	if spec.join != "" {
		info, err := membership.ParseJoinToken(spec.join)
		if err != nil {
			return err
		}
		spec.node.Address = effectiveAdvertiseAddr(spec.node.Address, listener.Addr().String())
		advertise, err := membership.AdvertiseAddr(spec.node.Address, info.ServerURL)
		if err != nil {
			return err
		}
		spec.node.Address = advertise
		if _, err := membership.Announce(ctx, nil, spec.join, spec.node); err != nil {
			return err
		}
	}
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}
	return nil
}

func buildNodeServer(args []string) (string, http.Handler, error) {
	spec, err := buildNodeServerSpec(args)
	if err != nil {
		return "", nil, err
	}
	return spec.addr, spec.handler, nil
}

type nodeServerSpec struct {
	addr    string
	handler http.Handler
	node    domain.Node
	join    string
}

func buildNodeServerSpec(args []string) (nodeServerSpec, error) {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:51847", "node agent listen address")
	backendListen := fs.String("backend-listen", "127.0.0.1:51848", "backend inference server listen address")
	id := fs.String("id", "node_local", "node id")
	name := fs.String("name", "local-node", "node name")
	llamaServer := fs.String("llama-server", "llama-server", "llama.cpp server binary")
	maxUtil := fs.Float64("max-util", 0.90, "maximum accelerator utilization")
	vramMB := fs.Int("vram-mb", 65536, "local allocatable memory in MB")
	join := fs.String("join", "", "mycjoin:// token for joining a server")
	if err := fs.Parse(args); err != nil {
		return nodeServerSpec{}, err
	}
	nodeAddr := *listen
	backendAddr := *backendListen
	if *join != "" {
		info, err := membership.ParseJoinToken(*join)
		if err != nil {
			return nodeServerSpec{}, err
		}
		advertise, err := membership.AdvertiseAddr(nodeAddr, info.ServerURL)
		if err != nil {
			return nodeServerSpec{}, err
		}
		nodeAddr = advertise
		backendAddr = joinedBackendAddr(backendAddr, advertise)
	}

	node := domain.Node{
		ID:          *id,
		Name:        *name,
		Address:     nodeAddr,
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
	agent := nodeagent.NewAgent(node, adapter, clock.System{}, nodeagent.WithListenAddr(backendAddr), nodeagent.WithAllocator(lease.NewAllocator()))
	return nodeServerSpec{addr: *listen, handler: nodeagent.HTTPServer{Agent: agent}, node: node, join: *join}, nil
}

func effectiveAdvertiseAddr(configured, actual string) string {
	host, port, err := net.SplitHostPort(configured)
	if err != nil || port != "0" {
		return configured
	}
	_, actualPort, err := net.SplitHostPort(actual)
	if err != nil {
		return configured
	}
	return net.JoinHostPort(host, actualPort)
}

func joinedBackendAddr(backendListen, nodeAdvertise string) string {
	backendHost, backendPort, err := net.SplitHostPort(backendListen)
	if err != nil {
		return backendListen
	}
	if backendHost != "127.0.0.1" && backendHost != "localhost" && backendHost != "::1" {
		return backendListen
	}
	nodeHost, _, err := net.SplitHostPort(nodeAdvertise)
	if err != nil {
		return backendListen
	}
	return net.JoinHostPort(nodeHost, backendPort)
}
