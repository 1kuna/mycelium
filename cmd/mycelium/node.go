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
	"mycelium/internal/estimate"
	"mycelium/internal/hardware"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	nodeagent "mycelium/internal/node"
	storesqlite "mycelium/internal/store/sqlite"
)

func runNode(ctx context.Context, args []string) error {
	if ctx.Err() != nil {
		return nil
	}
	spec, err := buildNodeServerSpec(ctx, args)
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
	spec, err := buildNodeServerSpec(context.Background(), args)
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

func buildNodeServerSpec(ctx context.Context, args []string) (nodeServerSpec, error) {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	configPath := fs.String("config", "", "node config JSON path")
	listen := fs.String("listen", "", "node agent listen address")
	backendListen := fs.String("backend-listen", "", "backend inference server listen address")
	id := fs.String("id", "", "node id")
	name := fs.String("name", "", "node name")
	llamaServer := fs.String("llama-server", "", "llama.cpp server binary")
	ggufParser := fs.String("gguf-parser", "", "GGUF parser binary")
	maxUtil := fs.Float64("max-util", 0, "maximum accelerator utilization")
	vramMB := fs.Int("vram-mb", 0, "local allocatable memory in MB")
	storePath := fs.String("state-db", "", "node process-ref SQLite store")
	join := fs.String("join", "", "mycjoin:// token for joining a server")
	if err := fs.Parse(args); err != nil {
		return nodeServerSpec{}, err
	}
	cfg, err := loadNodeConfig(*configPath)
	if err != nil {
		return nodeServerSpec{}, err
	}
	overrideString(listen, &cfg.Listen)
	overrideString(backendListen, &cfg.BackendListen)
	overrideString(id, &cfg.ID)
	overrideString(name, &cfg.Name)
	overrideString(llamaServer, &cfg.LlamaServer)
	overrideString(ggufParser, &cfg.GGUFParser)
	overrideString(storePath, &cfg.StorePath)
	overrideString(join, &cfg.Join)
	if *maxUtil != 0 {
		cfg.MaxUtil = *maxUtil
	}
	if *vramMB != 0 {
		cfg.VRAMMB = *vramMB
	}

	nodeAddr := cfg.Listen
	backendAddr := cfg.BackendListen
	if cfg.Join != "" {
		info, err := membership.ParseJoinToken(cfg.Join)
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

	seed := domain.Node{
		ID:         cfg.ID,
		Name:       cfg.Name,
		Address:    nodeAddr,
		MaxUtil:    cfg.MaxUtil,
		Status:     domain.NodeReady,
		SpeedClass: domain.SpeedClass{TokensPerSecRef: 1, Source: "class-default"},
	}
	node := seed
	if cfg.VRAMMB > 0 {
		node = explicitMemoryNode(seed, cfg.VRAMMB)
	} else {
		detected, err := hardware.NewDetector().Detect(ctx, seed)
		if err != nil {
			return nodeServerSpec{}, err
		}
		node = detected
	}
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		return nodeServerSpec{}, err
	}
	registry := nodeagent.StoreProcessRegistry{Store: store, NodeID: node.ID}
	adapter := llamacpp.NewAdapter(llamacpp.Config{BinaryPath: cfg.LlamaServer, ProcessRegistry: registry})
	if refs, err := store.ProcessRefs(ctx, node.ID); err == nil && len(refs) > 0 {
		reaper := nodeagent.NewReaperFromRefs(refs, nodeagent.BackendProcessKiller{Backend: adapter})
		if _, err := reaper.Reap(ctx); err != nil {
			return nodeServerSpec{}, err
		}
		if err := store.DeleteProcessRefs(ctx, node.ID); err != nil {
			return nodeServerSpec{}, err
		}
	} else if err != nil {
		return nodeServerSpec{}, err
	}
	opts := []nodeagent.Option{nodeagent.WithListenAddr(backendAddr), nodeagent.WithAllocator(lease.NewAllocator())}
	if cfg.GGUFParser != "" {
		opts = append(opts, nodeagent.WithModelInspector(nodeagent.ParserInspector{Parser: estimate.NewCommandParser(cfg.GGUFParser, []string{"{model}"})}))
	}
	agent := nodeagent.NewAgent(node, adapter, clock.System{}, opts...)
	return nodeServerSpec{addr: cfg.Listen, handler: nodeagent.HTTPServer{Agent: agent}, node: node, join: cfg.Join}, nil
}

func overrideString(flagValue *string, target *string) {
	if flagValue != nil && *flagValue != "" {
		*target = *flagValue
	}
}

func explicitMemoryNode(seed domain.Node, vramMB int) domain.Node {
	node := seed
	node.OS = "darwin"
	node.Labels = map[string]string{"gpu.vendor": "apple", "memory.class": "unified"}
	node.OOMSeverity = domain.OOMSoft
	node.UnifiedMemory = true
	node.Accelerators = []domain.Accelerator{{
		Index:         0,
		Vendor:        "apple",
		Kind:          "unified",
		VRAMTotalMB:   vramMB,
		UnifiedMemory: true,
	}}
	return node
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
