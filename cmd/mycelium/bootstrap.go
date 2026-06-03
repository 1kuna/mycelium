package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"

	"mycelium/internal/hardware"
	"mycelium/internal/membership"
	storesqlite "mycelium/internal/store/sqlite"
)

func runBootstrap(ctx context.Context, args []string) error {
	return runBootstrapWithServiceManager(ctx, args, nil, runtime.GOOS)
}

func runBootstrapWithServiceManager(ctx context.Context, args []string, manager serviceManager, goos string) error {
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	joinRaw := fs.String("join", "", "join URI")
	compute := fs.String("compute", "auto", "compute mode: auto, on, off")
	configPath := fs.String("config", "", "peer config JSON path")
	apply := fs.Bool("apply", false, "write config and state")
	installService := fs.Bool("install-service", false, "install and start durable service after apply")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *joinRaw == "" {
		return fmt.Errorf("--join is required")
	}
	join, err := parseJoinFlag(*joinRaw)
	if err != nil {
		return err
	}
	if join.RPCToken == "" {
		return fmt.Errorf("bootstrap join URI must include rpc_token")
	}
	path := *configPath
	if path == "" {
		path = defaultPeerConfigPath()
	}
	detector := hardware.NewDetector()
	cfg, err := generatePeerConfig(ctx, configInitOptions{
		Path:      path,
		Compute:   *compute,
		Listen:    "lan",
		Backend:   "auto",
		Detect:    detector.Detect,
		RandomHex: randomHex,
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
	})
	if err != nil {
		return err
	}
	cfg.JoinToken = join.Token
	cfg.RPCToken = join.RPCToken
	cfg.SeedPeers = appendSeedPeer(cfg.SeedPeers, join.Address)
	if !*apply {
		fmt.Printf("bootstrap\tplan\tconfig=%s\tcompute=%t\tseeds=%d\n", path, cfg.Compute, len(cfg.SeedPeers))
		return nil
	}
	if err := savePeerConfig(path, cfg); err != nil {
		return err
	}
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		return err
	}
	defer store.Close()
	if _, err := membership.NewPersistentTokenManager(ctx, cfg.JoinToken, store); err != nil {
		return err
	}
	fmt.Printf("bootstrap\tapplied\t%s\n", path)
	if *installService {
		if err := runServiceWithManager(ctx, []string{"install", "--config", path}, manager, goos); err != nil {
			return err
		}
	}
	return nil
}
