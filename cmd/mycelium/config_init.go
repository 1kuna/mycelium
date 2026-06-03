package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/hardware"
)

const (
	defaultPeerPort          = "51846"
	defaultBackendPort       = "51848"
	defaultDiscoveryPort     = "51850"
	sparkSafeVLLMGPUUtil     = 0.85
	unsafeVLLMGPUUtilization = 0.90
)

type configInitOptions struct {
	Path             string
	Compute          string
	Listen           string
	Backend          string
	MaxUtil          float64
	DiskMinFreeRatio float64
	Detect           func(context.Context, domain.Node) (domain.Node, error)
	RandomHex        func(int) (string, error)
	GOOS             string
	GOARCH           string
}

func runConfig(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mycelium config <init>")
	}
	switch args[0] {
	case "init":
		return runConfigInit(ctx, args[1:])
	default:
		return fmt.Errorf("unknown config command %q", args[0])
	}
}

func runConfigInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("config init", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	compute := fs.String("compute", "auto", "compute mode: auto, on, off")
	listen := fs.String("listen", "lan", "listen mode: lan, loopback, or host:port")
	backend := fs.String("backend", "auto", "backend: auto, llama.cpp, llamacpp, mlx, vllm, custom")
	maxUtil := fs.Float64("max-util", 0, "maximum accelerator utilization")
	diskMinFreeRatio := fs.Float64("disk-min-free-ratio", 0, "minimum free disk ratio required for placement")
	if err := fs.Parse(args); err != nil {
		return err
	}
	detector := hardware.NewDetector()
	cfg, err := generatePeerConfig(ctx, configInitOptions{
		Path:             *configPath,
		Compute:          *compute,
		Listen:           *listen,
		Backend:          *backend,
		MaxUtil:          *maxUtil,
		DiskMinFreeRatio: *diskMinFreeRatio,
		Detect:           detector.Detect,
		RandomHex:        randomHex,
		GOOS:             runtime.GOOS,
		GOARCH:           runtime.GOARCH,
	})
	if err != nil {
		return err
	}
	path := *configPath
	if path == "" {
		path = defaultPeerConfigPath()
	}
	if err := savePeerConfig(path, cfg); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "wrote\t%s\n", path)
	return nil
}

func generatePeerConfig(ctx context.Context, opts configInitOptions) (PeerConfig, error) {
	if opts.RandomHex == nil {
		opts.RandomHex = randomHex
	}
	peerID, err := prefixedRandomID("peer", opts.RandomHex)
	if err != nil {
		return PeerConfig{}, err
	}
	joinToken, err := opts.RandomHex(32)
	if err != nil {
		return PeerConfig{}, err
	}
	rpcToken, err := opts.RandomHex(32)
	if err != nil {
		return PeerConfig{}, err
	}
	listen, err := resolveListen(opts.Listen)
	if err != nil {
		return PeerConfig{}, err
	}
	backendListen, err := resolveBackendListen(listen)
	if err != nil {
		return PeerConfig{}, err
	}
	computeMode := opts.Compute
	if computeMode == "" {
		computeMode = "auto"
	}
	computeMode = strings.ToLower(computeMode)
	if computeMode != "auto" && computeMode != "on" && computeMode != "off" {
		return PeerConfig{}, fmt.Errorf("compute must be auto, on, or off")
	}
	backend, err := normalizeBackend(opts.Backend)
	if err != nil {
		return PeerConfig{}, err
	}
	maxUtil := opts.MaxUtil
	if maxUtil == 0 {
		maxUtil = 0.90
	}
	diskFloor := opts.DiskMinFreeRatio
	if diskFloor == 0 {
		diskFloor = domain.DefaultDiskMinFreeRatio
	}
	computeCfg := defaultedComputeConfig(ComputeConfig{
		BackendListen:    backendListen,
		ID:               peerID,
		Name:             peerID,
		Backend:          backend,
		MaxUtil:          maxUtil,
		DiskMinFreeRatio: diskFloor,
	})
	cfg := applyPeerConfigDefaults(PeerConfig{
		ID:              peerID,
		Listen:          listen,
		StorePath:       defaultControlStorePath(),
		CatalogDir:      defaultCatalogStore(),
		JoinToken:       joinToken,
		RPCToken:        rpcToken,
		ComputeConfig:   computeCfg,
		DiscoveryListen: ":" + defaultDiscoveryPort,
		DiscoveryAddr:   "255.255.255.255:" + defaultDiscoveryPort,
	})
	cfg.GGUFParser = cfg.ComputeConfig.GGUFParser
	detected, detectedOK, err := detectConfigNode(ctx, opts, cfg)
	if err != nil && computeMode == "on" {
		return PeerConfig{}, err
	}
	cfg.Compute = computeMode == "on" || (computeMode == "auto" && detectedOK)
	if computeMode == "off" {
		cfg.Compute = false
	}
	if backend == "" || opts.Backend == "" || strings.EqualFold(opts.Backend, "auto") {
		cfg.ComputeConfig.Backend = defaultBackendForHost(opts, detected)
	}
	if detectedOK {
		cfg.ComputeConfig.VRAMMB = largestAcceleratorMemory(detected)
		cfg.ComputeConfig.DiskTotalMB = detected.DiskTotalMB
		cfg.ComputeConfig.DiskFreeMB = detected.DiskFreeMB
		cfg.ComputeConfig.DiskMinFreeRatio = detected.DiskMinFreeRatio
		if detected.OOMSeverity == domain.OOMCatastrophic && cfg.ComputeConfig.Backend == domain.BackendVLLM && !hasVLLMGPUUtilization(cfg.ComputeConfig.CustomArgs) {
			cfg.ComputeConfig.CustomArgs = append(cfg.ComputeConfig.CustomArgs, "--gpu-memory-utilization", strconv.FormatFloat(sparkSafeVLLMGPUUtil, 'f', 2, 64))
		}
	}
	if cfg.ComputeConfig.Backend == domain.BackendVLLM && cfg.ComputeConfig.BackendBinary == "" {
		cfg.ComputeConfig.BackendBinary = "vllm"
	}
	if err := validatePeerConfig(cfg); err != nil {
		return PeerConfig{}, err
	}
	return cfg, nil
}

func detectConfigNode(ctx context.Context, opts configInitOptions, cfg PeerConfig) (domain.Node, bool, error) {
	if opts.Detect == nil {
		return domain.Node{}, false, nil
	}
	seed := domain.Node{
		ID:               cfg.ComputeConfig.ID,
		Name:             cfg.ComputeConfig.Name,
		Address:          cfg.Listen,
		MaxUtil:          cfg.ComputeConfig.MaxUtil,
		DiskMinFreeRatio: cfg.ComputeConfig.DiskMinFreeRatio,
		SpeedClass:       domain.SpeedClass{TokensPerSecRef: 1, Source: "class-default", ProbedAt: clock.System{}.Now().UTC()},
		Status:           domain.NodeReady,
	}
	node, err := opts.Detect(ctx, seed)
	if err != nil {
		return domain.Node{}, false, err
	}
	return node, len(node.Accelerators) > 0, nil
}

func resolveListen(raw string) (string, error) {
	if raw == "" || strings.EqualFold(raw, "lan") {
		host := firstPrivateIPv4()
		if host == "" {
			host = "0.0.0.0"
		}
		return net.JoinHostPort(host, defaultPeerPort), nil
	}
	if strings.EqualFold(raw, "loopback") {
		return net.JoinHostPort("127.0.0.1", defaultPeerPort), nil
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("listen must be lan, loopback, or host:port")
	}
	return net.JoinHostPort(host, port), nil
}

func resolveBackendListen(peerListen string) (string, error) {
	host, _, err := net.SplitHostPort(peerListen)
	if err != nil {
		return "", err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, defaultBackendPort), nil
}

func normalizeBackend(raw string) (domain.Backend, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" || raw == "auto" {
		return "", nil
	}
	switch raw {
	case "llama.cpp", "llamacpp":
		return domain.BackendLlamaCpp, nil
	case string(domain.BackendMLX):
		return domain.BackendMLX, nil
	case string(domain.BackendVLLM):
		return domain.BackendVLLM, nil
	case string(domain.BackendCustom):
		return domain.BackendCustom, nil
	default:
		return "", fmt.Errorf("unknown backend %q", raw)
	}
}

func defaultBackendForHost(opts configInitOptions, node domain.Node) domain.Backend {
	goos := opts.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	goarch := opts.GOARCH
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	if goos == "darwin" {
		return domain.BackendLlamaCpp
	}
	if goos == "linux" {
		for _, acc := range node.Accelerators {
			if acc.Vendor == "nvidia" {
				return domain.BackendVLLM
			}
		}
	}
	_ = goarch
	return domain.BackendLlamaCpp
}

func largestAcceleratorMemory(node domain.Node) int {
	max := 0
	for _, acc := range node.Accelerators {
		if acc.VRAMTotalMB > max {
			max = acc.VRAMTotalMB
		}
	}
	return max
}

func firstPrivateIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP == nil {
			continue
		}
		ip := ipNet.IP.To4()
		if ip == nil || ip.IsLoopback() {
			continue
		}
		if ip.IsPrivate() {
			return ip.String()
		}
	}
	return ""
}

func prefixedRandomID(prefix string, random func(int) (string, error)) (string, error) {
	id, err := random(6)
	if err != nil {
		return "", err
	}
	return prefix + "_" + id, nil
}

func randomHex(bytesLen int) (string, error) {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
