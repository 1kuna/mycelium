package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"mycelium/internal/domain"
)

type PeerConfig struct {
	ID                   string                         `json:"id"`
	Listen               string                         `json:"listen"`
	StorePath            string                         `json:"store_path"`
	CatalogDir           string                         `json:"catalog_dir"`
	Compute              bool                           `json:"compute"`
	ComputeConfig        ComputeConfig                  `json:"compute_config"`
	JoinToken            string                         `json:"join_token"`
	RPCToken             string                         `json:"rpc_token"`
	SeedPeers            []string                       `json:"seed_peers,omitempty"`
	DiscoveryListen      string                         `json:"discovery_listen"`
	DiscoveryAddr        string                         `json:"discovery_addr"`
	DiscoveryScanMS      int                            `json:"discovery_scan_ms"`
	DiscoveryAdvertiseMS int                            `json:"discovery_advertise_ms"`
	Overlay              bool                           `json:"overlay,omitempty"`
	OverlayListenAddrs   []string                       `json:"overlay_listen_addrs,omitempty"`
	OverlayBootstrap     []string                       `json:"overlay_bootstrap,omitempty"`
	GGUFParser           string                         `json:"gguf_parser"`
	Projects             []domain.Project               `json:"projects"`
	Presets              []domain.Preset                `json:"presets"`
	Reservations         []domain.Reservation           `json:"reservations"`
	PrivateStorageKey    string                         `json:"private_storage_key,omitempty"`
	SubmitterPolicy      map[string]SubmitterPolicyRule `json:"submitter_policy,omitempty"`
	DefaultProject       string                         `json:"default_project"`
	QueueDrainMS         int                            `json:"queue_drain_ms"`
	QueueDrainLimit      int                            `json:"queue_drain_limit"`
	OptimizerEvalMS      int                            `json:"optimizer_eval_ms"`
	RegistrySyncMS       int                            `json:"registry_sync_ms"`
}

type SubmitterPolicyRule struct {
	MaxPriority  domain.Priority `json:"max_priority,omitempty"`
	AllowPrivate bool            `json:"allow_private,omitempty"`
}

type ComputeConfig struct {
	BackendListen string         `json:"backend_listen"`
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Backend       domain.Backend `json:"backend"`
	BackendBinary string         `json:"backend_binary"`
	CustomArgs    []string       `json:"custom_args,omitempty"`
	HealthPath    string         `json:"health_path,omitempty"`
	StopGraceMS   int            `json:"stop_grace_ms,omitempty"`
	LlamaServer   string         `json:"llama_server"`
	GGUFParser    string         `json:"gguf_parser"`
	MaxUtil       float64        `json:"max_util"`
	VRAMMB        int            `json:"vram_mb"`
}

func loadPeerConfig(path string) (PeerConfig, error) {
	if path == "" {
		path = defaultPeerConfigPath()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return PeerConfig{}, fmt.Errorf("read peer config %s: %w", path, err)
	}
	var cfg PeerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return PeerConfig{}, fmt.Errorf("parse peer config %s: %w", path, err)
	}
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:51846"
	}
	if cfg.StorePath == "" {
		cfg.StorePath = defaultControlStorePath()
	}
	if cfg.CatalogDir == "" {
		cfg.CatalogDir = defaultCatalogStore()
	}
	if cfg.DefaultProject == "" && len(cfg.Projects) > 0 {
		cfg.DefaultProject = cfg.Projects[0].ID
	}
	if cfg.QueueDrainMS == 0 {
		cfg.QueueDrainMS = 1000
	}
	if cfg.QueueDrainLimit == 0 {
		cfg.QueueDrainLimit = 1
	}
	if cfg.OptimizerEvalMS == 0 {
		cfg.OptimizerEvalMS = 60000
	}
	if cfg.RegistrySyncMS == 0 {
		cfg.RegistrySyncMS = 1000
	}
	if cfg.DiscoveryScanMS == 0 {
		cfg.DiscoveryScanMS = 250
	}
	if cfg.DiscoveryAdvertiseMS == 0 {
		cfg.DiscoveryAdvertiseMS = 5000
	}
	cfg.ComputeConfig = defaultedComputeConfig(cfg.ComputeConfig)
	if cfg.ID == "" {
		cfg.ID = cfg.ComputeConfig.ID
	}
	return cfg, nil
}

func defaultedComputeConfig(cfg ComputeConfig) ComputeConfig {
	if cfg.BackendListen == "" {
		cfg.BackendListen = "127.0.0.1:51848"
	}
	if cfg.ID == "" {
		cfg.ID = "peer_local"
	}
	if cfg.Name == "" {
		cfg.Name = "local-peer"
	}
	if cfg.Backend == "" {
		cfg.Backend = domain.BackendLlamaCpp
	}
	if cfg.LlamaServer == "" {
		cfg.LlamaServer = "llama-server"
	}
	if cfg.MaxUtil == 0 {
		cfg.MaxUtil = 0.90
	}
	return cfg
}

func defaultPeerConfigPath() string {
	return filepath.Join(defaultMyceliumHome(), "peer.json")
}

func defaultControlStorePath() string {
	return filepath.Join(defaultMyceliumHome(), "mycelium.db")
}

func defaultCatalogStore() string {
	return filepath.Join(defaultMyceliumHome(), "catalog")
}

func defaultMyceliumHome() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".mycelium"
	}
	return filepath.Join(home, ".mycelium")
}
