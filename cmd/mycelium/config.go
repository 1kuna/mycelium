package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
	BackendListen    string         `json:"backend_listen"`
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Backend          domain.Backend `json:"backend"`
	BackendBinary    string         `json:"backend_binary"`
	CustomArgs       []string       `json:"custom_args,omitempty"`
	HealthPath       string         `json:"health_path,omitempty"`
	StopGraceMS      int            `json:"stop_grace_ms,omitempty"`
	LlamaServer      string         `json:"llama_server"`
	GGUFParser       string         `json:"gguf_parser"`
	MaxUtil          float64        `json:"max_util"`
	DiskPath         string         `json:"disk_path,omitempty"`
	DiskTotalMB      int            `json:"disk_total_mb,omitempty"`
	DiskFreeMB       int            `json:"disk_free_mb,omitempty"`
	DiskMinFreeRatio float64        `json:"disk_min_free_ratio,omitempty"`
	LoadTimeoutMS    int            `json:"load_timeout_ms,omitempty"`
	VRAMMB           int            `json:"vram_mb"`
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
	cfg = applyPeerConfigDefaults(cfg)
	if err := validatePeerConfig(cfg); err != nil {
		return PeerConfig{}, fmt.Errorf("validate peer config %s: %w", path, err)
	}
	return cfg, nil
}

func loadOrBootstrapPeerConfig(path string, allowBootstrap bool) (PeerConfig, string, bool, error) {
	if path == "" {
		path = defaultPeerConfigPath()
	}
	cfg, err := loadPeerConfig(path)
	if err == nil {
		return cfg, path, false, nil
	}
	if !allowBootstrap || !errors.Is(err, os.ErrNotExist) {
		return PeerConfig{}, path, false, err
	}
	cfg = applyPeerConfigDefaults(PeerConfig{})
	if err := savePeerConfig(path, cfg); err != nil {
		return PeerConfig{}, path, false, err
	}
	return cfg, path, true, nil
}

func applyPeerConfigDefaults(cfg PeerConfig) PeerConfig {
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
	return cfg
}

func savePeerConfig(path string, cfg PeerConfig) error {
	if path == "" {
		path = defaultPeerConfigPath()
	}
	cfg = applyPeerConfigDefaults(cfg)
	if err := validatePeerConfig(cfg); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}

func savePeerJoinConfig(path string, joined PeerConfig) error {
	persisted, err := loadPeerConfig(path)
	if err != nil {
		return err
	}
	persisted.JoinToken = joined.JoinToken
	persisted.RPCToken = joined.RPCToken
	persisted.SeedPeers = append([]string(nil), joined.SeedPeers...)
	return savePeerConfig(path, persisted)
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
	if cfg.DiskMinFreeRatio == 0 {
		cfg.DiskMinFreeRatio = domain.DefaultDiskMinFreeRatio
	}
	if cfg.LoadTimeoutMS == 0 {
		cfg.LoadTimeoutMS = int((5 * time.Minute) / time.Millisecond)
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
