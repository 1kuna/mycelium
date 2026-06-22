package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	EngineProfiles       []domain.EngineProfile         `json:"engine_profiles,omitempty"`
	JoinToken            string                         `json:"join_token"`
	RPCToken             string                         `json:"rpc_token"`
	GatewayToken         string                         `json:"gateway_token,omitempty"`
	GatewayProjectTokens []GatewayProjectToken          `json:"gateway_project_tokens,omitempty"`
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

type GatewayProjectToken struct {
	Project string `json:"project"`
	Token   string `json:"token"`
}

type ComputeConfig struct {
	BackendListen    string                 `json:"backend_listen"`
	ID               string                 `json:"id"`
	Name             string                 `json:"name"`
	Backend          domain.Backend         `json:"backend"`
	Backends         []BackendRuntimeConfig `json:"backends,omitempty"`
	BackendBinary    string                 `json:"backend_binary"`
	CustomArgs       []string               `json:"custom_args,omitempty"`
	HealthPath       string                 `json:"health_path,omitempty"`
	StopGraceMS      int                    `json:"stop_grace_ms,omitempty"`
	LlamaServer      string                 `json:"llama_server"`
	GGUFParser       string                 `json:"gguf_parser"`
	MaxUtil          float64                `json:"max_util"`
	DiskPath         string                 `json:"disk_path,omitempty"`
	DiskTotalMB      int                    `json:"disk_total_mb,omitempty"`
	DiskFreeMB       int                    `json:"disk_free_mb,omitempty"`
	DiskMinFreeRatio float64                `json:"disk_min_free_ratio,omitempty"`
	LoadTimeoutMS    int                    `json:"load_timeout_ms,omitempty"`
	VRAMMB           int                    `json:"vram_mb"`
}

type BackendRuntimeConfig struct {
	Backend       domain.Backend `json:"backend"`
	BackendBinary string         `json:"backend_binary,omitempty"`
	CustomArgs    []string       `json:"custom_args,omitempty"`
	HealthPath    string         `json:"health_path,omitempty"`
	StopGraceMS   int            `json:"stop_grace_ms,omitempty"`
	LlamaServer   string         `json:"llama_server,omitempty"`
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
	if cfg.DefaultProject == "" {
		if len(cfg.Projects) > 0 {
			cfg.DefaultProject = cfg.Projects[0].ID
		} else {
			cfg.DefaultProject = "default"
		}
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
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}

func savePeerConfigAtomic(path string, cfg PeerConfig, backupStamp string, postWriteValidate func(string) error) (string, error) {
	if path == "" {
		path = defaultPeerConfigPath()
	}
	cfg = applyPeerConfigDefaults(cfg)
	if err := validatePeerConfig(cfg); err != nil {
		return "", err
	}
	if postWriteValidate == nil {
		postWriteValidate = func(path string) error {
			_, err := loadPeerConfig(path)
			return err
		}
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return "", err
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return "", err
		}
	}
	original, readErr := os.ReadFile(path)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return "", fmt.Errorf("read existing peer config for backup: %w", readErr)
	}
	backupPath := ""
	if readErr == nil {
		backupPath = nextPeerConfigBackupPath(path, backupStamp)
		if err := writeFileAtomic(backupPath, original, 0600); err != nil {
			return "", fmt.Errorf("write peer config backup: %w", err)
		}
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return backupPath, err
	}
	if err := writeFileAtomic(path, append(data, '\n'), 0600); err != nil {
		return backupPath, err
	}
	if err := postWriteValidate(path); err != nil {
		restoreErr := restorePeerConfig(path, original, readErr)
		if restoreErr != nil {
			return backupPath, fmt.Errorf("post-write peer config validation failed: %w; restore failed: %v", err, restoreErr)
		}
		return backupPath, fmt.Errorf("post-write peer config validation failed: %w", err)
	}
	return backupPath, nil
}

func restorePeerConfig(path string, original []byte, readErr error) error {
	if errors.Is(readErr, os.ErrNotExist) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return writeFileAtomic(path, original, 0600)
}

func nextPeerConfigBackupPath(path, stamp string) string {
	if stamp == "" {
		stamp = "unknown"
	}
	stamp = strings.NewReplacer("/", "-", "\\", "-", ":", "-").Replace(stamp)
	base := path + ".backup." + stamp
	if _, err := os.Stat(base); errors.Is(err, os.ErrNotExist) {
		return base
	}
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s.%d", base, i)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
}

func backupStampForPlan(plan domain.BootstrapPlan) string {
	if plan.CreatedAt.IsZero() {
		return "unknown"
	}
	return plan.CreatedAt.UTC().Format("20060102T150405Z")
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func savePeerJoinConfig(path string, joined PeerConfig) error {
	persisted, err := loadPeerConfig(path)
	if err != nil {
		return err
	}
	persisted.JoinToken = joined.JoinToken
	persisted.RPCToken = joined.RPCToken
	persisted.GatewayToken = joined.GatewayToken
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
	for i := range cfg.Backends {
		cfg.Backends[i] = defaultedBackendRuntimeConfig(cfg.Backends[i])
	}
	return cfg
}

func defaultedBackendRuntimeConfig(cfg BackendRuntimeConfig) BackendRuntimeConfig {
	if cfg.Backend == "" {
		cfg.Backend = domain.BackendLlamaCpp
	}
	if cfg.LlamaServer == "" {
		cfg.LlamaServer = "llama-server"
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
