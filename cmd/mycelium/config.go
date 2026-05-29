package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"mycelium/internal/domain"
)

type ServerConfig struct {
	Listen          string               `json:"listen"`
	StorePath       string               `json:"store_path"`
	CatalogDir      string               `json:"catalog_dir"`
	JoinToken       string               `json:"join_token"`
	NodeURLs        []string             `json:"node_urls"`
	GGUFParser      string               `json:"gguf_parser"`
	Projects        []domain.Project     `json:"projects"`
	Presets         []domain.Preset      `json:"presets"`
	Reservations    []domain.Reservation `json:"reservations"`
	DefaultProject  string               `json:"default_project"`
	QueueDrainMS    int                  `json:"queue_drain_ms"`
	QueueDrainLimit int                  `json:"queue_drain_limit"`
	OptimizerEvalMS int                  `json:"optimizer_eval_ms"`
}

type NodeConfig struct {
	Listen        string         `json:"listen"`
	BackendListen string         `json:"backend_listen"`
	StorePath     string         `json:"store_path"`
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Backend       domain.Backend `json:"backend"`
	BackendBinary string         `json:"backend_binary"`
	LlamaServer   string         `json:"llama_server"`
	GGUFParser    string         `json:"gguf_parser"`
	MaxUtil       float64        `json:"max_util"`
	VRAMMB        int            `json:"vram_mb"`
	Join          string         `json:"join"`
}

func loadServerConfig(path string) (ServerConfig, error) {
	if path == "" {
		path = defaultServerConfigPath()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("read server config %s: %w", path, err)
	}
	var cfg ServerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return ServerConfig{}, fmt.Errorf("parse server config %s: %w", path, err)
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
	return cfg, nil
}

func loadNodeConfig(path string) (NodeConfig, error) {
	if path == "" {
		return defaultNodeConfig(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return NodeConfig{}, fmt.Errorf("read node config %s: %w", path, err)
	}
	cfg := defaultNodeConfig()
	if err := json.Unmarshal(data, &cfg); err != nil {
		return NodeConfig{}, fmt.Errorf("parse node config %s: %w", path, err)
	}
	if cfg.StorePath == "" {
		cfg.StorePath = defaultControlStorePath()
	}
	return cfg, nil
}

func defaultNodeConfig() NodeConfig {
	return NodeConfig{
		Listen:        "127.0.0.1:51847",
		BackendListen: "127.0.0.1:51848",
		StorePath:     defaultControlStorePath(),
		ID:            "node_local",
		Name:          "local-node",
		Backend:       domain.BackendLlamaCpp,
		LlamaServer:   "llama-server",
		MaxUtil:       0.90,
	}
}

func defaultServerConfigPath() string {
	return filepath.Join(defaultMyceliumHome(), "server.json")
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
