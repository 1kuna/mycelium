package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"mycelium/internal/domain"
)

type ServerConfig struct {
	Listen         string           `json:"listen"`
	StorePath      string           `json:"store_path"`
	CatalogDir     string           `json:"catalog_dir"`
	JoinToken      string           `json:"join_token"`
	NodeURLs       []string         `json:"node_urls"`
	Projects       []domain.Project `json:"projects"`
	Presets        []domain.Preset  `json:"presets"`
	DefaultProject string           `json:"default_project"`
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
	return cfg, nil
}

func defaultServerConfigPath() string {
	return filepath.Join(defaultMyceliumHome(), "server.json")
}

func defaultControlStorePath() string {
	return filepath.Join(defaultMyceliumHome(), "mycelium.db")
}

func defaultMyceliumHome() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".mycelium"
	}
	return filepath.Join(home, ".mycelium")
}
