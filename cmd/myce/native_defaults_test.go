package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// Configs written before managed Docker was retired (MY-49) still carry its
// keys. Reinstalling must accept them, drop them, and stop passing the flags.
func TestInstallDropsRetiredDockerKeys(t *testing.T) {
	c, probe := savedInstallFixture(t, "worker")
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	// Inject the old wire keys without keeping them in ServiceConfig.
	b = append(b[:len(b)-1], []byte(`,"docker_endpoint":"unix:///saved.sock","container_retention":3600000000000}`)...)
	if err := os.WriteFile(c.Config, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := myce.ReadServiceConfig(c.Config); err != nil {
		t.Fatalf("config with retired keys refused: %v", err)
	}
	if err := setupCommandWithProbe([]string{"install", "worker", "--config", c.Config}, probe, filepath.Dir(c.NAS)); err != nil {
		t.Fatal(err)
	}
	got, err := myce.ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(c.Config)
	if err != nil || strings.Contains(string(saved), "docker_endpoint") || strings.Contains(string(saved), "container_retention") {
		t.Fatalf("saved config still names retired keys: %v", err)
	}
	if args := strings.Join(got.Args(), " "); strings.Contains(args, "--docker-host") || strings.Contains(args, "--container-retention") {
		t.Fatalf("service argv still passes retired flags: %s", args)
	}
}

func TestRetiredDockerCLIRefused(t *testing.T) {
	for _, flag := range []string{"--docker-host=unix:///saved.sock", "--container-retention=1h"} {
		if err := run([]string{"join", flag}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("retired flag %s: %v", flag, err)
		}
	}
	if err := run([]string{"resolve-docker"}); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("retired command: %v", err)
	}
}
