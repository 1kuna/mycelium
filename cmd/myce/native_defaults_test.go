package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Configs written before managed Docker was retired (MY-49) still carry its
// keys. Reinstalling must accept them, drop them, and stop passing the flags.
func TestInstallDropsRetiredDockerKeys(t *testing.T) {
	c, probe := savedInstallFixture(t, "worker")
	retention := time.Hour
	c.DockerEndpoint, c.ContainerRetention = "unix:///saved.sock", &retention
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil || got.DockerEndpoint != "" || got.ContainerRetention != nil {
		t.Fatalf("retired keys kept: %q %v %v", got.DockerEndpoint, got.ContainerRetention, err)
	}
	saved, err := os.ReadFile(c.Config)
	if err != nil || strings.Contains(string(saved), "docker_endpoint") || strings.Contains(string(saved), "container_retention") {
		t.Fatalf("saved config still names retired keys: %v", err)
	}
	if args := strings.Join(got.Args(), " "); strings.Contains(args, "--docker-host") || strings.Contains(args, "--container-retention") {
		t.Fatalf("service argv still passes retired flags: %s", args)
	}
}
