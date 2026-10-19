package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestInstallDockerNativeDefaults(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///ambient.sock")
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "fresh"},
		{name: "opt-in", args: []string{"--docker-host", "unix:///explicit.sock"}, want: "unix:///explicit.sock"},
		{name: "explicit-empty", args: []string{"--docker-host="}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			out, err := os.CreateTemp(t.TempDir(), "stdout")
			if err != nil {
				t.Fatal(err)
			}
			saved := os.Stdout
			os.Stdout = out
			defer func() { os.Stdout = saved; out.Close() }()
			args := append([]string{"install", "worker", "--gpu", "fake", "--memory-gb", "8", "--nas", home, "--dry-run"}, tc.args...)
			if err := setupCommandWithProbe(args, myce.MachineProbe{OS: "linux", Arch: "amd64"}, home); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(out.Name())
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(b), "ambient.sock") || strings.Contains(string(b), "--docker-host") != (tc.want != "") || tc.want != "" && !strings.Contains(string(b), tc.want) {
				t.Fatalf("incorrect generated Docker argv: %s", b)
			}
		})
	}
	home := t.TempDir()
	c, err := myce.PlanInstall("worker", home, "", home, "http://queue.invalid", "native", myce.Detection{OS: "linux", Platform: "fake"})
	if err != nil || c.DockerEndpoint != "" {
		t.Fatalf("plan adopted ambient Docker: %q, %v", c.DockerEndpoint, err)
	}
}

func TestInstallDockerUpgrade(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///ambient.sock")
	for _, endpoint := range []string{"unix:///saved.sock", "", "missing"} {
		t.Run(endpoint, func(t *testing.T) {
			c, probe := savedInstallFixture(t, "worker")
			c.DockerEndpoint = endpoint
			b, _ := json.Marshal(c)
			if endpoint == "missing" {
				var fields map[string]json.RawMessage
				json.Unmarshal(b, &fields)
				delete(fields, "docker_endpoint")
				b, _ = json.Marshal(fields)
				endpoint = ""
			}
			if err := os.WriteFile(c.Config, b, 0600); err != nil {
				t.Fatal(err)
			}
			for _, flags := range [][]string{nil, {"--docker-host="}} {
				args := append([]string{"install", "worker", "--config", c.Config}, flags...)
				if err := setupCommandWithProbe(args, probe, filepath.Dir(c.NAS)); err != nil {
					t.Fatal(err)
				}
				got, err := myce.ReadServiceConfig(c.Config)
				if len(flags) > 0 {
					endpoint = ""
				}
				if err != nil || got.DockerEndpoint != endpoint || strings.Contains(strings.Join(got.Args(), " "), "--docker-host") != (endpoint != "") {
					t.Fatalf("upgrade endpoint=%q want=%q: %v", got.DockerEndpoint, endpoint, err)
				}
			}
		})
	}
}
