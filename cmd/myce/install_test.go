package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestInstallCLIDryRunFakeDetection(t *testing.T) {
	for _, platform := range []string{"cuda", "xpu", "metal"} {
		t.Run(platform, func(t *testing.T) {
			home := t.TempDir()
			p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
				switch n {
				case "nvidia-smi":
					if platform == "cuda" {
						return []byte("RTX 4090, 24576"), nil
					}
				case "sycl-ls":
					if platform == "xpu" {
						return []byte("[level_zero:gpu] Intel UHD Integrated GPU"), nil
					}
				case "system_profiler":
					if platform == "metal" {
						return []byte("Metal Support: Metal 3"), nil
					}
				}
				if n == "systemctl" || n == "launchctl" {
					t.Fatal("dry run touched service")
				}
				return nil, errors.New("absent")
			}}
			if platform == "metal" {
				p.OS = "darwin"
				p.Arch = "arm64"
			}
			if e := setupCommandWithProbe([]string{"install", "worker", "--nas", filepath.Join(home, "nas"), "--dry-run"}, p, home); e != nil {
				t.Fatal(e)
			}
			entries, _ := os.ReadDir(home)
			if len(entries) != 0 {
				t.Fatal("dry run wrote files")
			}
		})
	}
	home := t.TempDir()
	if e := setupCommandWithProbe([]string{"install", "queue", "--nas", filepath.Join(home, "nas"), "--dry-run"}, myce.MachineProbe{OS: "linux", Arch: "arm64"}, home); e != nil {
		t.Fatal(e)
	}
}

func TestInstallCLIStoresAbsoluteCatalogPaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("MYCE_TOKEN", "test-only-token")
	p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, nil }}
	if err := setupCommandWithProbe([]string{"install", "queue", "--nas", home, "--model-config", "models.yaml", "--environment-config", "environments/catalog.yaml"}, p, home); err != nil {
		t.Fatal(err)
	}
	c, err := myce.ReadServiceConfig(filepath.Join(home, ".local", "share", "mycelium", "queue", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	models, _ := filepath.Abs("models.yaml")
	environments, _ := filepath.Abs("environments/catalog.yaml")
	if c.ModelCatalog != models || c.EnvironmentCatalog != environments || c.EnvironmentRoot != filepath.Dir(environments) {
		t.Fatalf("relative service paths: %+v", c)
	}
}

func TestDoctorCLIUsesInjectedRunner(t *testing.T) {
	home := t.TempDir()
	t.Setenv("MYCE_TOKEN", "test-only-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/status" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := myce.ServiceConfig{Role: "queue", Detection: myce.Detection{OS: "linux"}, Queue: server.URL, NAS: home, State: home}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, "config.json")
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	lingering := "yes\n"
	calls := map[string]int{}
	p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		calls[name]++
		switch name {
		case "mount":
			return []byte("server:/nas on " + home + " type nfs (rw)\n"), nil
		case "uv":
			return []byte("uv test-version"), nil
		case "loginctl":
			return []byte(lingering), nil
		default:
			t.Fatalf("unexpected live command %s %v", name, args)
			return nil, errors.New("unexpected command")
		}
	}}
	if err = setupCommandWithProbe([]string{"doctor", "--config", path}, p, home); err != nil {
		t.Fatal(err)
	}
	lingering = "no\n"
	if err = setupCommandWithProbe([]string{"doctor", "--config", path}, p, home); err == nil || !strings.Contains(err.Error(), "doctor found problems") {
		t.Fatal(err)
	}
	for _, name := range []string{"mount", "uv", "loginctl"} {
		if calls[name] != 2 {
			t.Fatalf("%s calls=%d", name, calls[name])
		}
	}
}

func TestSetupCLIRejectsMissingOrUnknownAction(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}} {
		if err := setupCommandWithProbe(args, myce.MachineProbe{}, t.TempDir()); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
