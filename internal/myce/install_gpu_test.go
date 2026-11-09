package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallWorkerRuntimeAndServiceDetection(t *testing.T) {
	for _, instance := range []string{"", "gpu1"} {
		t.Run(instance, func(t *testing.T) {
			t.Setenv("LD_LIBRARY_PATH", "/opt/intel/lib:with space")
			t.Setenv("ZE_ENABLE_PCI_ID_DEVICE_ORDER", "1")
			t.Setenv("MYCE_TOKEN", "must-not-copy")
			t.Setenv("ZE_SECRET", "must-not-copy")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "must-not-copy")
			home := filepath.Join(t.TempDir(), "home%test")
			if err := os.MkdirAll(home, 0700); err != nil {
				t.Fatal(err)
			}
			c, _ := PlanInstall("worker", home, "", home, "http://queue:7331", "worker", Detection{OS: "linux", Arch: "amd64", Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16})
			if instance != "" {
				// A machine worker can offer only card 1 without a split unit.
				c.Device = "1"
			}
			binary := filepath.Join(home, "source")
			os.WriteFile(binary, []byte("fake"), 0700)
			checked, enabled := false, false
			runner := func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "cat" {
					return []byte("fixture " + c.NAS + " nfs rw 0 0\n"), nil
				}
				if name == "systemd-run" {
					checked = true
					wantProperty := "--property=EnvironmentFile=" + c.GPUEnvironmentFile()
					found := false
					for _, arg := range args {
						if arg == wantProperty {
							found = true
						}
					}
					if !found {
						t.Fatalf("systemd-run requires the literal EnvironmentFile path, not unit-file quotes: %v", args)
					}
					text, err := os.ReadFile(filepath.Join(filepath.Dir(c.Config), "gpu.env"))
					if err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(string(text), `LD_LIBRARY_PATH="/opt/intel/lib:with space"`) || !strings.Contains(string(text), `ZE_ENABLE_PCI_ID_DEVICE_ORDER="1"`) || strings.Contains(string(text), "must-not-copy") {
						t.Fatalf("unsafe or missing runtime environment: %s", text)
					}
					unit, _ := os.ReadFile(c.Service)
					if !strings.Contains(string(unit), "EnvironmentFile=") {
						t.Fatal("unit does not load runtime environment")
					}
					joined := strings.Join(args, " ")
					for _, want := range []string{"--user", "--wait", "--pipe", "EnvironmentFile=", c.Binary, "detect --config", c.Config} {
						if !strings.Contains(joined, want) {
							t.Fatalf("missing %q: %s", want, joined)
						}
					}
					b, _ := json.Marshal(c.Detection)
					return b, nil
				}
				if strings.Contains(strings.Join(args, " "), "enable --now") {
					if !checked {
						t.Fatal("enabled before detection")
					}
					enabled = true
				}
				return nil, nil
			}
			if err := Install(context.Background(), c, binary, "private-token", runner); err != nil {
				t.Fatal(err)
			}
			if !checked || !enabled {
				t.Fatalf("checked=%v enabled=%v", checked, enabled)
			}
			// A failed upgrade also refuses acceptance and retains its saved token.
			err := Install(context.Background(), c, binary, "changed", func(_ context.Context, n string, args ...string) ([]byte, error) {
				if n == "cat" {
					return []byte("fixture " + c.NAS + " nfs rw 0 0\n"), nil
				}
				if n == "systemd-run" {
					return nil, errors.New("no devices")
				}
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), "worker service GPU validation failed") {
				t.Fatal(err)
			}
			if token, _ := ReadServiceToken(c); token != "private-token" {
				t.Fatal("upgrade replaced token")
			}
		})
	}
}
