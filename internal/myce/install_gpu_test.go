package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
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
				if err := c.WorkerInstance(instance); err != nil {
					t.Fatal(err)
				}
				c.Device = "1"
			}
			binary := filepath.Join(home, "source")
			os.WriteFile(binary, []byte("fake"), 0700)
			checked, enabled := false, false
			runner := func(_ context.Context, name string, args ...string) ([]byte, error) {
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

func TestGPUDeviceAccessDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nodes, groups []string
		denied        bool
		want          string
	}{
		{name: "missing driver", want: "missing device nodes"},
		{name: "denied", nodes: []string{"/dev/dri/renderD129"}, denied: true, want: "sudo usermod"},
		{name: "accessible ACL", nodes: []string{"/dev/dri/renderD129"}},
		{name: "group alone insufficient", nodes: []string{"/dev/dri/renderD129"}, groups: []string{"render", "video"}, denied: true, want: "cannot open"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := GPUAccessProbe{Glob: func(pattern string) ([]string, error) {
				if pattern != "/dev/dri/renderD*" {
					t.Fatal(pattern)
				}
				return tc.nodes, nil
			}, Groups: func() ([]string, error) { return tc.groups, nil }, Open: func(string) error {
				if tc.denied {
					return os.ErrPermission
				}
				return nil
			}}
			err := p.Check("xpu")
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatal(err)
			}
		})
	}
}

func TestInstallWorkerRejectsNoDevicesAndRequiresExplicitOptOut(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(strconv.FormatBool(allow), func(t *testing.T) {
			home := t.TempDir()
			c, _ := PlanInstall("worker", home, "", home, "http://queue:7331", "worker", Detection{OS: "linux", Arch: "amd64", Platform: "fake"})
			c.AllowDeviceless = allow
			binary := filepath.Join(home, "source")
			os.WriteFile(binary, []byte("fake"), 0700)
			enabled := false
			err := Install(context.Background(), c, binary, "private", func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "systemd-run" {
					return []byte(`{}`), nil
				}
				if strings.Contains(strings.Join(args, " "), "enable --now") {
					enabled = true
				}
				return nil, nil
			})
			if allow {
				if err != nil || !enabled {
					t.Fatal(err, enabled)
				}
			} else if err == nil || enabled {
				t.Fatal("deviceless worker accepted", err, enabled)
			}
		})
	}
}

func TestDetectServiceSelectedXPUUsesWorkerBinding(t *testing.T) {
	t.Setenv("PATH", os.Getenv("PATH"))
	t.Setenv("ZE_AFFINITY_MASK", "old")
	t.Setenv("ONEAPI_DEVICE_SELECTOR", "old")
	c := ServiceConfig{Path: "/runtime/bin:/usr/bin", Device: "1", Detection: Detection{OS: "linux", Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16}}
	p := fakeProbe("linux", "amd64", "", "[level_zero:gpu] Intel Arc GPU")
	command := p.Command
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if os.Getenv("ZE_AFFINITY_MASK") != "1" || os.Getenv("ONEAPI_DEVICE_SELECTOR") != "level_zero:1" || os.Getenv("PATH") != c.Path {
			t.Fatal("service probe differs from job binding")
		}
		return command(ctx, name, args...)
	}
	access := GPUAccessProbe{Glob: func(string) ([]string, error) { return []string{"/dev/dri/renderD129"}, nil }, Groups: func() ([]string, error) { return []string{"render"}, nil }, Open: func(string) error { return nil }}
	d, err := DetectService(context.Background(), c, p, access)
	if err != nil || d.Platform != "xpu" || d.MemoryGB != 16 {
		t.Fatal(d, err)
	}
}
