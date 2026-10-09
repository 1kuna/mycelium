package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fakeProbe(osname, arch, nvidia, intel string) MachineProbe {
	return MachineProbe{OS: osname, Arch: arch, Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "nvidia-smi":
			if nvidia != "" {
				return []byte(nvidia), nil
			}
		case "sycl-ls":
			if intel != "" {
				return []byte(intel), nil
			}
		case "system_profiler":
			return []byte("Chipset Model: Apple M4\nMetal Support: Metal 3"), nil
		case "uv":
			return []byte("uv 0.8"), nil
		}
		return nil, errors.New("not installed")
	}}
}
func TestInstallDetectionDryPlans(t *testing.T) {
	cases := []struct {
		name           string
		p              MachineProbe
		platform, mode string
		reserve        float64
	}{
		{"4090", fakeProbe("linux", "amd64", "NVIDIA GeForce RTX 4090, 24564", ""), "cuda", "dedicated", 2},
		{"Spark", fakeProbe("linux", "arm64", "NVIDIA GB10, [N/A]", ""), "cuda", "unified", 16},
		{"Intel", fakeProbe("linux", "amd64", "", "[level_zero:gpu] Intel(R) UHD Integrated Graphics"), "xpu", "unified", 8},
		{"Mac", fakeProbe("darwin", "arm64", "", ""), "metal", "unified", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, e := tc.p.Detect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			if d.Platform != tc.platform || d.MemoryMode != tc.mode || d.ReserveGB != tc.reserve {
				t.Fatal(d)
			}
			home := t.TempDir()
			c, e := PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue:7331", "worker", d)
			if e != nil {
				t.Fatal(e)
			}
			if strings.Contains(c.ServiceText(), "token") {
				t.Fatal("secret in unit")
			}
			if !strings.Contains(c.ServiceText(), "service") {
				t.Fatal("missing service command")
			}
			if _, e = os.Stat(c.Config); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("dry plan wrote config")
			}
			if e = Uninstall(context.Background(), c, true, func(context.Context, string, ...string) ([]byte, error) {
				t.Fatal("dry uninstall ran service command")
				return nil, nil
			}); e != nil {
				t.Fatal(e)
			}
		})
	}
	if _, e := fakeProbe("linux", "amd64", "", "").Detect(context.Background()); e == nil {
		t.Fatal("missing GPU accepted")
	}
}
func TestInstallAndUninstallTempOnly(t *testing.T) {
	home := t.TempDir()
	c, e := PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue:7331", "worker", Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "dedicated"})
	if e != nil {
		t.Fatal(e)
	}
	executable := filepath.Join(home, "test-executable")
	os.WriteFile(executable, []byte("test binary"), 0700)
	commands := []string{}
	runner := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil, nil
	}
	if e = Install(context.Background(), c, executable, "private-test-secret", runner); e != nil {
		t.Fatal(e)
	}
	if len(commands) != 2 {
		t.Fatal(commands)
	}
	token, e := ReadServiceToken(c)
	if e != nil || token != "private-test-secret" {
		t.Fatal(e)
	}
	if b, _ := os.ReadFile(c.Config); strings.Contains(string(b), token) {
		t.Fatal("token in public config")
	}
	if e = Install(context.Background(), c, executable, token, runner); e == nil {
		t.Fatal("overwrote existing installation")
	}
	if e = Uninstall(context.Background(), c, false, runner); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(c.Config); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("config remained")
	}
	entries, _ := os.ReadDir(filepath.Join(home, ".local", "share", "mycelium", ".Trash"))
	if len(entries) != 2 {
		t.Fatal("uninstall not recoverable")
	}
}
func TestDoctorChecks(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{Platform: "cuda"}, NAS: "/nas", State: "/cache"}
	p := DoctorProbe{Machine: fakeProbe("linux", "amd64", "RTX 4090, 24564", ""), NAS: func(context.Context, string) error { return nil }, Disk: func(string) (int64, error) { return 2 << 30, nil }, Queue: func(context.Context, ServiceConfig) error { return nil }}
	checks := Doctor(context.Background(), c, p)
	if len(checks) != 5 {
		t.Fatal(checks)
	}
	for _, v := range checks {
		if !v.OK {
			t.Fatal(v)
		}
	}
	p.NAS = func(context.Context, string) error { return errors.New("NAS offline") }
	p.Queue = func(context.Context, ServiceConfig) error { return errors.New("queue offline") }
	p.Disk = func(string) (int64, error) { return 1, nil }
	checks = Doctor(context.Background(), c, p)
	failed := 0
	for _, v := range checks {
		if !v.OK {
			failed++
		}
	}
	if failed != 3 {
		t.Fatal(checks)
	}
}

func TestInstallDiscreteIntelUsesConservativeDeviceMemory(t *testing.T) {
	p := MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "sycl-ls" {
			if len(args) > 0 {
				return []byte("global_mem_size: 68719476736\nglobal_mem_size: 17179869184\n"), nil
			}
			return []byte("[level_zero:gpu] Intel Data Center GPU Max"), nil
		}
		return nil, errors.New("absent")
	}}
	d, e := p.Detect(context.Background())
	if e != nil || d.Platform != "xpu" || d.MemoryMode != "fixed" || d.MemoryGB != 16 {
		t.Fatalf("%+v %v", d, e)
	}
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "sycl-ls" {
			return []byte("[opencl:cpu] Intel CPU\n[opencl:gpu] Other GPU"), nil
		}
		return nil, errors.New("absent")
	}
	if _, e = p.Detect(context.Background()); e == nil {
		t.Fatal("CPU vendor used as GPU vendor")
	}
}
