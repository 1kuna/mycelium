package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
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
	if e = os.WriteFile(c.Service, []byte("[Service]\nKillMode=mixed\nTimeoutStopSec=infinity\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = Install(context.Background(), c, executable, "changed-token", runner); e != nil {
		t.Fatal(e)
	}
	if got, e := ReadServiceToken(c); e != nil || got != token {
		t.Fatal("upgrade changed credentials", e)
	}
	text, e := os.ReadFile(c.Service)
	if e != nil || !strings.Contains(string(text), "KillMode=control-group") {
		t.Fatal("unit not upgraded", e)
	}
	old, e := os.ReadDir(filepath.Join(filepath.Dir(c.Service), ".Trash"))
	if e != nil || len(old) != 1 {
		t.Fatal("old unit not recoverable", e)
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

func TestDoctorLinuxLingering(t *testing.T) {
	for _, tc := range []struct {
		name, os, role, result, detail string
		err                            error
		ok, wantCheck                  bool
	}{
		{name: "worker enabled", os: "linux", role: "worker", result: "yes\n", ok: true, wantCheck: true, detail: "starts at boot"},
		{name: "queue disabled", os: "linux", role: "queue", result: "no\n", wantCheck: true, detail: "loginctl enable-linger " + strconv.Itoa(os.Getuid())},
		{name: "unavailable", os: "linux", role: "queue", err: errors.New("loginctl unavailable"), wantCheck: true, detail: "loginctl unavailable"},
		{name: "unknown", os: "linux", role: "queue", result: "", wantCheck: true, detail: "unexpected loginctl result"},
		{name: "macOS", os: "darwin", role: "queue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			machine := fakeProbe(tc.os, "amd64", "RTX 4090, 24564", "")
			base := machine.Command
			machine.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "loginctl" {
					return base(ctx, name, args...)
				}
				calls++
				want := "show-user " + strconv.Itoa(os.Getuid()) + " --property=Linger --value"
				if strings.Join(args, " ") != want {
					t.Fatalf("unexpected command loginctl %v", args)
				}
				return []byte(tc.result), tc.err
			}
			p := DoctorProbe{Machine: machine, NAS: func(context.Context, string) error { return nil }, Queue: func(context.Context, ServiceConfig) error { return nil }, Disk: func(string) (int64, error) { return 2 << 30, nil }}
			checks := Doctor(context.Background(), ServiceConfig{Role: tc.role, Detection: Detection{OS: tc.os, Platform: "cuda"}}, p)
			found := false
			for _, check := range checks {
				if check.Name == "Linux user lingering" {
					found = true
					if check.OK != tc.ok || !strings.Contains(check.Detail, tc.detail) {
						t.Fatal(check)
					}
				}
			}
			wantCalls := 0
			if tc.wantCheck {
				wantCalls = 1
			}
			if found != tc.wantCheck || calls != wantCalls {
				t.Fatalf("found=%v calls=%d checks=%v", found, calls, checks)
			}
		})
	}
	p := DoctorProbe{}
	for _, check := range Doctor(context.Background(), ServiceConfig{Detection: Detection{OS: "linux"}}, p) {
		if check.Name == "Linux user lingering" && (check.OK || !strings.Contains(check.Detail, "command probe missing")) {
			t.Fatal(check)
		}
	}
}

func TestInstallMissingRunnerDoesNotWrite(t *testing.T) {
	home := t.TempDir()
	c, err := PlanInstall("queue", home, "", filepath.Join(home, "nas"), "http://queue:7331", "queue", Detection{OS: "linux"})
	if err != nil {
		t.Fatal(err)
	}
	if err = Install(context.Background(), c, "missing-executable", "test-token", nil); err == nil || !strings.Contains(err.Error(), "runner unavailable") {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatalf("install wrote files: %v %v", entries, err)
	}
}

func TestDoctorNASMountFormats(t *testing.T) {
	for _, tc := range []struct {
		name, suffix string
		ok           bool
	}{
		{"Linux NFS", " type nfs (rw)", true},
		{"Linux SMB", " type cifs (rw)", true},
		{"macOS SMB", " (smbfs, rw)", true},
		{"local filesystem", " type ext4 (rw)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "nested")
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			p := LocalDoctorProbeWithMachine("", MachineProbe{Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "mount" {
					t.Fatalf("unexpected command %s", name)
				}
				return []byte("server:/share on " + root + tc.suffix), nil
			}})
			if err := p.NAS(context.Background(), path); (err == nil) != tc.ok {
				t.Fatalf("NAS probe: %v", err)
			}
		})
	}
}
