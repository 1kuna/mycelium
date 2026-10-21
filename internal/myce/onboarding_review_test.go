package myce

import (
	"context"
	"errors"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestReviewUnsupportedDisplayUsesCPU(t *testing.T) {
	for _, tc := range []struct{ name, vendor, device, link string }{
		{"ASPEED", "0x1a03", "0x2000", "0"},
		{"virtio", "0x1af4", "0x1050", "0"},
		{"AMD discrete", "0x1002", "0x73bf", "16"},
		{"Intel integrated", "0x8086", "0x9a49", "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := reviewPCIProbe(tc.vendor, tc.device, tc.link)
			got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			if err != nil || got.Platform != "cpu" {
				t.Fatalf("want CPU, got %s %v", got.Platform, err)
			}
			_, err = p.Detect(context.Background())
			if !errors.Is(err, ErrNoGPU) {
				t.Fatalf("Detect: %v", err)
			}
		})
	}
}
func reviewPCIProbe(vendor, device, link string) MachineProbe {
	p := onboardingProbe("cpu")
	p.Glob = func(pattern string) ([]string, error) {
		if strings.HasSuffix(pattern, "/class") {
			return []string{"/fixture/pci/0000:01:00.0/class"}, nil
		}
		return nil, nil
	}
	p.ReadFile = func(path string) ([]byte, error) {
		switch filepath.Base(path) {
		case "class":
			return []byte("0x030000"), nil
		case "vendor":
			return []byte(vendor), nil
		case "device":
			return []byte(device), nil
		case "current_link_width":
			return []byte(link), nil
		}
		return nil, os.ErrNotExist
	}
	return p
}
func TestReviewSupportedGPUWithoutRuntime(t *testing.T) {
	for _, tc := range []struct{ name, vendor, device, link string }{{"NVIDIA", "0x10de", "0x2b85", "16"}, {"Intel Arc", "0x8086", "0x56a0", "8"}} {
		t.Run(tc.name, func(t *testing.T) {
			p := reviewPCIProbe(tc.vendor, tc.device, tc.link)
			_, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			requirePreflightCode(t, err, "gpu_driver_missing")
		})
	}
}
func TestReviewMissingMetalIsTyped(t *testing.T) {
	p := onboardingProbe("cpu")
	p.OS = "darwin"
	_, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	requirePreflightCode(t, err, "gpu_driver_unavailable")
}
func TestReviewNASNeverAdoptsCandidate(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	candidate := c.NAS
	c.NAS = "/mnt/nas"
	c.Checkpoints = filepath.Join(c.NAS, "mycelium", "checkpoints")
	c.ModelRoot = filepath.Join(c.NAS, "mycelium", "models")
	c.EnvironmentRoot = filepath.Join(c.NAS, "mycelium", "environments")
	got, err := PrepareOnboardingStorage(context.Background(), c, true, command)
	requirePreflightCode(t, err, "nas_not_configured")
	if got.NAS != c.NAS || !strings.Contains(err.Error(), candidate) || !strings.Contains(err.Error(), "--nas") {
		t.Fatal("missing explicit selection hint", got.NAS, err)
	}
}
func TestReviewNetworkMountClasses(t *testing.T) {
	for _, kind := range []string{"autofs", "fuse.sshfs", "fuse.rclone", "fuse.smbnetfs"} {
		t.Run(kind, func(t *testing.T) {
			roots, err := MountedNASRoots(context.Background(), "linux", func(context.Context, string, ...string) ([]byte, error) {
				return []byte("source /fixture/nas " + kind + " rw 0 0"), nil
			})
			if err != nil || len(roots) != 1 {
				t.Fatal(roots, err)
			}
		})
	}
}
func TestReviewLinuxServiceRetainsTypedFailure(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux", Platform: "cuda"}}
	err := ValidateWorkerServiceEnvironment(context.Background(), c, RealClock{}, func(context.Context, string, ...string) ([]byte, error) {
		return []byte("2026/10/21 04:00:00 gpu_permission_denied: gpu: private-value"), errors.New("exit 1")
	})
	requirePreflightCode(t, err, "gpu_permission_denied")
	if strings.Contains(err.Error(), "private-value") {
		t.Fatal("stderr leaked")
	}
}
func TestReviewLaunchdServiceRetainsTypedFailure(t *testing.T) {
	c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "darwin", Arch: "arm64", Platform: "metal", MemoryMode: "unified", ReserveGB: 4})
	if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "bootstrap" {
			b, err := os.ReadFile(args[2])
			if err != nil {
				return nil, err
			}
			_, tail, _ := strings.Cut(string(b), "<key>StandardErrorPath</key><string>")
			path, _, _ := strings.Cut(tail, "</string>")
			if path != "/dev/null" {
				if err = os.WriteFile(html.UnescapeString(path), []byte("2026/10/21 04:00:00 gpu_driver_unavailable: metal: private-value"), 0600); err != nil {
					return nil, err
				}
			}
			cancel()
		}
		return nil, nil
	}
	err := ValidateWorkerServiceEnvironment(ctx, c, RealClock{}, command)
	requirePreflightCode(t, err, "gpu_driver_unavailable")
	if strings.Contains(err.Error(), "private-value") {
		t.Fatal("stderr leaked")
	}
}
func TestReviewProbeCommandsStripToken(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "synthetic-test-secret")
	dir := t.TempDir()
	// Exercise the real prerequisite runner with an owned fake uv command.
	// Hardware probe commands remain guarded in tests.
	path := filepath.Join(dir, "uv")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nif [ \"${MYCE_TOKEN+x}\" = x ]; then exit 9; fi\nprintf 'uv 0.8.22\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p := LocalMachineProbe()
	for _, override := range []bool{false, true} {
		var err error
		if override {
			_, err = p.CommandEnv(context.Background(), map[string]string{"MYCE_TOKEN": "must-not-pass"}, path)
		} else {
			_, err = p.Command(context.Background(), path, "--version")
		}
		if err != nil {
			t.Errorf("probe inherited token (override=%v): %v", override, err)
		}
	}
}
func TestReviewGeneratedPlistsLint(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("plutil is a macOS system tool")
	}
	c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "darwin", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
	if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	lint := func(path string) {
		t.Helper()
		if b, err := exec.Command("/usr/bin/plutil", "-lint", path).CombinedOutput(); err != nil {
			t.Fatalf("plutil: %s: %v", b, err)
		}
	}
	if err := os.WriteFile(c.Service, []byte(c.ServiceText()), 0600); err != nil {
		if err = os.MkdirAll(filepath.Dir(c.Service), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(c.Service, []byte(c.ServiceText()), 0600); err != nil {
			t.Fatal(err)
		}
	}
	lint(c.Service)
	err := ValidateWorkerServiceEnvironment(context.Background(), c, RealClock{}, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "bootstrap" {
			lint(args[2])
			return nil, completeLaunchdProbe(t, args[2], c)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReviewIgnoredDisplayHasOneInformationalLine(t *testing.T) {
	p := reviewPCIProbe("0x1002", "0x73bf", "16")
	var lines []string
	p.Info = func(s string) { lines = append(lines, s) }
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err != nil || got.Platform != "cpu" || len(lines) != 1 || !strings.Contains(lines[0], "0x1002:0x73bf") {
		t.Fatal(got, lines, err)
	}
}
func TestReviewIntelRenderNodeWithoutOneAPI(t *testing.T) {
	p := reviewPCIProbe("0x8086", "0x56a0", "8")
	original := p.Glob
	p.Glob = func(pattern string) ([]string, error) {
		if strings.Contains(pattern, "renderD") {
			return []string{"/dev/dri/renderD128"}, nil
		}
		return original(pattern)
	}
	p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
		return nil, exec.ErrNotFound
	}
	_, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	requirePreflightCode(t, err, "gpu_driver_missing")
}
func TestReviewOtherRenderNodesDoNotRequireIntelRuntime(t *testing.T) {
	for _, vendor := range []string{"0x1a03", "0x1af4", "0x1002"} {
		p := reviewPCIProbe(vendor, "0x0001", "0")
		original := p.Glob
		p.Glob = func(pattern string) ([]string, error) {
			if strings.Contains(pattern, "renderD") {
				return []string{"/dev/dri/renderD128"}, nil
			}
			return original(pattern)
		}
		p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
			t.Fatal("unsupported render node invoked Intel runtime")
			return nil, nil
		}
		got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
		if err != nil || got.Platform != "cpu" {
			t.Fatal(vendor, got, err)
		}
	}
}
func TestReviewServiceStderrIsBoundedAndUnknownReasonsStayGeneric(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux", Platform: "cuda"}}
	for _, output := range []string{"private-value: cannot start", strings.Repeat("x", 4096) + "\ngpu_driver_missing: private-value"} {
		err := ValidateWorkerServiceEnvironment(context.Background(), c, RealClock{}, func(context.Context, string, ...string) ([]byte, error) {
			return []byte(output), errors.New("private-value")
		})
		requirePreflightCode(t, err, "service_environment_invalid")
		if strings.Contains(err.Error(), "private-value") {
			t.Fatal("stderr escaped")
		}
	}
}

func TestReviewLocalProbeCaptureIsBounded(t *testing.T) {
	p := LocalMachineProbe()
	path := filepath.Join(t.TempDir(), "uv")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec /usr/bin/awk \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	// os/exec copies from a pipe. This also catches io.Copy optimizations that
	// could bypass a bounded writer through a promoted bytes.Buffer.ReadFrom.
	output, err := p.Command(context.Background(), path, `BEGIN {for(i=0;i<1100000;i++) printf "x"; for(i=0;i<8192;i++) printf "e" > "/dev/stderr"; exit 1}`)
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("want child exit, got %v", err)
	}
	if len(output) != 1<<20 || len(exit.Stderr) != 4096 {
		t.Fatalf("probe capture escaped bounds: stdout=%d stderr=%d", len(output), len(exit.Stderr))
	}
}
