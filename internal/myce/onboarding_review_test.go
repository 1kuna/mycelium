package myce

import (
	"context"
	"errors"
	"html"
	"os"
	"path/filepath"
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
func TestReviewIgnoredDisplayHasOneInformationalLine(t *testing.T) {
	p := reviewPCIProbe("0x1002", "0x73bf", "16")
	var lines []string
	p.Info = func(s string) { lines = append(lines, s) }
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err != nil || got.Platform != "cpu" || len(lines) != 1 || !strings.Contains(lines[0], "0x1002:0x73bf") {
		t.Fatal(got, lines, err)
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
