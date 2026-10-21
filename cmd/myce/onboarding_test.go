package main

import (
	"context"
	"encoding/json"
	"errors"
	"mycelium/internal/myce"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOnboardingInstallWholeMachineSelectorsAndDriverRefusal(t *testing.T) {
	home := t.TempDir()
	fail := false
	p := myce.MachineProbe{OS: "linux", Arch: "arm64", Glob: func(pattern string) ([]string, error) {
		if strings.HasSuffix(pattern, "/class") {
			return []string{"/fixture/pci/0000:01:00.0/class", "/fixture/pci/0000:02:00.0/class"}, nil
		}
		return nil, nil
	}, ReadFile: func(path string) ([]byte, error) {
		switch filepath.Base(path) {
		case "class":
			return []byte("0x030000"), nil
		case "current_link_width":
			return []byte("16"), nil
		case "vendor":
			return []byte("0x10de"), nil
		}
		return nil, os.ErrNotExist
	}, Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
		if n != "nvidia-smi" {
			return nil, exec.ErrNotFound
		}
		if fail {
			return nil, errors.New("private-value")
		}
		if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
			return []byte("GPU-a, driver, 0000:01:00.0\nGPU-b, driver, 0000:02:00.0"), nil
		}
		return []byte("GPU, 24576"), nil
	}}
	text := captureJoiningOutput(t, func() error {
		return setupCommandWithProbe([]string{"install", "worker", "--device=1,0", "--nas", home, "--dry-run"}, p, home)
	})
	for _, want := range []string{"--device", `1,0`, "--device-settings", "GPU-a", "GPU-b"} {
		if !strings.Contains(text, want) {
			t.Fatalf("fresh plan omitted %s", want)
		}
	}
	fail = true
	err := setupCommandWithProbe([]string{"install", "worker", "--dry-run"}, p, home)
	var typed *myce.PreflightError
	if !errors.As(err, &typed) || typed.Code != "gpu_driver_unavailable" || strings.Contains(err.Error(), "private-value") {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(home)
	if len(entries) != 0 {
		t.Fatal("blocked install saved CPU or wrote config")
	}
}
func TestOnboardingInstallRedetectionFromSavedCPU(t *testing.T) {
	home := t.TempDir()
	c, _ := myce.PlanInstall("worker", home, "", home, "http://queue.invalid", "hypha", myce.Detection{OS: "darwin", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
	c.HostID = "saved-host"
	c.DailyUse, c.Laptop = true, true
	c.CPUReserveCores = 0
	os.MkdirAll(filepath.Dir(c.Config), 0700)
	b, _ := json.Marshal(c)
	os.WriteFile(c.Config, b, 0600)
	failed := false
	p := myce.MachineProbe{OS: "darwin", Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) {
		if failed {
			return nil, errors.New("driver failed")
		}
		return []byte("Metal Support: Metal 3"), nil
	}}
	text := captureJoiningOutput(t, func() error {
		return setupCommandWithProbe([]string{"install", "worker", "--config", c.Config, "--redetect", "--dry-run"}, p, home)
	})
	for _, want := range []string{"platform=metal", "unified", "--daily-use=true", "--laptop=true", "saved-host"} {
		if !strings.Contains(text, want) {
			t.Fatal("redetection lost state", want, text)
		}
	}
	failed = true
	if err := setupCommandWithProbe([]string{"install", "worker", "--config", c.Config, "--redetect", "--dry-run"}, p, home); err == nil {
		t.Fatal("broken driver accepted")
	}
	after, _ := os.ReadFile(c.Config)
	if string(after) != string(b) {
		t.Fatal("re-detection rewrote saved CPU before acceptance")
	}
}
