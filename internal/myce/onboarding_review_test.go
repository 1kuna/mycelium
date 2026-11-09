package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
