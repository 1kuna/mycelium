package myce

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestInstallXPUHealthAccess(t *testing.T) {
	for _, scenario := range []string{"install", "already provisioned", "denied", "queue", "cuda", "darwin", "machine xpu"} {
		t.Run(scenario, func(t *testing.T) {
			c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux", Platform: "xpu"}}
			switch scenario {
			case "queue":
				c.Role = "queue"
			case "cuda":
				c.Detection.Platform = "cuda"
			case "darwin":
				c.Detection.OS = "darwin"
			case "machine xpu":
				c.Detection.Platform = "cpu"
				c.MachineDevices = []DeviceInstall{{Detection: Detection{Platform: "xpu"}}}
			}
			calls := 0
			command := func(_ context.Context, name string, args ...string) ([]byte, error) {
				calls++
				if name != "sudo" || len(args) != 8 || args[0] != "-n" || args[2] != "/bin/sh" || args[6] != strconv.Itoa(os.Getgid()) || args[7] != xpuHealthRules(os.Getgid()) {
					t.Fatalf("unexpected installer command: %s %v", name, args)
				}
				if scenario == "denied" {
					return nil, errors.New("sensitive administrator diagnostic")
				}
				return nil, nil
			}
			read := func(string) ([]byte, error) {
				if scenario == "already provisioned" {
					return []byte(xpuHealthRules(os.Getgid())), nil
				}
				return nil, os.ErrNotExist
			}
			err := ensureXPUHealthAccess(t.Context(), c, command, read)
			if scenario == "denied" {
				if err == nil || !strings.Contains(err.Error(), "xpu_health_access_unavailable") || strings.Contains(err.Error(), "sensitive") {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			want := 1
			if scenario == "already provisioned" || scenario == "queue" || scenario == "cuda" || scenario == "darwin" {
				want = 0
			}
			if calls != want {
				t.Fatalf("provisioning calls=%d, want %d", calls, want)
			}
		})
	}
}

// These restrictions guard the privilege boundary: management-engine nodes and
// world access must never be included in the generated installer grant.
func TestInstallXPUHealthAccessScope(t *testing.T) {
	rules := xpuHealthRules(1234)
	lines := strings.Split(strings.TrimSpace(rules), "\n")
	if len(lines) != 3 || !strings.Contains(lines[1], `SUBSYSTEM=="mei"`) || !strings.Contains(lines[1], `DRIVERS=="mei_gsc"`) || !strings.Contains(lines[1], `GROUP="1234", MODE="0660"`) {
		t.Fatal("MEI grant must be restricted to GPU driver and worker group", rules)
	}
	if !strings.Contains(lines[2], `SUBSYSTEM=="intel_pmt"`) || !strings.Contains(lines[2], `DRIVERS=="xe"`) || !strings.Contains(lines[2], `/bin/chgrp 1234 /sys%p/telem`) || !strings.Contains(lines[2], `/bin/chmod 0440 /sys%p/telem`) {
		t.Fatal("PMT grant must be read-only and restricted to Xe telemetry", rules)
	}
}
