package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestHostCommandsOfflineAndServiceRestart(t *testing.T) {
	for _, system := range []string{"linux", "darwin"} {
		t.Run(system, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("MYCE_TOKEN", "")
			t.Setenv("MYCE_HOST_STATE_DIR", filepath.Join(home, "host"))
			c, err := myce.PlanInstall("worker", home, "", home, "http://127.0.0.1:1", "worker", myce.Detection{OS: system, Platform: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			os.MkdirAll(filepath.Dir(c.Config), 0700)
			os.MkdirAll(filepath.Dir(c.Service), 0700)
			b, _ := json.Marshal(c)
			os.WriteFile(c.Config, b, 0600)
			os.WriteFile(c.Service, []byte(c.ServiceText()), 0600)
			var calls []string
			probe := myce.MachineProbe{OS: system, Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
				calls = append(calls, n+" "+strings.Join(args, " "))
				return nil, nil
			}}
			var out bytes.Buffer
			if err = hostCommand([]string{"disable", "--now", "--reason", "Verso drill"}, probe, &out); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "stopped 0 owned process groups; unconfirmed: none") {
				t.Fatal(out.String())
			}
			// Both direct join and service start must exit successfully before token/GPU checks.
			if err = runWithProbe([]string{"join"}, myce.MachineProbe{}); err != nil {
				t.Fatal(err)
			}
			if err = serviceCommand([]string{"--config", c.Config}); err != nil {
				t.Fatal(err)
			}
			if len(calls) != 1 || strings.Contains(calls[0], "queue") {
				t.Fatalf("wrong service scope: %v", calls)
			}
			if err = hostCommand([]string{"enable"}, probe, &out); err != nil {
				t.Fatal(err)
			}
			if len(calls) != 2 {
				t.Fatalf("no service restart: %v", calls)
			}
			if reason, err := myce.HostDisabled(""); err != nil || reason != "" {
				t.Fatalf("flag retained %q %v", reason, err)
			}
		})
	}
}

func TestSafeResumeCommandNeverRecordsToken(t *testing.T) {
	got := strings.Join(safeJoinCommand([]string{"join", "--token=secret", "--name", "worker", "--token", "another-secret", "--gpu", "fake"}), " ")
	if got != "myce join --name worker --gpu fake" {
		t.Fatalf("unsafe resume command %q", got)
	}
}
