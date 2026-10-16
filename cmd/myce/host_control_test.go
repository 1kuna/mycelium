package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
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
				if system == "linux" && len(args) > 1 && args[1] == "show" {
					return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
				}
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
			expected := 1
			if system == "linux" {
				expected = 3
			}
			if len(calls) != expected || strings.Contains(calls[0], "queue") {
				t.Fatalf("wrong service scope: %v", calls)
			}
			if err = hostCommand([]string{"enable"}, probe, &out); err != nil {
				t.Fatal(err)
			}
			if len(calls) != expected+1 {
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

func TestDisableWritesFlagBeforeBrokenInstallConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("MYCE_HOST_STATE_DIR", filepath.Join(home, "host"))
	config := filepath.Join(home, "broken-config.json")
	if err := os.WriteFile(config, []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hostCommand([]string{"disable", "--now", "--config", config}, myce.MachineProbe{OS: "linux"}, &out); err == nil {
		t.Fatal("broken config succeeded")
	}
	if reason, err := myce.HostDisabled(""); err != nil || reason == "" {
		t.Fatalf("config failure prevented local fence: %q %v", reason, err)
	}
}

func TestHostCommandsInstanceSelection(t *testing.T) {
	for _, target := range []string{"", "myce-worker@gpu1.service"} {
		t.Run(target, func(t *testing.T) {
			home := t.TempDir()
			dir := filepath.Join(home, "host")
			t.Setenv("HOME", home)
			t.Setenv("MYCE_HOST_STATE_DIR", dir)
			t.Setenv("MYCE_TOKEN", "")
			t.Setenv("PATH", t.TempDir())
			for _, u := range []string{"myce-worker@gpu0.service", "myce-worker@gpu1.service", "verso.service"} {
				if err := myce.AdoptHostWorker(dir, myce.HostWorker{State: t.TempDir(), Unit: u}); err != nil {
					t.Fatal(err)
				}
			}
			var calls []string
			p := myce.MachineProbe{OS: "linux", Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
				calls = append(calls, strings.Join(args, " "))
				return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
			}}
			args := []string{"disable", "--now"}
			if target != "" {
				args = append(args, "--unit", target)
			}
			var out bytes.Buffer
			if err := hostCommand(args, p, &out); err != nil {
				t.Fatal(err, out.String())
			}
			joined := strings.Join(calls, "\n")
			if strings.Contains(joined, "verso.service") || !strings.Contains(joined, "myce-worker@gpu1.service") {
				t.Fatal(joined)
			}
			if strings.Contains(joined, "myce-worker@gpu0.service") != (target == "") {
				t.Fatal(joined)
			}
			if target != "" {
				if reason, err := myce.HostDisabled(dir); err != nil || reason != "" {
					t.Fatal("target fenced whole host", reason, err)
				}
				other, _ := myce.WorkerUnitDirectory(dir, "myce-worker@gpu0.service")
				if reason, err := myce.HostDisabled(other); err != nil || reason != "" {
					t.Fatal("target fenced sibling", reason, err)
				}
			}
			args[0] = "enable"
			calls = nil
			if err := hostCommand(args, p, &out); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(calls, "\n"), "start myce-worker@gpu1.service") {
				t.Fatal(calls)
			}
			if err := hostCommand([]string{"disable", "--unit", "myce-worker@unknown.service"}, p, &out); err == nil {
				t.Fatal("unknown target accepted")
			}
		})
	}
}

func TestHostCommandUnitNotifiesQueue(t *testing.T) {
	for _, unit := range []string{"myce-worker.service", "myce-worker@gpu0.service"} {
		t.Run(unit, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("MYCE_TOKEN", "test")
			t.Setenv("PATH", t.TempDir())
			dir := filepath.Join(home, "host")
			q, err := myce.Open(filepath.Join(home, "queue.db"), home, myce.RealClock{})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			server := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			m := myce.Machine{ID: "gpu0", HostID: "host", Session: "session", GPU: "fake", FreeMemoryGB: 8}
			if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
				t.Fatal(err)
			}
			state := t.TempDir()
			if err := os.WriteFile(filepath.Join(state, "state.json"), []byte(`{"session":"session"}`), 0600); err != nil {
				t.Fatal(err)
			}
			if err := myce.AdoptHostWorker(dir, myce.HostWorker{State: state, Unit: unit, Name: m.ID, Host: m.HostID, Queue: server.URL}); err != nil {
				t.Fatal(err)
			}
			p := myce.MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
				return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
			}}
			var out bytes.Buffer
			if err := hostCommand([]string{"disable", "--now", "--host-state-dir", dir, "--unit", unit}, p, &out); err != nil {
				t.Fatal(err)
			}
			snap, err := q.Snapshot()
			if err != nil || len(snap.Machines) != 1 || snap.Machines[0].Disabled == "" {
				t.Fatalf("unit notification missing: %+v %v", snap.Machines, err)
			}
		})
	}
}
