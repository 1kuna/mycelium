package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

type observerManagerExit int

func (e observerManagerExit) Error() string { return "fixture service absent" }
func (e observerManagerExit) ExitCode() int { return int(e) }

// Regression from the independent round-2 reproducer: real role publication /
// Trash operations, fake managers, and an executable client after role removal.
func TestObserverServiceSurvivesRoleUninstallAndReinstall(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	c, err := myce.PlanInstall("worker", home, filepath.Join(home, "install"), filepath.Join(home, "nas"), "http://127.0.0.1:7331", "fixture", myce.Detection{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu", MemoryMode: "fixed", MemoryGB: 1})
	if err != nil {
		t.Fatal(err)
	}
	c.AllowDeviceless = true
	c.RequireMountedNAS = false
	if err := os.Mkdir(c.NAS, 0700); err != nil {
		t.Fatal(err)
	}
	loaded := map[string]bool{}
	manager := func(_ context.Context, name string, args ...string) ([]byte, error) {
		// Installer filesystem operations are real; mount inventory is isolated
		// fixture evidence, just like the service managers below.
		if name == "myce:mounts-nowait" {
			return []byte("fixture on " + c.NAS + " (nfs, mounted)\n"), nil
		}
		if name == "cat" {
			return []byte("fixture " + c.NAS + " nfs rw 0 0\n"), nil
		}
		if name == "launchctl" {
			switch args[0] {
			case "print":
				if loaded[filepath.Base(args[1])] {
					return []byte("state = running\npid = 123\n"), nil
				}
				if strings.Count(args[1], "/") == 1 { // The GUI domain itself.
					return nil, nil
				}
				return nil, observerManagerExit(113)
			case "bootstrap":
				loaded[strings.TrimSuffix(filepath.Base(args[2]), ".plist")] = true
			case "bootout":
				delete(loaded, filepath.Base(args[1]))
			}
			return nil, nil
		}
		if name == "systemctl" {
			switch args[1] {
			case "show":
				if strings.Contains(strings.Join(args, " "), "UnitFileState") && loaded[args[2]] {
					return []byte("enabled"), nil
				}
			case "is-active":
				if !loaded[args[len(args)-1]] {
					return nil, observerManagerExit(3)
				}
			case "enable":
				loaded[args[len(args)-1]] = true
			case "disable":
				delete(loaded, args[len(args)-1])
			}
		}
		return nil, nil
	}
	observerManager := func(ctx context.Context, name string, args ...string) error {
		_, err := manager(ctx, name, args...)
		return err
	}
	root := filepath.Join(home, "registry")
	client := observerClientBinary(home)
	definition := filepath.Join(home, "Library", "LaunchAgents", observerServiceLabel+".plist")
	if runtime.GOOS == "linux" {
		definition = filepath.Join(home, "config", "systemd", "user", "myce-observer.service")
	}
	source := filepath.Join(home, "source-myce")
	install := func(version string) {
		t.Helper()
		release := "v1.0.0"
		if version == "second" {
			release = "v2.0.0"
		}
		script := fmt.Sprintf("#!/bin/sh\nif [ \"$2\" = \"--json\" ]; then printf '{\"version\":\"%s\",\"commit\":\"%s\"}'; else printf '%s'; fi\n", release, version, version)
		if err := os.WriteFile(source, []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		if err := myce.Install(context.Background(), c, source, "fixture", manager); err != nil {
			t.Fatal(err)
		}
		// This is the automatic service-start hook used after normal install and
		// upgrade; the fake manager admits it without launching a native service.
		if err := installObserverService(context.Background(), runtime.GOOS, home, c.Binary, root, nil, observerManager); err != nil {
			t.Fatal(err)
		}
	}
	checkClient := func(version string) {
		t.Helper()
		data, err := os.ReadFile(definition)
		if err != nil || !strings.Contains(string(data), client) || strings.Contains(string(data), c.Binary) {
			t.Fatal("supervisor still depends on the removable role slot", err)
		}
		cmd := exec.Command(client, "version")
		cmd.Env = observerEnvironment()
		out, err := cmd.Output()
		if err != nil || string(out) != version {
			t.Fatal("supervisor client is absent, stale or unexecutable", err)
		}
	}
	install("first")
	checkClient("first")
	receiptDir := observerLifecycleArm(t, root)
	arm := filepath.Join(receiptDir, "receipt.json")
	before, err := os.ReadFile(arm)
	if err != nil {
		t.Fatal(err)
	}
	if err := setupCommandWithProbe([]string{"uninstall", "--config", c.Config}, myce.MachineProbe{OS: runtime.GOOS, Command: manager}, home); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.Binary); !os.IsNotExist(err) {
		t.Fatal("ordinary uninstall did not remove the role binary", err)
	}
	checkClient("first")
	install("second")
	checkClient("second")
	if data, err := os.ReadFile(arm); err != nil || string(data) != string(before) {
		t.Fatal("role lifecycle changed a retained registration", err)
	}
}
