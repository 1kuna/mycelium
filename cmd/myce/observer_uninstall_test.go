package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// Adapted from the independent round-3 probe. Exercise the real service-start
// hook with version --json fixtures, including an older/reused release restart.
func TestObserverOlderRoleStartupDoesNotDowngradeClient(t *testing.T) {
	// serviceCommand applies the saved role PATH to this process.
	t.Setenv("PATH", os.Getenv("PATH"))
	home := t.TempDir()
	tools := filepath.Join(home, "tools")
	if err := os.MkdirAll(tools, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"launchctl", "systemctl"} {
		if err := os.WriteFile(filepath.Join(tools, name), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for key, value := range map[string]string{"HOME": home, "XDG_CONFIG_HOME": filepath.Join(home, "config"), "XDG_CACHE_HOME": filepath.Join(home, "cache"), "MYCE_DEV_SESSION_ROOT": "", "MYCE_PROCESS_SESSION": ""} {
		t.Setenv(key, value)
	}
	var publishedHash string
	for _, image := range []struct{ role, version, commit string }{{"queue", "v2.0.0", "new-queue"}, {"worker", "v1.0.0", "old-worker"}, {"worker", "v2.0.0", "reused-release"}} {
		binary := filepath.Join(home, image.role, "myce")
		if err := os.MkdirAll(filepath.Dir(binary), 0700); err != nil {
			t.Fatal(err)
		}
		info, _ := json.Marshal(myce.VersionInfo{Version: image.version, Commit: image.commit})
		if err := os.WriteFile(binary, []byte(fmt.Sprintf("#!/bin/sh\nprintf '%%s' '%s'\n", info)), 0700); err != nil {
			t.Fatal(err)
		}
		c := myce.ServiceConfig{Version: 1, Role: image.role, Binary: binary, Credentials: filepath.Join(home, "absent"), Config: filepath.Join(home, image.role, "config.json"), Path: tools + ":/usr/bin:/bin"}
		if err := saveProfileJSON(c.Config, c); err != nil {
			t.Fatal(err)
		}
		if err := serviceCommand([]string{"--config", c.Config}); err == nil || !strings.Contains(err.Error(), "service credentials require a literal EnvironmentFile") {
			t.Fatal("expected credential refusal after setup", err)
		}
		hash, err := myce.FileSHA256(observerClientBinary(home))
		if err != nil {
			t.Fatal(err)
		}
		if publishedHash == "" {
			publishedHash = hash
		}
		if hash != publishedHash {
			t.Fatal("older or reused role release replaced the upgraded shared client")
		}
	}
}

// Adapt the reviewer's filesystem reproducer to the public uninstall boundary:
// setupCommand owns shared-observer cleanup after the role installer retires it.
func TestObserverFullUninstallRemovesSupervision(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		for _, retained := range []string{"none", "sibling", "armed"} {
			t.Run(platform+"/"+retained, func(t *testing.T) {
				home := t.TempDir()
				t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
				c, err := myce.PlanInstall("worker", home, filepath.Join(home, "install"), filepath.Join(home, "nas"), "http://127.0.0.1:7331", "fixture", myce.Detection{OS: platform, Arch: "arm64", Platform: "cpu"})
				if err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{c.Binary, c.Service} {
					if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
						t.Fatal(err)
					}
				}
				if err := saveProfileJSON(c.Config, c); err != nil {
					t.Fatal(err)
				}
				root := filepath.Join(home, "registry")
				var calls []string
				observerStopped := false
				manager := func(_ context.Context, name string, args ...string) ([]byte, error) {
					calls = append(calls, name+" "+strings.Join(args, " "))
					if name == "launchctl" && strings.HasSuffix(args[1], "/"+observerServiceLabel) {
						if args[0] == "bootout" {
							observerStopped = true
						}
						if args[0] == "print" && observerStopped {
							return nil, observerManagerExit(113)
						}
					}
					if name == "launchctl" && args[0] == "print" && strings.HasSuffix(args[1], "/local.mycelium.worker") {
						return nil, observerManagerExit(113)
					}
					return nil, nil
				}
				run := func(ctx context.Context, name string, args ...string) error {
					_, err := manager(ctx, name, args...)
					return err
				}
				if err := installObserverService(context.Background(), platform, home, c.Binary, root, nil, run); err != nil {
					t.Fatal(err)
				}
				if retained == "sibling" {
					name := "local.mycelium.queue.plist"
					if platform == "linux" {
						name = "myce-queue.service"
					}
					if err := os.WriteFile(filepath.Join(filepath.Dir(c.Service), name), []byte("fixture sibling"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if retained == "armed" {
					observerLifecycleArm(t, root)
				}
				if err := setupCommandWithProbe([]string{"uninstall", "--config", c.Config}, myce.MachineProbe{OS: platform, Command: manager}, home); err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{observerServicePath(platform, home), observerClientBinary(home)} {
					_, err := os.Stat(path)
					if retained == "none" && !os.IsNotExist(err) {
						t.Fatal("full uninstall retained supervision/client", err)
					}
					if retained != "none" && err != nil {
						t.Fatal("role uninstall removed an independent consumer's supervision", err)
					}
				}
				if retained == "armed" {
					before := len(calls)
					if err := uninstallObserverService(context.Background(), platform, home, "", run); err == nil || !strings.HasPrefix(err.Error(), "observer_registrations_armed:") {
						t.Fatal("explicit removal did not preserve armed work", err)
					}
					if len(calls) != before {
						t.Fatal("armed refusal touched the manager")
					}
				}
			})
		}
	}
}

func observerLifecycleArm(t *testing.T, root string) string {
	t.Helper()
	dir := filepath.Join(root, "receipt")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), profileReceipt{Observation: "armed", ArmNonce: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := saveProfileJSON(filepath.Join(root, "fixture.json"), sharedProfileRegistration{Directory: dir}); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestObserverExplicitUninstallCommand(t *testing.T) {
	binary := devTestBinary(t)
	home := t.TempDir()
	tools := filepath.Join(home, "tools")
	if err := os.Mkdir(tools, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"launchctl", "systemctl"} {
		script := "#!/bin/sh\ncase \"$1:$2\" in bootout:*) touch \"$HOME/observer-stopped\";; bootstrap:*) rm -f \"$HOME/observer-stopped\";; print:gui/*/local.mycelium.observer) if [ -f \"$HOME/observer-stopped\" ]; then exit 113; fi;; esac\nexit 0\n"
		if err := os.WriteFile(filepath.Join(tools, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for key, value := range map[string]string{"HOME": home, "XDG_CONFIG_HOME": filepath.Join(home, "config"), "PATH": tools + ":/usr/bin:/bin", "MYCE_DEV_SESSION_ROOT": "", "MYCE_PROCESS_SESSION": ""} {
		t.Setenv(key, value)
	}
	root := filepath.Join(home, "registry")
	// Use the real CLI image for command dispatch with fake managers only.
	install := exec.Command(binary, "service", "observer", "install", "--root", root)
	install.Env = observerEnvironment()
	if err := install.Run(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		remove := exec.Command(binary, "service", "observer", "uninstall")
		remove.Env = observerEnvironment()
		if err := remove.Run(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(observerClientBinary(home)); !os.IsNotExist(err) {
		t.Fatal("explicit command retained client", err)
	}
	reinstall := exec.Command(binary, "service", "observer", "install", "--root", root)
	reinstall.Env = observerEnvironment()
	if err := reinstall.Run(); err != nil {
		t.Fatal("reinstall after complete removal", err)
	}
	if _, err := os.Stat(observerClientBinary(home)); err != nil {
		t.Fatal("reinstall did not restore client", err)
	}
}
