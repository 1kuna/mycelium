package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestInstallXPUHealthAccess(t *testing.T) {
	for _, scenario := range []string{"install", "reapply", "denied", "queue", "cuda", "darwin", "machine xpu"} {
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
				if name != "sudo" || len(args) != 11 || args[0] != "-n" || args[2] != "/bin/sh" || args[6] != "install" || args[7] != strconv.Itoa(os.Getgid()) || args[8] != xpuHealthOwner(c) || args[9] != xpuHealthRules(os.Getgid()) {
					t.Fatalf("unexpected installer command: %s %v", name, args)
				}
				if scenario == "denied" {
					return nil, errors.New("sensitive administrator diagnostic")
				}
				return nil, nil
			}
			err := EnsureXPUHealthAccess(t.Context(), c, command)
			if scenario == "reapply" {
				if err != nil {
					t.Fatal(err)
				}
				err = EnsureXPUHealthAccess(t.Context(), c, command)
			}
			if scenario == "denied" {
				if err == nil || !strings.Contains(err.Error(), "xpu_health_access_unavailable") || strings.Contains(err.Error(), "sensitive") {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			want := 1
			if scenario == "queue" || scenario == "cuda" || scenario == "darwin" {
				want = 0
			}
			if scenario == "reapply" {
				want = 2
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
	if len(lines) != 3 || !strings.Contains(lines[1], `SUBSYSTEM=="mei"`) || !strings.Contains(lines[1], `DRIVERS=="mei_gsc"`) || !strings.Contains(lines[1], `access.sh apply 1234 /dev/%k`) {
		t.Fatal("MEI grant must be restricted to GPU driver and worker group", rules)
	}
	if !strings.Contains(lines[2], `SUBSYSTEM=="intel_pmt"`) || !strings.Contains(lines[2], `DRIVERS=="xe"`) || !strings.Contains(lines[2], `access.sh apply 1234 /sys%p/telem`) {
		t.Fatal("PMT grant must be read-only and restricted to Xe telemetry", rules)
	}
}

// Execute the production shell logic against a private fake /sys and /dev.
// Only chown/udev/locking are faked; permission modes and receipts are real files.
func TestInstallXPUHealthAccessLifecycle(t *testing.T) {
	root := t.TempDir()
	write := func(path, data string, mode os.FileMode) {
		t.Helper()
		path = root + path
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), mode); err != nil {
			t.Fatal(err)
		}
	}
	link := func(path, target string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(root+path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(root+target, root+path); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/run/lock", "/etc/udev/rules.d", "/var/lib/mycelium", "/sys/bus/auxiliary/drivers/mei_gsc", "/sys/bus/auxiliary/drivers/mei_me", "/sys/bus/pci/drivers/xe"} {
		if err := os.MkdirAll(root+dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	write("/dev/mei0", "", 0600)
	write("/dev/mei1", "", 0600)
	link("/sys/class/mei/mei0/device/driver", "/sys/bus/auxiliary/drivers/mei_me")
	link("/sys/class/mei/mei1/device/driver", "/sys/bus/auxiliary/drivers/mei_gsc")
	write("/sys/devices/gpu/pmt/telem0/telem", "", 0400)
	write("/sys/devices/gpu/pmt/telem1/telem", "", 0400)
	link("/sys/devices/gpu/driver", "/sys/bus/pci/drivers/xe")
	link("/sys/class/intel_pmt/telem0", "/sys/devices/gpu/pmt/telem0")
	link("/sys/class/intel_pmt/telem1", "/sys/devices/gpu/pmt/telem1")
	write("/bin/chown", "#!/bin/sh\nexit 0\n", 0700)
	write("/bin/flock", "#!/bin/sh\nexit 0\n", 0700)
	write("/bin/udevadm", "#!/bin/sh\n[ ! -e '"+root+"/fail-reload' ]\n", 0700)
	if runtime.GOOS == "darwin" {
		write("/bin/stat", "#!/bin/sh\nshift\nformat=$1\nshift\ncase \"$format\" in '%g %a') format='%g %Lp';; esac\nexec /usr/bin/stat -L -f \"$format\" \"$@\"\n", 0700)
		write("/bin/sha256sum", "#!/bin/sh\nexec /usr/bin/shasum -a 256\n", 0700)
	}
	replace := strings.NewReplacer("/etc/udev", root+"/etc/udev", "/var/lib/mycelium", root+"/var/lib/mycelium", "/run/lock", root+"/run/lock", "/sys/", root+"/sys/", "/dev/", root+"/dev/", "PATH=/usr/sbin:/usr/bin:/sbin:/bin", "PATH="+root+"/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	script, rules := replace.Replace(xpuHealthAccessScript), replace.Replace(xpuHealthRules(os.Getgid()))
	owner, other := strings.Repeat("a", 64), strings.Repeat("b", 64)
	run := func(action, who string) error {
		t.Helper()
		cmd := exec.Command("/bin/sh", "-c", script, "xpu-health-fixture", action, strconv.Itoa(os.Getgid()), who, rules, script)
		cmd.Env = withoutMyceToken(os.Environ())
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("fixture %s: %s", action, data)
		}
		return err
	}
	mode := func(path string) os.FileMode {
		t.Helper()
		info, err := os.Stat(root + path)
		if err != nil {
			t.Fatal(err)
		}
		return info.Mode().Perm()
	}
	// A failure after publishing the rule must not allow activation, and matching
	// bytes on retry must still execute the reload and apply all current nodes.
	write("/fail-reload", "", 0600)
	if err := run("install", owner); err == nil {
		t.Fatal("accepted partial provisioning")
	}
	if _, err := os.Stat(root + xpuHealthRulePath); err != nil {
		t.Fatal("failure did not occur after rule publication", err)
	}
	if mode("/dev/mei1") != 0600 {
		t.Fatal("fixture failure ran too late")
	}
	if err := os.Remove(root + "/fail-reload"); err != nil {
		t.Fatal(err)
	}
	if err := run("install", owner); err != nil {
		t.Fatal("retry did not repair partial provision", err)
	}
	if mode("/dev/mei1") != 0660 || mode("/sys/devices/gpu/pmt/telem0/telem") != 0440 || mode("/dev/mei0") != 0600 {
		t.Fatal("incorrect permission scope")
	}
	// Reapply must preserve the original baseline, not snapshot our own grant.
	if err := run("install", owner); err != nil {
		t.Fatal(err)
	}
	if err := run("install", other); err != nil {
		t.Fatal(err)
	}
	if err := run("remove", owner); err != nil {
		t.Fatal(err)
	}
	if mode("/dev/mei1") != 0660 {
		t.Fatal("removed another installation's grant")
	}
	// Another installation cannot revoke the remaining owner's grant.
	if err := run("remove", owner); err != nil {
		t.Fatal(err)
	}
	// Edited rules are not ours to delete, and refusal must retain ownership.
	write("/etc/udev/rules.d/70-mycelium-xpu-health.rules", "administrator rule", 0644)
	if err := run("remove", other); err == nil {
		t.Fatal("removed a foreign rule")
	}
	write("/etc/udev/rules.d/70-mycelium-xpu-health.rules", rules, 0644)
	// A recreated node also gets an original-permission receipt through udev.
	if err := os.Rename(root+"/dev/mei1", root+"/dev/old-mei1"); err != nil {
		t.Fatal(err)
	}
	write("/dev/mei1", "", 0400)
	if err := run("apply", root+"/dev/mei1"); err != nil {
		t.Fatal(err)
	}
	// Subsequent administrator permission changes are preserved on uninstall.
	if err := os.Chmod(root+"/sys/devices/gpu/pmt/telem0/telem", 0600); err != nil {
		t.Fatal(err)
	}
	if err := run("remove", other); err != nil {
		t.Fatal(err)
	}
	if mode("/dev/mei1") != 0400 || mode("/dev/mei0") != 0600 || mode("/sys/devices/gpu/pmt/telem0/telem") != 0600 || mode("/sys/devices/gpu/pmt/telem1/telem") != 0400 {
		t.Fatal("uninstall did not restore baseline/preserve administrator edits")
	}
	for _, path := range []string{xpuHealthRulePath, xpuHealthAccessPath, "/var/lib/mycelium/xpu-health-access"} {
		if _, err := os.Stat(root + path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("uninstall retained access state", path, err)
		}
	}
}

func TestUninstallXPUHealthAccess(t *testing.T) {
	for _, scenario := range []string{"dry", "denied", "remove"} {
		t.Run(scenario, func(t *testing.T) {
			home := t.TempDir()
			c, err := PlanInstall("worker", home, "", home, "http://queue:7331", "worker", Detection{OS: "linux", Platform: "xpu"})
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{c.Config, c.Service} {
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			stopped, revoked := false, false
			err = Uninstall(t.Context(), c, scenario == "dry", func(_ context.Context, name string, args ...string) ([]byte, error) {
				if scenario == "dry" {
					t.Fatal("dry uninstall ran command")
				}
				if name == "systemctl" && slices.Contains(args, "disable") {
					stopped = true
				}
				if name == "sudo" {
					if !stopped || args[6] != "remove" || args[8] != xpuHealthOwner(c) {
						t.Fatal("cleanup lost stop/ownership ordering")
					}
					if _, err := os.Stat(c.Config); err != nil {
						t.Fatal("removed config before access cleanup", err)
					}
					if scenario == "denied" {
						return nil, errors.New("private root error")
					}
					revoked = true
				}
				return nil, nil
			})
			if scenario == "denied" {
				if err == nil || err.Error() != "xpu_health_access_cleanup_failed" {
					t.Fatal(err)
				}
				if _, err := os.Stat(c.Config); err != nil {
					t.Fatal("failed cleanup lost retry identity")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if scenario == "remove" && !revoked {
				t.Fatal("uninstall did not revoke access")
			}
		})
	}
}
