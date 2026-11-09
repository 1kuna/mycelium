package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func fakeProbe(osname, arch, nvidia, intel string) MachineProbe {
	p := MachineProbe{OS: osname, Arch: arch, Glob: func(string) ([]string, error) { return nil, nil }, ReadFile: func(string) ([]byte, error) { return nil, os.ErrNotExist }, Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "nvidia-smi":
			if nvidia != "" {
				if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
					return []byte("GPU-fixture, driver, 0000:01:00.0"), nil
				}
				return []byte(nvidia), nil
			}
		case "sycl-ls":
			if intel != "" {
				return []byte(intel), nil
			}
		case "/usr/sbin/system_profiler":
			return []byte("Chipset Model: Apple M4\nMetal Support: Metal 3"), nil
		case "uv":
			return []byte("uv 0.8"), nil
		}
		return nil, exec.ErrNotFound
	}}
	if nvidia != "" {
		link := "16"
		if strings.Contains(nvidia, "N/A") {
			link = "0"
		}
		facts := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", link, "fixture", "", true})
		p.Glob, p.ReadFile = facts.Glob, facts.ReadFile
	}
	return p

}

func TestInstallAndUninstallTempOnly(t *testing.T) {
	home := t.TempDir()
	c, e := PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue:7331", "worker", Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "dedicated"})
	if e != nil {
		t.Fatal(e)
	}
	executable := filepath.Join(home, "test-executable")
	if err := os.Mkdir(c.NAS, 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(executable, []byte("test binary"), 0700)
	commands := []string{}
	runner := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, name+" "+strings.Join(args, " "))
		if name == "cat" {
			return []byte("fixture " + c.NAS + " nfs rw 0 0\n"), nil
		}
		if name == "systemctl" && len(args) > 1 && args[1] == "is-active" {
			return nil, installTestExit(3)
		}
		if name == "systemd-run" {
			return json.Marshal(c.Detection)
		}
		return nil, nil
	}
	if e = Install(context.Background(), c, executable, "private-test-secret", runner); e != nil {
		t.Fatal(e)
	}
	// Snapshot the current unit, both obsolete units and effective drop-ins.
	if len(commands) != 12 {
		t.Fatal(commands)
	}
	if commands[1] != "systemctl --user is-active --quiet myce-worker.service" {
		t.Fatal("did not inspect prior worker state before activation", commands)
	}
	token, e := ReadServiceToken(c)
	if e != nil || token != "private-test-secret" {
		t.Fatal(e)
	}
	if b, _ := os.ReadFile(c.Config); strings.Contains(string(b), token) {
		t.Fatal("token in public config")
	}
	if e = os.WriteFile(c.Service, []byte("[Service]\nKillMode=control-group\nTimeoutStopSec=infinity\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = Install(context.Background(), c, executable, "changed-token", runner); e != nil {
		t.Fatal(e)
	}
	if got, e := ReadServiceToken(c); e != nil || got != token {
		t.Fatal("upgrade changed credentials", e)
	}
	text, e := os.ReadFile(c.Service)
	if e != nil || !strings.Contains(string(text), "KillMode=mixed") {
		t.Fatal("unit not upgraded", e)
	}
	old, e := os.ReadDir(filepath.Join(filepath.Dir(c.Service), ".Trash"))
	foundBackup := false
	for _, entry := range old {
		if strings.HasSuffix(entry.Name(), "-"+filepath.Base(c.Service)) {
			foundBackup = true
		}
	}
	if e != nil || !foundBackup {
		t.Fatal("old unit not recoverable", e)
	}
	if e = Uninstall(context.Background(), c, false, runner); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(c.Config); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("config remained")
	}
	entries, _ := os.ReadDir(filepath.Join(home, ".local", "share", "mycelium", ".Trash"))
	if len(entries) != 2 {
		t.Fatal("uninstall not recoverable")
	}
}

func TestInstallMissingRunnerDoesNotWrite(t *testing.T) {
	home := t.TempDir()
	c, err := PlanInstall("queue", home, "", filepath.Join(home, "nas"), "http://queue:7331", "queue", Detection{OS: "linux"})
	if err != nil {
		t.Fatal(err)
	}
	if err = Install(context.Background(), c, "missing-executable", "test-token", nil); err == nil || !strings.Contains(err.Error(), "runner unavailable") {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatalf("install wrote files: %v %v", entries, err)
	}
}

func TestInstallDeploymentNASAndUpgrade(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		t.Run(role, func(t *testing.T) {
			home := t.TempDir()
			nas := filepath.Join(home, "nas")
			os.Mkdir(nas, 0700)
			c, _ := PlanInstall(role, home, "", nas, "http://queue:7331", "test", Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
			c.AllowDeviceless = true
			c.RequireMountedNAS = true
			c.EnvironmentFile = filepath.Join(home, "fleet.env")
			os.WriteFile(c.EnvironmentFile, []byte("MYCE_TOKEN='private-test-secret'\nMYCE_QUEUE=http://queue:7331\n"), 0600)
			executable := filepath.Join(home, "binary")
			os.WriteFile(executable, []byte("version 1"), 0700)
			mounted := false
			runner := func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "cat" {
					if !mounted {
						return []byte("root / ext4 rw 0 0\n"), nil
					}
					return []byte("server " + nas + " cifs rw 0 0\n"), nil
				}
				return nil, nil
			}
			if err := Install(context.Background(), c, executable, "", runner); err == nil || !strings.Contains(err.Error(), "mount") {
				t.Fatal("unmounted NAS accepted", err)
			}
			if _, err := os.Stat(filepath.Join(nas, "mycelium")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("wrote to unmounted NAS")
			}
			mounted = true
			if err := Install(context.Background(), c, executable, "", runner); err != nil {
				t.Fatal(err)
			}
			for _, dir := range []string{"checkpoints", "models", "environments"} {
				entries, err := os.ReadDir(filepath.Join(nas, "mycelium", dir))
				if err != nil || len(entries) != 0 {
					t.Fatal("NAS probe not removed", entries, err)
				}
			}
			for _, p := range []string{c.Service, c.Config, c.GPUEnvironmentFile()} {
				b, _ := os.ReadFile(p)
				if strings.Contains(string(b), "private-test-secret") {
					t.Fatal("secret persisted", p)
				}
			}
			state := filepath.Join(c.State, "custody")
			os.WriteFile(state, []byte("preserve"), 0600)
			os.WriteFile(executable, []byte("version 2"), 0700)
			if err := Install(context.Background(), c, executable, "", runner); err != nil {
				t.Fatal(err)
			}
			b, _ := os.ReadFile(c.Binary)
			if string(b) != "version 2" {
				t.Fatal("binary not upgraded")
			}
			b, _ = os.ReadFile(state)
			if string(b) != "preserve" {
				t.Fatal("state lost")
			}
			changed := c
			changed.LogRetention = 24 * 60 * 60 * 1e9
			if err := Install(context.Background(), changed, executable, "", runner); err != nil {
				t.Fatal("explicit configuration change required --force", err)
			}
			got, err := ReadServiceConfig(c.Config)
			if err != nil || got.LogRetention != changed.LogRetention {
				t.Fatal("explicit update was not saved", err)
			}
		})
	}
}

func TestInstallDeploymentDefaultUnitAndNASBoundary(t *testing.T) {
	home := t.TempDir()
	nas := filepath.Join(home, "nas")
	os.Mkdir(nas, 0700)
	c, _ := PlanInstall("queue", home, "", nas, "http://queue:7331", "q", Detection{OS: "linux", Arch: "amd64"})
	for _, want := range []string{"PrivateDevices=no", "NoNewPrivileges=yes", "Nice=0", "CPUQuota=\n", "MemoryMax=infinity\n", "TasksMax=64", "--restart-grace", "3m0s", "--production-loss-timeout", "0s", "--deadline-loss-timeout", "0s", "--experiment-loss-timeout", "0s", "--upload-spool-bytes", strconv.FormatInt(DefaultUploadSpoolBytes, 10)} {
		if !strings.Contains(c.ServiceText(), want) {
			t.Error("missing default", want)
		}
	}
	if strings.Contains(c.ServiceText(), "--log-retention") {
		t.Fatal("omitted payload policy acquired a fixed expiry")
	}
	outside := t.TempDir()
	os.Symlink(outside, filepath.Join(nas, "mycelium"))
	executable := filepath.Join(home, "binary")
	os.WriteFile(executable, []byte("test"), 0700)
	if err := Install(context.Background(), c, executable, "token", func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "systemctl" || len(args) < 2 || (args[1] != "is-active" && !(args[1] == "show" && strings.Contains(strings.Join(args, " "), "--property=UnitFileState"))) {
			t.Fatal("service changed before NAS check")
		}
		return nil, nil
	}); err == nil {
		t.Fatal("NAS symlink followed")
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 0 {
		t.Fatal("outside namespace touched")
	}
}

func TestFleetEnvironmentPrivacyAndLiteralValues(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		mode       os.FileMode
		ok         bool
	}{
		{"literal", "MYCE_TOKEN='test $literal'\nMYCE_QUEUE=\"http://queue:7331\"\n", 0600, true},
		{"public", "MYCE_TOKEN=test\nMYCE_QUEUE=http://queue:7331\n", 0644, false},
		{"missing", "MYCE_TOKEN=test\n", 0600, false},
		{"malformed", "MYCE_TOKEN='test\nMYCE_QUEUE=http://queue:7331\n", 0600, false},
		{"token URL", "MYCE_TOKEN=test\nMYCE_QUEUE=http://user:secret@queue:7331\n", 0600, false},
		{"extra", "MYCE_TOKEN=test\nMYCE_QUEUE=http://queue:7331\nOTHER=secret\n", 0600, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "fleet.env")
			os.WriteFile(p, []byte(tc.text), tc.mode)
			token, queue, err := ReadFleetEnvironment(p)
			if (err == nil) != tc.ok {
				t.Fatal(err)
			}
			if tc.ok && (token != "test $literal" || queue != "http://queue:7331") {
				t.Fatal("literal changed")
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("credential in error")
			}
		})
	}
}

func TestUserUnitLiteralPaths(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		c, _ := PlanInstall(role, "/tmp/home%test", "", "/tmp/nas", "http://queue:7331", "test", Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "dedicated"})
		c.EnvironmentFile = "/tmp/fleet%test.env"
		text := c.ServiceText()
		for _, path := range []string{c.EnvironmentFile, c.GPUEnvironmentFile()} {
			if role == "queue" && path == c.GPUEnvironmentFile() {
				continue
			}
			if !strings.Contains(text, "EnvironmentFile="+strings.ReplaceAll(path, "%", "%%")+"\n") {
				t.Errorf("quoted or unescaped EnvironmentFile: %s", text)
			}
		}
		if role == "worker" && (!strings.Contains(text, "Delegate=memory cpu\n") || strings.Contains(text, "PrivateDevices=") || strings.Contains(text, "PrivateUsers=")) {
			t.Fatal("worker device access/delegation changed")
		}
		for _, bad := range []string{" ", "\t", "'", "\"", "\\", "\n", "\r", "\u00a0"} {
			for _, field := range []string{"environment", "config"} {
				broken := c
				switch field {
				case "environment":
					broken.EnvironmentFile += bad
				case "config":
					if role == "queue" {
						continue
					}
					broken.Config = filepath.Join(filepath.Dir(c.Config)+bad, "config.json")
				}
				if err := broken.ValidateInstall(); err == nil {
					t.Errorf("accepted unsafe %s path %q", field, bad)
				}
			}
		}
	}
	c, _ := PlanInstall("queue", "/tmp/home", "", "/tmp/nas", "http://queue:7331", "test", Detection{OS: "linux"})
	c.PrivateDevices = true
	if err := c.ValidateInstall(); err == nil {
		t.Fatal("accepted unsupported user sandbox")
	}
}
