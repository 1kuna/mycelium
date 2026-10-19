package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func savedInstallFixture(t *testing.T, roles ...string) (myce.ServiceConfig, myce.MachineProbe) {
	t.Helper()
	home := t.TempDir()
	role := "queue"
	if len(roles) > 0 {
		role = roles[0]
	}
	c, err := myce.PlanInstall(role, home, filepath.Join(home, "custom"), home, "http://queue.invalid", "fixture", myce.Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 3, ReserveGB: 1, GPU: "fixture inventory"})
	if err != nil {
		t.Fatal(err)
	}
	// Set every field, including fields without a CLI option, away from its default.
	v := reflect.ValueOf(&c).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		name := v.Type().Field(i).Name
		switch name {
		case "Version", "Force", "LocalDisableReceipts", "PrivateDevices", "RequireMemoryCeilings", "StartDisabled", "Detection", "Role", "Config", "Binary", "Credentials", "State", "Service", "HostID", "Name", "Queue", "NAS":
			continue
		}
		switch f.Kind() {
		case reflect.String:
			switch name {
			case "CPUQuota":
				f.SetString("37%")
			case "MemoryMax":
				f.SetString("700M")
			case "TasksMax":
				f.SetString("73")
			case "Path":
				f.SetString("/fixture/bin:/usr/bin")
			default:
				f.SetString(filepath.Join(home, "custom-"+name))
			}
		case reflect.Bool:
			f.SetBool(!f.Bool())
		case reflect.Int:
			f.SetInt(7)
		case reflect.Int64:
			if f.Type() == reflect.TypeOf(time.Duration(0)) {
				f.SetInt(int64(17 * time.Second))
			} else {
				f.SetInt(23456)
			}
		case reflect.Float64:
			f.SetFloat(3)
		}
	}
	c.HostID = "fixture-host"
	if role == "worker" {
		c.RequireMemoryCeilings = true
		c.StartDisabled = true
		c.WorkerUser = "fixture-worker"
		c.WorkerHome = home
		c.Device = "selected-device"
	}
	c.RequireMountedNAS = false
	c.EnvironmentFile = "" // synthetic credential file; no real fleet file is read
	c.JoiningFile = ""
	c.RequiredWorkers = []string{"fixture-one", "fixture-two"}
	retention := 3 * time.Hour
	c.ContainerRetention = &retention
	os.MkdirAll(filepath.Dir(c.Config), 0700)
	data, _ := json.Marshal(c)
	if err = os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(map[string]string{"token": "synthetic-install-fixture"})
	os.WriteFile(c.Credentials, data, 0600)
	os.WriteFile(c.Binary, []byte("old binary"), 0700)
	os.MkdirAll(filepath.Dir(c.Service), 0700)
	os.WriteFile(c.Service, []byte(c.ServiceText()), 0600)
	return c, myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, nil }}
}

func TestInstallPreservesEveryConfigField(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		t.Run(role, func(t *testing.T) {
			c, p := savedInstallFixture(t, role)
			if role == "worker" {
				c.JoiningFile = filepath.Join(c.NAS, "private-invitation.json")
				invitation := myce.Invitation{Version: 1, Fingerprint: strings.Repeat("ab", 32), Token: "synthetic-install-fixture", Address: "https://queue.invalid"}
				if err := myce.WriteInvitation(c.JoiningFile, invitation); err != nil {
					t.Fatal(err)
				}
				if _, err := myce.InitializeHostIntent(c.HostStateDir, c.HostID, myce.RealClock{}); err != nil {
					t.Fatal(err)
				}
			} else {
				c.EnvironmentFile = filepath.Join(c.NAS, "private-environment-reference")
				// Installing an upgrade must preserve the reference without reading it.
			}
			c.Version = 0 // current pre-versioning configs are upgraded, not default-filled
			data, _ := json.Marshal(c)
			if err := os.WriteFile(c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("MYCE_QUEUE", "http://ambient.invalid")
			t.Setenv("PATH", "/usr/bin:/bin")
			t.Setenv("DOCKER_HOST", "unix:///ambient.sock")
			if err := setupCommandWithProbe([]string{"install", role, "--config", c.Config}, p, filepath.Dir(c.NAS)); err != nil {
				t.Fatal(err)
			}
			got, err := myce.ReadServiceConfig(c.Config)
			if err != nil {
				t.Fatal(err)
			}
			c.Version = 1
			if !reflect.DeepEqual(c, got) {
				t.Fatal("omitted flags changed saved configuration")
			}
		})
	}
}

func TestInstallExplicitZeroFalse(t *testing.T) {
	c, p := savedInstallFixture(t)
	args := []string{"install", "queue", "--config", c.Config, "--nice=0", "--restart-grace=0", "--reserve-host-memory-gb=0", "--memory-ceiling-slack-gb=0", "--memory-swap-max-bytes=0", "--no-new-privileges=false"}
	if err := setupCommandWithProbe(args, p, c.NAS); err != nil {
		t.Fatal(err)
	}
	got, err := myce.ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if got.Nice != 0 || got.RestartGrace != 0 || got.ReserveHostMemoryGB != 0 || got.MemoryCeilingSlackGB != 0 || got.MemorySwapMaxBytes != 0 || got.NoNewPrivileges {
		t.Fatal("explicit zero/false was ignored")
	}
	if got.DB != c.DB || got.Path != c.Path || got.ModelCache != c.ModelCache {
		t.Fatal("unrelated field changed")
	}
}

func TestInstallWorkerExplicitOverlayAndIdentity(t *testing.T) {
	c, p := savedInstallFixture(t, "worker")
	if _, err := myce.InitializeHostIntent(c.HostStateDir, c.HostID, myce.RealClock{}); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(c.NAS, "new-checkpoints")
	args := []string{"install", "worker", "--config", c.Config, "--memory-gb=7", "--reserve-memory-gb=0", "--container-retention=0", "--require-memory-ceilings=false", "--start-disabled=false", "--checkpoint-root", checkpoint}
	if err := setupCommandWithProbe(args, p, c.NAS); err != nil {
		t.Fatal(err)
	}
	got, err := myce.ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if got.Detection.MemoryGB != 7 || got.Detection.ReserveGB != 0 || got.ContainerRetention == nil || *got.ContainerRetention != 0 || got.RequireMemoryCeilings || got.StartDisabled || got.Checkpoints != checkpoint {
		t.Fatal("worker explicit overlay ignored")
	}
	for _, flag := range []string{"--state-dir", "--host-state-dir", "--host-id", "--device"} {
		if err = setupCommandWithProbe([]string{"install", "worker", "--config", c.Config, flag, "different"}, p, c.NAS); err == nil {
			t.Fatal("changed immutable identity", flag)
		}
	}
}

func TestInstallProductionServiceEntryStaysClosed(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "myce")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v, output bytes=%d", err, len(out))
	}
	c, p := savedInstallFixture(t, "worker")
	// Missing credentials are intentional: a closed service must exit before
	// reading any secret or attempting discovery, GPU access or queue admission.
	c.EnvironmentFile = filepath.Join(c.NAS, "absent-private-reference")
	data, _ := json.Marshal(c)
	os.WriteFile(c.Config, data, 0600)
	if _, err := myce.InitializeHostIntent(c.HostStateDir, c.HostID, myce.RealClock{}); err != nil {
		t.Fatal(err)
	}
	if err := myce.Install(context.Background(), c, binary, "", p.Command); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.Binary, "service", "--config", c.Config)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + c.NAS}
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "Worker disabled by operator") {
		t.Fatalf("closed service entry failed: %v, output bytes=%d", err, len(out))
	}
	if reason, err := myce.HostDisabled(c.HostStateDir); err != nil || reason == "" {
		t.Fatal("service entry opened host", err)
	}
}
