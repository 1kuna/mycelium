package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"mycelium/internal/myce"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInstallWorkerRepairsJoiningAddressWithSavedPin(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mismatch bool
	}{{name: "new address"}, {name: "pin mismatch", mismatch: true}} {
		t.Run(tc.name, func(t *testing.T) {
			c, probe := savedInstallFixture(t, "worker")
			c.AllowDeviceless, c.Enrollment = true, false
			c.Queue = "https://old-queue.invalid:7443"
			c.JoiningFile = filepath.Join(filepath.Dir(c.Config), "joining.json")
			identity, err := myce.LoadJoiningIdentity(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			invitation := myce.Invitation{Version: 1, Fingerprint: identity.Fingerprint(), Token: "synthetic-install-fixture", Address: c.Queue}
			if err := myce.WriteInvitation(c.JoiningFile, invitation); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(c)
			if err := os.WriteFile(c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			if tc.mismatch {
				identity, err = myce.LoadJoiningIdentity(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server, _, err := myce.ServeJoining(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.mismatch {
					t.Error("sent a request to a queue with the wrong pin")
				}
				if r.URL.Path != "/api/ping" || r.Header.Get("Authorization") != "Bearer synthetic-install-fixture" {
					w.WriteHeader(http.StatusUnauthorized)
				}
			}), identity)
			if err != nil {
				listener.Close()
				t.Fatal(err)
			}
			defer server.Close()
			address := "https://" + listener.Addr().String()
			before := map[string][]byte{}
			for _, path := range []string{c.Config, c.JoiningFile, c.Binary, c.Service} {
				before[path], err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			err = setupCommandWithProbe([]string{"install", "worker", "--config", c.Config, "--queue", address}, probe, c.NAS)
			if tc.mismatch {
				if err == nil || !strings.Contains(err.Error(), "saved certificate pin") || !strings.Contains(err.Error(), "identity does not match") {
					t.Fatal("pin mismatch was not clearly refused", err)
				}
				for path, want := range before {
					got, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatal("refused pin mismatch changed installed files", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			saved, err := myce.ReadServiceConfig(c.Config)
			if err != nil || saved.Queue != address {
				t.Fatal("worker config did not save the new address", err)
			}
			got, err := myce.ReadInvitation(saved.JoiningFile)
			invitation.Address = address
			if err != nil || got != invitation {
				t.Fatal("joining address repair changed the pin/authority or lost the address", err)
			}
		})
	}
}

func savedInstallFixture(t *testing.T, roles ...string) (myce.ServiceConfig, myce.MachineProbe) {
	t.Helper()
	home := currentTestStateRoot(t)
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
		case "Version", "Force", "LocalDisableReceipts", "PrivateDevices", "RequireMemoryCeilings", "Detection", "Role", "Config", "Binary", "Credentials", "State", "Service", "HostID", "Name", "Queue", "NAS":
			continue
		}
		switch f.Kind() {
		case reflect.String:
			switch name {
			case "DiscoveryListen":
				f.SetString("127.0.0.1:7443")
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
	// Chassis declarations are exclusive; preserve both true and false policy
	// values without constructing an invalid laptop+desktop fixture.
	c.Desktop = false
	if role == "worker" {
		c.RequireMemoryCeilings = true

		c.WorkerUser = "fixture-worker"
		c.WorkerHome = home
		c.Device = "selected-device"
	}
	c.RequireMountedNAS = false
	c.EnvironmentFile = "" // synthetic credential file; no real fleet file is read
	c.JoiningFile = ""
	// Saved policy references must point to files the new binary can decode.
	for path, body := range map[string]string{
		c.HostConfig:    "hosts:\n  fixture-host: {production_hold: true}\n",
		c.ProjectConfig: "fixture: {weight: 1}\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}

	retention := 3 * time.Hour
	c.ContainerRetention = &retention
	os.MkdirAll(filepath.Dir(c.Config), 0700)
	data, _ := json.Marshal(c)
	if err = os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	data = []byte("MYCE_TOKEN=synthetic-install-fixture\nMYCE_QUEUE=" + c.Queue + "\n")
	os.WriteFile(c.Credentials, data, 0600)
	os.WriteFile(c.Binary, []byte("old binary"), 0700)
	os.MkdirAll(filepath.Dir(c.Service), 0700)
	os.WriteFile(c.Service, []byte(c.ServiceText()), 0600)
	return c, myce.MachineProbe{OS: "linux", Arch: "amd64", Command: fakeSetupManager}
}

func TestInstallQueueFileStoreWithExecStartOverride(t *testing.T) {
	c, p := savedInstallFixture(t)
	if !c.Enrollment {
		t.Fatal("fixture must exercise enrollment preflight")
	}
	path := filepath.Join(t.TempDir(), "override.conf")
	override := []byte("[Service]\nExecStart=\nExecStart=/fixture/myce queue --file-store-bytes 12 --file-store-production-reserve 6\n")
	if err := os.WriteFile(path, override, 0600); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, path := range []string{c.Config, c.Service} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = data
	}
	var warnings bytes.Buffer
	writer := log.Writer()
	log.SetOutput(&warnings)
	defer log.SetOutput(writer)
	injected := errors.New("injected reload failure before cutover")
	fail := true
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		switch strings.Join(args, " ") {
		case "--user show myce-queue.service --property=DropInPaths --value":
			return []byte(path), nil
		case "--user daemon-reload":
			if fail {
				fail = false
				return nil, injected
			}
		}
		return fakeSetupManager(ctx, name, args...)
	}
	args := []string{"install", "queue", "--config", c.Config, "--file-store-bytes=12", "--file-store-production-reserve=6"}
	if err := setupCommandWithProbe(args, p, c.NAS); !errors.Is(err, injected) {
		t.Fatalf("did not reach install with override present: %v", err)
	}
	for path, want := range before {
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
			t.Fatal("failed install did not restore previous config/unit", err)
		}
	}
	if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, override) {
		t.Fatal("failed install changed the effective override", err)
	}
	if err := setupCommandWithProbe(args, p, c.NAS); err != nil {
		t.Fatal(err)
	}
	got, err := myce.ReadServiceConfig(c.Config)
	if err != nil || got.FileStoreBytes != 12 || got.FileStoreProductionReserve != 6 {
		t.Fatal("override prevented saving replacement settings", err)
	}
	unit, err := os.ReadFile(c.Service)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []string{`"--file-store-bytes" "12"`, `"--file-store-production-reserve" "6"`} {
		if strings.Count(string(unit), pair) != 1 {
			t.Errorf("generated base unit must carry %s exactly once", pair)
		}
	}
	if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, override) {
		t.Fatal("installer changed the effective override", err)
	}
	if !strings.Contains(warnings.String(), "ExecStart drop-in still takes precedence") {
		t.Fatal("installer did not warn that the override remains authoritative")
	}
}

func TestInstallExplicitQueueNASName(t *testing.T) {
	for _, tc := range []struct{ role, field string }{{"queue", "queue"}, {"worker", "nas"}} {
		role, field := tc.role, tc.field
		t.Run(role+"/"+field, func(t *testing.T) {
			c, p := savedInstallFixture(t, role)
			if role == "queue" {
				// Omitted credential flags preserve an unread reference on upgrade.
				c.EnvironmentFile = filepath.Join(c.NAS, "private-environment-reference")
				data, err := json.Marshal(c)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.Config, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			before := map[string][]byte{}
			for _, path := range []string{c.Config, c.Service} {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				before[path] = data
			}
			dryProbe := p
			dryProbe.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name == "systemctl" || name == "launchctl" {
					t.Fatal("dry run touched service manager")
				}
				return p.Command(ctx, name, args...)
			}
			captureJoiningOutput(t, func() error {
				return setupCommandWithProbe([]string{"install", role, "--config", c.Config, "--dry-run"}, dryProbe, c.NAS)
			})
			for path, want := range before {
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatal("dry run wrote files")
				}
			}
			want := c
			value := "changed-fixture"
			switch field {
			case "queue":
				value = "http://changed.invalid"
				want.Queue = value
			case "nas":
				value = t.TempDir()
				want.NAS = value
			case "name":
				want.Name = value
			}
			if err := setupCommandWithProbe([]string{"install", role, "--config", c.Config, "--" + field, value}, p, c.NAS); err != nil {
				t.Fatal(err)
			}
			got, err := myce.ReadServiceConfig(c.Config)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("explicit --%s was ignored or changed unrelated saved fields", field)
			}
			c = got
			if role == "queue" {
				args := []string{"install", "queue", "--config", c.Config, "--nice=0", "--restart-grace=0", "--production-infrastructure-retries=0", "--reserve-host-memory-gb=0", "--memory-ceiling-slack-gb=0", "--memory-swap-max-bytes=0", "--no-new-privileges=false"}
				if err := setupCommandWithProbe(args, p, c.NAS); err != nil {
					t.Fatal(err)
				}
				got, err := myce.ReadServiceConfig(c.Config)
				if err != nil {
					t.Fatal(err)
				}
				if got.Nice != 0 || got.RestartGrace != 0 || got.ProductionInfrastructureRetries != 0 || got.ReserveHostMemoryGB != 0 || got.MemoryCeilingSlackGB != 0 || got.MemorySwapMaxBytes != 0 || got.NoNewPrivileges {
					t.Fatal("explicit zero/false was ignored")
				}
				if got.DB != c.DB || got.Path != c.Path || got.ModelCache != c.ModelCache {
					t.Fatal("unrelated field changed")
				}
				if err := setupCommandWithProbe([]string{"install", "queue", "--config", c.Config}, p, c.NAS); err != nil {
					t.Fatal(err)
				}
				preserved, err := myce.ReadServiceConfig(c.Config)
				if err != nil || !reflect.DeepEqual(got, preserved) {
					t.Fatal("omitted flags reset saved zero/false values", err)
				}
			} else {
				checkpoint := filepath.Join(c.NAS, "new-checkpoints")
				args := []string{"install", "worker", "--config", c.Config, "--memory-gb=7", "--reserve-memory-gb=0", "--container-retention=0", "--require-memory-ceilings=false", "--checkpoint-root", checkpoint}
				if err := setupCommandWithProbe(args, p, c.NAS); err != nil {
					t.Fatal(err)
				}
				got, err := myce.ReadServiceConfig(c.Config)
				if err != nil {
					t.Fatal(err)
				}
				if got.Detection.MemoryGB != 7 || got.Detection.ReserveGB != 0 || got.ContainerRetention == nil || *got.ContainerRetention != 0 || got.RequireMemoryCeilings || got.Checkpoints != checkpoint {
					t.Fatal("worker explicit overlay ignored")
				}
				for _, flag := range []string{"--host-id"} {
					if err = setupCommandWithProbe([]string{"install", "worker", "--config", c.Config, flag, "different"}, p, c.NAS); err == nil {
						t.Fatal("changed immutable identity", flag)
					}
				}
			}

		})
	}
	{
		err := setupCommandWithProbe([]string{"doctor"}, myce.MachineProbe{}, t.TempDir())
		if err == nil || !strings.Contains(err.Error(), "no install found; run myce install first") {
			t.Fatalf("missing install guidance: %v", err)
		}
	}
}

func TestInstallExplicitEmptyPaths(t *testing.T) {
	for _, name := range []string{"environment-root", "db"} {
		t.Run(name, func(t *testing.T) {
			c, probe := savedInstallFixture(t)
			if err := setupCommandWithProbe([]string{"install", "queue", "--config", c.Config, "--" + name + "="}, probe, c.NAS); err != nil {
				t.Fatal(err)
			}
			got, err := myce.ReadServiceConfig(c.Config)
			if err != nil {
				t.Fatal(err)
			}
			if got.InstallFlags()[name] != "" {
				t.Fatal("explicit empty path was ignored")
			}
			if name == "environment-root" {
				c = got
				if err := setupCommandWithProbe([]string{"install", "queue", "--config", c.Config}, probe, c.NAS); err != nil {
					t.Fatal(err)
				}
				got, err := myce.ReadServiceConfig(c.Config)
				if err != nil || !reflect.DeepEqual(c, got) {
					t.Fatal("omitted flag changed saved empty root", err)
				}
			}
		})
	}

	for _, name := range []string{"name"} {
		t.Run(name, func(t *testing.T) {
			home := currentTestStateRoot(t)
			probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) {
				t.Fatal("invalid value reached a service command")
				return nil, nil
			}}
			err := setupCommandWithProbe([]string{"install", "queue", "--dry-run", "--" + name + "="}, probe, home)
			if err == nil || !strings.HasPrefix(err.Error(), "usage:") {
				t.Fatal("empty required value was not a usage error", err)
			}
		})
	}

	for _, role := range []string{"queue"} {
		for _, withToken := range []bool{false, true} {
			name := role + "/without-token"
			if withToken {
				name = role + "/with-token"
			}
			t.Run(name, func(t *testing.T) {
				c, probe := savedInstallFixture(t, role)
				// Only the reference is needed: never read an environment file.
				c.EnvironmentFile = filepath.Join(c.NAS, "absent-private-reference")
				before, err := json.Marshal(c)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.Config, before, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(c.Credentials); err != nil {
					t.Fatal(err)
				}
				serviceBefore, err := os.ReadFile(c.Service)
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("MYCE_TOKEN", "")
				if withToken {
					t.Setenv("MYCE_TOKEN", "synthetic-empty-environment-test")
				}
				probe.Command = func(context.Context, string, ...string) ([]byte, error) {
					t.Fatal("invalid environment file reached a service command")
					return nil, nil
				}
				err = setupCommandWithProbe([]string{"install", role, "--config", c.Config, "--environment-file="}, probe, c.NAS)
				if err == nil || !strings.HasPrefix(err.Error(), "usage:") || !strings.Contains(err.Error(), "--environment-file") {
					t.Fatal("empty environment file was not a flag-specific usage error")
				}
				after, err := os.ReadFile(c.Config)
				if err != nil || string(after) != string(before) {
					t.Fatal("rejected install changed saved config")
				}
				serviceAfter, err := os.ReadFile(c.Service)
				if err != nil || string(serviceAfter) != string(serviceBefore) {
					t.Fatal("rejected install changed service definition")
				}
				if _, err := os.Lstat(c.Credentials); !os.IsNotExist(err) {
					t.Fatal("rejected install created credentials")
				}
			})
		}
	}

}
