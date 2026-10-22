package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"mycelium/internal/myce"
)

func TestEnrollmentPreflightRefusalLeavesHostUnchanged(t *testing.T) {
	previousVersion := myce.BuildVersion
	myce.BuildVersion = "v1.0.1"
	defer func() { myce.BuildVersion = previousVersion }()
	for _, failure := range []string{"state-root", "service-environment", "session", "manager"} {
		t.Run(failure, func(t *testing.T) {
			home := t.TempDir()
			var operations []string
			lingerEnabled := false
			var mu sync.Mutex
			record := func(value string) { mu.Lock(); defer mu.Unlock(); operations = append(operations, value) }
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record(r.Method + " queue")
				http.Error(w, "stop after preflights", http.StatusServiceUnavailable)
			}))
			defer server.Close()
			c, err := myce.PlanInstall("worker", home, "", home, server.URL, "hypha", myce.Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "fixed", MemoryGB: 8, ReserveGB: 1})
			if err != nil {
				t.Fatal(err)
			}
			c.EnvironmentFile, c.JoiningFile, c.Enrollment = "", filepath.Join(home, "joining.json"), true
			c.HostID = "hypha"
			if err = myce.WriteInvitation(c.JoiningFile, myce.Invitation{Version: 1, Address: server.URL, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(server.Certificate().Raw)), Token: "test"}); err != nil {
				t.Fatal(err)
			}
			if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(c.Binary, []byte("#!/bin/sh\necho '{\"version\":\"v1.0.0\"}'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(c)
			if err = os.WriteFile(c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			if failure == "state-root" {
				if err = os.MkdirAll(c.State, 0700); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(c.State, "legacy-data"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Glob: func(pattern string) ([]string, error) {
				if strings.HasSuffix(pattern, "/class") {
					return []string{"/fixture/0000:01:00.0/class"}, nil
				}
				return nil, nil
			}, ReadFile: func(path string) ([]byte, error) {
				switch filepath.Base(path) {
				case "class":
					return []byte("0x030000"), nil
				case "vendor":
					return []byte("0x10de"), nil
				case "current_link_width":
					return []byte("16"), nil
				}
				return nil, os.ErrNotExist
			}}
			probe.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
				record(name + " " + strings.Join(args, " "))
				switch name {
				case "nvidia-smi":
					if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
						return []byte("GPU-test, fixture-driver, 0000:01:00.0"), nil
					}
					return []byte("GPU, 8192"), nil
				case "cat":
					return []byte("fixture " + home + " nfs rw 0 0\n"), nil
				case "loginctl":
					if failure == "session" {
						return nil, errors.New("missing manager")
					}
					if len(args) > 0 && args[0] == "show-user" {
						if !lingerEnabled {
							return []byte("no"), nil
						}
						return []byte("yes"), nil
					}
					if len(args) > 0 && args[0] == "enable-linger" {
						lingerEnabled = true
					}
					return nil, nil
				case "systemctl":
					if failure == "manager" && strings.Contains(strings.Join(args, " "), "show-environment") {
						return nil, errors.New("missing user manager")
					}
					if strings.Contains(strings.Join(args, " "), "DropInPaths") || strings.Contains(strings.Join(args, " "), "show-environment") {
						return nil, nil
					}
					t.Fatalf("preflight changed service: %v", args)
				case "systemd-run":
					if failure == "service-environment" {
						return nil, errors.New("runtime inaccessible in manager")
					}
					return json.Marshal(myce.HardwarePreflight{Detection: c.Detection, Devices: []myce.DeviceInstall{{Selector: "GPU-test", Detection: c.Detection}}})
				}
				return nil, exec.ErrNotFound
			}
			err = enrollmentCommand([]string{"install", "worker", "--enroll", "--config", c.Config, "--allow-downgrade"}, probe, home, nil)
			if err == nil {
				t.Fatal("failure unexpectedly succeeded")
			}
			wantCode := map[string]string{"state-root": "legacy_state_present", "service-environment": "service_environment_invalid", "session": "lingering_unavailable", "manager": "service_session_unavailable"}[failure]
			if !strings.Contains(err.Error(), wantCode) {
				t.Fatalf("did not reach intended refusal %s: %v", wantCode, err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, operation := range operations {
				if strings.Contains(operation, "enable-linger") {
					t.Fatalf("persistent service-session mutation before state preflight: %v; result: %v", operations, err)
				}
			}
			queueReached := false
			for _, operation := range operations {
				if strings.HasPrefix(operation, "POST") {
					t.Fatal("preflight failure drained host", operations, err)
				}
				queueReached = queueReached || operation == "GET queue"
			}
			if queueReached {
				t.Fatalf("preflight ordering: operations=%v err=%v", operations, err)
			}
			if _, e := os.Stat(filepath.Join(home, "mycelium")); !os.IsNotExist(e) {
				t.Fatal("preflight left newly created NAS namespace behind", e)
			}
			after, e := os.ReadFile(c.Config)
			if e != nil || string(after) != string(data) {
				t.Fatal("preflight changed installed config", e)
			}
		})
	}
}
