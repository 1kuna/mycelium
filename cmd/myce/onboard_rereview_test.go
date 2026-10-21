package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestEnrollmentEqualVersionDifferentBytesRefused(t *testing.T) {
	for _, version := range []string{"v1.0.1", "v1.0.1+rebuild"} {
		t.Run(version, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			prior := myce.BuildVersion
			myce.BuildVersion = "v1.0.1"
			defer func() { myce.BuildVersion = prior }()
			binary := filepath.Join(home, "old-myce")
			if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' '{\"version\":\""+version+"\"}'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			joining := filepath.Join(home, "joining.json")
			if err := myce.WriteInvitation(joining, myce.Invitation{Version: 1, Token: "fixture", Address: "https://127.0.0.1:1", Fingerprint: strings.Repeat("a", 64)}); err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(home, "config.json")
			c := myce.ServiceConfig{Role: "worker", Config: config, Binary: binary, JoiningFile: joining, Enrollment: true, Detection: myce.Detection{OS: runtime.GOOS, Arch: runtime.GOARCH}}
			b, _ := json.Marshal(c)
			if err := os.WriteFile(config, b, 0600); err != nil {
				t.Fatal(err)
			}
			err := enrollmentCommand([]string{"install", "worker", "--enroll", "--config", config, "--release-version", "v1.0.1"}, myce.MachineProbe{OS: runtime.GOOS, Arch: runtime.GOARCH}, home, nil)
			if err == nil || !strings.Contains(err.Error(), "release_version_reused") || !strings.Contains(err.Error(), "--allow-downgrade") {
				t.Fatalf("equal version changed bytes passed release gate: %v", err)
			}
			err = enrollmentCommand([]string{"install", "worker", "--enroll", "--config", config, "--release-version", "v1.0.1", "--allow-downgrade"}, myce.MachineProbe{OS: runtime.GOOS, Arch: runtime.GOARCH}, home, nil)
			if err == nil || !strings.Contains(err.Error(), "upgrade state directory required") {
				t.Fatalf("explicit equal-version replacement did not pass release gate: %v", err)
			}
		})
	}
}
