package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestOnboardingSelectsOnlyClusterNAS(t *testing.T) {
	for _, mode := range []string{"one", "none", "two", "override"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			first, second := filepath.Join(home, "mounted first"), filepath.Join(home, "mounted second")
			identity := fmt.Sprintf("%x", sha256.Sum256([]byte("smb://storage.invalid/share")))
			authority := map[string]any{"version": 1, "fingerprint": strings.Repeat("a", 64), "token": "fixture", "address": "https://queue.invalid", "nas_identity": map[string]string{"fingerprint": identity, "checkpoint_path": "mycelium/checkpoints"}}
			path := filepath.Join(home, "invitation.json")
			data, _ := json.Marshal(authority)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			inventory := "//fixture:private@storage.invalid/share on " + first + " (smbfs, nodev)\n"
			if mode == "none" {
				inventory = "//storage.invalid/other on " + first + " (smbfs, nodev)\n"
			}
			if mode == "two" || mode == "override" {
				inventory += "//different-user@storage.invalid/share on " + second + " (smbfs, nodev)\n"
			}
			p := myce.MachineProbe{OS: "darwin", Arch: "arm64", Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
				if name != "/sbin/mount" {
					t.Fatalf("unexpected command %s", name)
				}
				return []byte(inventory), nil
			}}
			args := []string{"install", "worker", "--gpu", "cpu", "--joining-file", path, "--dry-run"}
			if mode == "override" {
				args = append(args, "--nas", second)
			}
			var result error
			output := captureJoiningOutput(t, func() error { result = setupCommandWithProbe(args, p, home); return nil })
			if mode == "none" || mode == "two" {
				code := "nas_identity_not_found"
				if mode == "two" {
					code = "nas_identity_ambiguous"
				}
				if result == nil || !strings.Contains(result.Error(), code) || !strings.Contains(result.Error(), "--nas") {
					t.Fatalf("expected %s refusal with override hint: %v", code, result)
				}
				if strings.Contains(result.Error(), "private") {
					t.Fatal("mount credentials disclosed")
				}
				return
			}
			if result != nil {
				t.Fatal(result)
			}
			want := first
			if mode == "override" {
				want = second
			}
			for _, line := range []string{"NAS: " + want + "\n", "Checkpoint root: " + filepath.Join(want, "mycelium", "checkpoints") + "\n", "--model-root " + filepath.Join(want, "mycelium", "models")} {
				if !strings.Contains(output, line) {
					t.Fatalf("selected wrong storage; missing %q in %s", line, output)
				}
			}
			if strings.Contains(output, "private") {
				t.Fatal("mount credentials disclosed")
			}
		})
	}
}
