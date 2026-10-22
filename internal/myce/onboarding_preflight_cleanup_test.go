package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoragePreflightRemovesOnlyItsNewDirectories(t *testing.T) {
	for _, mode := range []string{"success", "probe-failure", "cancel-during-mkdir"} {
		t.Run(mode, func(t *testing.T) {
			mount := t.TempDir()
			c, err := PlanInstall("worker", t.TempDir(), "", mount, "http://queue.invalid", "fixture", Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "fixed", MemoryGB: 8})
			if err != nil {
				t.Fatal(err)
			}
			// Existing directories and their contents belong to the user.
			keep := filepath.Join(mount, "keep")
			if err := os.WriteFile(keep, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fs := onboardingStorageFS{lstat: os.Lstat, removeCreated: true, mkdir: func(path string, perm os.FileMode) error {
				err := os.Mkdir(path, perm)
				if err == nil && mode == "cancel-during-mkdir" {
					cancel()
				}
				return err
			}, probe: func(string) error {
				if mode == "probe-failure" {
					return errors.New("injected write refusal")
				}
				return nil
			}}
			_, err = prepareOnboardingStorageFS(ctx, c, false, func(context.Context, string, ...string) ([]byte, error) {
				return []byte("fixture " + mount + " nfs rw 0 0\n"), nil
			}, fs)
			// Cancellation may return while the owned filesystem sequence is
			// unwinding. Join it before checking cleanup, never race its defer.
			onboardingNASIO.mu.Lock()
			pending := onboardingNASIO.pending
			onboardingNASIO.mu.Unlock()
			if pending != nil {
				select {
				case <-pending.done:
				case <-time.After(time.Second):
					t.Fatal("preflight cleanup did not finish")
				}
			}
			if (err == nil) != (mode == "success") {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
			if _, err := os.Stat(filepath.Join(mount, "mycelium")); !os.IsNotExist(err) {
				t.Fatal("preflight retained new directories", err)
			}
			if data, err := os.ReadFile(keep); err != nil || string(data) != "preserve" {
				t.Fatal("preflight changed existing data", err)
			}
		})
	}
}
