package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestInstallSuccessOutputRequiresSuccessfulInstall(t *testing.T) {
	for _, mode := range []string{"dry-run", "storage-refusal", "install-failure", "success"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			args := []string{"install", "worker", "--gpu", "fake", "--memory-mode", "fixed", "--memory-gb", "2", "--allow-deviceless", "--nas", home}
			if mode == "storage-refusal" {
				args = []string{"install", "worker", "--gpu", "cpu", "--allow-deviceless", "--nas", home}
				if err := os.Mkdir(filepath.Join(home, "mycelium"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(home, "mycelium", "checkpoints"), []byte("not a directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "dry-run" {
				args = append(args, "--dry-run")
			}
			called := false
			var installErr error
			output := captureJoiningOutput(t, func() error {
				installErr = setupCommandWithInstaller(args, myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) {
					return []byte("fixture " + home + " nfs rw 0 0\n"), nil
				}}, home, func(string) (myce.WorkerAccount, error) { return myce.WorkerAccount{}, myce.ErrHostIdentity },
					func(context.Context, myce.ServiceConfig, string, string, func(context.Context, string, ...string) ([]byte, error)) error {
						called = true
						// A marker in the same output stream makes the ordering observable.
						_, _ = os.Stdout.WriteString("INSTALL CALLBACK\n")
						if mode == "install-failure" {
							return errors.New("injected install refusal")
						}
						return nil
					})
				return nil // Capture a deliberate failure too.
			})
			if (installErr != nil) != (mode == "install-failure" || mode == "storage-refusal") {
				t.Fatalf("unexpected result: %v", installErr)
			}
			if mode == "storage-refusal" && (installErr == nil || !strings.Contains(installErr.Error(), "nas_root_invalid")) {
				t.Fatalf("did not reach storage refusal: %v", installErr)
			}
			if called != (mode != "dry-run" && mode != "storage-refusal") {
				t.Fatalf("installer invoked=%v", called)
			}
			success := strings.Index(output, "Worker installed;")
			if mode != "success" && (success >= 0 || strings.Contains(output, "Installed. Run")) {
				t.Fatalf("reported success without install: %s", output)
			}
			if mode == "success" && (success < strings.Index(output, "INSTALL CALLBACK") || success < 0) {
				t.Fatalf("success preceded installation: %s", output)
			}
		})
	}
}
