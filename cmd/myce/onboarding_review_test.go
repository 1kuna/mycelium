package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

type offlineOnboardingTransport struct{ calls *int }

func (r offlineOnboardingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	*r.calls++
	return nil, errors.New("offline fixture")
}

func TestReviewOfflineInstallDoesNotDownloadTools(t *testing.T) {
	home := t.TempDir()
	credentials := filepath.Join(home, "synthetic-credentials.env")
	if err := os.WriteFile(credentials, []byte("MYCE_TOKEN=synthetic-test-secret\nMYCE_QUEUE=http://queue.invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Join(home, "empty-path"))
	downloads := 0
	old := http.DefaultTransport
	http.DefaultTransport = offlineOnboardingTransport{&downloads}
	defer func() { http.DefaultTransport = old }()
	p := myce.MachineProbe{OS: "linux", Arch: "arm64", Glob: func(string) ([]string, error) { return nil, nil }, ReadFile: func(string) ([]byte, error) { return nil, os.ErrNotExist }}
	p.Command = func(_ context.Context, n string, args ...string) ([]byte, error) {
		switch n {
		case "cat":
			return []byte("fixture " + home + " nfs rw 0 0\n"), nil
		case "loginctl":
			return []byte("yes"), nil
		case "systemctl":
			if len(args) > 1 && args[1] == "is-active" {
				return nil, installFixtureExit(3)
			}
			return nil, nil
		case "systemd-run":
			return json.Marshal(myce.HardwarePreflight{Detection: myce.Detection{OS: "linux", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"}})
		case "uv", "pixi":
			t.Error("install eagerly invoked backend")
		}
		return nil, exec.ErrNotFound
	}
	err := setupCommandWithAccountLookup([]string{"install", "worker", "--nas", home, "--gpu", "cpu", "--environment-file", credentials}, p, home, func(string) (myce.WorkerAccount, error) { return myce.WorkerAccount{}, myce.ErrHostIdentity })
	if err != nil || downloads != 0 {
		t.Fatalf("offline/no-recipe onboarding: downloads=%d err=%v", downloads, err)
	}
	c, err := myce.ReadServiceConfig(filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json"))
	if err != nil || c.Path != os.Getenv("PATH") {
		t.Fatal("service PATH changed", err)
	}
}

func TestReviewNASSelectionBeforeEitherPlan(t *testing.T) {
	for _, dry := range []bool{true, false} {
		t.Run(map[bool]string{true: "dry", false: "real"}[dry], func(t *testing.T) {
			home := t.TempDir()
			candidate := filepath.Join(home, "other-share")
			p := myce.MachineProbe{OS: "linux", Arch: "arm64", Glob: func(string) ([]string, error) { return nil, nil }, Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
				if n == "cat" {
					return []byte("fixture " + candidate + " nfs rw 0 0\n"), nil
				}
				return nil, exec.ErrNotFound
			}}
			args := []string{"install", "worker", "--gpu", "cpu"}
			if dry {
				args = append(args, "--dry-run")
			}
			var err error
			// Capture ourselves: captureJoiningOutput requires command success.
			read, write, e := os.Pipe()
			if e != nil {
				t.Fatal(e)
			}
			original := os.Stdout
			os.Stdout = write
			err = setupCommandWithProbe(args, p, home)
			os.Stdout = original
			write.Close()
			var output strings.Builder
			_, e = io.Copy(&output, read)
			read.Close()
			if e != nil {
				t.Fatal(e)
			}
			var typed *myce.PreflightError
			if !errors.As(err, &typed) || typed.Code != "nas_not_configured" {
				t.Fatalf("selection error: %v", err)
			}
			if strings.Contains(output.String(), "NAS:") || strings.Contains(output.String(), "Command:") {
				t.Fatal("printed an unresolved plan")
			}
			if !strings.Contains(err.Error(), candidate) || !strings.Contains(err.Error(), "--nas") {
				t.Fatal("missing mount selection hint", err)
			}
			entries, e := os.ReadDir(home)
			if e != nil || len(entries) != 0 {
				t.Fatal("refusal wrote files", entries, e)
			}
		})
	}
}

func TestReviewDesktopTrueSurvivesInstallRerun(t *testing.T) {
	c, p := savedInstallFixture(t, "worker")
	c.DailyUse, c.Desktop, c.Laptop = true, true, false
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(c.Config, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = setupCommandWithProbe([]string{"install", "worker", "--config", c.Config}, p, c.WorkerHome); err != nil {
		t.Fatal(err)
	}
	got, err := myce.ReadServiceConfig(c.Config)
	if err != nil || !got.Desktop || !got.DailyUse || got.Laptop {
		t.Fatal("desktop policy lost", err)
	}
	if !strings.Contains(strings.Join(got.Args(), " "), "--desktop=true") {
		t.Fatal("desktop lost from service command")
	}
}
