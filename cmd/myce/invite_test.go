package main

import (
	"context"
	"encoding/json"
	"html"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestInvitePrintsCommandWithoutCredential(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCE_JOINING_FILE", "")
	t.Setenv("MYCE_QUEUE", "") // Fleet jobs inherit the worker service queue address.
	dir := t.TempDir()
	identity, err := myce.LoadJoiningIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	token := "invite-private-test-only"
	t.Setenv("MYCE_TOKEN", token)
	path := filepath.Join(t.TempDir(), "private invite.json")
	output := captureJoiningOutput(t, func() error { return inviteCommand([]string{"--private-file", "--identity-dir", dir, "--out", path}) })
	if strings.Contains(output, token) || !strings.Contains(output, "myce install worker --joining-file 'private invite.json'") {
		t.Fatal("unsafe invite output")
	}
	invitation, err := myce.ReadInvitation(path)
	if err != nil || invitation.Token != token || invitation.Fingerprint != identity.Fingerprint() {
		t.Fatal("invitation missing authority", err)
	}
	if err = inviteCommand([]string{"--private-file", "--identity-dir", dir, "--out", path}); err == nil {
		t.Fatal("overwrote invitation")
	}
	copyPath := filepath.Join(t.TempDir(), "copy.json")
	captureJoiningOutput(t, func() error {
		return inviteCommand([]string{"--private-file", "--joining-file", path, "--out", copyPath})
	})
	copy, err := myce.ReadInvitation(copyPath)
	if err != nil || copy != invitation {
		t.Fatal("existing hypha cannot invite", err)
	}
}
func captureJoiningOutput(t *testing.T, run func() error) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()
	// Drain concurrently: output larger than the pipe buffer would block run.
	type readResult struct {
		data []byte
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(r)
		read <- readResult{data, err}
	}()
	err = run()
	w.Close()
	os.Stdout = old
	got := <-read
	data, readErr := got.data, got.err
	r.Close()
	if err != nil || readErr != nil {
		t.Fatal("command failed", err, readErr)
	}
	return string(data)
}
func TestInvitationInstallPersistsTrustAndStatus(t *testing.T) {
	home := t.TempDir()
	identity, err := myce.LoadJoiningIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	invitation := myce.Invitation{Version: 1, Fingerprint: identity.Fingerprint(), Token: "test-install-private", Address: "https://saved.invalid:7332"}
	path := filepath.Join(home, "invite.json")
	if err = myce.WriteInvitation(path, invitation); err != nil {
		t.Fatal(err)
	}
	c, err := myce.PlanInstall("worker", home, "", home, invitation.Address, "test", myce.Detection{OS: "darwin", Arch: "arm64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 4})
	if err != nil {
		t.Fatal(err)
	}
	c.JoiningFile = path
	binary := filepath.Join(home, "binary")
	if err = os.WriteFile(binary, []byte("test binary"), 0700); err != nil {
		t.Fatal(err)
	}
	loaded := false
	if err = myce.Install(context.Background(), c, binary, "", func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "launchctl" && args[0] == "bootstrap" && filepath.Base(args[2]) == "probe.plist" {
			b, err := os.ReadFile(args[2])
			if err != nil {
				return nil, err
			}
			_, rest, ok := strings.Cut(string(b), "<key>StandardOutPath</key><string>")
			if !ok {
				t.Fatal("launchd probe output missing")
			}
			output, _, _ := strings.Cut(rest, "</string>")
			data, _ := json.Marshal(myce.HardwarePreflight{Detection: c.Detection})
			return nil, os.WriteFile(html.UnescapeString(output), data, 0600)
		}
		if name == "launchctl" {
			switch args[0] {
			case "print":
				if !loaded {
					return nil, installFixtureExit(113)
				}
				return []byte("state = running\npid = 42\n"), nil
			case "bootstrap":
				loaded = true
			case "bootout":
				loaded = false
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	installed, err := myce.ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if installed.JoiningFile == path || installed.JoiningFile == "" {
		t.Fatal("invitation not copied into installation")
	}
	got, err := myce.ReadInvitation(installed.JoiningFile)
	if err != nil || got != invitation {
		t.Fatal("trust not persisted", err)
	}
	args := strings.Join(installed.Args(), " ")
	if !strings.Contains(args, "--joining-file") || strings.Contains(args, "--queue") || strings.Contains(args, invitation.Token) {
		t.Fatal("unsafe service arguments")
	}
	profile := &myce.CapabilityProfile{MemoryLayout: "unified", LogicalCPUs: 8, Onboarding: "hardware-facts-only; throughput pending"}
	text := captureJoiningOutput(t, func() error {
		printStatus(myce.Snapshot{Machines: []*myce.Machine{{ID: "test", CapabilityProfile: profile}}}, false)
		return nil
	})
	if !strings.Contains(text, "capability profile v0") || !strings.Contains(text, "unified") {
		t.Fatal("profile missing from CLI status")
	}
}

func TestInviteExplicitIdentityOverridesHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("MYCE_JOINING_FILE", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_TOKEN", "synthetic-inviter")
	other := myce.Invitation{Version: 1, Fingerprint: strings.Repeat("b", 64), Token: "synthetic-other"}
	joining := filepath.Join(home, "other.json")
	if err := myce.WriteInvitation(joining, other); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local/share/mycelium/worker/config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte(`{"joining_file":"`+joining+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	identity, err := myce.LoadJoiningIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "invitation.json")
	captureJoiningOutput(t, func() error { return inviteCommand([]string{"--private-file", "--identity-dir", dir, "--out", out}) })
	got, err := myce.ReadInvitation(out)
	if err != nil || got.Fingerprint != identity.Fingerprint() {
		t.Fatal("explicit identity lost to HOME config", err)
	}
}
func TestInviteDeadInstallFlagRemoved(t *testing.T) {
	if err := inviteCommand([]string{"--install=false"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatal("dead install flag accepted", err)
	}
}
