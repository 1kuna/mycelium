package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestInvitePrintsCommandWithoutCredential(t *testing.T) {
	dir := t.TempDir()
	identity, err := myce.LoadJoiningIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	token := "invite-private-test-only"
	t.Setenv("MYCE_TOKEN", token)
	path := filepath.Join(t.TempDir(), "private invite.json")
	output := captureJoiningOutput(t, func() error { return inviteCommand([]string{"--identity-dir", dir, "--out", path}) })
	if strings.Contains(output, token) || !strings.Contains(output, "myce install worker --joining-file 'private invite.json'") {
		t.Fatal("unsafe invite output")
	}
	invitation, err := myce.ReadInvitation(path)
	if err != nil || invitation.Token != token || invitation.Fingerprint != identity.Fingerprint() {
		t.Fatal("invitation missing authority", err)
	}
	if err = inviteCommand([]string{"--identity-dir", dir, "--out", path}); err == nil {
		t.Fatal("overwrote invitation")
	}
	copyPath := filepath.Join(t.TempDir(), "copy.json")
	captureJoiningOutput(t, func() error { return inviteCommand([]string{"--joining-file", path, "--out", copyPath}) })
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
	err = run()
	w.Close()
	os.Stdout = old
	data, readErr := io.ReadAll(r)
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
	if err = myce.Install(context.Background(), c, binary, "", func(context.Context, string, ...string) ([]byte, error) { return nil, nil }); err != nil {
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
