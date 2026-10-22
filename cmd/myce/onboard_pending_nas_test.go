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
	"sync/atomic"
	"testing"

	"mycelium/internal/myce"
)

func TestEnrollmentResumesPendingAuthorityWithCurrentNAS(t *testing.T) {
	home := t.TempDir()
	mount := filepath.Join(home, "mounted share")
	if err := os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	identity := myce.NASIdentity{Fingerprint: fmt.Sprintf("%x", sha256.Sum256([]byte("smb://storage.invalid/share"))), CheckpointPath: "mycelium/checkpoints"}
	var metadata, redemptions atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/status" && r.Header.Get("Authorization") == "Bearer fixture-authority" {
			json.NewEncoder(w).Encode(myce.Snapshot{})
			return
		}
		if r.URL.Path == "/api/invites/redeem" {
			redemptions.Add(1)
		}
		if r.Method != "GET" || r.URL.Path != "/api/invites/release" || r.Header.Get("Authorization") != "Bearer fixture-authority" {
			http.Error(w, "unexpected request", 403)
			return
		}
		metadata.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"nas_identity": identity})
	}))
	defer server.Close()
	authority := myce.Invitation{Version: 1, Address: server.URL, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(server.Certificate().Raw)), Token: "fixture-authority"}
	pending := filepath.Join(home, ".cache", "mycelium", "bootstrap", "invitation.json")
	if err := os.MkdirAll(filepath.Dir(pending), 0700); err != nil {
		t.Fatal(err)
	}
	if err := myce.WriteInvitation(pending, authority); err != nil {
		t.Fatal(err)
	}
	probe := myce.MachineProbe{OS: "linux", Arch: "arm64", Glob: func(string) ([]string, error) { return nil, nil }, ReadFile: func(string) ([]byte, error) { return nil, os.ErrNotExist }}
	probe.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "cat":
			return []byte("//fixture@storage.invalid/share " + strings.ReplaceAll(mount, " ", `\040`) + " cifs rw 0 0\n"), nil
		case "loginctl":
			return []byte("yes"), nil
		case "systemctl":
			return nil, nil
		case "systemd-run":
			return nil, errors.New("deliberate service environment refusal after NAS preflight")
		}
		return nil, exec.ErrNotFound
	}
	var result error
	output := captureJoiningOutput(t, func() error {
		result = enrollmentCommand([]string{"install", "worker", "--enroll", "--queue", server.URL, "--pin", authority.Fingerprint, "--gpu", "cpu"}, probe, home, func(string) (myce.WorkerAccount, error) { return myce.WorkerAccount{}, myce.ErrHostIdentity })
		return nil
	})
	if result == nil || !strings.Contains(result.Error(), "service_environment_invalid") {
		t.Fatalf("resumed enrollment did not pass automatic NAS selection: %v", result)
	}
	if metadata.Load() != 1 || redemptions.Load() != 0 {
		t.Fatalf("metadata=%d redemptions=%d", metadata.Load(), redemptions.Load())
	}
	saved, err := myce.ReadInvitation(pending)
	if err != nil || saved.NASIdentity != identity {
		t.Fatal("current NAS identity not saved privately", err)
	}
	saved.NASIdentity = myce.NASIdentity{}
	if saved != authority {
		t.Fatal("resume changed private authority")
	}
	if st, err := os.Stat(pending); err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("pending authority permissions changed", err)
	}
	if !strings.Contains(output, "NAS: "+mount+"\n") || strings.Contains(output, "Worker installed") || strings.Contains(output, "Installed.") {
		t.Fatal("incorrect selection or premature success output")
	}
	if _, err := os.Stat(filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")); !os.IsNotExist(err) {
		t.Fatal("refusal installed a worker", err)
	}
}
