package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"mycelium/internal/myce"
)

func TestReleaseApproveUsesInstalledAuthorityWithoutReconfiguration(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	m := myce.ReleaseManifest{Version: "v1.0.2", StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol}
	for _, target := range myce.ReleaseTargets {
		m.Artifacts = append(m.Artifacts, myce.ReleaseArtifact{OS: target.OS, Arch: target.Arch, File: "myce-" + target.OS + "-" + target.Arch, SHA256: strings.Repeat("a", 64), Size: 123, NativeSensing: target.NativeSensing})
	}
	payload, _ := json.Marshal(m)
	signed, _ := json.Marshal(myce.SignedRelease{Manifest: payload, Signature: ed25519.Sign(private, payload)})
	var requests atomic.Int32
	var ca []byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != "POST" || r.URL.Path != "/api/invites/release" || r.Header.Get("Authorization") != "Bearer fixture-authority" {
			t.Error("approval did not use authenticated admin endpoint")
		}
		var request myce.EnrollmentReleaseApproval
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		verified, err := myce.VerifyRelease(request.SignedRelease, pub)
		if err != nil || verified.Version != m.Version || !bytes.Equal(request.PublicKey, pub) || !request.AllowDowngrade || request.CACert != string(ca) || request.Origin != "https://publisher.invalid/new" {
			t.Error("signed release, provisioned trust or explicit force not forwarded", err)
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	root := t.TempDir()
	joining, config := filepath.Join(root, "joining.json"), filepath.Join(root, "config.json")
	hash := sha256.Sum256(server.Certificate().Raw)
	if err = myce.WriteInvitation(joining, myce.Invitation{Version: 1, Fingerprint: hex.EncodeToString(hash[:]), Token: "fixture-authority", Address: server.URL}); err != nil {
		t.Fatal(err)
	}
	saved, _ := json.Marshal(myce.ServiceConfig{Role: "queue", JoiningFile: joining, DB: "queue.db", EnvironmentCatalog: "environments.yaml", ModelCatalog: "models.yaml", Listen: "127.0.0.1:7331", CPUQuota: "80%"})
	keyDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath, keyPath, caPath := filepath.Join(root, "release.json"), filepath.Join(root, "publisher.pem"), filepath.Join(root, "ca.pem")
	ca = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	for path, data := range map[string][]byte{config: saved, manifestPath: signed, keyPath: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: keyDER}), caPath: ca} {
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"approve", "--config", config, "--release-manifest", manifestPath, "--release-key", keyPath, "--release-url", "https://publisher.invalid/new", "--release-ca", caPath, "--allow-downgrade"}
	output := captureJoiningOutput(t, func() error { return releaseCommand(args) })
	if requests.Load() != 1 || !strings.Contains(output, `"approved_version":"v1.0.2"`) || strings.Contains(output, "fixture-authority") {
		t.Fatalf("unexpected approval output: %q (%d requests)", output, requests.Load())
	}
	after, err := os.ReadFile(config)
	if err != nil || !bytes.Equal(saved, after) {
		t.Fatal("approval rewrote service configuration", err)
	}
	if err = os.WriteFile(manifestPath, payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err = releaseCommand(args); err == nil || requests.Load() != 1 {
		t.Fatal("unsigned manifest reached queue despite local verification", err)
	}
}

func TestReleaseVersionRefusesWrongArtifactProtocolOrTarget(t *testing.T) {
	for _, args := range [][]string{
		{"--expect-version", "wrong-version"},
		{"--expect-os", "wrong-os"},
		{"--expect-arch", "wrong-arch"},
		{"--expect-start-gate", "wrong-protocol"},
		{"--expect-attempt-protocol", "wrong-protocol"},
	} {
		if err := versionCommand(args); err == nil {
			t.Fatal("wrong artifact accepted", args)
		}
	}
	captureJoiningOutput(t, func() error {
		return versionCommand([]string{"--expect-version", myce.BuildVersion, "--expect-os", runtime.GOOS, "--expect-arch", runtime.GOARCH, "--expect-start-gate", myce.StartGateProtocol, "--expect-attempt-protocol", myce.AttemptProtocol, "--require-native-sensing"})
	})
}
