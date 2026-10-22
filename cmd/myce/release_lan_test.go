package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// Exercise the real invite and generated shell with curl's TLS verification.
// Only the downloaded executable is synthetic; no installation is performed.
func TestInviteLANPublisherTrust(t *testing.T) {
	for _, mode := range []string{"trusted", "untrusted", "wrong-ca", "hostname", "redirect", "checksum"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			payload := []byte("#!/bin/sh\nif test \"$1\" = version; then exit 0; fi\nread -r ticket\necho downloaded-and-verified\n")
			digest := sha256.Sum256(payload)
			publisher := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "redirect" {
					w.Header().Set("Location", "https://redirect.invalid/asset")
					w.WriteHeader(http.StatusFound)
					return
				}
				if mode == "checksum" {
					w.Write([]byte(strings.Repeat("x", len(payload))))
					return
				}
				w.Write(payload)
			}))
			defer publisher.Close()
			ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: publisher.Certificate().Raw}))
			origin := publisher.URL
			if mode == "untrusted" {
				ca = ""
			}
			if mode == "wrong-ca" {
				identity, err := myce.LoadJoiningIdentity(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				ca = string(identity.Certificate)
			}
			if mode == "hostname" {
				origin = strings.Replace(origin, "127.0.0.1", "localhost", 1)
			}
			m := myce.ReleaseManifest{Version: "v1.0.1", StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol}
			for _, target := range myce.ReleaseTargets {
				m.Artifacts = append(m.Artifacts, myce.ReleaseArtifact{OS: target.OS, Arch: target.Arch, File: "myce-" + target.OS + "-" + target.Arch, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(payload)), NativeSensing: target.NativeSensing})
			}
			identity, err := myce.LoadJoiningIdentity(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			queue := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer synthetic-fleet-secret" {
					w.WriteHeader(401)
					return
				}
				if r.URL.Path == "/api/invites/release" {
					json.NewEncoder(w).Encode(map[string]any{"manifest": m, "origin": origin, "ca_cert": ca})
				} else {
					json.NewEncoder(w).Encode(map[string]any{"ticket": strings.Repeat("b", 64)})
				}
			}))
			cert, err := identity.TLS()
			if err != nil {
				t.Fatal(err)
			}
			queue.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
			queue.StartTLS()
			defer queue.Close()
			joining := filepath.Join(home, "invitation.json")
			if err = myce.WriteInvitation(joining, myce.Invitation{Version: 1, Fingerprint: identity.Fingerprint(), Token: "synthetic-fleet-secret", Address: queue.URL}); err != nil {
				t.Fatal(err)
			}
			command := captureJoiningOutput(t, func() error { return inviteCommand([]string{"--joining-file", joining}) })
			if strings.Count(command, "\n") != 1 || strings.Contains(command, "synthetic-fleet-secret") {
				t.Fatal("unsafe invite output")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			child := exec.CommandContext(ctx, "sh", "-c", strings.TrimSpace(command))
			child.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin", "CURL_CA_BUNDLE=/dev/null"}
			out, err := child.CombinedOutput()
			if mode == "trusted" {
				if err != nil || !strings.Contains(string(out), "downloaded-and-verified") {
					t.Fatalf("LAN release failed: %v: %s", err, out)
				}
			} else if err == nil || strings.Contains(string(out), "downloaded-and-verified") {
				t.Fatal("untrusted release executed", mode)
			}
			if _, err := os.Stat(filepath.Join(home, ".cache/mycelium/bootstrap")); err != nil {
				t.Fatal(err)
			}
			leftovers, err := filepath.Glob(filepath.Join(home, ".cache/mycelium/bootstrap/.release-ca.*"))
			if err != nil || len(leftovers) != 0 {
				t.Fatal("temporary publisher trust retained", err)
			}
		})
	}
}
