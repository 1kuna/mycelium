package myce

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func signedTestRelease(t *testing.T) ([]byte, ed25519.PublicKey, ReleaseManifest) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	m := ReleaseManifest{Version: "v1.0.1", StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol}
	for _, target := range [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "arm64"}} {
		m.Artifacts = append(m.Artifacts, ReleaseArtifact{OS: target[0], Arch: target[1], File: "myce-" + target[0] + "-" + target[1], SHA256: strings.Repeat("a", 64), Size: 123, NativeSensing: target[0] == "darwin"})
	}
	data, _ := json.Marshal(m)
	envelope, _ := json.Marshal(SignedRelease{Manifest: data, Signature: ed25519.Sign(key, data)})
	return envelope, pub, m
}
func TestReleaseTrustMatrixProtocolAndMacSensing(t *testing.T) {
	data, pub, m := signedTestRelease(t)
	verified, err := VerifyRelease(data, pub)
	if err != nil {
		t.Fatal(err)
	}
	wrong, _, _ := ed25519.GenerateKey(rand.Reader)
	if _, err = VerifyRelease(data, wrong); err == nil {
		t.Fatal("unprovisioned signing key accepted")
	}
	for _, a := range m.Artifacts {
		if _, err = verified.Select(a.OS, a.Arch); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = verified.Select("linux", "386"); err == nil {
		t.Fatal("unsupported target accepted")
	}
	data[len(data)/2] ^= 1
	if _, err = VerifyRelease(data, pub); err == nil {
		t.Fatal("tampered manifest accepted")
	}
	m.StartGate = "old"
	if err = m.Validate(); err == nil {
		t.Fatal("wrong protocol accepted")
	}
	m.StartGate = StartGateProtocol
	m.Artifacts[2].NativeSensing = false
	if err = m.Validate(); err == nil {
		t.Fatal("Mac without native sensing accepted")
	}
	m.Artifacts[2].NativeSensing = true
	m.Artifacts[0].File = "../other"
	if err = m.Validate(); err == nil {
		t.Fatal("artifact traversal accepted")
	}
}

func TestReleaseQueueApprovalAndAuthentication(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer((API{Queue: q, Token: "test-authority"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test-authority"}
	var release EnrollmentRelease
	if err := client.Call(context.Background(), "GET", "/api/invites/release", nil, &release); err == nil {
		t.Fatal("unconfigured queue approved release")
	}
	data, pub, m := signedTestRelease(t)
	bad := append([]byte{}, data...)
	bad[len(bad)/2] ^= 1
	if err := q.ConfigureEnrollmentRelease(bad, pub, "https://publisher.invalid"); err == nil {
		t.Fatal("queue approved tampered release")
	}
	if err := q.ConfigureEnrollmentRelease(data, pub, "https://publisher.invalid/releases"); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(context.Background(), "GET", "/api/invites/release", nil, &release); err != nil || release.Manifest.Version != m.Version {
		t.Fatal("approved release unavailable", err)
	}
	client.Token = "limited-ticket"
	if err := client.Call(context.Background(), "GET", "/api/invites/release", nil, nil); err == nil {
		t.Fatal("unauthenticated release API")
	}
	response, err := http.Get(server.URL + "/api/invites/release")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatal("anonymous release API")
	}
}
func TestBootstrapRequiresTrustedHTTPSOrigin(t *testing.T) {
	_, _, m := signedTestRelease(t)
	for _, origin := range []string{"http://publisher.invalid", "https://user:secret@publisher.invalid", "https://publisher.invalid/?token=oops"} {
		if _, err := BootstrapCommand(m, origin, "https://queue.invalid", strings.Repeat("a", 64), strings.Repeat("b", 64)); err == nil {
			t.Fatal("untrusted origin accepted")
		}
	}
}

func TestReleaseDowngradeOrdering(t *testing.T) {
	for _, tc := range []struct {
		target, installed string
		allow, ok         bool
	}{
		{"v1.9.0", "v1.10.0", false, false},
		{"v1.9.0", "v1.10.0", true, true},
		{"v1.10.0", "v1.9.0", false, true},
		{"v2.0.0-rc.1", "v2.0.0", false, false},
		{"v2.0.0", "v2.0.0-rc.1", false, true},
		{"v1.0.0", "v1.0.0", false, true},
		{"opaque", "v1.0.0", true, false},
	} {
		if err := CheckReleaseUpgrade(tc.target, tc.installed, tc.allow); (err == nil) != tc.ok {
			t.Fatal("incorrect semantic version ordering", tc, err)
		}
	}
}

func TestReleaseMatrixUsesConfiguredTargets(t *testing.T) {
	old := ReleaseTargets
	defer func() { ReleaseTargets = old }()
	_, _, m := signedTestRelease(t)
	ReleaseTargets = append(append([]ReleaseTarget{}, old...), ReleaseTarget{OS: "linux", Arch: "riscv64"})
	if err := m.Validate(); err == nil {
		t.Fatal("new required target ignored")
	}
	m.Artifacts = append(m.Artifacts, ReleaseArtifact{OS: "linux", Arch: "riscv64", File: "myce-linux-riscv64", SHA256: strings.Repeat("a", 64), Size: 123})
	if err := m.Validate(); err != nil {
		t.Fatal("data-driven target refused", err)
	}
	if _, err := m.Select("linux", "riscv64"); err != nil {
		t.Fatal(err)
	}
}
