package myce

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEnrollmentReleaseSignedUpdatePreservesCatalogs(t *testing.T) {
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	environments := EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{
		"tools": {"fake": binaryRecipe(t, "fake")},
	}}
	models := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{
		Name: "example", Engine: "fake", Path: "example.bin", MemoryGB: 1,
		Platforms: []string{"fake"}, Environment: "tools",
	}}}
	if err = q.ConfigureEnvironments(environments); err != nil {
		t.Fatal(err)
	}
	if err = q.ConfigureModels(models); err != nil {
		t.Fatal(err)
	}
	_, _, old := signedTestRelease(t)
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	oldPayload, _ := json.Marshal(old)
	data, _ := json.Marshal(SignedRelease{Manifest: oldPayload, Signature: ed25519.Sign(private, oldPayload)})
	if err = q.ConfigureEnrollmentRelease(data, pub, "https://publisher.invalid/old"); err != nil {
		t.Fatal(err)
	}
	// Simulate installing the next signed runtime, then approving its release
	// through the running queue rather than rebuilding the queue command/config.
	oldBuild := BuildVersion
	BuildVersion = "v1.0.2"
	t.Cleanup(func() { BuildVersion = oldBuild })
	next := old
	next.Version = Version().Version
	payload, _ := json.Marshal(next)
	signed, _ := json.Marshal(SignedRelease{Manifest: payload, Signature: ed25519.Sign(private, payload)})
	request := map[string]any{"signed_release": signed, "public_key": pub, "origin": "https://publisher.invalid/new"}
	server := httptest.NewServer((API{Queue: q, Token: "fixture-authority"}).Handler())
	client := Client{URL: server.URL, Token: "fixture-authority"}
	t.Cleanup(server.Close)
	if err = client.Call(context.Background(), "POST", "/api/invites/release", request, nil); err != nil {
		t.Fatal("signed update could not advance default approval:", err)
	}
	var current EnrollmentRelease
	if err = client.Call(context.Background(), "GET", "/api/invites/release", nil, &current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Manifest, next) || current.Origin != request["origin"] {
		t.Errorf("default invite still uses old release: %+v", current)
	}
	if !reflect.DeepEqual(q.environments, environments) || !reflect.DeepEqual(q.models, models) {
		t.Error("approval changed environment/model catalogs")
	}
	server.Close()
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	server = httptest.NewServer((API{Queue: restarted, Token: "fixture-authority"}).Handler())
	defer server.Close()
	client.URL = server.URL
	if err = client.Call(context.Background(), "GET", "/api/invites/release", nil, &current); err != nil || !reflect.DeepEqual(current.Manifest, next) {
		t.Fatal("new default did not survive queue restart:", current, err)
	}
	if err = client.Call(context.Background(), "POST", "/api/invites/release", request, nil); err != nil {
		t.Fatal("identical approval was not idempotent:", err)
	}
}

func TestEnrollmentReleaseApprovalTrustAndVersionFences(t *testing.T) {
	_, _, manifest := signedTestRelease(t)
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(m ReleaseManifest) []byte {
		payload, _ := json.Marshal(m)
		data, _ := json.Marshal(SignedRelease{Manifest: payload, Signature: ed25519.Sign(private, payload)})
		return data
	}
	current := sign(manifest)
	older := manifest
	older.Version = "v1.0.0"
	changed := manifest
	changed.Artifacts = append([]ReleaseArtifact{}, manifest.Artifacts...)
	changed.Artifacts[0].Size++
	unsigned, _ := json.Marshal(manifest)
	tampered := append([]byte{}, current...)
	tampered[len(tampered)/2] ^= 1
	wrong, wrongPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	attackerPayload, _ := json.Marshal(manifest)
	attackerSigned, _ := json.Marshal(SignedRelease{Manifest: attackerPayload, Signature: ed25519.Sign(wrongPrivate, attackerPayload)})
	for _, tc := range []struct {
		name  string
		data  []byte
		key   ed25519.PublicKey
		token string
		force bool
		code  string
	}{
		{"unauthenticated", current, pub, "limited-ticket", false, "authentication_required"},
		{"wrong-key", current, wrong, "fixture-authority", false, "release_publisher_key_mismatch"},
		{"attacker-signed-even-if-forced", attackerSigned, wrong, "fixture-authority", true, "release_publisher_key_mismatch"},
		{"attacker-signature-stored-key", attackerSigned, pub, "fixture-authority", true, "release_approval_invalid"},
		{"unsigned", unsigned, pub, "fixture-authority", false, "release_approval_invalid"},
		{"tampered-even-if-forced", tampered, pub, "fixture-authority", true, "release_approval_invalid"},
		{"older", sign(older), pub, "fixture-authority", false, "release_downgrade_refused"},
		{"equal-changed-bytes", sign(changed), pub, "fixture-authority", false, "release_downgrade_refused"},
		{"forced-older", sign(older), pub, "fixture-authority", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			const origin = "https://publisher.invalid/releases"
			if err := q.ConfigureEnrollmentRelease(current, pub, origin); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer((API{Queue: q, Token: "fixture-authority"}).Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: tc.token}
			request := EnrollmentReleaseApproval{SignedRelease: tc.data, PublicKey: tc.key, Origin: origin, AllowDowngrade: tc.force}
			err := client.Call(context.Background(), "POST", "/api/invites/release", request, nil)
			var httpErr *HTTPError
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.As(err, &httpErr) || httpErr.Code != tc.code {
				t.Errorf("expected %s, got %v", tc.code, err)
			}
			client.Token = "fixture-authority"
			var approved EnrollmentRelease
			if err = client.Call(context.Background(), "GET", "/api/invites/release", nil, &approved); err != nil {
				t.Fatal(err)
			}
			want := manifest
			if tc.code == "" {
				want = older
			}
			if !reflect.DeepEqual(approved.Manifest, want) {
				t.Errorf("unexpected default approval after request: %+v", approved)
			}
		})
	}
}

func TestEnrollmentReleaseApprovalRequiresLocallyProvisionedKey(t *testing.T) {
	q, _ := fixture(t)
	data, pub, m := signedTestRelease(t)
	// Model a pre-upgrade database: approved metadata but no persisted key.
	if err := q.update(func() error {
		q.s.EnrollmentRelease = &EnrollmentRelease{Manifest: m, Origin: "https://publisher.invalid"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "fixture-authority"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "fixture-authority"}
	request := EnrollmentReleaseApproval{SignedRelease: data, PublicKey: pub, Origin: "https://publisher.invalid", AllowDowngrade: true}
	err := client.Call(context.Background(), "POST", "/api/invites/release", request, nil)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != 503 || httpErr.Code != "release_publisher_key_unavailable" || !strings.Contains(httpErr.Message, "queue host") || !strings.Contains(httpErr.Message, "--release-key") {
		t.Fatalf("missing local trust must refuse with provisioning guidance: %v", err)
	}
	if len(q.s.EnrollmentReleaseKey) != 0 {
		t.Fatal("API provisioned trust from request")
	}
	if err = q.ConfigureStartupEnrollmentRelease(data, pub, request.Origin, "", false); err != nil {
		t.Fatal("local provisioning of legacy approval failed", err)
	}
	if err = client.Call(context.Background(), "POST", "/api/invites/release", request, nil); err != nil {
		t.Fatal("locally provisioned legacy approval remained unavailable", err)
	}
}

func TestEnrollmentReleaseApprovalStorageFailure(t *testing.T) {
	q, _ := fixture(t)
	data, pub, _ := signedTestRelease(t)
	if err := q.ConfigureEnrollmentRelease(data, pub, "https://publisher.invalid"); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(q.s)
	if _, err := q.db.Exec("PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	defer q.db.Exec("PRAGMA query_only=OFF")
	server := httptest.NewServer((API{Queue: q, Token: "fixture-authority"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "fixture-authority"}
	request := EnrollmentReleaseApproval{SignedRelease: data, PublicKey: pub, Origin: "https://publisher.invalid/changed", AllowDowngrade: true}
	err := client.Call(context.Background(), "POST", "/api/invites/release", request, nil)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != 500 || httpErr.Code != "release_approval_failed" {
		t.Fatalf("storage failure must be internal, not a manifest refusal: %v", err)
	}
	after, _ := json.Marshal(q.s)
	if !bytes.Equal(before, after) {
		t.Fatal("failed commit changed default approval or trust")
	}
}

func TestEnrollmentReleaseStartupPreservesAdvancedApproval(t *testing.T) {
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	_, _, original := signedTestRelease(t)
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(m ReleaseManifest) []byte {
		payload, _ := json.Marshal(m)
		data, _ := json.Marshal(SignedRelease{Manifest: payload, Signature: ed25519.Sign(private, payload)})
		return data
	}
	if err = q.ConfigureStartupEnrollmentRelease(sign(original), pub, "https://publisher.invalid/old", "", false); err != nil {
		t.Fatal(err)
	}
	next := original
	next.Version = "v1.0.2"
	if err = q.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: sign(next), PublicKey: pub, Origin: "https://publisher.invalid/new"}); err != nil {
		t.Fatal(err)
	}
	// Reopen the same SQLite state to exercise the persisted publisher key too.
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	for _, manifest := range []ReleaseManifest{original, next} {
		if err = restarted.ConfigureStartupEnrollmentRelease(sign(manifest), pub, "https://publisher.invalid/old", "", false); err != nil {
			t.Fatal("retained startup flags prevented restart", err)
		}
		if restarted.s.EnrollmentRelease.Manifest.Version != next.Version || restarted.s.EnrollmentRelease.Origin != "https://publisher.invalid/new" {
			t.Fatal("retained startup flags replaced default")
		}
	}
	if strings.Count(output.String(), "skipped: retaining persisted approval v1.0.2") != 2 {
		t.Fatalf("startup skips were not logged: %s", output.String())
	}
	if err = restarted.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: sign(next), PublicKey: pub, Origin: "https://publisher.invalid/new"}); err != nil {
		t.Fatal("persisted publisher key not available after restart", err)
	}
	tampered := sign(original)
	tampered[len(tampered)/2] ^= 1
	for _, force := range []bool{false, true} {
		if err = restarted.ConfigureStartupEnrollmentRelease(tampered, pub, "https://publisher.invalid/old", "", force); err == nil {
			t.Error("unverified startup manifest accepted or skipped")
		}
	}
	if err = restarted.ConfigureStartupEnrollmentRelease(sign(original), pub, "https://publisher.invalid/old", "", true); err != nil {
		t.Fatal("explicit startup downgrade refused", err)
	}
	if restarted.s.EnrollmentRelease.Manifest.Version != original.Version {
		t.Fatal("explicit startup downgrade not applied")
	}
	rotated, rotatedPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(original)
	rotatedSigned, _ := json.Marshal(SignedRelease{Manifest: payload, Signature: ed25519.Sign(rotatedPrivate, payload)})
	if err = restarted.ConfigureStartupEnrollmentRelease(rotatedSigned, rotated, "https://publisher.invalid/old", "", false); err != nil {
		t.Fatal("local key rotation failed when startup manifest was skipped", err)
	}
	if err = restarted.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: sign(next), PublicKey: pub, Origin: "https://publisher.invalid/new", AllowDowngrade: true}); err == nil {
		t.Fatal("retired publisher key could still approve releases")
	}
	if err = restarted.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: rotatedSigned, PublicKey: rotated, Origin: "https://publisher.invalid/old"}); err != nil {
		t.Fatal("locally rotated key could not approve release", err)
	}
}
