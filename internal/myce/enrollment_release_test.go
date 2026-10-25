package myce

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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
	data, pub, old := signedTestRelease(t)
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
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
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
	wrong, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		data  []byte
		key   ed25519.PublicKey
		token string
		force bool
		code  string
	}{
		{"unauthenticated", current, pub, "limited-ticket", false, "authentication_required"},
		{"wrong-key", current, wrong, "fixture-authority", false, "release_approval_invalid"},
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
