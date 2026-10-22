package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnrollmentReleaseProjectsCurrentNASIdentity(t *testing.T) {
	q, _ := fixture(t)
	root := filepath.Dir(q.root)
	if err := q.ConfigureOnboardingNAS(context.Background(), "linux", func(context.Context, string, ...string) ([]byte, error) {
		return []byte("//private:secret@storage.invalid/share " + root + " cifs rw 0 0\n"), nil
	}); err != nil {
		t.Fatal(err)
	}
	data, key, _ := signedTestRelease(t)
	if err := q.ConfigureEnrollmentRelease(data, key, "https://publisher.invalid"); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(q.s.EnrollmentRelease)
	server := httptest.NewServer((API{Queue: q, Token: "fixture-authority"}).Handler())
	defer server.Close()
	var current struct {
		NASIdentity NASIdentity `json:"nas_identity"`
	}
	client := Client{URL: server.URL, Token: "fixture-authority"}
	if err := client.Call(context.Background(), "GET", "/api/invites/release", nil, &current); err != nil {
		t.Fatal(err)
	}
	if current.NASIdentity.Fingerprint != nasSourceFingerprint("//storage.invalid/share", "smbfs") || current.NASIdentity.CheckpointPath != filepath.Base(q.root) {
		t.Fatalf("current share metadata missing: %+v", current.NASIdentity)
	}
	after, _ := json.Marshal(q.s.EnrollmentRelease)
	if !bytes.Equal(before, after) {
		t.Fatal("metadata projection changed persisted release")
	}
	public, _ := json.Marshal(current)
	for _, private := range []string{root, "private", "secret", "storage.invalid"} {
		if strings.Contains(string(public), private) {
			t.Fatal("NAS metadata exposed private mount details")
		}
	}
	client.Token = "wrong-authority"
	if err := client.Call(context.Background(), "GET", "/api/invites/release", nil, nil); err == nil {
		t.Fatal("NAS metadata served without authentication")
	}
}
