package myce

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestLANReleaseApprovalPersistsPublicTrust(t *testing.T) {
	db := filepath.Join(t.TempDir(), "queue.db")
	q, err := Open(db, t.TempDir(), RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	data, pub, _ := signedTestRelease(t)
	identity, err := LoadJoiningIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ca := string(identity.Certificate)
	for _, invalid := range []string{string(identity.Key), ca + string(identity.Key), "text" + ca, " ", strings.Repeat("x", 65<<10)} {
		if err = q.ConfigureEnrollmentReleaseWithCA(data, pub, "https://publisher.invalid", invalid); err == nil {
			t.Fatal("non-certificate metadata approved")
		}
	}
	if err = q.ConfigureEnrollmentReleaseWithCA(data, pub, "https://publisher.invalid", ca); err != nil {
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, t.TempDir(), RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer((API{Queue: q, Token: "synthetic-authority"}).Handler())
	defer server.Close()
	var approved EnrollmentRelease
	if err = (Client{URL: server.URL, Token: "synthetic-authority"}).Call(context.Background(), "GET", "/api/invites/release", nil, &approved); err != nil {
		t.Fatal(err)
	}
	if approved.CACert != ca || approved.Validate() != nil {
		t.Fatal("publisher trust not retained across restart")
	}
	if err = q.ConfigureEnrollmentReleaseWithCA(data, pub, "https://publisher.invalid", ca); err != nil {
		t.Fatal("identical approval not idempotent", err)
	}
	if err = q.ConfigureEnrollmentReleaseWithCA(data, pub, "https://publisher.invalid/changed", ca); err == nil {
		t.Fatal("equal version with changed metadata bypassed version fence")
	}
}
