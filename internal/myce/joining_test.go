package myce

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeDiscovery struct {
	addresses []string
	err       error
	calls     int
}

func (d *fakeDiscovery) Browse(context.Context) ([]string, error) {
	d.calls++
	return d.addresses, d.err
}
func joiningTestServer(t *testing.T, handler http.Handler) (*httptest.Server, Invitation) {
	t.Helper()
	identity, err := LoadJoiningIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cert, err := identity.TLS()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, Invitation{Version: 1, Token: "throwaway-test-only", Fingerprint: identity.Fingerprint(), Address: server.URL}
}
func TestJoiningAuthenticatesBeforeSendingCredential(t *testing.T) {
	var received atomic.Int32
	evil, _ := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1) }))
	good, invitation := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer throwaway-test-only" {
			t.Error("missing credential")
			w.WriteHeader(401)
		}
	}))
	d := &fakeDiscovery{addresses: []string{evil.URL, good.URL}}
	invitation.Address = ""
	client, err := ResolveJoining(context.Background(), invitation, "", d)
	if err != nil || client.URL != good.URL || received.Load() != 0 {
		t.Fatalf("selection=%s err=%v untrusted requests=%d", client.URL, err, received.Load())
	}
	client.HTTP.CloseIdleConnections()
	d = &fakeDiscovery{addresses: []string{evil.URL}}
	if _, err = ResolveJoining(context.Background(), invitation, "", d); err == nil {
		t.Fatal("untrusted discovery admitted")
	}
	if received.Load() != 0 {
		t.Fatal("credential leaked to impersonating endpoint")
	}
}
func TestJoiningFallbackExplicitAndWrongCredential(t *testing.T) {
	server, i := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer throwaway-test-only" {
			w.WriteHeader(401)
		}
	}))
	for _, d := range []*fakeDiscovery{{}, {err: errors.New("multicast blocked")}, {addresses: []string{"http://untrusted.invalid"}}} {
		c, err := ResolveJoining(context.Background(), i, "", d)
		if err != nil || c.URL != server.URL {
			t.Fatal(c.URL, err)
		}
		c.HTTP.CloseIdleConnections()
	}
	d := &fakeDiscovery{}
	c, err := ResolveJoining(context.Background(), i, server.URL, d)
	if err != nil || d.calls != 0 {
		t.Fatal(err, d.calls)
	}
	c.HTTP.CloseIdleConnections()
	i.Token = "wrong"
	if _, err = ResolveJoining(context.Background(), i, "", d); err == nil {
		t.Fatal("wrong credential accepted")
	}
	i.Token = ""
	if _, err = ResolveJoining(context.Background(), i, "", d); err == nil {
		t.Fatal("discovery granted trust")
	}
}
func TestJoiningNeverFollowsRedirects(t *testing.T) {
	var leaked atomic.Bool
	evil := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer evil.Close()
	_, i := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, evil.URL, 307) }))
	if _, err := ResolveJoining(context.Background(), i, "", &fakeDiscovery{}); err == nil || leaked.Load() {
		t.Fatal("redirect admitted or followed")
	}
}
func TestInvitationPrivacyAndIdentityPersistence(t *testing.T) {
	dir := t.TempDir()
	a, err := LoadJoiningIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadJoiningIdentity(dir)
	if err != nil || a.Fingerprint() != b.Fingerprint() {
		t.Fatal("identity changed", err)
	}
	path := filepath.Join(dir, "invite.json")
	i := Invitation{Version: 1, Fingerprint: a.Fingerprint(), Token: "private-test-only"}
	if err = WriteInvitation(path, i); err != nil {
		t.Fatal(err)
	}
	got, err := ReadInvitation(path)
	if err != nil || got != i {
		t.Fatal("invitation roundtrip", err)
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadInvitation(path); err == nil {
		t.Fatal("publicly readable invitation accepted")
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias")
	if err = os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadInvitation(alias); err == nil {
		t.Fatal("symlink invitation accepted")
	}
	for _, address := range []string{"http://example.invalid", "https://user:secret@example.invalid", "https://example.invalid/?token=secret"} {
		if _, err = i.Client(address); err == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
}
func TestProfileCheckinAndPersistence(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	m := machine("profile-test", "fake")
	m.CapabilityProfile = &CapabilityProfile{Version: 0, Onboarding: "hardware-facts-only; throughput pending", MemoryLayout: "unified", SystemMemoryBytes: 16 << 30, LogicalCPUs: 8, RecordedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var reply Reply
	if err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, Fresh: true}, &reply); err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "GET", "/api/status", nil, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Machines) != 1 || snapshot.Machines[0].CapabilityProfile == nil || !strings.Contains(snapshot.Machines[0].CapabilityProfile.Summary(), "unified") {
		t.Fatal("profile missing from status")
	}
}

func TestJoiningFailureIdentifiesStage(t *testing.T) {
	_, invitation := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	invitation.Address = ""
	evil, _ := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted request") }))
	tests := []struct {
		name, want string
		discovery  *fakeDiscovery
	}{
		{"empty", "mDNS browse returned no compatible addresses", &fakeDiscovery{}},
		{"transport", "mDNS browse failed", &fakeDiscovery{err: errors.New("private diagnostic")}},
		{"pin", "identity verification rejected", &fakeDiscovery{addresses: []string{evil.URL}}},
		{"credential", "credential rejected", &fakeDiscovery{addresses: []string{"placeholder"}}},
		{"connect", "connection failed", &fakeDiscovery{addresses: []string{"https://127.0.0.1:1"}}},
	}
	server, valid := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			i := invitation
			if tc.name == "credential" {
				i = valid
				i.Address = ""
				tc.discovery.addresses = []string{server.URL}
			}
			_, err := ResolveJoining(context.Background(), i, "", tc.discovery)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "private diagnostic") || strings.Contains(err.Error(), i.Token) {
				t.Fatal("private data in error")
			}
		})
	}
}
