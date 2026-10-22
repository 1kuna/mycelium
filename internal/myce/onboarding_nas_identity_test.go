package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRedeemedInvitationCarriesShareIdentityWithoutLocalPaths(t *testing.T) {
	q, _ := fixture(t)
	queueMount := filepath.Dir(q.root)
	command := func(context.Context, string, ...string) ([]byte, error) {
		return []byte("//private:secret@Storage.Invalid/Share " + queueMount + " cifs rw,password=secret 0 0\n"), nil
	}
	if err := q.ConfigureOnboardingNAS(context.Background(), "linux", command); err != nil {
		t.Fatal(err)
	}
	var handler http.Handler
	server, authority := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
	handler = (API{Queue: q, Token: authority.Token}).HandlerWithJoiningIdentity(authority.Fingerprint)
	ticket, err := q.MintEnrollmentTicket(authority.Fingerprint, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	invitation, err := RedeemInvitation(context.Background(), server.URL, authority.Fingerprint, ticket.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	if invitation.NASIdentity.Fingerprint != nasSourceFingerprint("//other@storage.invalid/share", "smbfs") || invitation.NASIdentity.Fingerprint == "" || invitation.NASIdentity.CheckpointPath != filepath.Base(q.root) {
		t.Fatalf("identity not conveyed: %+v", invitation.NASIdentity)
	}
	b, _ := json.Marshal(invitation.NASIdentity)
	for _, private := range []string{queueMount, "private", "secret", "Storage.Invalid"} {
		if strings.Contains(string(b), private) {
			t.Fatal("invitation leaked mount metadata")
		}
	}
	path := filepath.Join(t.TempDir(), "invitation.json")
	if err := WriteInvitation(path, invitation); err != nil {
		t.Fatal(err)
	}
	saved, err := ReadInvitation(path)
	if err != nil || saved != invitation {
		t.Fatal("identity lost in invitation roundtrip", err)
	}
	c, _ := PlanInstall("worker", t.TempDir(), "", "/missing/default", "https://queue.invalid", "fixture", Detection{OS: "darwin", Arch: "arm64", Platform: "cpu"})
	localMount := filepath.Join(t.TempDir(), "local share")
	got, err := ResolveOnboardingStorage(context.Background(), c, true, func(context.Context, string, ...string) ([]byte, error) {
		return []byte("//user@storage.invalid/share on " + localMount + " (smbfs, nodev)\n"), nil
	}, saved.NASIdentity)
	if err != nil || got.NAS != localMount || got.Checkpoints != filepath.Join(localMount, filepath.Base(q.root)) {
		t.Fatal("queue-relative checkpoint path not adopted", err)
	}
}

func TestNASIdentityKeepsExplicitRootsAndRejectsUnsafeRelativePath(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	c.Checkpoints = filepath.Join(c.NAS, "custom-checkpoints")
	c.ModelRoot = filepath.Join(c.NAS, "custom-models")
	identity := NASIdentity{nasSourceFingerprint("//storage.invalid/share", "smbfs"), "custom/queue-checkpoints"}
	inventory := func(context.Context, string, ...string) ([]byte, error) {
		return []byte("//storage.invalid/share on " + c.NAS + " (smbfs, nodev)"), nil
	}
	got, err := ResolveOnboardingStorage(context.Background(), c, true, inventory, identity)
	if err != nil || got.Checkpoints != c.Checkpoints || got.ModelRoot != c.ModelRoot {
		t.Fatal("custom paths changed", err)
	}
	identity.CheckpointPath = "../../escape"
	_, err = ResolveOnboardingStorage(context.Background(), c, true, command, identity)
	requirePreflightCode(t, err, "nas_identity_invalid")
}
