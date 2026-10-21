package myce

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnrollmentTicketExpiryReplayRestartAndScope(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	dir := t.TempDir()
	q, err := Open(filepath.Join(dir, "queue.db"), dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	pin := strings.Repeat("a", 64)
	ticket, err := q.MintEnrollmentTicket(pin, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var digest string
	if err = q.db.QueryRow("SELECT digest FROM enrollment_tickets").Scan(&digest); err != nil || digest == ticket.Ticket || digest != ticketHash(ticket.Ticket) {
		t.Fatal("SQLite retained a bearer ticket", err)
	}
	for _, ttl := range []time.Duration{-time.Second, 2 * time.Hour} {
		if _, err = q.MintEnrollmentTicket(pin, ttl); err == nil {
			t.Fatal("unbounded expiry accepted")
		}
	}
	if err = q.RedeemEnrollmentTicket(ticket.Ticket, strings.Repeat("b", 64)); err == nil {
		t.Fatal("wrong pin consumed a ticket")
	}
	if err = q.RedeemEnrollmentTicket(ticket.Ticket, pin); err != nil {
		t.Fatal(err)
	}
	if err = q.RedeemEnrollmentTicket(ticket.Ticket, pin); err == nil {
		t.Fatal("ticket replay accepted")
	}
	expiring, err := q.MintEnrollmentTicket(pin, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err = q.RedeemEnrollmentTicket(expiring.Ticket, pin); err == nil {
		t.Fatal("expired ticket accepted")
	}
	q.Close()
	q, err = Open(filepath.Join(dir, "queue.db"), dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err = q.RedeemEnrollmentTicket(ticket.Ticket, pin); err == nil {
		t.Fatal("restart resurrected ticket")
	}
	concurrent, err := q.MintEnrollmentTicket(pin, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); results <- q.RedeemEnrollmentTicket(concurrent.Ticket, pin) }()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("redemption count %d", accepted)
	}
}

func TestEnrollmentTicketPinnedHTTPAndRedirect(t *testing.T) {
	q, _ := fixture(t)
	// Install the TLS identity before building the handler, as the queue does.
	var handler http.Handler
	s, authority := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
	handler = (API{Queue: q, Token: authority.Token}).HandlerWithJoiningIdentity(authority.Fingerprint)
	client, err := authority.Client(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	var ticket EnrollmentTicket
	if err = client.Call(context.Background(), "POST", "/api/invites", InviteRequest{TTL: time.Minute}, &ticket); err != nil {
		t.Fatal(err)
	}
	// A ticket has no authority at any ordinary API endpoint.
	limited := client
	limited.Token = ticket.Ticket
	if err = limited.Call(context.Background(), "GET", "/api/status", nil, nil); err == nil {
		t.Fatal("ticket granted fleet access")
	}
	wrong := authority
	wrong.Fingerprint = strings.Repeat("b", 64)
	if _, err = RedeemInvitation(context.Background(), s.URL, wrong.Fingerprint, ticket.Ticket); err == nil {
		t.Fatal("wrong TLS pin accepted")
	}
	invitation, err := RedeemInvitation(context.Background(), s.URL, authority.Fingerprint, ticket.Ticket)
	if err != nil || invitation.Token != authority.Token {
		t.Fatal("redemption failed", err)
	}
	if _, err = RedeemInvitation(context.Background(), s.URL, authority.Fingerprint, ticket.Ticket); err == nil {
		t.Fatal("HTTP replay accepted")
	}
	var redirected atomic.Int32
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/accepted" {
			redirected.Add(1)
			respond(w, authority, nil)
			return
		}
		http.Redirect(w, r, s.URL+"/accepted", http.StatusTemporaryRedirect)
	})
	if _, err = RedeemInvitation(context.Background(), s.URL, authority.Fingerprint, "limited-ticket"); err == nil {
		t.Fatal("redirect accepted")
	}
	if redirected.Load() != 0 {
		t.Fatal("client followed redirect before returning an error")
	}
}

func TestEnrollmentTicketHTTPConcurrentRedemption(t *testing.T) {
	q, _ := fixture(t)
	var handler http.Handler
	server, authority := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
	handler = (API{Queue: q, Token: authority.Token}).HandlerWithJoiningIdentity(authority.Fingerprint)
	ticket, err := q.MintEnrollmentTicket(authority.Fingerprint, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := RedeemInvitation(context.Background(), server.URL, authority.Fingerprint, ticket.Ticket); err == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatal("HTTP redemption count", accepted.Load())
	}
}
func TestEnrollmentTicketPrunesExpiredAndUsed(t *testing.T) {
	q, clock := fixture(t)
	pin := strings.Repeat("a", 64)
	used, err := q.MintEnrollmentTicket(pin, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.RedeemEnrollmentTicket(used.Ticket, pin); err != nil {
		t.Fatal(err)
	}
	if _, err = q.MintEnrollmentTicket(pin, time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if _, err = q.MintEnrollmentTicket(pin, time.Minute); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = q.db.QueryRow("SELECT COUNT(*) FROM enrollment_tickets").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("expired/used rows retained", count)
	}
}
