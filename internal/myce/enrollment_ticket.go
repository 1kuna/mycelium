package myce

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"time"
)

type InviteRequest struct {
	TTL time.Duration `json:"ttl"`
}
type EnrollmentTicket struct {
	Ticket    string    `json:"ticket"`
	ExpiresAt time.Time `json:"expires_at"`
}
type redeemRequest struct {
	Ticket      string `json:"ticket"`
	Fingerprint string `json:"fingerprint"`
}

func validPin(pin string) bool {
	b, err := hex.DecodeString(pin)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == pin
}
func ticketHash(ticket string) string {
	h := sha256.Sum256([]byte(ticket))
	return hex.EncodeToString(h[:])
}

// Tickets grant only one TLS-pinned enrollment. SQLite keeps digests, never
// bearer tickets or the fleet token, including after a queue restart.
func (q *Queue) MintEnrollmentTicket(pin string, ttl time.Duration) (EnrollmentTicket, error) {
	var out EnrollmentTicket
	if !validPin(pin) || ttl <= 0 || ttl > time.Hour {
		return out, errors.New("invite requires a queue pin and expiry between zero and one hour")
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return out, err
	}
	out.Ticket = hex.EncodeToString(secret[:])
	q.mu.Lock()
	defer q.mu.Unlock()
	out.ExpiresAt = q.clock.Now().Add(ttl)
	_, err := q.db.Exec("INSERT INTO enrollment_tickets (digest,pin,expires,used) VALUES (?,?,?,0)", ticketHash(out.Ticket), pin, out.ExpiresAt.UnixNano())
	return out, err
}
func (q *Queue) RedeemEnrollmentTicket(ticket, pin string) error {
	if len(ticket) != 64 || !validPin(pin) {
		return enrollmentTicketRejected()
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	result, err := q.db.Exec("UPDATE enrollment_tickets SET used=1 WHERE digest=? AND pin=? AND used=0 AND expires>?", ticketHash(ticket), pin, q.clock.Now().UnixNano())
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return enrollmentTicketRejected()
	}
	return nil
}
func enrollmentTicketRejected() error {
	return &HTTPError{Status: 403, Code: "invite_rejected", Message: "invite expired, already used, or does not match this queue"}
}

func (a API) registerInvites(mux *http.ServeMux, pin string) {
	mux.HandleFunc("POST /api/invites", func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || !validPin(pin) {
			respond(w, nil, enrollmentTicketRejected())
			return
		}
		var in InviteRequest
		if !decode(w, r, &in) {
			return
		}
		if in.TTL == 0 {
			in.TTL = 15 * time.Minute
		}
		out, err := a.Queue.MintEnrollmentTicket(pin, in.TTL)
		respondOperation(w, out, err, "request_invalid")
	})
}
func (a API) redeemInvite(w http.ResponseWriter, r *http.Request, pin string) {
	w.Header().Set("Cache-Control", "no-store")
	if r.TLS == nil || r.Method != http.MethodPost || !validPin(pin) {
		respond(w, nil, enrollmentTicketRejected())
		return
	}
	var in redeemRequest
	if !decode(w, r, &in) {
		return
	}
	if in.Fingerprint != pin {
		respond(w, nil, enrollmentTicketRejected())
		return
	}
	if err := a.Queue.RedeemEnrollmentTicket(in.Ticket, in.Fingerprint); err != nil {
		respondOperation(w, nil, err, "invite_rejected")
		return
	}
	respond(w, Invitation{Version: 1, Fingerprint: pin, Token: a.Token}, nil)
}

// PinnedClient centralizes exact certificate validation and redirect refusal
// for enrollment and installed joining. No credential is sent before TLS trust.
func PinnedClient(address, pin string) (Client, error) {
	return (Invitation{Version: 1, Fingerprint: pin, Token: "enrollment-transport"}).Client(address)
}
func RedeemInvitation(ctx context.Context, address, pin, ticket string) (Invitation, error) {
	client, err := PinnedClient(address, pin)
	if err != nil {
		return Invitation{}, err
	}
	defer client.HTTP.CloseIdleConnections()
	client.Token = "" // The one-time ticket belongs in the body, never URL/logs.
	var invitation Invitation
	if err = client.Call(ctx, "POST", "/api/invites/redeem", redeemRequest{ticket, pin}, &invitation); err != nil {
		return invitation, err
	}
	invitation.Address = address
	if err = invitation.Validate(); err != nil || invitation.Fingerprint != pin {
		return Invitation{}, enrollmentTicketRejected()
	}
	return invitation, nil
}
