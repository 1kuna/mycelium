package myce

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
)

// Retained regression: #1076 wrapped permanent TLS integrity/identity refusals.
func TestClientRestartTransportClassification(t *testing.T) {
	for _, err := range []error{
		&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
	} {
		wrapped := fmt.Errorf("listener restart: %w", err)
		if IsTLSIdentityError(wrapped) || !RetryStatusRead(wrapped) {
			t.Fatalf("transport interruption must preserve observation: %v", err)
		}
	}
	for _, err := range []error{
		errJoiningIdentity,
		&tls.CertificateVerificationError{Err: errors.New("fixture certificate refusal")},
		tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"},
		errors.New("remote error: tls: bad certificate"),
		errors.New("tls: bad record MAC"),
		errors.New("tls: invalid server finished hash"),
		errors.New("remote error: tls: protocol version not supported"),
	} {
		wrapped := fmt.Errorf("listener restart: %w", err)
		if !IsTLSIdentityError(wrapped) || RetryStatusRead(wrapped) {
			t.Fatalf("restart context must not bypass identity refusal: %v", err)
		}
	}
}
