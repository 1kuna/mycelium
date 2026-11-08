package myce

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
)

func TestClientRestartTransportClassification(t *testing.T) {
	for _, err := range []error{
		&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		io.EOF, io.ErrUnexpectedEOF,
		errors.New("tls: use of closed connection"),
		errors.New("remote error: tls: internal error"),
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
	} {
		wrapped := fmt.Errorf("listener restart: %w", err)
		if !IsTLSIdentityError(wrapped) || RetryStatusRead(wrapped) {
			t.Fatalf("restart context must not bypass identity refusal: %v", err)
		}
	}
}
