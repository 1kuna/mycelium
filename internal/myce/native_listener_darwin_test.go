//go:build darwin && cgo

package myce

import (
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
)

func TestNativeListenerDarwinNoListener(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	// Inspect our real process group after its reserved port has been closed.
	err = verifyNativeListener(port, syscall.Getpgrp())
	if errors.Is(err, errNativeForeignListener) {
		t.Fatalf("absent listener misclassified as foreign: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "ownership unconfirmed") {
		t.Fatalf("expected unconfirmed ownership, got %v", err)
	}
}
