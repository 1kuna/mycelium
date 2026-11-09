package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Interrupt the first ClientHello, then serve the unchanged TLS identity at the
// same address. An alert models a listener unable to complete a handshake; a
// partial record models a listener disappearing during it.
type restartingProfileListener struct {
	net.Listener
	alert bool
	first bool
}

func (l *restartingProfileListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil || l.first {
			return conn, err
		}
		l.first = true
		var header [5]byte
		if _, err := io.ReadFull(conn, header[:]); err == nil {
			_, _ = io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint16(header[3:])))
			if l.alert {
				// TLS fatal internal_error (RFC 8446), not a certificate refusal.
				_, _ = conn.Write([]byte{21, 3, 3, 0, 2, 2, 80})
			} else {
				_, _ = conn.Write([]byte{22, 3})
			}
		}
		_ = conn.Close()
	}
}

func TestSharedProfileObserverListenerRestart(t *testing.T) {
	for _, mode := range []string{"handshake_eof", "wrong_pin"} {
		t.Run(mode, func(t *testing.T) {
			clock := &waitClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
			var mu sync.Mutex
			var notBefore time.Time
			requests := 0
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				requests++
				if mode == "wrong_pin" {
					t.Error("HTTP credential sent before identity verification")
				}
				if requests == 1 {
					notBefore = clock.Now().Add(7 * time.Second)
					w.Header().Set("Retry-After", "7")
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if clock.Now().Before(notBefore) {
					t.Error("restart recovery ignored current server pressure")
				}
				adaptiveReply(w, r, "succeeded")
			}))
			if mode != "wrong_pin" {
				server.Listener = &restartingProfileListener{Listener: server.Listener, alert: mode == "internal_alert"}
			}
			server.StartTLS()
			defer server.Close()
			// Pin the fixture certificate, without changing trust during recovery.
			digest := sha256.Sum256(server.Certificate().Raw)
			pin := hex.EncodeToString(digest[:])
			if mode == "wrong_pin" {
				pin = strings.Repeat("00", 32)
			}
			joining := filepath.Join(t.TempDir(), "joining.json")
			if err := saveProfileJSON(joining, myce.Invitation{Version: 1, Fingerprint: pin, Token: "fixture", Address: server.URL}); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			payload := filepath.Join(t.TempDir(), "notifications.jsonl")
			hook := filepath.Join(t.TempDir(), "notify")
			if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat >> "+shellArg(payload)+"\nprintf '\\n' >> "+shellArg(payload)+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			var dirs []string
			for _, name := range []string{"first", "second"} {
				dir := adaptiveReceipt(t, root, server.URL, name, time.Time{})
				dirs = append(dirs, dir)
				reg := sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: server.URL, Joining: joining, Notify: hook}}
				if err := saveProfileJSON(filepath.Join(root, name+".json"), reg); err != nil {
					t.Fatal(err)
				}
			}
			lock, err := profileLock(filepath.Join(root, "observer.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = runSharedProfileLoop(ctx, root, lock, clock, func(_ context.Context, d time.Duration) error {
				for _, dir := range dirs {
					var r profileReceipt
					if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &r); err != nil || r.Observation != "armed" || r.ObservationError != "" {
						t.Errorf("restart retired a pending watch: %+v, %v", r, err)
					}
				}
				clock.advance(d)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(payload)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			if len(lines) != len(dirs) {
				t.Fatalf("notifications=%d, want one per receipt", len(lines))
			}
			seen := map[string]bool{}
			for _, line := range lines {
				var event profileNotification
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					t.Fatal(err)
				}
				want, cause := "succeeded", ""
				if mode == "wrong_pin" {
					want, cause = "observation_failed", "tls_identity"
				}
				if event.Status != want || event.ObservationError != cause || seen[event.DeliveryID] {
					t.Fatalf("wrong or duplicate delivery: %+v", event)
				}
				seen[event.DeliveryID] = true
			}
		})
	}
}
