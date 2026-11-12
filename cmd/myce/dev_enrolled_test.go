package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// installFakeFleet gives home an installed worker whose joining authority
// names a pinned HTTPS fleet queue; any request reaching it is counted.
func installFakeFleet(t *testing.T, home, token string) *atomic.Int64 {
	t.Helper()
	var requests atomic.Int64
	fleet := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	t.Cleanup(fleet.Close)
	hash := sha256.Sum256(fleet.Certificate().Raw)
	joining := filepath.Join(home, "joining.json")
	if err := myce.WriteInvitation(joining, myce.Invitation{Version: 1, Fingerprint: hex.EncodeToString(hash[:]), Token: token, Address: fleet.URL}); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(myce.ServiceConfig{Role: "worker", Queue: fleet.URL, JoiningFile: joining})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	return &requests
}

// #1270: on an enrolled machine, `myce dev -- myce run` uses the dev queue,
// and the installed fleet authority neither reaches nor receives anything.
func TestDevCommandOnEnrolledMachine(t *testing.T) {
	binary := devTestBinary(t)
	home := t.TempDir()
	const fleetToken = "fake-fleet-token-1270"
	requests := installFakeFleet(t, home, fleetToken)
	t.Setenv("HOME", home)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out, errOut bytes.Buffer
	err := devCommandSession(ctx, []string{"sh", "-ec", `"$1" run -- echo hi; "$1" run -- sh -c 'env'`, "fixture", binary}, nil, &out, &errOut)
	if err != nil {
		t.Fatalf("dev run on an enrolled machine: %v\n%s%s", err, out.String(), errOut.String())
	}
	if !strings.Contains(out.String(), "hi\n") {
		t.Fatalf("missing job output:\n%s", out.String())
	}
	if strings.Contains(out.String()+errOut.String(), fleetToken) || requests.Load() != 0 {
		t.Fatalf("fleet authority used inside dev: %d requests\n%s%s", requests.Load(), out.String(), errOut.String())
	}
}

func TestDevSessionAuthorityPrecedence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	// An unreadable installed config proves the dev path never opens it.
	if err := os.MkdirAll(filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_JOINING_FILE", "")
	t.Setenv("MYCE_DEV_SESSION_ROOT", t.TempDir())
	t.Setenv("MYCE_QUEUE", "http://127.0.0.1:9/")
	t.Setenv("MYCE_TOKEN", "dev-token-1270")
	for _, c := range []struct{ address, token string }{{"", ""}, {"http://127.0.0.1:9", ""}, {"", "dev-token-1270"}} {
		client, err := runClient("", "", c.address, c.token)
		if err != nil || client.URL != "http://127.0.0.1:9" || client.Token != "dev-token-1270" {
			t.Fatalf("%+v: dev session authority not chosen: %v %q", c, err, client.URL)
		}
	}
	// Neither credential crosses between the dev queue and any other queue.
	for _, c := range []struct{ address, token string }{{"https://fleet.invalid", ""}, {"", "fleet-token"}} {
		if _, err := runClient("", "", c.address, c.token); err == nil {
			t.Fatalf("%+v: dev session credential crossed queues", c)
		}
	}
	// Explicit fleet authority cannot pin the dev queue's plain HTTP address.
	fleetHome := t.TempDir()
	requests := installFakeFleet(t, fleetHome, "fake-fleet-token-1270")
	if _, err := runClient(filepath.Join(fleetHome, ".local", "share", "mycelium", "worker", "config.json"), "", "http://127.0.0.1:9", ""); err == nil || requests.Load() != 0 {
		t.Fatal("explicit fleet authority reached the dev queue")
	}
	// Outside a dev session, installed authority is still required.
	t.Setenv("MYCE_DEV_SESSION_ROOT", "")
	if _, err := runClient("", "", "", "dev-token-1270"); err == nil {
		t.Fatal("outside dev, an unreadable installed config was skipped")
	}
}
