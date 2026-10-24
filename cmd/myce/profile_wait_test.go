package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestProfileObserverTimeoutAndTransport(t *testing.T) {
	for _, mode := range []string{"timeout", "transport_error", "interrupted"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "transport_error" {
					w.WriteHeader(401)
					return
				}
				io.WriteString(w, `{"jobs":[{"id":"fixture","state":"queued"}]}`)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if mode == "interrupted" {
				cancel()
			}
			dir := t.TempDir()
			hook := filepath.Join(dir, "notify")
			payload := filepath.Join(dir, "notification.json")
			if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat > "+shellArg(payload)+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			r := profileReceipt{Key: "key", Project: "test", JobID: "fixture", Status: "queued", Observation: "armed", Nonce: "receipt-nonce", Delivery: "armed"}
			if err := observeProfile(ctx, myce.Client{URL: server.URL, Token: "fixture"}, dir, r, profileObserver{Notify: hook}); err == nil {
				t.Fatal("observation should fail")
			}
			var saved profileReceipt
			if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.ObservationError != mode || saved.Status != "queued" || saved.Delivery == "armed" {
				t.Fatal(saved, err)
			}
			var notification profileNotification
			if err := readProfileJSON(payload, &notification); err != nil || notification.Status != "observation_failed" || notification.ObservationError != mode || notification.DeliveryID == "" {
				t.Fatal("abnormal end not notified", notification, err)
			}
			terminalID := profileHash([]string{r.Nonce, r.Project, r.Key, r.JobID, "succeeded", ""})
			if notification.DeliveryID == terminalID {
				t.Fatal("failure reused terminal delivery identity")
			}
		})
	}
}

// A receiver rejection (as observed with a webhook HTTP 403) is a delivery
// failure, never a failed computation or a confirmed delivered wakeup.
func TestProfileNotifierDeliveryFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"jobs":[{"id":"fixture","state":"succeeded"}]}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	hook := filepath.Join(dir, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{Key: "key", Project: "test", JobID: "fixture", Status: "running", Observation: "armed", Delivery: "armed"}
	if err := observeProfile(context.Background(), myce.Client{URL: server.URL, Token: "fixture"}, dir, receipt, profileObserver{Notify: hook}); err == nil {
		t.Fatal("delivery rejection hidden")
	}
	var saved profileReceipt
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.Status != "succeeded" || saved.DeliveryStatus != "failed" || saved.Observation != "not_armed" || saved.Delivery == "armed" {
		t.Fatal("delivery rejection changed computation state", saved, err)
	}
}

func TestProfileObserverRearmDeliveryIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer server.Close()
	root := t.TempDir()
	profile := `name: fixture
project: test
class: experiment
source: {git: HEAD}
run: {command: [true]}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
	path := filepath.Join(root, "fixture.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	key := "rearm"
	dir := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", key}))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{Key: key, Project: "test", JobID: "fixture", Status: "queued", Nonce: rand.Text(), Intent: profileHash(struct {
		Profile string
		Args    []string
	}{profile, []string{"literal"}})}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
		t.Fatal(err)
	}
	payload := filepath.Join(root, "notification.json")
	hook := filepath.Join(root, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nif [ \"${1:-}\" = --check ]; then exit 0; fi\ncat > "+shellArg(payload)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	binary := devTestBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	var ids, arms []string
	for range 2 {
		cmd := exec.CommandContext(ctx, binary, "run", "--queue", server.URL, "--profile", path, "--key", key, "--notify", hook, "--", "literal")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "MYCE_TOKEN=fixture")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("arm: %v %s", err, out)
		}
		wait(t, ctx, func() bool {
			if readProfileJSON(filepath.Join(dir, "receipt.json"), &receipt) != nil || receipt.ObservationError != "transport_error" || receipt.DeliveryStatus != "delivered" {
				return false
			}
			lock, err := profileLock(filepath.Join(dir, "watch.lock"))
			if err != nil {
				return false
			}
			lock.Close()
			return true
		})
		var notification profileNotification
		data, err := os.ReadFile(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &notification); err != nil || notification.Status != "observation_failed" {
			t.Fatal(notification, err)
		}
		ids = append(ids, notification.DeliveryID)
		arms = append(arms, receipt.ArmNonce)
	}
	if ids[0] == "" || ids[0] == ids[1] || arms[0] == "" || arms[0] == arms[1] {
		t.Fatal("rearmed observation failure was deduplicated", ids, arms)
	}
}
