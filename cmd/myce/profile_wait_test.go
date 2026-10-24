package main

import (
	"context"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
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
