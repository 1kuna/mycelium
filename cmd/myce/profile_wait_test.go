package main

import (
	"context"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestProfileObserverTimeoutAndTransport(t *testing.T) {
	for _, mode := range []string{"timeout", "transport_error"} {
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
			dir := t.TempDir()
			r := profileReceipt{Key: "key", Project: "test", JobID: "fixture", Status: "queued", Observation: "armed", Delivery: "not_configured"}
			if err := observeProfile(ctx, myce.Client{URL: server.URL, Token: "fixture"}, dir, r, profileObserver{}); err == nil {
				t.Fatal("observation should fail")
			}
			var saved profileReceipt
			if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.ObservationError != mode || saved.Status != "queued" {
				t.Fatal(saved, err)
			}
		})
	}
}
