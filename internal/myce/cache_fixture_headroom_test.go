package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestExplicitFixtureCacheLimitUnderLowHeadroom(t *testing.T) {
	lowHeadroom := func(string) (int64, error) { return environmentCacheSize(23 << 30) }
	auto := EnvironmentCache{Root: t.TempDir(), sizeLimit: lowHeadroom}
	if err := auto.init(); err == nil {
		t.Fatal("automatic sizing accepted less than 24 GiB free")
	}
	explicit := EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, sizeLimit: lowHeadroom}
	if err := explicit.init(); err != nil {
		t.Fatalf("explicit fixture limit depends on disk headroom: %v", err)
	}
	config := ServiceConfig{Role: "worker", EnvironmentCache: t.TempDir(), EnvironmentCacheBytes: 1 << 20}
	if err := resolveEnvironmentCacheSizing(&config, true, lowHeadroom); err != nil || config.EnvironmentCacheBytes != 1<<20 {
		t.Fatalf("explicit installer fixture limit was not preserved: %+v, %v", config, err)
	}
}

// Exercise the installer sizing seam and pass its explicit fixture limit to a
// real worker check-in under the same simulated low cache headroom.
func TestFixtureWorkerCheckinAfterLowHeadroomSizing(t *testing.T) {
	lowHeadroom := func(string) (int64, error) { return environmentCacheSize(23 << 30) }
	config := ServiceConfig{Role: "worker", EnvironmentCache: t.TempDir()}
	if err := resolveEnvironmentCacheSizing(&config, false, lowHeadroom); err == nil {
		t.Fatal("automatic installer sizing accepted low headroom")
	}
	autoWorker := recoveryWorker(t)
	autoWorker.EnvironmentLimit = 0
	autoWorker.environmentSizeLimit = lowHeadroom
	if err := autoWorker.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "24 GiB") {
		t.Fatalf("automatic worker sizing did not refuse low headroom: %v", err)
	}
	config.EnvironmentCacheBytes = 1 << 20
	if err := resolveEnvironmentCacheSizing(&config, true, lowHeadroom); err != nil {
		t.Fatal(err)
	}
	var seen atomic.Bool
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/workers/checkin" {
			http.NotFound(rw, req)
			return
		}
		var checkin Checkin
		if err := json.NewDecoder(req.Body).Decode(&checkin); err != nil {
			t.Error(err)
			return
		}
		seen.Store(true)
		if err := json.NewEncoder(rw).Encode(attemptReply(checkin, Reply{Leave: true})); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	w := recoveryWorker(t)
	w.EnvironmentRoot, w.EnvironmentLimit = config.EnvironmentCache, config.EnvironmentCacheBytes
	w.environmentSizeLimit = lowHeadroom
	w.Client = Client{URL: server.URL, Token: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !seen.Load() {
		t.Fatal("worker did not check in with explicit fixture limit")
	}
}
