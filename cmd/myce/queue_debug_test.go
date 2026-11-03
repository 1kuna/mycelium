package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestQueueDebugDisabledByDefault(t *testing.T) {
	listener, server, err := queueDebugListener("")
	if err != nil || listener != nil || server != nil {
		t.Fatalf("default debug listener: %v %v %v", listener, server, err)
	}
	_, help, _ := captureUsageOutput(t, []string{"queue", "--help"})
	if !strings.Contains(help, "-debug-listen") || !strings.Contains(help, "disabled by default") {
		t.Fatal(help)
	}
}

func TestQueueDebugRejectsNonLoopback(t *testing.T) {
	for _, address := range []string{":0", "0.0.0.0:0", "[::]:0", "192.0.2.1:0", "localhost:0", "example.invalid:0"} {
		t.Run(address, func(t *testing.T) {
			listener, _, err := queueDebugListener(address)
			if listener != nil {
				listener.Close()
				t.Fatal("opened non-loopback listener")
			}
			if err == nil {
				t.Fatal("accepted non-loopback debug address")
			}
		})
	}
}

func TestQueueDebugProfilesAndRuntime(t *testing.T) {
	listener, server, err := queueDebugListener("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Close(); <-done }()
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/profile?seconds=1", "/debug/runtime"} {
		response, err := http.Get("http://" + listener.Addr().String() + path)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || len(data) == 0 {
			t.Fatalf("%s: %s %v", path, response.Status, err)
		}
		if path == "/debug/runtime" {
			var stats map[string]uint64
			if err := json.Unmarshal(data, &stats); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"gc_pause_total_ns", "heap_alloc_bytes", "heap_inuse_bytes"} {
				if _, ok := stats[key]; !ok {
					t.Fatalf("missing %s", key)
				}
			}
		}
	}
}

func TestQueueDebugAbsentFromFleetAPI(t *testing.T) {
	// API routes are a separate mux even though net/http/pprof registers globals.
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), myce.RealClock{}, myce.WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	api := myce.API{Queue: q, Token: "test-token"}
	for _, handler := range []http.Handler{api.Handler(), api.HandlerWithJoiningIdentity("test-pin")} {
		for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/runtime"} {
			request := httptest.NewRequest("GET", path, nil)
			request.Header.Set("Authorization", "Bearer test-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code == 200 {
				t.Fatalf("fleet exposes %s", path)
			}
		}
	}
}
