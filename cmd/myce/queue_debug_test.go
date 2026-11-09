package main

import (
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

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

	{
		listener, server, err := queueDebugListener("")
		if err != nil || listener != nil || server != nil {
			t.Fatalf("default debug listener: %v %v %v", listener, server, err)
		}

		{

			for _, address := range []string{"0.0.0.0:0"} {
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

	}

	{
		listener, _, err := queueDebugListener("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
	}
}
