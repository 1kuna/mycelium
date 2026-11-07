package main

import (
	"encoding/json"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestControlCommand(t *testing.T) {
	page := myce.ControlPage{First: 7, HighWater: 9, Next: 9, Telemetry: &myce.ControlTelemetry{Pending: 2, Recent: []myce.ControlEvent{{Kind: "rejection", Operation: "status", Client: "anonymous"}}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/api/control?after=6" {
			t.Errorf("unexpected control read %s", r.URL.RequestURI())
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	for _, asJSON := range []bool{false, true} {
		args := []string{"control", "--queue", server.URL, "--token", "test-only", "--after", "6"}
		if asJSON {
			args = append(args, "--json")
		}
		out, _, err := captureUsageOutput(t, args)
		if err != nil {
			t.Fatal(err)
		}
		if asJSON {
			var got myce.ControlPage
			if err = json.Unmarshal([]byte(out), &got); err != nil || got.Next != 9 {
				t.Fatal(out, err)
			}
		} else if !strings.Contains(out, "control: locks=") || !strings.Contains(out, "last 429: route=status") {
			t.Fatal(out)
		}
	}
}
