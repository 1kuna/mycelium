package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestEfficiencyCommand(t *testing.T) {
	report := &myce.EfficiencyReport{CPUStatus: "unknown", Stranded: []myce.StrandedDemand{{JobID: "waiting", Host: "peer", Fields: []string{"targets"}}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/efficiency" {
			t.Errorf("broad read: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(report)
	}))
	defer server.Close()
	for _, asJSON := range []bool{false, true} {
		args := []string{"efficiency", "--queue", server.URL, "--token", "test"}
		if asJSON {
			args = append(args, "--json")
		}
		out, _, err := captureUsageOutput(t, args)
		if err != nil {
			t.Fatal(err)
		}
		if asJSON {
			var got myce.EfficiencyReport
			if err := json.Unmarshal([]byte(out), &got); err != nil || len(got.Stranded) != 1 {
				t.Fatal(out, err)
			}
		} else if !strings.Contains(out, "excluded by targets") || !strings.Contains(out, "CPU use unknown") {
			t.Fatal(out)
		}
	}
}
