package main

import (
	"encoding/json"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestUsageCommandCallsAuthenticatedAPI(t *testing.T) {
	r := myce.UsageReport{Projects: []myce.UsageSummary{{Name: "atlas", GPUHours: 2}}, Machines: []myce.UsageSummary{{Name: "fake", GPUHours: 2, IdleHours: 3, BusyPercent: 40}}, Preemptions: []myce.Preemption{{Project: "atlas", Job: "job", For: "production"}}, ProductionStarts: []myce.ProductionStart{{Job: "prod", Seconds: 1.2}}}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/usage" || request.URL.Query().Get("since") != "168h0m0s" || request.Header.Get("Authorization") != "Bearer test-only" {
			t.Errorf("bad usage request %s %s", request.URL, request.Header.Get("Authorization"))
		}
		json.NewEncoder(w).Encode(r)
	}))
	defer s.Close()
	saved := os.Stdout
	f, e := os.CreateTemp(t.TempDir(), "usage-output")
	if e != nil {
		t.Fatal(e)
	}
	os.Stdout = f
	defer func() { os.Stdout = saved; f.Close() }()
	if e = run([]string{"usage", "--queue", s.URL, "--token", "test-only", "--since", "7d", "--by", "project"}); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(f.Name())
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"atlas: 2.000 GPU-hours", "40.0% busy", "3.000 idle hours", "Preemptions: 1", "1.200s time-to-start"} {
		if !strings.Contains(string(data), want) {
			t.Fatal(want, string(data))
		}
	}

}
