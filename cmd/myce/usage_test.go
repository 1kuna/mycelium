package main

import (
	"bytes"
	"fmt"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestUsageOutput(t *testing.T) {
	var b bytes.Buffer
	r := myce.UsageReport{Projects: []myce.UsageSummary{{Name: "atlas", GPUHours: 2}}, Machines: []myce.UsageSummary{{Name: "fake", GPUHours: 2, IdleHours: 3, BusyPercent: 40}}, Preemptions: []myce.Preemption{{Project: "atlas", Job: "job", For: "production"}}, ProductionStarts: []myce.ProductionStart{{Job: "prod", Seconds: 1.2}}}
	if e := printUsage(&b, r, "project"); e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"atlas: 2.000 GPU-hours", "40.0% busy", "3.000 idle hours", "Preemptions: 1", "1.200s time-to-start"} {
		if !strings.Contains(b.String(), want) {
			t.Fatal(want, b.String())
		}
	}
	if e := printUsage(&b, r, "bogus"); e == nil {
		t.Fatal("invalid grouping accepted")
	}
}
func TestUsageSince(t *testing.T) {
	for _, s := range []string{"7d", "24h", "90d"} {
		if _, e := usageDuration(s); e != nil {
			t.Fatal(s, e)
		}
	}
	for _, s := range []string{"NaNd", "0d", "91d", "-1h", "bad"} {
		if _, e := usageDuration(s); e == nil {
			t.Fatal(s)
		}
	}
}

func TestUsageCommandCallsAuthenticatedAPI(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/usage" || r.URL.Query().Get("since") != "168h0m0s" || r.Header.Get("Authorization") != "Bearer test-only" {
			t.Errorf("bad usage request %s %s", r.URL, r.Header.Get("Authorization"))
		}
		fmt.Fprint(w, `{"machines":[],"projects":[]}`)
	}))
	defer s.Close()
	saved := os.Stdout
	f, e := os.CreateTemp(t.TempDir(), "usage-output")
	if e != nil {
		t.Fatal(e)
	}
	os.Stdout = f
	defer func() { os.Stdout = saved; f.Close() }()
	if e = run([]string{"usage", "--queue", s.URL, "--token", "test-only", "--since", "7d", "--by", "machine"}); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(f.Name())
	if e != nil || !strings.Contains(string(data), "No observed usage yet.") {
		t.Fatal(string(data), e)
	}
}
