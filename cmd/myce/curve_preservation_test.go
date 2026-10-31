package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestCurvePreservationCommand(t *testing.T) {
	root := t.TempDir()
	write := func(name string, value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, name)
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	snap := myce.Snapshot{Now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	before := write("before.json", snap)
	after := write("after.json", snap)
	ledgers := write("ledgers.json", []myce.JobLedgerPage{})
	args := []string{"--before", before, "--after", after, "--ledgers", ledgers}
	var out bytes.Buffer
	if err := curvePreservationCommand(args, &out); err != nil || !json.Valid(out.Bytes()) {
		t.Fatal(out.String(), err)
	}
	snap.WorkCurves = []myce.WorkObservation{{Samples: 1}}
	write("after.json", snap)
	out.Reset()
	if err := curvePreservationCommand(args, &out); err == nil || !json.Valid(out.Bytes()) {
		t.Fatal("missing accepted provenance passed or missing receipt", out.String(), err)
	}
}

func TestCurvePreservationCaptureFullStatus(t *testing.T) {
	snapshot := myce.Snapshot{Now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), Jobs: make([]*myce.Job, 1200), WorkCurves: []myce.WorkObservation{{Samples: 12}}}
	for i := range snapshot.Jobs {
		snapshot.Jobs[i] = &myce.Job{ID: fmt.Sprintf("retained-%d", i), State: "succeeded"}
	}
	full := map[string]any{"now": snapshot.Now, "jobs": snapshot.Jobs, "work_curves": snapshot.WorkCurves, "jobs_page": map[string]any{"mode": "all", "total": 1200, "returned": 1200}}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.RequestURI() != "/api/status?detail=full" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("guard did not request authenticated full status: %s %s", r.Method, r.URL.RequestURI())
		}
		json.NewEncoder(w).Encode(full)
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := curvePreservationCommand([]string{"capture", "--queue", server.URL, "--token", "fixture"}, &out); err != nil {
		t.Fatal(err)
	}
	var got myce.Snapshot
	if err := decodeCurvePreservationSnapshot(out.Bytes(), &got); err != nil || len(got.Jobs) != 1200 || len(got.WorkCurves) != 1 || got.Jobs[0].ID != "retained-0" || got.WorkCurves[0].Samples != 12 || calls != 1 {
		t.Fatal("incomplete captured inventory", len(got.Jobs), got.WorkCurves, err)
	}
	root := t.TempDir()
	completePath := filepath.Join(root, "complete.json")
	ledgerPath := filepath.Join(root, "ledgers.json")
	if err := os.WriteFile(completePath, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ledgerPath, []byte("[]"), 0600); err != nil {
		t.Fatal(err)
	}
	// A default recent-job reply, or a partially collected all-jobs page, must
	// never make empty/partial curves appear to have survived unchanged.
	for _, mode := range []string{"recent", "all"} {
		t.Run(mode, func(t *testing.T) {
			full["jobs"] = snapshot.Jobs[:100]
			full["work_curves"] = nil
			full["jobs_page"] = map[string]any{"mode": mode, "total": 1200, "returned": 100, "next_before": "retained-100"}
			out.Reset()
			if err := curvePreservationCommand([]string{"capture", "--queue", server.URL, "--token", "fixture"}, &out); err == nil || out.Len() != 0 {
				t.Fatal("incomplete response captured", err)
			}
			data, _ := json.Marshal(full)
			path := filepath.Join(t.TempDir(), "partial.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			// Both saved-snapshot inputs go through the same completeness check.
			for _, flag := range []string{"--before", "--after"} {
				before, after := completePath, completePath
				if flag == "--before" {
					before = path
				} else {
					after = path
				}
				err := curvePreservationCommand([]string{"--before", before, "--after", after, "--ledgers", ledgerPath}, &out)
				if err == nil || !strings.Contains(err.Error(), "bounded or incomplete") {
					t.Fatal("incomplete saved snapshot accepted", flag, err)
				}
			}
			if err := decodeCurvePreservationSnapshot(data, &got); err == nil {
				t.Fatal("bounded snapshot accepted for verification")
			}
		})
	}
	delete(full, "work_curves")
	data, _ := json.Marshal(full)
	if err := decodeCurvePreservationSnapshot(data, &got); err == nil {
		t.Fatal("missing curves accepted")
	}
}
