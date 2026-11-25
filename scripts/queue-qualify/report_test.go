package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSummarizeUsesNearestRank(t *testing.T) {
	var v []float64
	for i := 1; i <= 1000; i++ {
		v = append(v, float64(i))
	}
	s := summarize(v, 3)
	if s.Count != 1000 || s.Errors != 3 || s.P50Ms != 500 || s.P95Ms != 950 || s.P99Ms != 990 || s.P999Ms != 999 || s.MaxMs != 1000 || s.MeanMs != 500.5 {
		t.Fatalf("%+v", s)
	}
	if one := summarize([]float64{7}, 0); one.P50Ms != 7 || one.P999Ms != 7 {
		t.Fatalf("%+v", one)
	}
	if empty := summarize(nil, 2); empty.Count != 0 || empty.Errors != 2 {
		t.Fatalf("%+v", empty)
	}
}

func TestVerdictFailsBeforeIncomplete(t *testing.T) {
	if v := verdict([]check{{Status: statusPass}, {Status: statusInfo}, {Status: statusWarn}}); v != statusPass {
		t.Fatal(v)
	}
	if v := verdict([]check{{Status: statusPass}, {Status: statusNotExercised}}); v != "incomplete" {
		t.Fatal(v)
	}
	if v := verdict([]check{{Status: statusNotExercised}, {Status: statusFail}}); v != statusFail {
		t.Fatal(v)
	}
}

func TestReportsWriteJSONMarkdownAndSummary(t *testing.T) {
	dir := t.TempDir()
	mk := func(condition string, append latencyStats, st string) string {
		o := &outputFlags{dir: dir, condition: condition, volume: "busy"}
		r := newReport("commit", o)
		r.StartedAt = r.StartedAt.Add(time.Duration(len(condition)) * time.Second)
		r.Results = &commitResults{Append: append, AchievedRate: 11.6}
		r.Checks = []check{{Name: "x", Status: st, Measured: "m", Limit: "l", Prevents: "p"}}
		var out strings.Builder
		err := r.finish(&out)
		if (st == statusFail) != (err == errChecksFailed) {
			t.Fatalf("finish error %v for %s", err, st)
		}
		matches, _ := filepath.Glob(filepath.Join(dir, "commit-"+condition+"-busy-*.json"))
		if len(matches) != 1 {
			t.Fatalf("report files %v", matches)
		}
		md, err := os.ReadFile(strings.TrimSuffix(matches[0], ".json") + ".md")
		if err != nil || !strings.Contains(string(md), "| x | "+st+" |") {
			t.Fatalf("markdown %q %v", md, err)
		}
		return matches[0]
	}
	idle := mk("idle", latencyStats{Count: 10, P99Ms: 4}, statusPass)
	bulk := mk("bulk-same-volume", latencyStats{Count: 10, P99Ms: 900}, statusFail)
	var out strings.Builder
	sumDir := filepath.Join(dir, "summary")
	err := cmdSummarize(context.Background(), []string{"--out", sumDir, idle, bulk}, &out)
	if err != errChecksFailed {
		t.Fatalf("summary of a failed run must fail: %v", err)
	}
	matches, _ := filepath.Glob(filepath.Join(sumDir, "summary-*.json"))
	b, _ := os.ReadFile(matches[0])
	var got struct{ Results []summaryRow }
	if err := json.Unmarshal(b, &got); err != nil || len(got.Results) != 2 || got.Results[1].Commit.Append.P99Ms != 900 {
		t.Fatalf("summary %s: %v", b, err)
	}
	md, _ := os.ReadFile(strings.TrimSuffix(matches[0], ".json") + ".md")
	if !strings.Contains(string(md), "| bulk-same-volume | busy | fail | 0.00 | 0.00 | 900.00 |") {
		t.Fatalf("summary markdown:\n%s", md)
	}
}
