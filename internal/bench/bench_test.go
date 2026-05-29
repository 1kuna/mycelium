package bench

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/test/mocks"
)

func TestRunnerWritesOneOutputPerModelAndMetrics(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	client := &fakeClient{clock: clock, outputs: map[string]Completion{
		"owner/model:a": {Text: "alpha", TokensPerSec: 10, TTFTms: 20, ContextTokens: 3},
		"owner/model b": {Text: "beta", TokensPerSec: 12, TTFTms: 21, ContextTokens: 4},
	}}
	dir := t.TempDir()
	results, err := (Runner{Client: client, Clock: clock}).Run(context.Background(), Request{
		Prompt:    "say hi",
		Models:    []string{"owner/model:a", "owner/model b"},
		OutputDir: dir,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(results) != 2 || results[0].Bytes != 5 || results[0].DurationMS != 25 {
		t.Fatalf("results = %+v", results)
	}
	for _, result := range results {
		body, err := os.ReadFile(result.OutputPath)
		if err != nil {
			t.Fatalf("read output: %v", err)
		}
		if len(body) != result.Bytes {
			t.Fatalf("body/result mismatch: %q %+v", body, result)
		}
	}
	metricsBody, err := os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	var metrics []Result
	if err := json.Unmarshal(metricsBody, &metrics); err != nil {
		t.Fatalf("unmarshal metrics: %v", err)
	}
	if len(metrics) != 2 || metrics[1].ContextTokens != 4 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestRunnerRecordsModelErrorsAndRejectsBadRequests(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	client := &fakeClient{clock: clock, err: errors.New("backend failed")}
	dir := t.TempDir()
	results, err := (Runner{Client: client, Clock: clock}).Run(context.Background(), Request{Prompt: "p", Models: []string{"m"}, OutputDir: dir})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(results) != 1 || !strings.Contains(results[0].Error, "backend failed") {
		t.Fatalf("results = %+v", results)
	}
	for _, req := range []Request{
		{Prompt: "p", Models: []string{"m"}, OutputDir: dir},
		{Models: []string{"m"}, OutputDir: dir},
		{Prompt: "p", OutputDir: dir},
		{Prompt: "p", Models: []string{"m"}},
	} {
		runner := Runner{Client: client, Clock: clock}
		if req.Prompt == "p" && len(req.Models) == 1 && req.OutputDir == dir {
			runner.Client = nil
		}
		if _, err := runner.Run(context.Background(), req); err == nil {
			t.Fatalf("request %+v expected error", req)
		}
	}
	if _, err := (Runner{Client: client}).Run(context.Background(), Request{Prompt: "p", Models: []string{"m"}, OutputDir: dir}); err == nil {
		t.Fatal("expected missing clock error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Runner{Client: client, Clock: clock}).Run(ctx, Request{Prompt: "p", Models: []string{"m"}, OutputDir: dir}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v", err)
	}
}

func TestBenchNameHelpers(t *testing.T) {
	used := map[string]int{}
	if got := safeName("///"); got != "model" {
		t.Fatalf("safe empty = %q", got)
	}
	if first, second := uniqueName("model.txt", used), uniqueName("model.txt", used); first != "model.txt" || second != "model-2.txt" {
		t.Fatalf("names = %s %s", first, second)
	}
	if elapsedMS(-time.Second) != 0 {
		t.Fatal("negative duration should clamp to zero")
	}
}

type fakeClient struct {
	clock   *mocks.FakeClock
	outputs map[string]Completion
	err     error
}

func (c *fakeClient) Complete(_ context.Context, model, _ string) (Completion, error) {
	c.clock.Advance(25 * time.Millisecond)
	if c.err != nil {
		return Completion{}, c.err
	}
	return c.outputs[model], nil
}
