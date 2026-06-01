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

	"mycelium/internal/domain"
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

func TestRunnerFanOutBenchmarkJobRecordsChildrenAndNoWinner(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	client := &fakeClient{
		clock: clock,
		outputs: map[string]Completion{
			"same/model": {Text: "alpha", TokensPerSec: 10, TTFTms: 20, ContextTokens: 3},
			"same:model": {Text: "beta", TokensPerSec: 12, TTFTms: 21, ContextTokens: 4},
		},
		errorsByModel: map[string]error{"bad/model": errors.New("backend failed")},
	}
	store := &benchJobStore{}
	dir := t.TempDir()
	parent := domain.Job{
		ID:        "bench-a",
		TaskType:  "benchmark",
		Project:   "project-a",
		SpeedPref: domain.SpeedAuto,
		Benchmark: &domain.BenchmarkSpec{
			Prompt:    "say hi",
			Models:    []string{"same/model", "same:model", "bad/model"},
			OutputDir: dir,
		},
	}
	results, err := (Runner{Client: client, Clock: clock, Store: store}).RunJob(context.Background(), parent)
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if len(results) != 3 || results[0].OutputPath == results[1].OutputPath || !strings.Contains(results[2].Error, "backend failed") {
		t.Fatalf("results = %+v", results)
	}
	for _, result := range results {
		if result.UserPick != nil {
			t.Fatalf("Mycelium set benchmark winner: %+v", result)
		}
	}
	metricsBody, err := os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	if !strings.Contains(string(metricsBody), `"tokens_per_sec": 10`) || !strings.Contains(string(metricsBody), `"error": "backend failed"`) {
		t.Fatalf("metrics = %s", metricsBody)
	}
	if _, err := os.Stat(filepath.Join(dir, "same-model.txt")); err != nil {
		t.Fatalf("first duplicate output: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "same-model-2.txt")); err != nil {
		t.Fatalf("second duplicate output: %v", err)
	}
	parentJob := store.latest("bench-a")
	if parentJob.Status != domain.JobDone || parentJob.Benchmark == nil {
		t.Fatalf("parent job = %+v", parentJob)
	}
	for _, childID := range []string{"bench-a-same-model", "bench-a-same-model-2", "bench-a-bad-model"} {
		child := store.latest(childID)
		if child.ParentID != parent.ID || child.TaskType != "benchmark_child" || child.Priority != domain.PriorityBackground {
			t.Fatalf("child %s = %+v", childID, child)
		}
	}
	if child := store.latest("bench-a-bad-model"); child.Status != domain.JobFailed || !strings.Contains(child.Error, "backend failed") {
		t.Fatalf("failed child = %+v", child)
	}
}

func TestRunnerRunJobRejectsBadBenchmarkJobs(t *testing.T) {
	runner := Runner{Client: &fakeClient{clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))}, Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), Store: &benchJobStore{}}
	for _, job := range []domain.Job{
		{Benchmark: &domain.BenchmarkSpec{Prompt: "p", Models: []string{"m"}, OutputDir: t.TempDir()}},
		{ID: "bench-a"},
	} {
		if _, err := runner.RunJob(context.Background(), job); err == nil {
			t.Fatalf("job %+v expected error", job)
		}
	}
	if _, err := (Runner{Client: runner.Client, Clock: runner.Clock}).RunJob(context.Background(), domain.Job{ID: "bench-a", Benchmark: &domain.BenchmarkSpec{Prompt: "p", Models: []string{"m"}, OutputDir: t.TempDir()}}); err == nil {
		t.Fatal("missing store expected error")
	}
	if _, err := (Runner{Client: runner.Client, Clock: runner.Clock, Store: &benchJobStore{errAt: 1}}).RunJob(context.Background(), domain.Job{ID: "bench-a", Benchmark: &domain.BenchmarkSpec{Prompt: "p", Models: []string{"m"}, OutputDir: t.TempDir()}}); err == nil {
		t.Fatal("initial store error expected")
	}
	if _, err := (Runner{Client: runner.Client, Clock: runner.Clock, Store: &benchJobStore{errAt: 4}}).RunJob(context.Background(), domain.Job{ID: "bench-a", Benchmark: &domain.BenchmarkSpec{Prompt: "p", Models: []string{"m"}, OutputDir: t.TempDir()}}); err == nil {
		t.Fatal("final store error expected")
	}
}

func TestRunnerRunJobMarksParentFailedOnOutputWriteError(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	client := &fakeClient{clock: clock, outputs: map[string]Completion{"same/model": {Text: "alpha"}}}
	store := &benchJobStore{}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "same-model.txt"), 0755); err != nil {
		t.Fatalf("mkdir conflicting output: %v", err)
	}
	results, err := (Runner{Client: client, Clock: clock, Store: store}).RunJob(context.Background(), domain.Job{
		ID: "bench-a",
		Benchmark: &domain.BenchmarkSpec{
			Prompt:    "p",
			Models:    []string{"same/model"},
			OutputDir: dir,
		},
	})
	if err == nil || len(results) != 0 {
		t.Fatalf("RunJob err=%v results=%+v", err, results)
	}
	if parent := store.latest("bench-a"); parent.Status != domain.JobFailed || parent.Error == "" {
		t.Fatalf("parent = %+v", parent)
	}
	if child := store.latest("bench-a-same-model"); child.Status != domain.JobFailed || child.Error == "" {
		t.Fatalf("child = %+v", child)
	}
}

func TestWriteMetricsFailsForInvalidOutputDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := writeMetrics(path, []Result{{Model: "m"}}); err == nil {
		t.Fatal("writeMetrics accepted file output dir")
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
	clock         *mocks.FakeClock
	outputs       map[string]Completion
	err           error
	errorsByModel map[string]error
}

func (c *fakeClient) Complete(_ context.Context, model, _ string) (Completion, error) {
	c.clock.Advance(25 * time.Millisecond)
	if c.err != nil {
		return Completion{}, c.err
	}
	if err := c.errorsByModel[model]; err != nil {
		return Completion{}, err
	}
	return c.outputs[model], nil
}

type benchJobStore struct {
	jobs  map[string][]domain.Job
	errAt int
	calls int
}

func (s *benchJobStore) SaveJob(_ context.Context, job domain.Job) error {
	s.calls++
	if s.errAt == s.calls {
		return errors.New("store failed")
	}
	if s.jobs == nil {
		s.jobs = map[string][]domain.Job{}
	}
	s.jobs[job.ID] = append(s.jobs[job.ID], job)
	return nil
}

func (s *benchJobStore) latest(id string) domain.Job {
	history := s.jobs[id]
	if len(history) == 0 {
		return domain.Job{}
	}
	return history[len(history)-1]
}
