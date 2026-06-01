package controlcli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
	"mycelium/internal/optimizer"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/pkg/api"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func directHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Result(), nil
	})}
}

func TestRunAddModelPersistsCatalogAndControlPreset(t *testing.T) {
	storeDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "control.db")
	model := filepath.Join(t.TempDir(), "tiny.gguf")
	if err := os.WriteFile(model, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}

	var err error
	output := captureStdout(t, func() {
		err = Run(context.Background(), []string{"add-model", "--store", storeDir, "--db", dbPath, "--id", "tiny", "--model", "tiny-model", model})
	})
	if err != nil {
		t.Fatalf("Run add-model: %v", err)
	}
	if !strings.Contains(output, "job\tinstall-tiny\tstarted") || !strings.Contains(output, "job\tinstall-tiny\tready\tpreset is materialized") {
		t.Fatalf("add-model output = %q", output)
	}
	preset, err := catalog.ReadPreset(storeDir, "tiny")
	if err != nil {
		t.Fatalf("ReadPreset: %v", err)
	}
	if preset.ModelRef == model || strings.Join(preset.Aliases, ",") != "tiny-model" {
		t.Fatalf("catalog preset = %+v", preset)
	}
	control, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open control store: %v", err)
	}
	defer control.Close()
	if got, err := control.Preset(context.Background(), "tiny"); err != nil || got.ID != "tiny" || strings.Join(got.Aliases, ",") != "tiny-model" {
		t.Fatalf("control preset = %+v, %v", got, err)
	}
	jobs, err := control.ListJobs(context.Background())
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "install-tiny" || jobs[0].TaskType != "catalog_install" || jobs[0].Status != domain.JobDone || len(jobs[0].Progress) == 0 {
		t.Fatalf("install jobs = %+v", jobs)
	}
	jobsOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"jobs", "list", "--db", dbPath})
	})
	if err != nil {
		t.Fatalf("jobs list: %v", err)
	}
	if !strings.Contains(jobsOutput, "install-tiny\tcatalog_install") || !strings.Contains(jobsOutput, "ready:preset is materialized") {
		t.Fatalf("jobs output = %q", jobsOutput)
	}
}

func TestRunAddModelFailurePersistsFailedInstallJobWithoutPreset(t *testing.T) {
	storeDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "control.db")
	missing := filepath.Join(t.TempDir(), "missing.gguf")

	err := Run(context.Background(), []string{"add-model", "--store", storeDir, "--db", dbPath, "--id", "tiny", missing})
	if err == nil {
		t.Fatal("missing source install succeeded")
	}
	control, openErr := storesqlite.Open(dbPath)
	if openErr != nil {
		t.Fatalf("open control store: %v", openErr)
	}
	defer control.Close()
	if _, presetErr := control.Preset(context.Background(), "tiny"); presetErr == nil {
		t.Fatal("failed install registered control preset")
	}
	jobs, listErr := control.ListJobs(context.Background())
	if listErr != nil {
		t.Fatalf("ListJobs: %v", listErr)
	}
	if len(jobs) != 1 || jobs[0].ID != "install-tiny" || jobs[0].Status != domain.JobFailed || jobs[0].Error == "" {
		t.Fatalf("failed install jobs = %+v", jobs)
	}
}

func TestRunListCommandsAndProjectSet(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPreset("tiny")); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-a", Name: "Node A", Address: "127.0.0.1:1", Status: domain.NodeReady}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{ID: "job-a", Model: "tiny", Project: "project-a", Status: domain.JobQueued}); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	if err := store.SaveRecommendation(context.Background(), domain.RecommendationRecord{ID: "rec-a", ProjectID: "project-a", Type: "context", RecommendedValue: 4096, CreatedAt: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatalf("SaveRecommendation: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	commands := [][]string{
		{"models", "list", "--db", dbPath},
		{"nodes", "list", "--db", dbPath},
		{"jobs", "list", "--db", dbPath},
		{"recommendations", "list", "--db", dbPath, "--project", "project-a"},
		{"recommendations", "calibrate-speed", "--db", dbPath},
		{"projects", "set", "--db", dbPath, "--id", "project-b", "--default-model", "preset-b", "--priority", "background", "--speed-pref", "latency", "--context-cap", "4096", "--latency-target-ms", "250", "--preemption", "hard", "--auto-apply"},
	}
	for _, args := range commands {
		if err := Run(context.Background(), args); err != nil {
			t.Fatalf("Run(%v): %v", args, err)
		}
	}
	verifyStore, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open verify: %v", err)
	}
	project, err := verifyStore.Project(context.Background(), "project-b")
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if err := verifyStore.Close(); err != nil {
		t.Fatalf("Close verify: %v", err)
	}
	if project.LatencyTargetMS != 250 {
		t.Fatalf("project = %+v", project)
	}

	for _, args := range [][]string{
		nil,
		{"unknown"},
		{"models", "bad"},
		{"nodes", "bad"},
		{"projects", "bad"},
		{"projects", "set", "--db", dbPath},
		{"jobs", "bad"},
		{"recommendations"},
		{"recommendations", "bad"},
		{"recommendations", "generate", "--db", dbPath},
		{"recommendations", "apply", "--db", dbPath},
		{"benchmark"},
		{"benchmark", "run", "--db", dbPath},
	} {
		if err := Run(context.Background(), args); err == nil {
			t.Fatalf("Run(%v) expected error", args)
		}
	}
}

func TestRunBenchmarkFanOutPersistsJobsAndOutputs(t *testing.T) {
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		var req api.OpenAIChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{
			Model: req.Model,
			Choices: []api.OpenAIChatChoice{{
				Message: api.OpenAIMessage{Role: "assistant", Content: "answer from " + req.Model},
			}},
			Usage: api.OpenAIUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
		})
	}))
	dbPath := filepath.Join(t.TempDir(), "control.db")
	outDir := filepath.Join(t.TempDir(), "bench")

	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"benchmark", "run",
			"--db", dbPath,
			"--url", "http://gateway.test",
			"--id", "bench-a",
			"--project", "project-a",
			"--prompt", "Say hi",
			"--out", outDir,
			"--model", "same/model",
			"--model", "same/model",
		}, client)
	})
	if err != nil {
		t.Fatalf("benchmark run: %v", err)
	}
	if !strings.Contains(output, "benchmark\tbench-a\tstarted") || !strings.Contains(output, "benchmark\tbench-a\tdone") {
		t.Fatalf("benchmark output = %q", output)
	}
	if data, err := os.ReadFile(filepath.Join(outDir, "same-model.txt")); err != nil || !strings.Contains(string(data), "answer from same/model") {
		t.Fatalf("first output = %q %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "same-model-2.txt")); err != nil {
		t.Fatalf("second output missing: %v", err)
	}
	metrics, err := os.ReadFile(filepath.Join(outDir, "metrics.json"))
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if strings.Contains(string(metrics), "user_pick") || !strings.Contains(string(metrics), `"context_tokens": 5`) {
		t.Fatalf("metrics = %s", metrics)
	}
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	jobs, err := store.ListJobs(context.Background())
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	byID := map[string]domain.Job{}
	for _, job := range jobs {
		byID[job.ID] = job
	}
	if byID["bench-a"].Status != domain.JobDone || byID["bench-a"].Benchmark == nil {
		t.Fatalf("parent job = %+v", byID["bench-a"])
	}
	if byID["bench-a-same-model"].ParentID != "bench-a" || byID["bench-a-same-model-2"].ParentID != "bench-a" {
		t.Fatalf("child jobs = %+v", byID)
	}
}

func TestRunBenchmarkPrintsChildErrorsAndUsesDefaultID(t *testing.T) {
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "backend saturated", http.StatusTooManyRequests)
	}))
	dbPath := filepath.Join(t.TempDir(), "control.db")
	outDir := filepath.Join(t.TempDir(), "bench")

	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"benchmark", "run",
			"--db", dbPath,
			"--url", "http://gateway.test",
			"--prompt", "Say hi",
			"--out", outDir,
			"--model", "tiny",
		}, client)
	})
	if err != nil {
		t.Fatalf("benchmark run with child error: %v", err)
	}
	if !strings.Contains(output, "benchmark-result\ttiny\terror") || !strings.Contains(output, "benchmark\tbenchmark-") {
		t.Fatalf("benchmark error output = %q", output)
	}
	metrics, err := os.ReadFile(filepath.Join(outDir, "metrics.json"))
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if !strings.Contains(string(metrics), "backend saturated") {
		t.Fatalf("metrics = %s", metrics)
	}
}

func TestListCommandsRejectBadFlags(t *testing.T) {
	for _, args := range [][]string{
		{"models", "list", "--bad"},
		{"nodes", "list", "--bad"},
		{"jobs", "list", "--bad"},
		{"recommendations", "list", "--bad"},
		{"recommendations", "calibrate-speed", "--bad"},
		{"projects", "set", "--bad"},
		{"add-model", "--bad"},
		{"benchmark", "run", "--bad"},
	} {
		if err := Run(context.Background(), args); err == nil {
			t.Fatalf("Run(%v) expected flag error", args)
		}
	}
}

func TestRunRecommendationsGenerateAndApply(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	project := domain.Project{ID: "project-a", ContextCap: 16000}
	if err := store.SaveProject(context.Background(), project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{
		ID:           "node-a",
		Status:       domain.NodeReady,
		MaxUtil:      1,
		Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 24576}},
	}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPresetWithContext("small", 6000)); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPresetWithContext("large", 16000)); err != nil {
		t.Fatalf("SavePreset large: %v", err)
	}
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	for _, metric := range []domain.RunMetric{
		{JobID: "job-a", Project: project.ID, ContextUsed: 3500, At: now},
		{JobID: "job-b", Project: project.ID, ContextUsed: 4000, At: now.Add(time.Second)},
	} {
		if err := store.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := Run(context.Background(), []string{"recommendations", "generate", "--db", dbPath, "--project", project.ID}); err != nil {
		t.Fatalf("generate: %v", err)
	}
	store, err = storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	recs, err := store.ListRecommendations(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("ListRecommendations: %v", err)
	}
	if len(recs) != 1 || recs[0].PresetID != "large" || recs[0].RecommendedValue != 6000 || recs[0].Applied {
		t.Fatalf("recs = %+v", recs)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	if err := Run(context.Background(), []string{"recommendations", "apply", "--db", dbPath, "--id", recs[0].ID}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	store, err = storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen apply: %v", err)
	}
	defer store.Close()
	appliedProject, err := store.Project(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	appliedPreset, err := store.Preset(context.Background(), "large")
	if err != nil {
		t.Fatalf("Preset: %v", err)
	}
	if appliedProject.ContextCap != 6000 || appliedProject.AutoApply || appliedPreset.ContextLength != 6000 {
		t.Fatalf("project=%+v preset=%+v", appliedProject, appliedPreset)
	}
}

func TestRunRecommendationsApplyEngineSetsProjectDefault(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	project := domain.Project{ID: "project-a"}
	rec := domain.RecommendationRecord{
		ID:                  "rec-engine",
		Type:                optimizer.RecommendationEngineParameter,
		ProjectID:           project.ID,
		RecommendedPresetID: "fast-preset",
		CreatedAt:           time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC),
	}
	if err := store.SaveProject(context.Background(), project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SaveRecommendation(context.Background(), rec); err != nil {
		t.Fatalf("SaveRecommendation: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := Run(context.Background(), []string{"recommendations", "apply", "--db", dbPath, "--id", rec.ID}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	store, err = storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	gotProject, err := store.Project(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	gotRec, err := store.Recommendation(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("Recommendation: %v", err)
	}
	if gotProject.DefaultModel != rec.RecommendedPresetID || !gotRec.Applied {
		t.Fatalf("project=%+v rec=%+v", gotProject, gotRec)
	}
}

func TestRunRecommendationsApplyRejectsInvalidRecords(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	project := domain.Project{ID: "project-a"}
	for _, rec := range []domain.RecommendationRecord{
		{ID: "rec-context-missing-preset", Type: optimizer.RecommendationContextCap, ProjectID: project.ID, CreatedAt: time.Unix(1, 0).UTC()},
		{ID: "rec-engine-missing-preset", Type: optimizer.RecommendationEngineParameter, ProjectID: project.ID, CreatedAt: time.Unix(2, 0).UTC()},
		{ID: "rec-unknown", Type: "unknown", ProjectID: project.ID, CreatedAt: time.Unix(3, 0).UTC()},
		{ID: "rec-rejected", Type: optimizer.RecommendationContextCap, ProjectID: project.ID, Rejected: true, RejectReason: "fit proof failed", CreatedAt: time.Unix(4, 0).UTC()},
	} {
		if err := store.SaveRecommendation(context.Background(), rec); err != nil {
			t.Fatalf("SaveRecommendation %s: %v", rec.ID, err)
		}
	}
	if err := store.SaveProject(context.Background(), project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, id := range []string{"rec-context-missing-preset", "rec-engine-missing-preset", "rec-unknown", "rec-rejected"} {
		if err := Run(context.Background(), []string{"recommendations", "apply", "--db", dbPath, "--id", id}); err == nil {
			t.Fatalf("apply %s expected error", id)
		}
	}
}

func TestRepeatedStringAndProgressFormatting(t *testing.T) {
	var values repeatedString
	if err := values.Set("model-a"); err != nil {
		t.Fatalf("Set model-a: %v", err)
	}
	if err := values.Set(""); err == nil {
		t.Fatal("empty model was accepted")
	}
	if got := values.String(); got != "model-a" {
		t.Fatalf("String = %q", got)
	}
	if got := jobProgressSummary(domain.Job{Progress: []domain.JobProgress{{Stage: "download"}}}); got != "download" {
		t.Fatalf("stage-only progress = %q", got)
	}
	if got := recommendationTarget(domain.RecommendationRecord{RecommendedPresetID: "preset-a", RecommendedValue: 12}); got != "preset-a" {
		t.Fatalf("preset target = %q", got)
	}
	if got := recommendationTarget(domain.RecommendationRecord{}); got != "-" {
		t.Fatalf("empty target = %q", got)
	}
}

func TestBenchmarkGatewayClientErrors(t *testing.T) {
	ctx := context.Background()
	errorClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusTooManyRequests)
	}))
	if _, err := (benchmarkGatewayClient{BaseURL: "http://gateway-error.test", Client: errorClient}).Complete(ctx, "tiny", "prompt"); err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("status error = %v", err)
	}

	badJSON := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	if _, err := (benchmarkGatewayClient{BaseURL: "http://gateway-bad-json.test", Client: badJSON}).Complete(ctx, "tiny", "prompt"); err == nil {
		t.Fatal("bad JSON response accepted")
	}

	noChoices := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{})
	}))
	if _, err := (benchmarkGatewayClient{BaseURL: "http://gateway-no-choices.test", Client: noChoices}).Complete(ctx, "tiny", "prompt"); err == nil || !strings.Contains(err.Error(), "no choices") {
		t.Fatalf("no choices err = %v", err)
	}
}

func TestDefaultStoresUseHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if got := defaultMyceliumHome(); got != filepath.Join(home, ".mycelium") {
		t.Fatalf("home = %s", got)
	}
	if got := defaultControlStorePath(); got != filepath.Join(home, ".mycelium", "mycelium.db") {
		t.Fatalf("control store = %s", got)
	}
	if got := defaultCatalogStore(); got != filepath.Join(home, ".mycelium", "catalog") {
		t.Fatalf("catalog store = %s", got)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	fn()
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	os.Stdout = old
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	return string(data)
}

func testPreset(id string) domain.Preset {
	return domain.Preset{
		ID:            id,
		ModelRef:      id,
		Backend:       domain.BackendLlamaCpp,
		ContextLength: 2048,
		Capabilities:  []domain.Capability{domain.CapabilityChat},
		EstWeightsMB:  1,
		KVPerTokenMB:  0.01,
	}
}

func testPresetWithContext(id string, contextLen int) domain.Preset {
	preset := testPreset(id)
	preset.ContextLength = contextLen
	return preset
}
