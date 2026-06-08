package controlcli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/bench"
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

func TestRunTelemetrySamplesPrintsFilteredSessionSeries(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	for _, sample := range []domain.SessionMetric{
		{
			SessionID:   "session-a",
			Sequence:    1,
			JobID:       "job-a",
			Phase:       domain.TelemetryPhasePlaced,
			NodeID:      "node-a",
			Project:     "project-a",
			PresetID:    "preset-a",
			ContextUsed: 0,
			BytesIn:     120,
			At:          now,
		},
		{
			SessionID:    "session-a",
			Sequence:     2,
			JobID:        "job-a",
			Phase:        domain.TelemetryPhaseComplete,
			NodeID:       "node-a",
			Project:      "project-a",
			PresetID:     "preset-a",
			ContextUsed:  42,
			BytesIn:      120,
			BytesOut:     240,
			TokensPerSec: 12.5,
			TTFTms:       34,
			ElapsedMS:    56,
			At:           now.Add(time.Second),
		},
		{
			SessionID: "session-b",
			Sequence:  1,
			JobID:     "job-b",
			Phase:     domain.TelemetryPhaseComplete,
			NodeID:    "node-b",
			Project:   "project-b",
			At:        now.Add(2 * time.Second),
		},
	} {
		if err := store.RecordSample(context.Background(), sample); err != nil {
			t.Fatalf("RecordSample: %v", err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	output := captureStdout(t, func() {
		err = Run(context.Background(), []string{"telemetry", "samples", "--db", dbPath, "--project", "project-a", "--session", "session-a", "--limit", "2"})
	})
	if err != nil {
		t.Fatalf("telemetry samples: %v", err)
	}
	if !strings.Contains(output, "sample\tsession-a\t1\tplaced") || !strings.Contains(output, "sample\tsession-a\t2\tcomplete") || strings.Contains(output, "session-b") {
		t.Fatalf("telemetry output = %q", output)
	}
}

func TestRunTelemetrySamplesRejectsBadUsageAndTimeBounds(t *testing.T) {
	for _, args := range [][]string{
		{"telemetry"},
		{"telemetry", "wat"},
		{"telemetry", "samples", "--since", "not-time"},
		{"telemetry", "samples", "--until", "not-time"},
	} {
		if err := Run(context.Background(), args); err == nil {
			t.Fatalf("Run(%v) expected error", args)
		}
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
		{"projects", "set", "--db", dbPath, "--id", "project-b", "--default-model", "preset-b", "--priority", "background", "--speed-pref", "latency", "--context-cap", "4096", "--expected-concurrency", "3", "--latency-target-ms", "250", "--preemption", "hard", "--auto-apply"},
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
	if project.LatencyTargetMS != 250 || project.ExpectedConcurrency != 3 {
		t.Fatalf("project = %+v", project)
	}
	if err := Run(context.Background(), []string{"projects", "set", "--db", dbPath, "--id", "project-bad", "--priority", "urgent"}); err == nil || !strings.Contains(err.Error(), "priority") {
		t.Fatalf("invalid project priority err = %v", err)
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

func TestRunModelsStageAndLocalityReport(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	preset := testPreset("tiny")
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-a", Name: "Node A", Address: "127.0.0.1:51846", Status: domain.NodeReady}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/catalog/stage" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer rpc-secret" {
			t.Fatalf("auth = %s", got)
		}
		var req struct {
			Preset domain.Preset `json:"preset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Preset.ID != "tiny" {
			t.Fatalf("request preset = %+v", req.Preset)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"locality": domain.ModelLocality{
				ID:             "node-a:tiny",
				PresetID:       "tiny",
				NodeID:         "node-a",
				State:          domain.ModelLocalityReady,
				ModelRef:       "/catalog/models/tiny.gguf",
				ArtifactSizeMB: preset.ArtifactSizeMB,
				Managed:        true,
				Reason:         "catalog stage committed",
				UpdatedAt:      time.Unix(10, 0).UTC(),
			},
		})
	}))

	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{"models", "stage", "--db", dbPath, "--preset", "tiny", "--node", "node-a", "--rpc-token", "rpc-secret"}, client)
	})
	if runErr != nil {
		t.Fatalf("models stage: %v", runErr)
	}
	if !strings.Contains(output, "stage\ttiny\tnode-a\tready") {
		t.Fatalf("stage output = %q", output)
	}
	verify, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open verify: %v", err)
	}
	localities, err := verify.ListModelLocalities(context.Background())
	if err != nil {
		t.Fatalf("ListModelLocalities: %v", err)
	}
	if len(localities) != 1 || localities[0].ID != "node-a:tiny" {
		t.Fatalf("localities = %+v", localities)
	}
	if err := verify.Close(); err != nil {
		t.Fatalf("Close verify: %v", err)
	}
	report := captureStdout(t, func() {
		runErr = Run(context.Background(), []string{"models", "locality", "report", "--db", dbPath})
	})
	if runErr != nil {
		t.Fatalf("locality report: %v", runErr)
	}
	if !strings.Contains(report, "tiny\tnode-a\tready\t/catalog/models/tiny.gguf\ttrue") {
		t.Fatalf("locality report = %q", report)
	}
	if err := Run(context.Background(), []string{"models", "stage", "--db", dbPath, "--preset", "tiny"}); err == nil {
		t.Fatal("models stage accepted missing node")
	}
	if err := Run(context.Background(), []string{"models", "locality"}); err == nil {
		t.Fatal("models locality accepted missing subcommand")
	}
}

func TestCatalogStageClientAndStageFailureBranches(t *testing.T) {
	if got := catalogPeerBaseURL("127.0.0.1:51846"); got != "http://127.0.0.1:51846" {
		t.Fatalf("base url = %s", got)
	}
	if got := catalogPeerBaseURL("https://peer.local"); got != "https://peer.local" {
		t.Fatalf("https base url = %s", got)
	}
	client := catalogStageHTTPClient{}
	if _, err := client.StageModel(context.Background(), domain.Peer{ID: "empty"}, testPreset("tiny")); err == nil {
		t.Fatal("stage accepted peer without address")
	}
	errorClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"no source"}`))
	}))
	if _, err := (catalogStageHTTPClient{Client: errorClient}).StageModel(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"http://peer.test"}}, testPreset("tiny")); err == nil || !strings.Contains(err.Error(), "no source") {
		t.Fatalf("stage error = %v", err)
	}
	emptyClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"locality": domain.ModelLocality{}})
	}))
	if _, err := (catalogStageHTTPClient{Client: emptyClient}).StageModel(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"http://peer.test"}}, testPreset("tiny")); err == nil || !strings.Contains(err.Error(), "returned no locality") {
		t.Fatalf("empty locality error = %v", err)
	}
	malformedClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	if _, err := (catalogStageHTTPClient{Client: malformedClient}).StageModel(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"http://peer.test"}}, testPreset("tiny")); err == nil {
		t.Fatal("malformed catalog stage response accepted")
	}
	dialErrorClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})}
	if _, err := (catalogStageHTTPClient{Client: dialErrorClient}).StageModel(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"http://peer.test"}}, testPreset("tiny")); err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("transport error = %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPreset("tiny")); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-a", Status: domain.NodeReady}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := Run(context.Background(), []string{"models", "stage", "--db", dbPath, "--preset", "tiny", "--node", "node-a"}); err == nil || !strings.Contains(err.Error(), "no reachable address") {
		t.Fatalf("missing node address err = %v", err)
	}
}

func TestRunModelsLocalityPlanAndApply(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	preset := testPreset("tiny")
	preset.ArtifactSizeMB = 100
	preset.EstWeightsMB = 100
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	node := domain.Node{
		ID:               "node-a",
		Name:             "Node A",
		Address:          "127.0.0.1:51846",
		Status:           domain.NodeReady,
		Labels:           map[string]string{domain.LabelPeerBackend: string(domain.BackendLlamaCpp)},
		DiskTotalMB:      1000,
		DiskFreeMB:       700,
		DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
		MaxUtil:          0.8,
		Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}},
	}
	if err := store.SaveNode(context.Background(), node); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var runErr error
	output := captureStdout(t, func() {
		runErr = Run(context.Background(), []string{"models", "locality", "plan", "--db", dbPath, "--id", "plan-a"})
	})
	if runErr != nil {
		t.Fatalf("locality plan: %v", runErr)
	}
	if !strings.Contains(output, "locality-action\tstage:node-a:tiny\tstage\ttiny\tnode-a") {
		t.Fatalf("plan output = %q", output)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/catalog/stage" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			t.Fatalf("auth = %s", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"locality": domain.ModelLocality{
				ID:             "node-a:tiny",
				PresetID:       "tiny",
				NodeID:         "node-a",
				State:          domain.ModelLocalityReady,
				ModelRef:       "/catalog/models/tiny.gguf",
				Managed:        true,
				ArtifactSizeMB: 100,
				UpdatedAt:      time.Unix(1, 0).UTC(),
			},
		})
	}))
	output = captureStdout(t, func() {
		runErr = runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "plan-a", "--rpc-token", "rpc-secret"}, catalogStageHTTPClient{Client: client})
	})
	if runErr != nil {
		t.Fatalf("locality apply stage: %v", runErr)
	}
	if !strings.Contains(output, "locality-apply\tstage\ttiny\tnode-a\tready") {
		t.Fatalf("apply output = %q", output)
	}
	verify, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open verify: %v", err)
	}
	localities, err := verify.ListModelLocalities(context.Background())
	if err != nil || len(localities) != 1 || localities[0].ID != "node-a:tiny" {
		t.Fatalf("localities = %+v %v", localities, err)
	}
	plan, err := verify.LocalityPlan(context.Background(), "plan-a")
	if err != nil {
		t.Fatalf("LocalityPlan: %v", err)
	}
	if action := plan.Actions[0]; action.State != domain.ModelLocalityReady || action.Error != "" {
		t.Fatalf("applied action = %+v", action)
	}
	if err := verify.SaveLocalityPlan(context.Background(), domain.LocalityPlan{
		ID:        "plan-evict",
		CreatedAt: time.Unix(2, 0).UTC(),
		Actions: []domain.LocalityAction{{
			ID:       "evict:node-a:tiny",
			Kind:     domain.LocalityActionEvict,
			PresetID: "tiny",
			NodeID:   "node-a",
			State:    domain.ModelLocalityPlanned,
		}},
	}); err != nil {
		t.Fatalf("SaveLocalityPlan evict: %v", err)
	}
	if err := verify.Close(); err != nil {
		t.Fatalf("Close verify: %v", err)
	}
	evictClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/catalog/evict" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"locality": domain.ModelLocality{ID: "node-a:tiny", PresetID: "tiny", NodeID: "node-a", State: domain.ModelLocalityEvicted},
		})
	}))
	output = captureStdout(t, func() {
		runErr = runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "plan-evict"}, catalogStageHTTPClient{Client: evictClient})
	})
	if runErr != nil {
		t.Fatalf("locality apply evict: %v", runErr)
	}
	if !strings.Contains(output, "locality-apply\tevict\ttiny\tnode-a\tevicted") {
		t.Fatalf("evict output = %q", output)
	}
	verify, err = storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open verify evict: %v", err)
	}
	localities, err = verify.ListModelLocalities(context.Background())
	if err != nil || len(localities) != 0 {
		t.Fatalf("localities after evict = %+v %v", localities, err)
	}
	if err := verify.Close(); err != nil {
		t.Fatalf("Close verify evict: %v", err)
	}
}

func TestRefreshLocalityPeerSnapshots(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := refreshLocalityPeerSnapshots(context.Background(), store, nil, "", nil); err != nil {
		t.Fatalf("empty refresh: %v", err)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			t.Fatalf("auth = %s", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{
			Node:      domain.Node{ID: "node-a", Address: "127.0.0.1:1", Status: domain.NodeReady},
			Instances: []domain.ModelInstance{{ID: "inst-a", NodeID: "node-a", PresetID: "tiny", State: domain.InstReady}},
		})
	}))
	if err := refreshLocalityPeerSnapshots(context.Background(), store, []string{"peer.test"}, "rpc-secret", client); err != nil {
		t.Fatalf("refresh snapshots: %v", err)
	}
	if node, err := store.Node(context.Background(), "node-a"); err != nil || node.ID != "node-a" {
		t.Fatalf("Node = %+v %v", node, err)
	}
	if inst, err := store.Instance(context.Background(), "inst-a"); err != nil || inst.PresetID != "tiny" {
		t.Fatalf("Instance = %+v %v", inst, err)
	}
	errorClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"no"}`))
	}))
	if err := refreshLocalityPeerSnapshots(context.Background(), store, []string{"http://peer.test"}, "", errorClient); err == nil || !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("snapshot error = %v", err)
	}
	emptyClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{})
	}))
	if err := refreshLocalityPeerSnapshots(context.Background(), store, []string{"http://peer.test"}, "", emptyClient); err == nil || !strings.Contains(err.Error(), "empty node") {
		t.Fatalf("empty snapshot err = %v", err)
	}
	malformedClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	if err := refreshLocalityPeerSnapshots(context.Background(), store, []string{"http://peer.test"}, "", malformedClient); err == nil {
		t.Fatal("malformed snapshot accepted")
	}
	dialErrorClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})}
	if err := refreshLocalityPeerSnapshots(context.Background(), store, []string{"http://peer.test"}, "", dialErrorClient); err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("transport snapshot err = %v", err)
	}
}

func TestRunModelsLocalityApplyRecordsFailures(t *testing.T) {
	if err := runModelsLocalityApply(context.Background(), nil, catalogStageHTTPClient{}); err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("missing id err = %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPreset("tiny")); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-a", Address: "127.0.0.1:51846", Status: domain.NodeReady}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.SaveLocalityPlan(context.Background(), domain.LocalityPlan{
		ID:        "plan-fail",
		CreatedAt: time.Unix(1, 0).UTC(),
		Actions: []domain.LocalityAction{{
			ID:       "stage:node-a:tiny",
			Kind:     domain.LocalityActionStage,
			PresetID: "tiny",
			NodeID:   "node-a",
		}},
	}); err != nil {
		t.Fatalf("SaveLocalityPlan: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"stage failed"}`))
	}))
	if err := runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "plan-fail"}, catalogStageHTTPClient{Client: client}); err == nil || !strings.Contains(err.Error(), "stage failed") {
		t.Fatalf("apply failure err = %v", err)
	}
	verify, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open verify: %v", err)
	}
	plan, err := verify.LocalityPlan(context.Background(), "plan-fail")
	if err != nil {
		t.Fatalf("LocalityPlan: %v", err)
	}
	if plan.Actions[0].State != domain.ModelLocalityFailed || !strings.Contains(plan.Actions[0].Error, "stage failed") {
		t.Fatalf("failed action = %+v", plan.Actions[0])
	}
	if err := verify.SaveLocalityPlan(context.Background(), domain.LocalityPlan{
		ID:        "plan-unknown",
		CreatedAt: time.Unix(2, 0).UTC(),
		Actions: []domain.LocalityAction{{
			ID:       "mystery:node-a:tiny",
			Kind:     domain.LocalityActionKind("mystery"),
			PresetID: "tiny",
			NodeID:   "node-a",
		}},
	}); err != nil {
		t.Fatalf("SaveLocalityPlan unknown: %v", err)
	}
	if err := verify.SaveLocalityPlan(context.Background(), domain.LocalityPlan{
		ID:        "plan-keep",
		CreatedAt: time.Unix(3, 0).UTC(),
		Actions: []domain.LocalityAction{{
			ID:       "keep:node-a:tiny",
			Kind:     domain.LocalityActionKeep,
			PresetID: "tiny",
			NodeID:   "node-a",
		}},
	}); err != nil {
		t.Fatalf("SaveLocalityPlan keep: %v", err)
	}
	if err := verify.SaveLocalityPlan(context.Background(), domain.LocalityPlan{
		ID:        "plan-missing-preset",
		CreatedAt: time.Unix(4, 0).UTC(),
		Actions: []domain.LocalityAction{{
			ID:       "stage:node-a:missing",
			Kind:     domain.LocalityActionStage,
			PresetID: "missing",
			NodeID:   "node-a",
		}},
	}); err != nil {
		t.Fatalf("SaveLocalityPlan missing preset: %v", err)
	}
	if err := verify.SaveLocalityPlan(context.Background(), domain.LocalityPlan{
		ID:        "plan-missing-node",
		CreatedAt: time.Unix(5, 0).UTC(),
		Actions: []domain.LocalityAction{{
			ID:       "stage:missing:tiny",
			Kind:     domain.LocalityActionStage,
			PresetID: "tiny",
			NodeID:   "missing",
		}},
	}); err != nil {
		t.Fatalf("SaveLocalityPlan missing node: %v", err)
	}
	if err := verify.Close(); err != nil {
		t.Fatalf("Close verify: %v", err)
	}
	if err := runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "plan-keep"}, catalogStageHTTPClient{}); err != nil {
		t.Fatalf("keep apply: %v", err)
	}
	if err := runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "missing-plan"}, catalogStageHTTPClient{}); err == nil {
		t.Fatal("missing plan accepted")
	}
	if err := runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "plan-missing-preset"}, catalogStageHTTPClient{}); err == nil {
		t.Fatal("missing preset apply accepted")
	}
	if err := runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "plan-missing-node"}, catalogStageHTTPClient{}); err == nil {
		t.Fatal("missing node apply accepted")
	}
	if err := runModelsLocalityApply(context.Background(), []string{"--db", dbPath, "--id", "plan-unknown"}, catalogStageHTTPClient{}); err == nil || !strings.Contains(err.Error(), "unknown locality action") {
		t.Fatalf("unknown action err = %v", err)
	}
	warningOutput := captureStdout(t, func() {
		printLocalityPlan(domain.LocalityPlan{ID: "warn", Warnings: []string{"disk floor"}})
	})
	if !strings.Contains(warningOutput, "locality-warning\tdisk floor") {
		t.Fatalf("warning output = %q", warningOutput)
	}
}

func TestCatalogEvictClientFailureBranches(t *testing.T) {
	action := domain.LocalityAction{PresetID: "tiny", NodeID: "node-a"}
	if _, err := (catalogStageHTTPClient{}).EvictModel(context.Background(), domain.Peer{ID: "empty"}, action); err == nil {
		t.Fatal("evict accepted peer without address")
	}
	errorClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"protected"}`))
	}))
	if _, err := (catalogStageHTTPClient{Client: errorClient}).EvictModel(context.Background(), domain.Peer{ID: "node-a", Addresses: []string{"http://peer.test"}}, action); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("evict status err = %v", err)
	}
	malformedClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	if _, err := (catalogStageHTTPClient{Client: malformedClient}).EvictModel(context.Background(), domain.Peer{ID: "node-a", Addresses: []string{"http://peer.test"}}, action); err == nil {
		t.Fatal("malformed evict response accepted")
	}
	wrongStateClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"locality": domain.ModelLocality{State: domain.ModelLocalityReady}})
	}))
	if _, err := (catalogStageHTTPClient{Client: wrongStateClient}).EvictModel(context.Background(), domain.Peer{ID: "node-a", Addresses: []string{"http://peer.test"}}, action); err == nil || !strings.Contains(err.Error(), "returned state") {
		t.Fatalf("wrong state err = %v", err)
	}
	dialErrorClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})}
	if _, err := (catalogStageHTTPClient{Client: dialErrorClient}).EvictModel(context.Background(), domain.Peer{ID: "node-a", Addresses: []string{"http://peer.test"}}, action); err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("evict transport err = %v", err)
	}
}

func TestRunBenchmarkFanOutPersistsJobsAndOutputs(t *testing.T) {
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer gateway-secret" {
			t.Fatalf("gateway auth = %q", got)
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
			"--gateway-token", "gateway-secret",
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
			"--gateway-token", "gateway-secret",
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

func TestRunBenchmarkUsesGatewayTokenEnv(t *testing.T) {
	t.Setenv("MYCELIUM_TEST_GATEWAY_TOKEN", "env-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer env-secret" {
			t.Fatalf("gateway auth = %q", got)
		}
		_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{
			Choices: []api.OpenAIChatChoice{{Message: api.OpenAIMessage{Role: "assistant", Content: "env ok"}}},
			Usage:   api.OpenAIUsage{TotalTokens: 1},
		})
	}))
	if err := RunWithClient(context.Background(), []string{
		"benchmark", "run",
		"--db", filepath.Join(t.TempDir(), "control.db"),
		"--url", "http://gateway.test",
		"--gateway-token-env", "MYCELIUM_TEST_GATEWAY_TOKEN",
		"--prompt", "Say hi",
		"--out", filepath.Join(t.TempDir(), "bench"),
		"--model", "tiny",
	}, client); err != nil {
		t.Fatalf("benchmark env token: %v", err)
	}
	if err := RunWithClient(context.Background(), []string{
		"benchmark", "run",
		"--db", filepath.Join(t.TempDir(), "control.db"),
		"--url", "http://gateway.test",
		"--gateway-token-env", "MYCELIUM_TEST_MISSING_GATEWAY_TOKEN",
		"--prompt", "Say hi",
		"--out", filepath.Join(t.TempDir(), "bench"),
		"--model", "tiny",
	}, client); err == nil || !strings.Contains(err.Error(), "token env") {
		t.Fatalf("missing env err = %v", err)
	}
}

func TestRunBenchmarkFleetSimulateWritesArtifact(t *testing.T) {
	cfg := controlFleetConfig()
	configPath := filepath.Join(t.TempDir(), "fleet.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	outDir := filepath.Join(t.TempDir(), "fleet-out")

	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{
			"benchmark", "fleet",
			"--config", configPath,
			"--out", outDir,
			"--simulate",
		}, directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTeapot)
		})))
	})
	if runErr != nil {
		t.Fatalf("benchmark fleet simulate: %v", runErr)
	}
	if !strings.Contains(output, "benchmark-fleet\tfleet-cli\tdone") {
		t.Fatalf("output = %q", output)
	}
	if _, err := os.Stat(filepath.Join(outDir, "fleet-cli", "report.html")); err != nil {
		t.Fatalf("report missing: %v", err)
	}
	results, err := os.ReadFile(filepath.Join(outDir, "fleet-cli", "results.json"))
	if err != nil || !strings.Contains(string(results), "disk-headroom") {
		t.Fatalf("results = %q %v", results, err)
	}
}

func TestRunBenchmarkFleetRejectsBadRequests(t *testing.T) {
	if err := RunWithClient(context.Background(), []string{"benchmark", "fleet", "--out", t.TempDir()}, nil); err == nil || !strings.Contains(err.Error(), "--config") {
		t.Fatalf("missing config err = %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"benchmark", "fleet", "--config", "missing.json"}, nil); err == nil || !strings.Contains(err.Error(), "--out") {
		t.Fatalf("missing out err = %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"benchmark", "fleet", "--config", filepath.Join(t.TempDir(), "missing.json"), "--out", t.TempDir()}, nil); err == nil {
		t.Fatal("missing config file accepted")
	}
}

func TestListCommandsRejectBadFlags(t *testing.T) {
	for _, args := range [][]string{
		{"models", "list", "--bad"},
		{"models", "stage", "--bad"},
		{"models", "locality", "report", "--bad"},
		{"models", "locality", "plan", "--bad"},
		{"models", "locality", "apply", "--bad"},
		{"nodes", "list", "--bad"},
		{"jobs", "list", "--bad"},
		{"recommendations", "list", "--bad"},
		{"recommendations", "calibrate-speed", "--bad"},
		{"projects", "set", "--bad"},
		{"add-model", "--bad"},
		{"benchmark", "run", "--bad"},
		{"benchmark", "fleet", "--bad"},
	} {
		if err := Run(context.Background(), args); err == nil {
			t.Fatalf("Run(%v) expected flag error", args)
		}
	}
}

func TestRunNodesAdminCommands(t *testing.T) {
	var sawInviteAuth, sawRotate, sawRevoke bool
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/invite", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		sawInviteAuth = true
		_ = json.NewEncoder(w).Encode(map[string]string{"join": "mycjoin://127.0.0.1:51846?token=join-secret"})
	})
	mux.HandleFunc("/admin/tokens", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode([]domain.JoinTokenRecord{{Hash: "hash-a", Active: true, Current: true}})
	})
	mux.HandleFunc("/admin/tokens/rotate", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("rotate body: %v", err)
		}
		sawRotate = body["token"] == "next-secret"
		_ = json.NewEncoder(w).Encode(map[string]string{"join": "mycjoin://127.0.0.1:51846?token=next-secret"})
	})
	mux.HandleFunc("/admin/tokens/revoke", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("revoke body: %v", err)
		}
		sawRevoke = body["token"] == "old-secret"
		w.WriteHeader(http.StatusNoContent)
	})
	client := directHTTPClient(mux)
	home := t.TempDir()
	t.Setenv("HOME", home)
	configDir := filepath.Join(home, ".mycelium")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("mkdir config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "peer.json"), []byte(`{"listen":"http://peer.test","rpc_token":"rpc-secret"}`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	for _, tt := range []struct {
		args []string
		want string
	}{
		{args: []string{"nodes", "invite"}, want: "mycjoin://127.0.0.1:51846"},
		{args: []string{"nodes", "tokens", "list"}, want: "hash-a\ttrue\ttrue"},
		{args: []string{"nodes", "invite", "--url", "http://peer.test", "--rpc-token", "rpc-secret"}, want: "mycjoin://127.0.0.1:51846"},
		{args: []string{"nodes", "tokens", "list", "--url", "http://peer.test", "--rpc-token", "rpc-secret"}, want: "hash-a\ttrue\ttrue"},
		{args: []string{"nodes", "tokens", "rotate", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--token", "next-secret"}, want: "next-secret"},
		{args: []string{"nodes", "tokens", "revoke", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--token", "old-secret"}, want: "revoked"},
	} {
		output := captureStdout(t, func() {
			if err := RunWithClient(context.Background(), tt.args, client); err != nil {
				t.Fatalf("RunWithClient(%v): %v", tt.args, err)
			}
		})
		if !strings.Contains(output, tt.want) {
			t.Fatalf("output for %v = %q, want %q", tt.args, output, tt.want)
		}
	}
	if !sawInviteAuth || !sawRotate || !sawRevoke {
		t.Fatalf("saw invite=%t rotate=%t revoke=%t", sawInviteAuth, sawRotate, sawRevoke)
	}
	for _, args := range [][]string{
		{"nodes"},
		{"nodes", "tokens"},
		{"nodes", "tokens", "revoke", "--url", "http://peer.test", "--rpc-token", "rpc-secret"},
		{"nodes", "wat"},
	} {
		if err := RunWithClient(context.Background(), args, client); err == nil {
			t.Fatalf("RunWithClient(%v) expected error", args)
		}
	}
	if err := RunWithClient(context.Background(), []string{"nodes", "invite", "--url", "http://peer.test", "--rpc-token", "wrong"}, client); err == nil {
		t.Fatal("wrong-token invite succeeded")
	}
}

func TestNodeAdminDefaultsAndOverrides(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	defaults, err := loadNodeAdminDefaults()
	if err != nil {
		t.Fatalf("empty load defaults: %v", err)
	}
	if defaults != (nodeAdminDefaults{}) {
		t.Fatalf("empty defaults = %+v", defaults)
	}
	if got := adminBaseURL(""); got != "" {
		t.Fatalf("empty admin URL = %q", got)
	}
	if got := adminBaseURL("https://peer.test"); got != "https://peer.test" {
		t.Fatalf("absolute admin URL = %q", got)
	}
	if got := adminBaseURL("127.0.0.1:7777"); got != "http://127.0.0.1:7777" {
		t.Fatalf("host admin URL = %q", got)
	}
	if got := firstNonEmpty("", "first", "second"); got != "first" {
		t.Fatalf("firstNonEmpty = %q", got)
	}
	if got := firstNonEmpty("", ""); got != "" {
		t.Fatalf("empty firstNonEmpty = %q", got)
	}

	configDir := filepath.Join(home, ".mycelium")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("mkdir config: %v", err)
	}
	configPath := filepath.Join(configDir, "peer.json")
	if err := os.WriteFile(configPath, []byte(`{`), 0644); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := loadNodeAdminDefaults(); err == nil || !strings.Contains(err.Error(), "parse peer config") {
		t.Fatalf("bad config err = %v", err)
	}
	if err := os.WriteFile(configPath, []byte(`{"listen":"127.0.0.1:7777","rpc_token":"rpc-secret"}`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	defaults, err = loadNodeAdminDefaults()
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if defaults.BaseURL != "http://127.0.0.1:7777" || defaults.RPCToken != "rpc-secret" {
		t.Fatalf("defaults = %+v", defaults)
	}
	admin, err := nodeAdminFromArgs(nil, nil)
	if err != nil {
		t.Fatalf("nodeAdminFromArgs: %v", err)
	}
	if admin.BaseURL != "http://127.0.0.1:7777" || admin.AuthToken != "rpc-secret" {
		t.Fatalf("admin defaults = %+v", admin)
	}
	admin, token, err := nodeAdminWithTokenFromArgs([]string{"--url", "https://override.test", "--rpc-token", "override-secret", "--token", "join-secret"}, nil, true)
	if err != nil {
		t.Fatalf("nodeAdminWithTokenFromArgs: %v", err)
	}
	if admin.BaseURL != "https://override.test" || admin.AuthToken != "override-secret" || token != "join-secret" {
		t.Fatalf("admin override = %+v token=%q", admin, token)
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
	recordSustainedContextMetrics(t, store, project.ID, now)
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

func TestControlHTTPBodyReadLimit(t *testing.T) {
	if _, err := readControlHTTPBody(io.LimitReader(repeatByteReader{'x'}, maxControlHTTPBodyBytes+1), "control body"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("limit err = %v", err)
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

type repeatByteReader struct {
	b byte
}

func (r repeatByteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
	}
	return len(p), nil
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

func recordSustainedContextMetrics(t *testing.T, store interface {
	Record(context.Context, domain.RunMetric) error
}, projectID string, start time.Time) {
	t.Helper()
	for i := 0; i < 25; i++ {
		contextUsed := 3500
		if i >= 20 {
			contextUsed = 4000
		}
		if err := store.Record(context.Background(), domain.RunMetric{
			JobID:       "ctx-" + string(rune('a'+i)),
			Project:     projectID,
			ContextUsed: contextUsed,
			At:          start.Add(time.Duration(i) * time.Hour),
		}); err != nil {
			t.Fatalf("Record sustained metric: %v", err)
		}
	}
}

func controlFleetConfig() bench.FleetBenchmarkConfig {
	return bench.FleetBenchmarkConfig{
		ID:                           "fleet-cli",
		Project:                      "project-a",
		RPCToken:                     "rpc-secret",
		GatewayToken:                 "gateway-secret",
		TrustedControlHeaderTestMode: true,
		Gateways: []bench.FleetGateway{
			{ID: "macbook-gw", URL: "http://macbook.test", NodeID: "macbook"},
			{ID: "macmini-gw", URL: "http://macmini.test", NodeID: "mac-mini"},
		},
		Peers: []bench.FleetPeer{{ID: "spark", URL: "http://spark.test", RPCToken: "rpc-secret"}},
		Prompts: []bench.FleetPrompt{{
			ID:   "default",
			Text: "answer briefly",
		}},
		Models: []bench.FleetModel{
			{ID: "qwen9b", RequestModel: "qwen9b", PresetID: "preset-9b", PromptID: "default", Priority: domain.PriorityInteractive, SpeedPref: domain.SpeedThroughput, Preemption: domain.PreemptSoft, MaxTokens: 8},
			{ID: "qwen122b", RequestModel: "qwen122b", PresetID: "preset-122b", PromptID: "default", Priority: domain.PriorityInteractive, SpeedPref: domain.SpeedThroughput, Preemption: domain.PreemptHardForInteractive, MaxTokens: 8},
		},
		Waves: []bench.FleetWave{
			{ID: "cold-9b", Jobs: []bench.FleetWaveJob{{ModelID: "qwen9b", GatewayID: "macbook-gw"}}},
			{ID: "warm-9b", Jobs: []bench.FleetWaveJob{{ModelID: "qwen9b", GatewayID: "macmini-gw"}}},
			{ID: "fit-forced-122b", Jobs: []bench.FleetWaveJob{{ModelID: "qwen122b", GatewayID: "macbook-gw"}}},
		},
		Simulation: bench.FleetSimulationConfig{
			Nodes: []domain.Node{
				{
					ID:               "spark",
					Name:             "dgx-spark",
					MaxUtil:          0.90,
					DiskTotalMB:      1_000_000,
					DiskFreeMB:       900_000,
					DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
					OOMSeverity:      domain.OOMCatastrophic,
					Status:           domain.NodeReady,
					Labels:           map[string]string{"gpu.kind": "gb10"},
					Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 122000}},
					SpeedClass:       domain.SpeedClass{TokensPerSecRef: 145},
				},
				{
					ID:               "b70",
					Name:             "arc-b70",
					MaxUtil:          0.85,
					DiskTotalMB:      1_000_000,
					DiskFreeMB:       700_000,
					DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
					OOMSeverity:      domain.OOMSoft,
					Status:           domain.NodeReady,
					Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 32768}},
					SpeedClass:       domain.SpeedClass{TokensPerSecRef: 70},
				},
				{
					ID:               "disk-full",
					Name:             "disk-full",
					MaxUtil:          0.90,
					DiskTotalMB:      1000,
					DiskFreeMB:       250,
					DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
					OOMSeverity:      domain.OOMSoft,
					Status:           domain.NodeReady,
					Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 200000}},
					SpeedClass:       domain.SpeedClass{TokensPerSecRef: 999},
				},
			},
			Presets: []domain.Preset{
				{ID: "preset-9b", ModelRef: "qwen9b", Backend: domain.BackendLlamaCpp, ContextLength: 8000, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 7000, ArtifactSizeMB: 7000, KVPerTokenMB: 0.05},
				{ID: "preset-27b", ModelRef: "qwen27b", Backend: domain.BackendLlamaCpp, ContextLength: 8000, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 30000, ArtifactSizeMB: 30000, KVPerTokenMB: 0.25},
				{ID: "preset-122b", ModelRef: "qwen122b", Backend: domain.BackendLlamaCpp, ContextLength: 8000, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 76000, ArtifactSizeMB: 76000, KVPerTokenMB: 0},
			},
			Instances: []domain.ModelInstance{{
				ID:             "inst-27b-background",
				PresetID:       "preset-27b",
				NodeID:         "spark",
				AcceleratorSet: []int{0},
				Claim:          domain.Claim{WeightsMB: 30000, KVReservedMB: 2000},
				State:          domain.InstReady,
				Priority:       domain.PriorityBackground,
			}},
		},
		Safety: bench.FleetBenchmarkSafety{MinDiskFreeRatio: domain.DefaultDiskMinFreeRatio, MaxSparkGPUMemoryUtil: 0.85},
	}
}
