package controlcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/bench"
	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/optimizer"
	peercoord "mycelium/internal/peer"
	"mycelium/internal/reconcile"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/internal/workload"
	"mycelium/pkg/api"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

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

func TestRunReconcilePlacementReportsTasks(t *testing.T) {
	dir := t.TempDir()
	decisionPath := filepath.Join(dir, "decision.json")
	decision := domain.PlacementDecision{
		JobID:  "job-reconcile",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
		Trace: []domain.TraceStep{{
			Step: "filter",
			Data: map[string]any{"dropped": map[string]string{
				"node-engine": "label." + domain.LabelPeerBackends,
				"node-down":   string(domain.NodeUnreachable),
			}},
		}},
	}
	data, err := json.Marshal(decision)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	if err := os.WriteFile(decisionPath, data, 0o600); err != nil {
		t.Fatalf("write decision: %v", err)
	}

	output := captureStdout(t, func() {
		err = Run(context.Background(), []string{"reconcile", "placement", "--decision-file", decisionPath})
	})
	if err != nil {
		t.Fatalf("reconcile placement: %v", err)
	}
	for _, want := range []string{
		"reconcile\tjob=job-reconcile\tmodel=model-a\tpreset=preset-a\taction=queued",
		"task\tkind=repair_owner\tnode=node-down",
		"task\tkind=verify_engine_readiness\tnode=node-engine",
		"requires_approval=true",
		"audit\tkind=report_only",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("reconcile output missing %q:\n%s", want, output)
		}
	}

	jsonOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"reconcile", "placement", "--decision-file", decisionPath, "--json", "--allow-owner-repair"})
	})
	if err != nil {
		t.Fatalf("reconcile placement json: %v", err)
	}
	var report struct {
		Tasks []struct {
			Kind               string `json:"kind"`
			NodeID             string `json:"node_id"`
			AutomaticCandidate bool   `json:"automatic_candidate"`
			RequiresApproval   bool   `json:"requires_approval"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal([]byte(jsonOutput), &report); err != nil {
		t.Fatalf("decode reconcile json: %v\n%s", err, jsonOutput)
	}
	if len(report.Tasks) != 2 {
		t.Fatalf("tasks = %+v", report.Tasks)
	}
	for _, task := range report.Tasks {
		if task.NodeID == "node-down" && (!task.AutomaticCandidate || task.RequiresApproval) {
			t.Fatalf("owner repair policy not applied: %+v", report.Tasks)
		}
	}

	dbPath := filepath.Join(dir, "control.db")
	saveOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"reconcile", "placement", "--decision-file", decisionPath, "--db", dbPath, "--save"})
	})
	if err != nil {
		t.Fatalf("reconcile placement save: %v", err)
	}
	if !strings.Contains(saveOutput, "task\tkind=repair_owner") {
		t.Fatalf("save output = %q", saveOutput)
	}
	statusOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"reconcile", "status", "--db", dbPath, "--job", "job-reconcile"})
	})
	if err != nil {
		t.Fatalf("reconcile status: %v", err)
	}
	for _, want := range []string{
		"reconcile-status\ttasks=2",
		"status=blocked\tkind=repair_owner",
		"status=allowed\tkind=verify_engine_readiness",
		"blocker=operator_approval_required",
	} {
		if !strings.Contains(statusOutput, want) {
			t.Fatalf("status output missing %q:\n%s", want, statusOutput)
		}
	}
}

func TestRunReconcileDrainExecutesAllowedTasks(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "control.db")
	decisionPath := filepath.Join(dir, "decision.json")
	decision := domain.PlacementDecision{
		JobID:  "job-drain",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
		Trace: []domain.TraceStep{{
			Step: "filter",
			Data: map[string]any{"dropped": map[string]string{
				"node-incompat": "label.gpu.vendor",
				"node-down":     string(domain.NodeUnreachable),
			}},
		}},
	}
	data, err := json.Marshal(decision)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	if err := os.WriteFile(decisionPath, data, 0o600); err != nil {
		t.Fatalf("write decision: %v", err)
	}
	if err := Run(context.Background(), []string{"reconcile", "placement", "--decision-file", decisionPath, "--db", dbPath, "--save"}); err != nil {
		t.Fatalf("reconcile placement save: %v", err)
	}

	output := captureStdout(t, func() {
		err = Run(context.Background(), []string{"reconcile", "drain", "--db", dbPath, "--job", "job-drain"})
	})
	if err != nil {
		t.Fatalf("reconcile drain: %v", err)
	}
	for _, want := range []string{
		"reconcile-drain\tdrained=1\tskipped=1",
		"kind=record_incompatibility\tstatus=done\taction=record_incompatibility",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("drain output missing %q:\n%s", want, output)
		}
	}

	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	tasks, err := store.ListReconciliationTasks(context.Background(), "job-drain")
	if err != nil {
		t.Fatalf("ListReconciliationTasks: %v", err)
	}
	statusByKind := map[string]domain.ReconciliationTaskStatus{}
	for _, task := range tasks {
		statusByKind[task.Kind] = task.Status
	}
	if statusByKind[string(reconcile.TaskRecordIncompatibility)] != domain.ReconciliationTaskDone {
		t.Fatalf("task statuses = %+v", tasks)
	}
	if statusByKind[string(reconcile.TaskRepairOwner)] != domain.ReconciliationTaskBlocked {
		t.Fatalf("repair task was not left blocked: %+v", tasks)
	}
	incompatibilities, err := store.ListIncompatibilities(context.Background())
	if err != nil || len(incompatibilities) != 1 || incompatibilities[0].JobID != "job-drain" || incompatibilities[0].Reason != "label.gpu.vendor" {
		t.Fatalf("incompatibilities = %+v, %v", incompatibilities, err)
	}
}

func TestRunReconcileDrainPlansArtifactStaging(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	preset := testPreset("tiny")
	preset.ArtifactSizeMB = 64
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-a", Status: domain.NodeReady}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	decisionPath := filepath.Join(dir, "decision.json")
	decision := domain.PlacementDecision{
		JobID:  "job-stage",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: preset.ID, ModelRef: preset.ModelRef},
		Trace: []domain.TraceStep{{
			Step: "filter",
			Data: map[string]any{"dropped": map[string]string{"node-a": "disk.free_after_model"}},
		}},
	}
	data, err := json.Marshal(decision)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	if err := os.WriteFile(decisionPath, data, 0o600); err != nil {
		t.Fatalf("write decision: %v", err)
	}
	if err := Run(context.Background(), []string{"reconcile", "placement", "--decision-file", decisionPath, "--db", dbPath, "--save"}); err != nil {
		t.Fatalf("reconcile placement save: %v", err)
	}
	output := captureStdout(t, func() {
		err = Run(context.Background(), []string{"reconcile", "drain", "--db", dbPath, "--job", "job-stage"})
	})
	if err != nil {
		t.Fatalf("reconcile drain: %v", err)
	}
	if !strings.Contains(output, "kind=stage_model_artifact\tstatus=done\taction=plan_model_artifact_stage") {
		t.Fatalf("drain output = %q", output)
	}

	verify, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open verify: %v", err)
	}
	defer verify.Close()
	plans, err := verify.ListLocalityPlans(context.Background())
	if err != nil || len(plans) != 1 || len(plans[0].Actions) != 1 {
		t.Fatalf("ListLocalityPlans = %+v, %v", plans, err)
	}
	if action := plans[0].Actions[0]; action.Kind != domain.LocalityActionStage || action.PresetID != preset.ID || action.NodeID != "node-a" || action.State != domain.ModelLocalityPlanned {
		t.Fatalf("locality action = %+v", action)
	}
	localities, err := verify.ListModelLocalities(context.Background())
	if err != nil || len(localities) != 1 || localities[0].PresetID != preset.ID || localities[0].NodeID != "node-a" || localities[0].State != domain.ModelLocalityPlanned {
		t.Fatalf("ListModelLocalities = %+v, %v", localities, err)
	}
}

func TestDecodePlacementDecisionAcceptsJobAndWrapper(t *testing.T) {
	decision := domain.PlacementDecision{JobID: "job-a", Action: domain.ActionLoadedNew}
	jobData, err := json.Marshal(domain.Job{ID: "job-a", Result: &domain.JobResult{Decision: decision}})
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	got, err := decodePlacementDecision(jobData)
	if err != nil || got.JobID != "job-a" || got.Action != domain.ActionLoadedNew {
		t.Fatalf("decode job decision = %+v, %v", got, err)
	}

	wrapperData, err := json.Marshal(map[string]any{"result": domain.JobResult{Decision: domain.PlacementDecision{JobID: "job-b", Action: domain.ActionQueued}}})
	if err != nil {
		t.Fatalf("marshal wrapper: %v", err)
	}
	got, err = decodePlacementDecision(wrapperData)
	if err != nil || got.JobID != "job-b" || got.Action != domain.ActionQueued {
		t.Fatalf("decode wrapper decision = %+v, %v", got, err)
	}

	if _, err := decodePlacementDecision([]byte(`{"job_id":""}`)); err == nil || !strings.Contains(err.Error(), "placement decision") {
		t.Fatalf("decode empty decision err = %v", err)
	}
	if err := Run(context.Background(), []string{"reconcile", "placement"}); err == nil || !strings.Contains(err.Error(), "--decision-file") {
		t.Fatalf("missing decision-file err = %v", err)
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

func TestRunJobsAdmitAndStatusUseWorkloadRPCWithoutPrintingPayload(t *testing.T) {
	dir := t.TempDir()
	intentPath := filepath.Join(dir, "workload.json")
	intent := workload.AdmissionRequest{
		Job: domain.Job{
			ID:                  "job-a",
			TaskType:            "chat",
			Model:               "model-a",
			PresetID:            "preset-a",
			Project:             "project-a",
			Priority:            domain.PriorityInteractive,
			SpeedPref:           domain.SpeedLatency,
			ContextRequest:      8192,
			ExpectedConcurrency: 2,
			Preemption:          domain.PreemptSoft,
		},
		Payload: json.RawMessage(`{"kind":"openai_chat","body":{"model":"model-a","messages":[]}}`),
	}
	data, err := json.Marshal(intent)
	if err != nil {
		t.Fatalf("Marshal intent: %v", err)
	}
	if err := os.WriteFile(intentPath, data, 0600); err != nil {
		t.Fatalf("WriteFile intent: %v", err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			t.Fatalf("auth header = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/workloads":
			var req workload.AdmissionRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode workload admit: %v", err)
			}
			if req.Job.ContextRequest != 8192 || !req.QueueOnly || string(req.Payload) != `{"kind":"openai_chat","body":{"model":"model-a","messages":[]}}` {
				t.Fatalf("admission request = %+v payload=%s", req.Job, req.Payload)
			}
			w.WriteHeader(http.StatusAccepted)
			result := &domain.JobResult{Source: "scheduler", Action: domain.ActionQueued, PresetID: req.Job.PresetID, PayloadBytes: len(req.Payload), PayloadStored: true, PayloadRedacted: true, ResultRedacted: true}
			_ = json.NewEncoder(w).Encode(workload.AdmissionResponse{
				Job:           domain.Job{ID: req.Job.ID, Model: req.Job.Model, PresetID: req.Job.PresetID, Project: req.Job.Project, Status: domain.JobQueued, Result: result},
				Decision:      domain.PlacementDecision{JobID: req.Job.ID, Action: domain.ActionQueued, Preset: domain.Preset{ID: req.Job.PresetID}},
				Result:        result,
				Queued:        true,
				PayloadBytes:  len(req.Payload),
				PayloadStored: true,
				Execution:     workload.ExecutionEvidence{State: "queued_not_executed", PayloadContract: "runtime_payload_not_inspectable"},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/workloads/job-a":
			result := &domain.JobResult{Source: "scheduler", Action: domain.ActionLoadedNew, NodeID: "node-a", PresetID: "preset-a", LeaseID: "lease-a", PayloadBytes: len(intent.Payload), PayloadStored: true, PayloadRedacted: true, ResultRedacted: true}
			_ = json.NewEncoder(w).Encode(workload.StatusResponse{Job: domain.Job{
				ID:                  "job-a",
				Model:               "model-a",
				PresetID:            "preset-a",
				Project:             "project-a",
				Priority:            domain.PriorityInteractive,
				ContextRequest:      8192,
				ExpectedConcurrency: 2,
				Preemption:          domain.PreemptSoft,
				Status:              domain.JobQueued,
				Result:              result,
			}, Result: result, Execution: workload.ExecutionEvidence{State: "queued_not_executed", PayloadContract: "registry_executable_gateway_request", PayloadKind: workload.ExecutablePayloadKindOpenAIChat, PayloadExecutable: true}})
		case r.Method == http.MethodPost && r.URL.Path == "/workloads/drain" && r.URL.Query().Get("limit") == "2" && r.URL.Query().Get("execute") == "true":
			result := &domain.JobResult{Source: "scheduler", Action: domain.ActionLoadedNew, NodeID: "node-a", PresetID: "preset-a", LeaseID: "lease-a", PayloadBytes: len(intent.Payload), PayloadStored: true, PayloadRedacted: true, ResultRedacted: true}
			_ = json.NewEncoder(w).Encode(workload.DrainResponse{Results: []workload.AdmissionResponse{{
				Job:           domain.Job{ID: "job-a", Model: "model-a", PresetID: "preset-a", Project: "project-a", Status: domain.JobRunning, Result: result},
				Decision:      domain.PlacementDecision{JobID: "job-a", Action: domain.ActionLoadedNew, NodeID: "node-a", Preset: domain.Preset{ID: "preset-a"}},
				Result:        result,
				PayloadBytes:  len(intent.Payload),
				PayloadStored: true,
				Execution:     workload.ExecutionEvidence{State: "running_needs_finish", PayloadContract: "registry_executable_gateway_request", PayloadKind: workload.ExecutablePayloadKindOpenAIChat, PayloadExecutable: true},
			}}})
		case r.Method == http.MethodPost && r.URL.Path == "/workloads/job-a/execute":
			result := &domain.JobResult{Source: "scheduler", Action: domain.ActionLoadedNew, NodeID: "node-a", PresetID: "preset-a", LeaseID: "lease-a", PayloadBytes: len(intent.Payload), PayloadStored: true, PayloadRedacted: true, ResultArtifact: "stored://workload/job-a", ResultMediaType: "application/json", ResultBytes: len(`{"answer":"ok"}`), ResultSHA256: sha256Hex([]byte(`{"answer":"ok"}`)), ResultRedacted: true, Terminal: true, ProviderCapture: true}
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{Job: domain.Job{ID: "job-a", Model: "model-a", PresetID: "preset-a", Project: "project-a", Status: domain.JobDone, Result: result}, Result: result, Execution: workload.ExecutionEvidence{State: "terminal_result_replayable", AutomaticProviderOutputCapture: true, ResultReplayable: true, ResultBodyStored: true, ResultArtifact: true, ResultBytes: len(`{"answer":"ok"}`), ResultSHA256: true}})
		case r.Method == http.MethodPost && r.URL.Path == "/workloads/job-a/finish":
			var req workload.FinishRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode workload finish: %v", err)
			}
			if req.Error != "" {
				t.Fatalf("finish error = %q", req.Error)
			}
			if req.ResultArtifact != "stored://workload/job-a" || req.ResultMediaType != "application/json" || req.ResultBytes != len(`{"answer":"ok"}`) || req.ResultSHA256 != sha256Hex([]byte(`{"answer":"ok"}`)) || string(req.ResultBody) != `{"answer":"ok"}` {
				t.Fatalf("finish result manifest = %+v", req)
			}
			result := &domain.JobResult{Source: "scheduler", Action: domain.ActionLoadedNew, NodeID: "node-a", PresetID: "preset-a", LeaseID: "lease-a", PayloadBytes: len(intent.Payload), PayloadStored: true, PayloadRedacted: true, ResultArtifact: req.ResultArtifact, ResultMediaType: req.ResultMediaType, ResultBytes: req.ResultBytes, ResultSHA256: req.ResultSHA256, ResultRedacted: true, Terminal: true}
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{Job: domain.Job{ID: "job-a", Model: "model-a", PresetID: "preset-a", Project: "project-a", Status: domain.JobDone, Result: result}, Result: result, Execution: workload.ExecutionEvidence{State: "terminal_result_replayable", ResultReplayable: true, ResultBodyStored: true, ResultArtifact: true, ResultBytes: len(`{"answer":"ok"}`), ResultSHA256: true}})
		case r.Method == http.MethodGet && r.URL.Path == "/workloads/job-a/result":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Myc-Result-SHA256", sha256Hex([]byte(`{"answer":"ok"}`)))
			_, _ = w.Write([]byte(`{"answer":"ok"}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})
	client := directHTTPClient(handler)
	admitOut := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"jobs", "admit", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--file", intentPath, "--queue-only"}, client)
	})
	if err != nil {
		t.Fatalf("jobs admit: %v", err)
	}
	if !strings.Contains(admitOut, "workload\tjob-a\tstatus=queued\tqueued=true") || !strings.Contains(admitOut, "payload_contract=runtime_payload_not_inspectable") || !strings.Contains(admitOut, "execution_state=queued_not_executed") || !strings.Contains(admitOut, "provider_output_capture=false") || strings.Contains(admitOut, "messages") {
		t.Fatalf("admit output = %q", admitOut)
	}
	statusOut := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"jobs", "status", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--id", "job-a"}, client)
	})
	if err != nil {
		t.Fatalf("jobs status: %v", err)
	}
	if !strings.Contains(statusOut, "context=8192\tconcurrency=2\tpreemption=soft") || !strings.Contains(statusOut, "payload_contract=registry_executable_gateway_request") || !strings.Contains(statusOut, "payload_kind=openai_chat") || !strings.Contains(statusOut, "payload_executable=true") || !strings.Contains(statusOut, "execution_state=queued_not_executed") || !strings.Contains(statusOut, "result_replayable=false") || strings.Contains(statusOut, "messages") {
		t.Fatalf("status output = %q", statusOut)
	}
	drainOut := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"jobs", "drain", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--limit", "2", "--execute"}, client)
	})
	if err != nil {
		t.Fatalf("jobs drain: %v", err)
	}
	if !strings.Contains(drainOut, "drained\t1") || !strings.Contains(drainOut, "node=node-a") || !strings.Contains(drainOut, "payload_executable=true") || !strings.Contains(drainOut, "execution_state=running_needs_finish") || strings.Contains(drainOut, "messages") {
		t.Fatalf("drain output = %q", drainOut)
	}
	executeOut := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"jobs", "execute", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--id", "job-a"}, client)
	})
	if err != nil {
		t.Fatalf("jobs execute: %v", err)
	}
	if !strings.Contains(executeOut, "status=done") || !strings.Contains(executeOut, "provider_output_capture=true") || !strings.Contains(executeOut, "execution_state=terminal_result_replayable") || strings.Contains(executeOut, "messages") || strings.Contains(executeOut, "answer") {
		t.Fatalf("execute output = %q", executeOut)
	}
	resultFile := filepath.Join(dir, "result.json")
	if err := os.WriteFile(resultFile, []byte(`{"answer":"ok"}`), 0600); err != nil {
		t.Fatalf("write result file: %v", err)
	}
	finishOut := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"jobs", "finish", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--id", "job-a", "--result-media-type", "application/json", "--result-file", resultFile}, client)
	})
	if err != nil {
		t.Fatalf("jobs finish: %v", err)
	}
	if !strings.Contains(finishOut, "status=done") || !strings.Contains(finishOut, "terminal=true") || !strings.Contains(finishOut, "execution_state=terminal_result_replayable") || !strings.Contains(finishOut, "result_replayable=true") || !strings.Contains(finishOut, "result_artifact=stored://workload/job-a") || !strings.Contains(finishOut, "result_bytes=15") || strings.Contains(finishOut, "messages") || strings.Contains(finishOut, "answer") {
		t.Fatalf("finish output = %q", finishOut)
	}
	resultOutPath := filepath.Join(dir, "retrieved.json")
	resultOut := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"jobs", "result", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--id", "job-a", "--out", resultOutPath}, client)
	})
	if err != nil {
		t.Fatalf("jobs result: %v", err)
	}
	if !strings.Contains(resultOut, "workload-result\t") || !strings.Contains(resultOut, "bytes=15") || !strings.Contains(resultOut, "sha256="+sha256Hex([]byte(`{"answer":"ok"}`))) {
		t.Fatalf("result output = %q", resultOut)
	}
	retrieved, err := os.ReadFile(resultOutPath)
	if err != nil || string(retrieved) != `{"answer":"ok"}` {
		t.Fatalf("retrieved result = %q err=%v", retrieved, err)
	}
	jsonOut := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"jobs", "status", "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--id", "job-a", "--json"}, client)
	})
	if err != nil {
		t.Fatalf("jobs status json: %v", err)
	}
	if !strings.Contains(jsonOut, `"payload_redacted":true`) || strings.Contains(jsonOut, "input_bytes") {
		t.Fatalf("json output = %q", jsonOut)
	}
}

func TestRunJobsAdmitWrapsExecutablePayloadFile(t *testing.T) {
	dir := t.TempDir()
	intentPath := filepath.Join(dir, "workload.json")
	intent := workload.AdmissionRequest{
		Job: domain.Job{
			ID:       "job-a",
			TaskType: "chat",
			Model:    "model-a",
			Project:  "project-a",
		},
	}
	data, err := json.Marshal(intent)
	if err != nil {
		t.Fatalf("Marshal intent: %v", err)
	}
	if err := os.WriteFile(intentPath, data, 0600); err != nil {
		t.Fatalf("WriteFile intent: %v", err)
	}
	payloadPath := filepath.Join(dir, "body.json")
	if err := os.WriteFile(payloadPath, []byte(`{"model":"model-a","messages":[]}`), 0600); err != nil {
		t.Fatalf("WriteFile payload: %v", err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req workload.AdmissionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode workload admit: %v", err)
		}
		executable, err := workload.DecodeExecutablePayload(req.Payload)
		if err != nil {
			t.Fatalf("DecodeExecutablePayload: %v payload=%s", err, req.Payload)
		}
		if executable.Kind != workload.ExecutablePayloadKindOpenAIChat || string(executable.Body) != `{"model":"model-a","messages":[]}` {
			t.Fatalf("executable payload = %+v", executable)
		}
		result := &domain.JobResult{Source: "scheduler", Action: domain.ActionQueued, PresetID: "preset-a", PayloadBytes: len(req.Payload), PayloadStored: true, PayloadRedacted: true, ResultRedacted: true}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(workload.AdmissionResponse{
			Job:           domain.Job{ID: req.Job.ID, Model: req.Job.Model, Project: req.Job.Project, Status: domain.JobQueued, Result: result},
			Decision:      domain.PlacementDecision{JobID: req.Job.ID, Action: domain.ActionQueued, Preset: domain.Preset{ID: "preset-a"}},
			Result:        result,
			Queued:        true,
			PayloadBytes:  len(req.Payload),
			PayloadStored: true,
			Execution:     workload.ExecutionEvidence{State: "queued_not_executed", PayloadContract: "runtime_payload_not_inspectable"},
		})
	})
	client := directHTTPClient(handler)
	out := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"jobs", "admit",
			"--url", "http://peer.test",
			"--file", intentPath,
			"--payload-kind", workload.ExecutablePayloadKindOpenAIChat,
			"--payload-file", payloadPath,
		}, client)
	})
	if err != nil {
		t.Fatalf("jobs admit: %v", err)
	}
	if !strings.Contains(out, "payload_contract=runtime_payload_not_inspectable") || strings.Contains(out, "messages") {
		t.Fatalf("admit output = %q", out)
	}
	for _, args := range [][]string{
		{"jobs", "admit", "--url", "http://peer.test", "--file", intentPath, "--payload-kind", workload.ExecutablePayloadKindOpenAIChat},
		{"jobs", "admit", "--url", "http://peer.test", "--file", intentPath, "--payload-file", payloadPath},
	} {
		if err := RunWithClient(context.Background(), args, client); err == nil || !strings.Contains(err.Error(), "payload-kind") {
			t.Fatalf("RunWithClient(%v) err = %v", args, err)
		}
	}
}

func TestRunJobsAdmitAndStatusErrorBranches(t *testing.T) {
	err := runJobsAdmit(context.Background(), nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("missing url err = %v", err)
	}
	err = runJobsAdmit(context.Background(), []string{"--url", "http://peer.test"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--file") {
		t.Fatalf("missing file err = %v", err)
	}
	badJSON := filepath.Join(t.TempDir(), "bad.json")
	if writeErr := os.WriteFile(badJSON, []byte(`{`), 0600); writeErr != nil {
		t.Fatalf("WriteFile bad json: %v", writeErr)
	}
	err = runJobsAdmit(context.Background(), []string{"--url", "http://peer.test", "--file", badJSON}, http.DefaultClient)
	if err == nil {
		t.Fatal("bad intent json accepted")
	}
	errorClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no instance", http.StatusConflict)
	}))
	goodIntent := filepath.Join(t.TempDir(), "good.json")
	if writeErr := os.WriteFile(goodIntent, []byte(`{"job":{"id":"job-a","model":"model-a","project":"project-a"}}`), 0600); writeErr != nil {
		t.Fatalf("WriteFile good intent: %v", writeErr)
	}
	err = runJobsAdmit(context.Background(), []string{"--url", "http://peer.test", "--file", goodIntent}, errorClient)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("admit error response err = %v", err)
	}
	badResponseClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	err = runJobsAdmit(context.Background(), []string{"--url", "http://peer.test", "--file", goodIntent}, badResponseClient)
	if err == nil {
		t.Fatal("bad admit response accepted")
	}

	err = runJobsStatus(context.Background(), nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("status missing url err = %v", err)
	}
	err = runJobsStatus(context.Background(), []string{"--url", "http://peer.test"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("status missing id err = %v", err)
	}
	err = runJobsStatus(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, errorClient)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("status error response err = %v", err)
	}
	err = runJobsStatus(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, badResponseClient)
	if err == nil {
		t.Fatal("bad status response accepted")
	}
	err = runJobsDrain(context.Background(), nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("drain missing url err = %v", err)
	}
	err = runJobsDrain(context.Background(), []string{"--url", "http://peer.test", "--limit", "0"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--limit") {
		t.Fatalf("drain bad limit err = %v", err)
	}
	err = runJobsDrain(context.Background(), []string{"--url", "http://peer.test"}, errorClient)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("drain error response err = %v", err)
	}
	err = runJobsDrain(context.Background(), []string{"--url", "http://peer.test"}, badResponseClient)
	if err == nil {
		t.Fatal("bad drain response accepted")
	}
	err = runJobsExecute(context.Background(), nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("execute missing url err = %v", err)
	}
	err = runJobsExecute(context.Background(), []string{"--url", "http://peer.test"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("execute missing id err = %v", err)
	}
	err = runJobsExecute(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, errorClient)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("execute error response err = %v", err)
	}
	err = runJobsExecute(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, badResponseClient)
	if err == nil {
		t.Fatal("bad execute response accepted")
	}
	err = runJobsFinish(context.Background(), nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("finish missing url err = %v", err)
	}
	err = runJobsFinish(context.Background(), []string{"--url", "http://peer.test"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("finish missing id err = %v", err)
	}
	err = runJobsFinish(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, errorClient)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("finish error response err = %v", err)
	}
	err = runJobsFinish(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, badResponseClient)
	if err == nil {
		t.Fatal("bad finish response accepted")
	}
	err = runJobsCancel(context.Background(), nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("cancel missing url err = %v", err)
	}
	err = runJobsCancel(context.Background(), []string{"--url", "http://peer.test"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("cancel missing id err = %v", err)
	}
	err = runJobsCancel(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, errorClient)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("cancel error response err = %v", err)
	}
	err = runJobsCancel(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, badResponseClient)
	if err == nil {
		t.Fatal("bad cancel response accepted")
	}
	err = runJobsResult(context.Background(), nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("result missing url err = %v", err)
	}
	err = runJobsResult(context.Background(), []string{"--url", "http://peer.test"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("result missing id err = %v", err)
	}
	err = runJobsResult(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "--out") {
		t.Fatalf("result missing out err = %v", err)
	}
	err = runJobsResult(context.Background(), []string{"--url", "http://peer.test", "--id", "job-a", "--out", filepath.Join(t.TempDir(), "result.json")}, errorClient)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("result error response err = %v", err)
	}
}

func TestRunJobsCancelPostsRedactedReason(t *testing.T) {
	var gotPath, gotAuth, gotReason string
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		var req workload.CancelRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode cancel: %v", err)
		}
		gotReason = req.Reason
		result := &domain.JobResult{Source: "operator_cancel", Terminal: true, ResultRedacted: true}
		_ = json.NewEncoder(w).Encode(workload.FinishResponse{
			Job:       domain.Job{ID: "job-a", Model: "model-a", Status: domain.JobFailed, Error: req.Reason, Result: result},
			Result:    result,
			Execution: workload.ExecutionEvidence{State: "terminal_manifest_only", ResultArtifact: true},
		})
	}))
	var err error
	output := captureStdout(t, func() {
		err = runJobsCancel(context.Background(), []string{
			"--url", "http://peer.test",
			"--rpc-token", "rpc-secret",
			"--id", "job-a",
			"--reason", "stale cleanup",
		}, client)
	})
	if err != nil {
		t.Fatalf("jobs cancel: %v", err)
	}
	if gotPath != "/workloads/job-a/cancel" || gotAuth != "Bearer rpc-secret" || gotReason != "stale cleanup" {
		t.Fatalf("request path=%s auth=%s reason=%s", gotPath, gotAuth, gotReason)
	}
	if !strings.Contains(output, "workload\tjob-a\tstatus=failed") || !strings.Contains(output, "terminal=true") || strings.Contains(output, "messages") {
		t.Fatalf("output = %q", output)
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

func TestRunModelsPrewarmUsesWorkloadAdmissionAndFinish(t *testing.T) {
	var admittedJobID string
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer rpc-secret" {
			t.Fatalf("auth = %s", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/workloads":
			var req workload.AdmissionRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode admission: %v", err)
			}
			if req.Job.TaskType != string(domain.CapabilityChat) || req.Job.Model != "vision-model" || req.Job.Project != "operator" || req.Job.Priority != domain.PriorityBackground || req.Job.ExpectedConcurrency != 2 || req.Job.ContextRequest != 8192 {
				t.Fatalf("job = %+v", req.Job)
			}
			if !strings.Contains(string(req.Payload), `"kind":"model_prewarm"`) || !strings.Contains(string(req.Payload), `"model":"vision-model"`) {
				t.Fatalf("payload = %s", req.Payload)
			}
			admittedJobID = req.Job.ID
			decision := domain.PlacementDecision{
				JobID:      req.Job.ID,
				Action:     domain.ActionLoadedNew,
				NodeID:     "node-a",
				Preset:     domain.Preset{ID: "vision-preset"},
				InstanceID: "inst-a",
			}
			inst := domain.ModelInstance{ID: "inst-a", NodeID: "node-a", PresetID: "vision-preset", State: domain.InstReady}
			lease := domain.Lease{ID: "lease-a", JobID: req.Job.ID, NodeID: "node-a", InstanceID: "inst-a"}
			req.Job.Status = domain.JobRunning
			req.Job.Result = &domain.JobResult{Decision: decision, Instance: inst, LeaseID: lease.ID, NodeID: "node-a", PresetID: "vision-preset", Action: decision.Action}
			_ = json.NewEncoder(w).Encode(workload.AdmissionResponse{Job: req.Job, Decision: decision, Instance: inst, Lease: lease})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/finish"):
			if admittedJobID == "" || r.URL.Path != "/workloads/"+admittedJobID+"/finish" {
				t.Fatalf("finish path = %s admitted=%s", r.URL.Path, admittedJobID)
			}
			job := domain.Job{ID: admittedJobID, Model: "vision-model", Status: domain.JobDone}
			result := &domain.JobResult{NodeID: "node-a", PresetID: "vision-preset", Action: domain.ActionLoadedNew, LeaseID: "lease-a", Instance: domain.ModelInstance{ID: "inst-a"}, Terminal: true}
			job.Result = result
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{Job: job, Result: result})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))

	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{
			"models", "prewarm",
			"--url", "http://peer.test",
			"--rpc-token", "rpc-secret",
			"--model", "vision-model",
			"--project", "operator",
			"--context", "8192",
			"--concurrency", "2",
		}, client)
	})
	if runErr != nil {
		t.Fatalf("models prewarm: %v", runErr)
	}
	if !strings.Contains(output, "models-prewarm\tmodel=vision-model") || !strings.Contains(output, "warmed=1") || !strings.Contains(output, "instance=inst-a") || !strings.Contains(output, "finished=true") {
		t.Fatalf("output = %q", output)
	}
}

func TestRunModelsPrewarmAllFittingTargetsDistinctOwners(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "peer.json")
	cfg := operatorPeerConfig{
		ID:       "gateway",
		Listen:   "127.0.0.1:9000",
		RPCToken: "rpc-secret",
		SeedPeers: []string{
			"http://owner-a.test",
			"http://owner-b.test",
		},
		Presets: []domain.Preset{{
			ID:             "vision-vllm",
			ModelRef:       "model/vision",
			Aliases:        []string{"vision"},
			Backend:        domain.BackendVLLM,
			ContextLength:  4096,
			Capabilities:   []domain.Capability{domain.CapabilityChat, domain.CapabilityVision},
			EstWeightsMB:   100,
			KVPerTokenMB:   0.01,
			ArtifactSizeMB: 100,
		}},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	targeted := map[string]bool{}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer rpc-secret" {
			t.Fatalf("auth = %s", got)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/snapshot":
			nodeID := "owner-a"
			if strings.Contains(r.URL.Host, "owner-b") {
				nodeID = "owner-b"
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: domain.Node{
				ID:           nodeID,
				Address:      r.URL.Host,
				Status:       domain.NodeReady,
				DiskTotalMB:  10000,
				DiskFreeMB:   9000,
				MaxUtil:      0.85,
				Labels:       map[string]string{domain.LabelPeerBackend: string(domain.BackendVLLM)},
				Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}},
			}})
		case r.Method == http.MethodPost && r.URL.Path == "/workloads":
			var req workload.AdmissionRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode admission: %v", err)
			}
			nodeID := req.Job.NodeSelector[domain.LabelNodeID]
			if nodeID != "owner-a" && nodeID != "owner-b" {
				t.Fatalf("node selector = %+v", req.Job.NodeSelector)
			}
			if !strings.Contains(string(req.Payload), `"kind":"model_prewarm"`) || !strings.Contains(string(req.Payload), nodeID) {
				t.Fatalf("payload = %s", req.Payload)
			}
			targeted[nodeID] = true
			decision := domain.PlacementDecision{JobID: req.Job.ID, Action: domain.ActionLoadedNew, NodeID: nodeID, Preset: domain.Preset{ID: "vision-vllm"}, InstanceID: "inst-" + nodeID}
			inst := domain.ModelInstance{ID: "inst-" + nodeID, NodeID: nodeID, PresetID: "vision-vllm", State: domain.InstReady}
			lease := domain.Lease{ID: "lease-" + nodeID, JobID: req.Job.ID, NodeID: nodeID, InstanceID: inst.ID}
			req.Job.Status = domain.JobRunning
			req.Job.Result = &domain.JobResult{Decision: decision, Instance: inst, LeaseID: lease.ID, NodeID: nodeID, PresetID: "vision-vllm", Action: decision.Action}
			_ = json.NewEncoder(w).Encode(workload.AdmissionResponse{Job: req.Job, Decision: decision, Instance: inst, Lease: lease})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/finish"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/workloads/"), "/finish")
			job := domain.Job{ID: id, Model: "vision", Status: domain.JobDone}
			result := &domain.JobResult{Action: domain.ActionLoadedNew, Terminal: true}
			job.Result = result
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{Job: job, Result: result})
		default:
			t.Fatalf("unexpected request %s %s host=%s", r.Method, r.URL.Path, r.URL.Host)
		}
	}))

	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{
			"models", "scale",
			"--config", configPath,
			"--url", "http://gateway.test",
			"--model", "vision",
			"--all-fitting",
		}, client)
	})
	if runErr != nil {
		t.Fatalf("models scale: %v", runErr)
	}
	if !targeted["owner-a"] || !targeted["owner-b"] {
		t.Fatalf("targeted = %+v", targeted)
	}
	if !strings.Contains(output, "desired=2") || !strings.Contains(output, "fit\tnode=owner-a\tstatus=fits") || !strings.Contains(output, "fit\tnode=owner-b\tstatus=fits") {
		t.Fatalf("output = %q", output)
	}
}

func TestRunModelsPrewarmValidation(t *testing.T) {
	if err := RunWithClient(context.Background(), []string{"models", "prewarm", "--model", "m"}, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "--url") {
		t.Fatalf("missing url err = %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"models", "scale", "--url", "http://peer.test"}, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "--model or --preset") {
		t.Fatalf("missing model err = %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"models", "scale", "--url", "http://peer.test", "--model", "m", "--replicas", "0"}, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "--replicas") {
		t.Fatalf("bad replicas err = %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"models", "scale", "--url", "http://peer.test", "--model", "m", "--priority", "urgent"}, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "unknown priority") {
		t.Fatalf("bad priority err = %v", err)
	}
}

func TestPrewarmFitCandidatesClassifiesOwnerReadiness(t *testing.T) {
	preset := domain.Preset{
		ID:             "vision-vllm",
		ModelRef:       "model/vision",
		Backend:        domain.BackendVLLM,
		ContextLength:  4096,
		ArtifactSizeMB: 100,
		EstWeightsMB:   100,
		KVPerTokenMB:   0.01,
	}
	ready := domain.Node{
		ID:           "ready",
		Address:      "http://ready.test",
		Status:       domain.NodeReady,
		MaxUtil:      0.9,
		DiskTotalMB:  10000,
		DiskFreeMB:   9000,
		Labels:       map[string]string{domain.LabelPeerBackends: "vllm,llamacpp"},
		Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}},
	}
	down := ready
	down.ID = "down"
	down.Status = domain.NodeUnreachable
	wrongBackend := ready
	wrongBackend.ID = "wrong-backend"
	wrongBackend.Labels = map[string]string{domain.LabelPeerBackend: string(domain.BackendLlamaCpp)}
	lowDisk := ready
	lowDisk.ID = "low-disk"
	lowDisk.DiskFreeMB = 2600
	loading := ready
	loading.ID = "loading"
	loading.OOMSeverity = domain.OOMCatastrophic
	fleet := domain.FleetSnapshot{
		Nodes: []domain.Node{wrongBackend, ready, down, lowDisk, loading},
		Instances: []domain.ModelInstance{{
			ID:             "inst-loading",
			NodeID:         "loading",
			PresetID:       "other",
			AcceleratorSet: []int{0},
			State:          domain.InstLoading,
			Loading:        true,
		}},
	}
	got := prewarmFitCandidates(context.Background(), preset, 4096, 1, fleet)
	byNode := map[string]modelPrewarmFit{}
	for _, fit := range got {
		byNode[fit.NodeID] = fit
	}
	if byNode["ready"].Status != "fits" || len(byNode["ready"].AcceleratorSet) != 1 || byNode["ready"].Claim.WeightsMB == 0 {
		t.Fatalf("ready fit = %+v", byNode["ready"])
	}
	for node, reason := range map[string]string{
		"down":          string(domain.NodeUnreachable),
		"wrong-backend": "label." + domain.LabelPeerBackend,
		"low-disk":      "disk.free_after_model",
		"loading":       "load_in_flight",
	} {
		if byNode[node].Status != "not_fit" || byNode[node].Reason != reason {
			t.Fatalf("%s fit = %+v want reason %s", node, byNode[node], reason)
		}
	}
}

func TestPrewarmFitCandidatesHonorsPinnedPresetNode(t *testing.T) {
	preset := domain.Preset{
		ID:             "vision-vllm",
		Backend:        domain.BackendVLLM,
		NodeID:         "owner-a",
		ArtifactSizeMB: 100,
		EstWeightsMB:   100,
		KVPerTokenMB:   0.01,
	}
	node := func(id string) domain.Node {
		return domain.Node{
			ID:           id,
			Status:       domain.NodeReady,
			MaxUtil:      0.9,
			DiskTotalMB:  10000,
			DiskFreeMB:   9000,
			Labels:       map[string]string{domain.LabelPeerBackends: "vllm"},
			Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}},
		}
	}
	got := prewarmFitCandidates(context.Background(), preset, 2048, 1, domain.FleetSnapshot{Nodes: []domain.Node{node("owner-b"), node("owner-a")}})
	byNode := map[string]modelPrewarmFit{}
	for _, fit := range got {
		byNode[fit.NodeID] = fit
	}
	if byNode["owner-a"].Status != "fits" {
		t.Fatalf("owner-a fit = %+v", byNode["owner-a"])
	}
	if byNode["owner-b"].Reason != "preset.node_id" {
		t.Fatalf("owner-b fit = %+v", byNode["owner-b"])
	}
}

func TestPrewarmAcceleratorSetsEnumeratesDeterministically(t *testing.T) {
	got := prewarmAcceleratorSets(domain.Node{Accelerators: []domain.Accelerator{{Index: 2}, {Index: 0}, {Index: 1}}})
	want := [][]int{{0}, {1}, {2}, {0, 1}, {0, 2}, {1, 2}, {0, 1, 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("accelerator sets = %+v, want %+v", got, want)
	}
	if got := prewarmAcceleratorSets(domain.Node{}); len(got) != 0 {
		t.Fatalf("empty accelerator sets = %+v", got)
	}
}

func TestPresetDiskMismatchForPrewarmUsesExistingModelEvidence(t *testing.T) {
	preset := domain.Preset{ID: "vision-vllm", ArtifactSizeMB: 600}
	node := domain.Node{ID: "owner-a", DiskTotalMB: 1000, DiskFreeMB: 650}
	if reason, mismatch := presetDiskMismatchForPrewarm(preset, node, domain.FleetSnapshot{}); !mismatch || reason != "disk.free_after_model" {
		t.Fatalf("new model mismatch = %q %t", reason, mismatch)
	}
	fleet := domain.FleetSnapshot{Instances: []domain.ModelInstance{{NodeID: node.ID, PresetID: preset.ID}}}
	if reason, mismatch := presetDiskMismatchForPrewarm(preset, node, fleet); mismatch {
		t.Fatalf("existing model mismatch = %q", reason)
	}
	pinned := preset
	pinned.NodeID = node.ID
	if reason, mismatch := presetDiskMismatchForPrewarm(pinned, node, domain.FleetSnapshot{}); mismatch {
		t.Fatalf("pinned model mismatch = %q", reason)
	}
}

func TestFetchPrewarmSnapshotClassifiesBadPeerResponses(t *testing.T) {
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Host {
		case "bad-status.test":
			http.Error(w, "offline", http.StatusServiceUnavailable)
		case "bad-json.test":
			_, _ = w.Write([]byte("{"))
		case "empty-node.test":
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{})
		case "ready.test":
			if got := r.Header.Get("Authorization"); got != "Bearer rpc-secret" {
				t.Fatalf("auth = %q", got)
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: domain.Node{ID: "owner-a", Status: domain.NodeReady}})
		default:
			t.Fatalf("unexpected host %s", r.URL.Host)
		}
	}))
	for _, tc := range []struct {
		url  string
		want string
	}{
		{"http://bad-status.test", "returned 503"},
		{"http://bad-json.test", "unexpected end"},
		{"http://empty-node.test", "empty node id"},
	} {
		if _, err := fetchPrewarmSnapshot(context.Background(), tc.url, "rpc-secret", client); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("fetch %s err = %v, want %q", tc.url, err, tc.want)
		}
	}
	snap, err := fetchPrewarmSnapshot(context.Background(), "http://ready.test", "rpc-secret", client)
	if err != nil || snap.Node.ID != "owner-a" {
		t.Fatalf("ready snapshot = %+v err=%v", snap, err)
	}
}

func TestPrewarmResultFromAdmissionUsesSchedulerEvidence(t *testing.T) {
	out := workload.AdmissionResponse{
		Job: domain.Job{ID: "job-a", Model: "model-a", PresetID: "preset-a", Status: domain.JobRunning},
		Result: &domain.JobResult{
			Action:   domain.ActionWarmInstance,
			NodeID:   "owner-a",
			PresetID: "preset-a",
			Instance: domain.ModelInstance{ID: "inst-a"},
		},
		Decision: domain.PlacementDecision{Action: domain.ActionLoadedNew, NodeID: "decision-owner", Preset: domain.Preset{ID: "decision-preset"}, InstanceID: "decision-inst"},
		Instance: domain.ModelInstance{ID: "response-inst"},
	}
	got := prewarmResultFromAdmission(out)
	if got.Action != domain.ActionWarmInstance || got.NodeID != "decision-owner" || got.PresetID != "preset-a" || got.Instance != "inst-a" {
		t.Fatalf("result from job evidence = %+v", got)
	}
	out.Result = nil
	got = prewarmResultFromAdmission(out)
	if got.Action != domain.ActionLoadedNew || got.NodeID != "decision-owner" || got.PresetID != "preset-a" || got.Instance != "response-inst" {
		t.Fatalf("result from response evidence = %+v", got)
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
		if req.MaxTokens != 24 {
			t.Fatalf("max tokens = %d", req.MaxTokens)
		}
		if !req.Stream || req.StreamOptions == nil || !req.StreamOptions.IncludeUsage {
			t.Fatalf("stream options = stream:%v opts:%+v", req.Stream, req.StreamOptions)
		}
		writeBenchmarkOpenAIStream(w, []string{"answer ", "from " + req.Model}, api.OpenAIUsage{
			PromptTokens:     3,
			CompletionTokens: 2,
			TotalTokens:      5,
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
			"--max-tokens", "24",
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
	if !strings.Contains(string(metrics), `"tokens_per_sec"`) || !strings.Contains(string(metrics), `"ttft_ms"`) {
		t.Fatalf("stream metrics missing = %s", metrics)
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
		writeBenchmarkOpenAIStream(w, []string{"env ok"}, api.OpenAIUsage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3})
	}))
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"benchmark", "run",
			"--db", filepath.Join(t.TempDir(), "control.db"),
			"--url", "http://gateway.test",
			"--gateway-token-env", "MYCELIUM_TEST_GATEWAY_TOKEN",
			"--prompt", "Say hi",
			"--out", filepath.Join(t.TempDir(), "bench"),
			"--model", "tiny",
		}, client)
	})
	if err != nil {
		t.Fatalf("benchmark env token: %v", err)
	}
	if !strings.Contains(output, "benchmark-auth\tMYCELIUM_TEST_GATEWAY_TOKEN\tconfigured=true") || strings.Contains(output, "env-secret") {
		t.Fatalf("benchmark auth output = %q", output)
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

func TestRunBenchmarkIgnoresAmbientOpenAIKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "")
	t.Setenv("MYCELIUM_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "openai-secret")
	client := directHTTPClient(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("benchmark should not call gateway with ambient OPENAI_API_KEY")
	}))
	err := RunWithClient(context.Background(), []string{
		"benchmark", "run",
		"--db", filepath.Join(t.TempDir(), "control.db"),
		"--url", "http://gateway.test",
		"--prompt", "Say hi",
		"--out", filepath.Join(t.TempDir(), "bench"),
		"--model", "tiny",
	}, client)
	if err == nil || !strings.Contains(err.Error(), "gateway_token or token_env") {
		t.Fatalf("ambient OPENAI_API_KEY benchmark err = %v", err)
	}
}

func TestRunGatewayPreflightUsesMyceliumEnvAndPrintsTelemetry(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "")
	t.Setenv("MYCELIUM_API_KEY", "project-secret")
	t.Setenv("OPENAI_API_KEY", "openai-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer project-secret" {
			t.Fatalf("gateway auth = %q", got)
		}
		if got := r.Header.Get("X-Myc-Project"); got != "strict-json-canary" {
			t.Fatalf("project = %q", got)
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		responseFormat, ok := req["response_format"].(map[string]any)
		if !ok || responseFormat["type"] != "json_schema" {
			t.Fatalf("response_format = %#v", req["response_format"])
		}
		w.Header().Set("X-Myc-Job", "job-a")
		w.Header().Set("X-Myc-Node", "b70")
		w.Header().Set("X-Myc-Backend", "vllm")
		w.Header().Set("X-Myc-Decision", "placed_on_warm_instance")
		w.Header().Set("X-Myc-Structured-Output", "json_schema")
		w.Header().Set("X-Myc-Guided-Decoding-Requested", "true")
		w.Header().Set("X-Myc-Tools-Requested", "false")
		w.Header().Set("X-Myc-Tool-Choice", "none")
		w.Header().Set("X-Myc-VLLM-Tool-Parser-Configured", "false")
		w.Header().Set("X-Myc-VLLM-Auto-Tool-Choice-Configured", "false")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]string{"content": `{"ok":true}`, "reasoning_content": "checked"},
				"finish_reason": "stop",
			}},
			"usage": map[string]int{"prompt_tokens": 4, "completion_tokens": 3, "total_tokens": 7},
		})
	}))

	var err error
	outDir := filepath.Join(t.TempDir(), "preflight-evidence")
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "strict-json-model",
			"--project", "strict-json-canary",
			"--consumer-run", "strict-json-run-001",
			"--consumer-item", "row-004",
			"--consumer-status", "ok",
			"--consumer-elapsed-ms", "1200",
			"--consumer-result", "pass",
			"--consumer-quality-gate", "strict-json-validation",
			"--out", outDir,
		}, client)
	})
	if err != nil {
		t.Fatalf("gateway preflight: %v", err)
	}
	for _, want := range []string{
		"gateway\thttp://gateway.test\tok",
		"auth\tMYCELIUM_API_KEY\tconfigured",
		"header\tX-Myc-Node\tb70",
		"header\tX-Myc-Structured-Output\tjson_schema",
		"header\tX-Myc-Guided-Decoding-Requested\ttrue",
		"header\tX-Myc-Tools-Requested\tfalse",
		"header\tX-Myc-Tool-Choice\tnone",
		"finish_reason\tstop",
		"content_length\t11",
		"reasoning_length\t7",
		"tool_calls\tcount=0\tnames=-",
		"textual_tool_call\tfalse",
		"json\ttrue",
		"usage\tprompt=4\tcompletion=3\ttotal=7",
		"consumer\trun=strict-json-run-001\titem=row-004\tstatus=ok\telapsed_ms=1200\terror=-\tresult=pass\tquality_gate=strict-json-validation",
		"evidence\t" + filepath.Join(outDir, "preflight.json"),
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("preflight output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "project-secret") || strings.Contains(output, "openai-secret") {
		t.Fatalf("preflight output leaked token: %s", output)
	}
	evidenceData, err := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if err != nil {
		t.Fatalf("read preflight evidence: %v", err)
	}
	if strings.Contains(string(evidenceData), "project-secret") || strings.Contains(string(evidenceData), "openai-secret") || strings.Contains(string(evidenceData), "Return") || strings.Contains(string(evidenceData), `{"ok":true}`) {
		t.Fatalf("preflight evidence leaked secret/prompt/content: %s", evidenceData)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(evidenceData, &evidence); err != nil {
		t.Fatalf("decode preflight evidence: %v", err)
	}
	if evidence.AuthSource != "MYCELIUM_API_KEY" || evidence.Headers["X-Myc-Node"] != "b70" || evidence.ContentLength != 11 || evidence.ReasoningLength != 7 || evidence.ToolCallCount != 0 || evidence.TextualToolCall || !evidence.JSONContent || evidence.Usage.TotalTokens != 7 {
		t.Fatalf("preflight evidence = %+v", evidence)
	}
	if evidence.Request.MaxTokens != 16 || evidence.Request.Streaming || evidence.Request.StructuredOutput != "json_schema" || evidence.Request.ToolRequested || evidence.Request.ToolName != "" || evidence.Request.ToolChoice != "" {
		t.Fatalf("preflight request evidence = %+v", evidence.Request)
	}
	if evidence.Expectations != nil {
		t.Fatalf("unexpected preflight expectations = %+v", evidence.Expectations)
	}
	if evidence.Consumer == nil || evidence.Consumer.RunID != "strict-json-run-001" || evidence.Consumer.ItemID != "row-004" || evidence.Consumer.Status != "ok" || evidence.Consumer.ElapsedMS != 1200 || evidence.Consumer.Result != "pass" || evidence.Consumer.QualityGate != "strict-json-validation" {
		t.Fatalf("preflight consumer evidence = %+v", evidence.Consumer)
	}
}

func TestRunGatewayPreflightImageFileSendsOpenAIImageContentAndRedactsEvidence(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "gateway-secret")
	imagePath := filepath.Join(t.TempDir(), "canary.png")
	pngData := []byte{
		0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
		0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
		0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
		0xde, 0x00, 0x00, 0x00, 0x0c, 0x49, 0x44, 0x41,
		0x54, 0x08, 0xd7, 0x63, 0xf8, 0xff, 0xff, 0x3f,
		0x00, 0x05, 0xfe, 0x02, 0xfe, 0xdc, 0xcc, 0x59,
		0xe7, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e,
		0x44, 0xae, 0x42, 0x60, 0x82,
	}
	if err := os.WriteFile(imagePath, pngData, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if _, ok := raw["response_format"]; ok {
			t.Fatalf("response_format should be omitted for structured-output none")
		}
		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content []struct {
					Type     string `json:"type"`
					Text     string `json:"text"`
					ImageURL struct {
						URL string `json:"url"`
					} `json:"image_url"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(raw["messages"], &req.Messages); err != nil {
			t.Fatalf("decode messages: %v", err)
		}
		if len(req.Messages) != 1 || req.Messages[0].Role != "user" || len(req.Messages[0].Content) != 2 {
			t.Fatalf("messages = %+v", req.Messages)
		}
		if req.Messages[0].Content[0].Type != "text" || req.Messages[0].Content[0].Text != "Read this image." {
			t.Fatalf("text part = %+v", req.Messages[0].Content[0])
		}
		if got := req.Messages[0].Content[1].Type; got != "image_url" {
			t.Fatalf("image type = %s", got)
		}
		if url := req.Messages[0].Content[1].ImageURL.URL; !strings.HasPrefix(url, "data:image/png;base64,") {
			t.Fatalf("image url = %.40q", url)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]string{"content": `{"ok":true}`},
				"finish_reason": "stop",
			}},
		})
	}))
	outDir := filepath.Join(t.TempDir(), "preflight-evidence")
	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "vision-model",
			"--prompt", "Read this image.",
			"--image-file", imagePath,
			"--structured-output", "none",
			"--out", outDir,
		}, client)
	})
	if runErr != nil {
		t.Fatalf("image preflight: %v", runErr)
	}
	if !strings.Contains(output, "gateway\thttp://gateway.test\tok") || strings.Contains(output, imagePath) || strings.Contains(output, "gateway-secret") {
		t.Fatalf("image preflight output = %s", output)
	}
	evidenceData, err := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if err != nil {
		t.Fatalf("read evidence: %v", err)
	}
	evidenceText := string(evidenceData)
	if strings.Contains(evidenceText, imagePath) || strings.Contains(evidenceText, "data:image") || strings.Contains(evidenceText, "Read this image") || strings.Contains(evidenceText, "gateway-secret") {
		t.Fatalf("image preflight evidence leaked private request data: %s", evidenceText)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(evidenceData, &evidence); err != nil {
		t.Fatalf("decode evidence: %v", err)
	}
	if !evidence.Request.ImageRequested || evidence.Request.ImageMediaType != "image/png" || evidence.Request.StructuredOutput != "none" {
		t.Fatalf("image request evidence = %+v", evidence.Request)
	}
}

func TestGatewayPreflightMessagesAndStructuredOutputValidation(t *testing.T) {
	messages, err := gatewayPreflightMessages(gatewayPreflightConfig{Prompt: "hello"})
	if err != nil {
		t.Fatalf("text messages: %v", err)
	}
	if len(messages) != 1 || messages[0]["content"] != "hello" {
		t.Fatalf("text messages = %+v", messages)
	}
	if mode, err := validatePreflightStructuredOutput(""); err != nil || mode != "json_schema" {
		t.Fatalf("default structured output = %q, %v", mode, err)
	}
	if mode, err := validatePreflightStructuredOutput("none"); err != nil || mode != "none" {
		t.Fatalf("none structured output = %q, %v", mode, err)
	}
	if _, err := validatePreflightStructuredOutput("xml"); err == nil || !strings.Contains(err.Error(), "json_schema or none") {
		t.Fatalf("invalid structured output err = %v", err)
	}
	if media := imageMediaTypeForPath("scan.jpeg"); media != "image/jpeg" {
		t.Fatalf("jpeg media = %s", media)
	}
	if media := imageMediaTypeForBytes("scan.bin", []byte("<html></html>")); media != "text/html; charset=utf-8" {
		t.Fatalf("detected media = %s", media)
	}
	if _, err := gatewayPreflightMessages(gatewayPreflightConfig{ImageFile: filepath.Join(t.TempDir(), "missing.png")}); err == nil || !strings.Contains(err.Error(), "read preflight image file") {
		t.Fatalf("missing image err = %v", err)
	}
}

func TestRunGatewayPreflightIgnoresAmbientOpenAIKeyUnlessExplicit(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "")
	t.Setenv("MYCELIUM_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "openai-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer openai-secret" {
			t.Fatalf("gateway auth = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]string{"content": `{"ok":true}`},
				"finish_reason": "stop",
			}},
		})
	}))
	if err := RunWithClient(context.Background(), []string{
		"gateway", "preflight",
		"--url", "http://gateway.test",
		"--model", "tiny",
	}, client); err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") || !strings.Contains(err.Error(), "--gateway-token-env OPENAI_API_KEY") {
		t.Fatalf("ambient OPENAI_API_KEY err = %v", err)
	}
	output := captureStdout(t, func() {
		err := RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "tiny",
			"--gateway-token-env", "OPENAI_API_KEY",
		}, client)
		if err != nil {
			t.Fatalf("explicit OPENAI_API_KEY preflight: %v", err)
		}
	})
	if !strings.Contains(output, "auth\tOPENAI_API_KEY\tconfigured") || strings.Contains(output, "openai-secret") {
		t.Fatalf("explicit token source output = %q", output)
	}
}

func TestRunGatewayPreflightRequiresLiveModelCatalog(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "project-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			if got := r.Header.Get("Authorization"); got != "Bearer project-secret" {
				t.Fatalf("models auth = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]string{{"id": "strict-json-model"}},
			})
		case "/v1/chat/completions":
			w.Header().Set("X-Myc-Job", "job-live")
			w.Header().Set("X-Myc-Node", "node-json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"message":       map[string]string{"content": `{"ok":true}`},
					"finish_reason": "stop",
				}},
				"usage": map[string]int{"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
			})
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	outDir := filepath.Join(t.TempDir(), "live-model-preflight")
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "strict-json-model",
			"--require-live-model",
			"--out", outDir,
		}, client)
	})
	if err != nil {
		t.Fatalf("live model preflight: %v\n%s", err, output)
	}
	if !strings.Contains(output, "live_model\tstatus=ok\tavailable=true\tmatch=strict-json-model\tmodels=1\terror=-") {
		t.Fatalf("live model output = %s", output)
	}
	data, readErr := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if readErr != nil {
		t.Fatalf("read live model evidence: %v", readErr)
	}
	if strings.Contains(string(data), "project-secret") {
		t.Fatalf("live model evidence leaked token: %s", data)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode live model evidence: %v", err)
	}
	if evidence.LiveModel == nil || evidence.LiveModel.Status != "ok" || !evidence.LiveModel.ModelAvailable || evidence.LiveModel.ModelMatch != "strict-json-model" || evidence.Expectations == nil || !evidence.Expectations.RequireLiveModel {
		t.Fatalf("live model evidence = %+v expectations=%+v", evidence.LiveModel, evidence.Expectations)
	}
}

func TestRunGatewayPreflightRequireLiveModelFailsBeforeChatAndWritesEvidence(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "project-secret")
	chatCalled := false
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]string{{"id": "other-model"}},
			})
		case "/v1/chat/completions":
			chatCalled = true
			t.Fatalf("chat request should not run when live model catalog is missing")
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	outDir := filepath.Join(t.TempDir(), "missing-live-model")
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "strict-json-model",
			"--require-live-model",
			"--out", outDir,
		}, client)
	})
	if err == nil || !strings.Contains(err.Error(), "live model check failed") || !strings.Contains(err.Error(), "status=missing") || !strings.Contains(err.Error(), "preflight.json") {
		t.Fatalf("missing live model err = %v\noutput=%s", err, output)
	}
	if chatCalled {
		t.Fatal("chat endpoint was called")
	}
	if !strings.Contains(output, "live_model\tstatus=missing\tavailable=false\tmatch=-\tmodels=1\terror=-") {
		t.Fatalf("missing live model output = %s", output)
	}
	data, readErr := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if readErr != nil {
		t.Fatalf("read missing live model evidence: %v", readErr)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode missing live model evidence: %v", err)
	}
	if evidence.StatusCode != 0 || evidence.LiveModel == nil || evidence.LiveModel.Status != "missing" || evidence.LiveModel.ModelAvailable || evidence.LiveModel.ModelCount != 1 || evidence.LiveModel.ModelMatch != "" {
		t.Fatalf("missing live model evidence = %+v", evidence)
	}
}

func TestRunGatewayRoutesPrintsSecretSafeConsumerContracts(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "peer.json")
	cfg := operatorPeerConfig{
		ID:           "tool-node",
		Listen:       "http://gateway.test",
		GatewayToken: "global-secret",
		GatewayProjectTokens: []operatorProjectToken{
			{Project: "tool-project", Token: "project-secret"},
		},
		Projects: []domain.Project{
			{ID: "strict-json-project", DefaultModel: "strict-json-model", ContextCap: 4096},
			{ID: "tool-project", DefaultModel: "tool-model-8192", ContextCap: 8192},
			{ID: "missing-route", DefaultModel: "does-not-exist", ContextCap: 2048},
		},
		Presets: []domain.Preset{
			{
				ID:            "strict-json-vllm",
				ModelRef:      "strict-json-model",
				Aliases:       []string{"strict-json-model"},
				Backend:       domain.BackendVLLM,
				ContextLength: 4096,
				Capabilities:  []domain.Capability{domain.CapabilityChat},
				NodeID:        "node-json",
			},
			{
				ID:            "strict-json-vllm-alt",
				ModelRef:      "strict-json-model",
				Aliases:       []string{"strict-json-model"},
				Backend:       domain.BackendVLLM,
				ContextLength: 8192,
				Capabilities:  []domain.Capability{domain.CapabilityChat},
				NodeID:        "node-json-alt",
			},
			{
				ID:              "tool-model-8192-vllm",
				ModelRef:        "tool-model",
				Aliases:         []string{"tool-model-8192"},
				Backend:         domain.BackendVLLM,
				ContextLength:   8192,
				Capabilities:    []domain.Capability{domain.CapabilityChat},
				NodeID:          "node-tool",
				ProviderProfile: "openai-compatible",
				LaunchArgs:      []string{"--enable-auto-tool-choice", "--tool-call-parser", "hermes"},
			},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal peer config: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatalf("write peer config: %v", err)
	}

	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{"gateway", "routes", "--config", configPath}, nil)
	})
	if runErr != nil {
		t.Fatalf("gateway routes: %v", runErr)
	}
	for _, want := range []string{
		"gateway\thttp://gateway.test\tbase_url=http://gateway.test/v1\tpeer=tool-node",
		"route\tproject=strict-json-project\tmodel=strict-json-model\tpreset=strict-json-vllm\tnode=node-json\tbackend=vllm\tcontext=4096\tproject_cap=4096\teffective_context=4096\tmin_context=0\tcontext_issue=-\treadiness=config_ready\ttool_readiness=not_tool_configured\tstatus=ready_from_config\tauth=global_gateway_token\tproject_token=false\tglobal_token=true\tstructured=true\tstreaming=true\ttool_parser=false\tauto_tool=false",
		"route\tproject=tool-project\tmodel=tool-model-8192\tpreset=tool-model-8192-vllm\tnode=node-tool\tbackend=vllm\tcontext=8192\tproject_cap=8192\teffective_context=8192\tmin_context=0\tcontext_issue=-\treadiness=config_ready\ttool_readiness=auto_tool_configured\tstatus=ready_from_config\tauth=project_token\tproject_token=true\tglobal_token=true\tstructured=true\tstreaming=true\ttool_parser=true\tauto_tool=true\ttool_choice=forced-function-first; auto requires model/template proof",
		"route\tproject=missing-route\tmodel=does-not-exist\tpreset=-\tnode=-\tbackend=-\tcontext=0\tproject_cap=2048\teffective_context=2048\tmin_context=0\tcontext_issue=-\treadiness=preset_missing\ttool_readiness=not_tool_configured\tstatus=preset_missing\tauth=global_gateway_token",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("gateway routes output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "global-secret") || strings.Contains(output, "project-secret") {
		t.Fatalf("gateway routes leaked token material: %s", output)
	}

	var jsonErr error
	jsonOutput := captureStdout(t, func() {
		jsonErr = RunWithClient(context.Background(), []string{"gateway", "routes", "--config", configPath, "--min-context-cap", "32768", "--json"}, nil)
	})
	if jsonErr != nil {
		t.Fatalf("gateway routes json: %v", jsonErr)
	}
	if strings.Contains(jsonOutput, "global-secret") || strings.Contains(jsonOutput, "project-secret") {
		t.Fatalf("gateway routes JSON leaked token material: %s", jsonOutput)
	}
	var report gatewayRoutesReport
	if err := json.Unmarshal([]byte(jsonOutput), &report); err != nil {
		t.Fatalf("decode gateway routes JSON: %v", err)
	}
	if report.BaseURL != "http://gateway.test/v1" || len(report.Routes) != 3 {
		t.Fatalf("gateway routes report = %+v", report)
	}
	if got := report.Routes[1]; got.Project != "strict-json-project" || got.ModelGroupSize != 2 || strings.Join(got.ModelGroupPresets, ",") != "strict-json-vllm,strict-json-vllm-alt" {
		t.Fatalf("strict JSON route group = %+v", got)
	}
	if got := report.Routes[1]; len(got.ModelGroupEnvelopes) != 2 || got.ModelGroupEnvelopes[0].PresetID != "strict-json-vllm" || got.ModelGroupEnvelopes[0].NodeID != "node-json" || got.ModelGroupEnvelopes[0].ContextLength != 4096 || got.ModelGroupEnvelopes[1].PresetID != "strict-json-vllm-alt" || got.ModelGroupEnvelopes[1].NodeID != "node-json-alt" || got.ModelGroupEnvelopes[1].ContextLength != 8192 {
		t.Fatalf("strict JSON route envelopes = %+v", got.ModelGroupEnvelopes)
	}
	if !strings.Contains(output, "group_envelopes=strict-json-vllm@node-json:vllm:ctx4096,strict-json-vllm-alt@node-json-alt:vllm:ctx8192") {
		t.Fatalf("gateway routes output missing group envelopes:\n%s", output)
	}
	if got := report.Routes[2]; got.Project != "tool-project" || !got.ProjectTokenConfigured || !got.ToolParserConfigured || !got.AutoToolChoiceConfigured || got.ToolReadiness != "auto_tool_configured" || got.RouteReadiness != "undersized_context" || got.MinContextCap != 32768 || got.EffectiveContextCap != 8192 || got.ContextIssue != "undersized_context" || got.ContextAdequate {
		t.Fatalf("tool route contract = %+v", got)
	}

	toolRequiredOutput := captureStdout(t, func() {
		jsonErr = RunWithClient(context.Background(), []string{"gateway", "routes", "--config", configPath, "--require-tools", "--json"}, nil)
	})
	if jsonErr != nil {
		t.Fatalf("gateway routes tool-required json: %v", jsonErr)
	}
	report = gatewayRoutesReport{}
	if err := json.Unmarshal([]byte(toolRequiredOutput), &report); err != nil {
		t.Fatalf("decode tool-required gateway routes JSON: %v", err)
	}
	if got := report.Routes[1]; got.Project != "strict-json-project" || got.RouteReadiness != "tool_flags_missing" {
		t.Fatalf("strict JSON route readiness = %+v", got)
	}
	if got := report.Routes[2]; got.Project != "tool-project" || got.RouteReadiness != "config_ready" {
		t.Fatalf("tool route readiness = %+v", got)
	}
}

func TestRunGatewayRoutesComparesLiveModels(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "peer.json")
	cfg := operatorPeerConfig{
		ID:           "macbook",
		Listen:       "http://gateway.test",
		GatewayToken: "global-secret",
		Projects: []domain.Project{
			{ID: "strict-json-project", DefaultModel: "strict-json-model"},
			{ID: "tool-project", DefaultModel: "tool-model-8192"},
		},
		Presets: []domain.Preset{
			{ID: "strict-json-vllm", ModelRef: "strict-json-model", Aliases: []string{"strict-json-model"}, Backend: domain.BackendVLLM, ContextLength: 4096, NodeID: "node-json"},
			{ID: "strict-json-vllm-alt", ModelRef: "strict-json-model", Aliases: []string{"strict-json-model"}, Backend: domain.BackendVLLM, ContextLength: 8192, NodeID: "node-json-alt"},
			{ID: "tool-model-8192-vllm", ModelRef: "tool-model", Aliases: []string{"tool-model-8192"}, Backend: domain.BackendVLLM, NodeID: "node-tool"},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal peer config: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatalf("write peer config: %v", err)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer global-secret" {
			t.Fatalf("auth = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data": []map[string]string{
				{"id": "strict-json-model", "object": "model", "owned_by": "mycelium"},
			},
		})
	}))

	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{"gateway", "routes", "--config", configPath, "--live"}, client)
	})
	if runErr != nil {
		t.Fatalf("gateway routes live: %v", runErr)
	}
	for _, want := range []string{
		"live\tgateway=http://gateway.test\tstatus=ok\tmodels=1",
		"route\tproject=strict-json-project\tmodel=strict-json-model",
		"readiness=live_ready",
		"live_model=true\tlive_match=strict-json-model",
		"group_envelopes=strict-json-vllm@node-json:vllm:ctx4096:live=strict-json-model,strict-json-vllm-alt@node-json-alt:vllm:ctx8192:live=strict-json-model",
		"route\tproject=tool-project\tmodel=tool-model-8192",
		"readiness=live_catalog_missing",
		"live_model=false\tlive_match=-",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("gateway live routes output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "global-secret") {
		t.Fatalf("gateway live routes leaked token material: %s", output)
	}

	var jsonErr error
	jsonOutput := captureStdout(t, func() {
		jsonErr = RunWithClient(context.Background(), []string{"gateway", "routes", "--config", configPath, "--live", "--json"}, client)
	})
	if jsonErr != nil {
		t.Fatalf("gateway routes live json: %v", jsonErr)
	}
	if strings.Contains(jsonOutput, "global-secret") {
		t.Fatalf("gateway live routes JSON leaked token material: %s", jsonOutput)
	}
	var report gatewayRoutesReport
	if err := json.Unmarshal([]byte(jsonOutput), &report); err != nil {
		t.Fatalf("decode live routes JSON: %v", err)
	}
	if !report.Live.Checked || report.Live.Status != "ok" || report.Live.ModelCount != 1 {
		t.Fatalf("live report = %+v", report.Live)
	}
	if !report.Routes[0].LiveModelAvailable || report.Routes[1].LiveModelAvailable {
		t.Fatalf("live route availability = %+v", report.Routes)
	}
	if got := report.Routes[0].ModelGroupEnvelopes; len(got) != 2 || !got[0].LiveModelAvailable || got[0].LiveModelMatch != "strict-json-model" || !got[1].LiveModelAvailable || got[1].LiveModelMatch != "strict-json-model" {
		t.Fatalf("live route envelopes = %+v", got)
	}
	if report.Routes[0].RouteReadiness != "live_ready" || report.Routes[1].RouteReadiness != "live_catalog_missing" {
		t.Fatalf("live route readiness = %+v", report.Routes)
	}
}

func TestGatewayRouteReadinessClassifiesConsumerRequirements(t *testing.T) {
	ready := gatewayRouteContract{DiscoveryStatus: "ready_from_config"}
	live := gatewayRoutesLive{Checked: true, Status: "ok"}

	cases := []struct {
		name  string
		route gatewayRouteContract
		live  gatewayRoutesLive
		opts  gatewayRouteReadinessOptions
		want  string
	}{
		{
			name:  "missing preset",
			route: gatewayRouteContract{DiscoveryStatus: "preset_missing"},
			want:  "preset_missing",
		},
		{
			name:  "live catalog missing",
			route: ready,
			live:  live,
			want:  "live_catalog_missing",
		},
		{
			name:  "undersized context",
			route: gatewayRouteContract{DiscoveryStatus: "ready_from_config", ContextIssue: "undersized_context"},
			opts:  gatewayRouteReadinessOptions{RequireTools: true},
			want:  "undersized_context",
		},
		{
			name:  "tool parser missing",
			route: ready,
			opts:  gatewayRouteReadinessOptions{RequireTools: true},
			want:  "tool_flags_missing",
		},
		{
			name:  "auto tool missing",
			route: gatewayRouteContract{DiscoveryStatus: "ready_from_config", ToolParserConfigured: true},
			opts:  gatewayRouteReadinessOptions{RequireAutoTools: true},
			want:  "auto_tool_flags_missing",
		},
		{
			name:  "live ready",
			route: gatewayRouteContract{DiscoveryStatus: "ready_from_config", LiveModelAvailable: true},
			live:  live,
			want:  "live_ready",
		},
		{
			name:  "config ready",
			route: ready,
			want:  "config_ready",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gatewayRouteReadiness(tc.route, tc.live, tc.opts); got != tc.want {
				t.Fatalf("gatewayRouteReadiness() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFetchGatewayLiveModelsDedupesAndClassifiesErrors(t *testing.T) {
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Host {
		case "ready.test":
			if got := r.Header.Get("Authorization"); got != "Bearer gateway-secret" {
				t.Fatalf("auth = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]string{
					{"id": "model-a"},
					{"id": ""},
					{"id": "model-a"},
					{"id": "model-b"},
				},
			})
		case "bad-status.test":
			http.Error(w, "catalog unavailable", http.StatusNotFound)
		case "bad-json.test":
			_, _ = w.Write([]byte("{"))
		default:
			t.Fatalf("unexpected host %s", r.URL.Host)
		}
	}))
	models, err := fetchGatewayLiveModels(context.Background(), "http://ready.test", "gateway-secret", client)
	if err != nil {
		t.Fatalf("fetch models: %v", err)
	}
	if !reflect.DeepEqual(models, []string{"model-a", "model-b"}) {
		t.Fatalf("models = %+v", models)
	}
	for _, tc := range []struct {
		url  string
		want string
	}{
		{"http://bad-status.test", "returned 404"},
		{"http://bad-json.test", "decode gateway models response"},
	} {
		if _, err := fetchGatewayLiveModels(context.Background(), tc.url, "", client); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("fetch %s err=%v want %q", tc.url, err, tc.want)
		}
	}
}

func TestFetchGatewayRoutesLiveCatalogReportsStatusBranches(t *testing.T) {
	dir := t.TempDir()
	configWithToken := filepath.Join(dir, "with-token.json")
	if err := os.WriteFile(configWithToken, []byte(`{"gateway_token":"gateway-secret"}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	configWithoutToken := filepath.Join(dir, "without-token.json")
	if err := os.WriteFile(configWithoutToken, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Host == "error.test" {
			http.Error(w, "models missing", http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer gateway-secret" {
			t.Fatalf("auth = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]string{{"id": "model-b"}, {"id": "model-a"}},
		})
	}))

	live, models := fetchGatewayRoutesLiveCatalog(context.Background(), configWithToken, "http://ready.test", gatewayRoutesLiveOptions{Enabled: true, Client: client})
	if live.Status != "ok" || live.ModelCount != 2 || live.AuthSource != configWithToken+" gateway_token" {
		t.Fatalf("live catalog = %+v", live)
	}
	if _, ok := models["model-a"]; !ok {
		t.Fatalf("models = %+v", models)
	}
	live, _ = fetchGatewayRoutesLiveCatalog(context.Background(), "", "", gatewayRoutesLiveOptions{Enabled: true, Client: client})
	if live.Status != "unavailable" || !strings.Contains(live.Error, "no gateway") {
		t.Fatalf("unavailable live catalog = %+v", live)
	}
	live, _ = fetchGatewayRoutesLiveCatalog(context.Background(), configWithoutToken, "http://ready.test", gatewayRoutesLiveOptions{Enabled: true, Client: client})
	if live.Status != "auth_error" || !strings.Contains(live.Error, "no gateway_token") {
		t.Fatalf("auth-error live catalog = %+v", live)
	}
	live, _ = fetchGatewayRoutesLiveCatalog(context.Background(), configWithToken, "http://error.test", gatewayRoutesLiveOptions{Enabled: true, Client: client})
	if live.Status != "error" || !strings.Contains(live.Error, "returned 404") {
		t.Fatalf("error live catalog = %+v", live)
	}
}

func TestRunGatewayPreflightToolCanaryRecordsResponseShape(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "tool-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tool-secret" {
			t.Fatalf("gateway auth = %q", got)
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if _, hasSchema := req["response_format"]; hasSchema {
			t.Fatalf("tool preflight should not request schema response_format: %#v", req["response_format"])
		}
		tools, ok := req["tools"].([]any)
		if !ok || len(tools) != 1 {
			t.Fatalf("tools = %#v", req["tools"])
		}
		tool, ok := tools[0].(map[string]any)
		if !ok || tool["strict"] != true {
			t.Fatalf("tool strict flag = %#v", tools[0])
		}
		choice, ok := req["tool_choice"].(map[string]any)
		if !ok || choice["type"] != "function" {
			t.Fatalf("tool_choice = %#v", req["tool_choice"])
		}
		w.Header().Set("X-Myc-Job", "job-tool")
		w.Header().Set("X-Myc-Node", "spark")
		w.Header().Set("X-Myc-Backend", "vllm")
		w.Header().Set("X-Myc-Decision", "placed_on_warm_instance")
		w.Header().Set("X-Myc-Tools-Requested", "true")
		w.Header().Set("X-Myc-Tool-Choice", "function:echo")
		w.Header().Set("X-Myc-VLLM-Tool-Parser-Configured", "true")
		w.Header().Set("X-Myc-VLLM-Auto-Tool-Choice-Configured", "true")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message": map[string]any{
					"tool_calls": []map[string]any{{
						"id":   "call_1",
						"type": "function",
						"function": map[string]string{
							"name":      "echo",
							"arguments": `{"value":"ok"}`,
						},
					}},
				},
				"finish_reason": "tool_calls",
			}},
			"usage": map[string]int{"prompt_tokens": 11, "completion_tokens": 5, "total_tokens": 16},
		})
	}))

	outDir := filepath.Join(t.TempDir(), "tool-preflight")
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "spark-qwen9b",
			"--project", "tool-canary",
			"--prompt", "Call echo with value ok.",
			"--tool-name", "echo",
			"--tool-choice", "function",
			"--expect-node", "spark",
			"--require-tool-call",
			"--reject-textual-tool-call",
			"--min-prompt-tokens", "10",
			"--out", outDir,
		}, client)
	})
	if err != nil {
		t.Fatalf("tool gateway preflight: %v\n%s", err, output)
	}
	for _, want := range []string{
		"header\tX-Myc-Node\tspark",
		"header\tX-Myc-Tool-Choice\tfunction:echo",
		"finish_reason\ttool_calls",
		"tool_calls\tcount=1\tnames=echo",
		"textual_tool_call\tfalse",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("tool preflight output missing %q:\n%s", want, output)
		}
	}
	data, err := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if err != nil {
		t.Fatalf("read tool preflight evidence: %v", err)
	}
	if strings.Contains(string(data), "tool-secret") || strings.Contains(string(data), "Call echo") || strings.Contains(string(data), `{"value":"ok"}`) {
		t.Fatalf("tool evidence leaked secret/prompt/content: %s", data)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode tool preflight evidence: %v", err)
	}
	if evidence.ToolCallCount != 1 || !reflect.DeepEqual(evidence.ToolCallNames, []string{"echo"}) || evidence.TextualToolCall {
		t.Fatalf("tool preflight evidence = %+v", evidence)
	}
	if evidence.Request.MaxTokens != 16 || evidence.Request.StructuredOutput != "none" || !evidence.Request.ToolRequested || evidence.Request.ToolName != "echo" || evidence.Request.ToolChoice != "function" {
		t.Fatalf("tool request evidence = %+v", evidence.Request)
	}
	if evidence.Expectations == nil || evidence.Expectations.ExpectNode != "spark" || !evidence.Expectations.RequireToolCall || !evidence.Expectations.RejectTextualToolCall || evidence.Expectations.MinPromptTokens != 10 {
		t.Fatalf("tool expectation evidence = %+v", evidence.Expectations)
	}
}

func TestRunGatewayPreflightStreamRecordsToolAndUsageEvidence(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "stream-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer stream-secret" {
			t.Fatalf("gateway auth = %q", got)
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req["stream"] != true {
			t.Fatalf("stream = %#v", req["stream"])
		}
		streamOptions, ok := req["stream_options"].(map[string]any)
		if !ok || streamOptions["include_usage"] != true {
			t.Fatalf("stream_options = %#v", req["stream_options"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Myc-Job", "job-stream-tool")
		w.Header().Set("X-Myc-Node", "spark")
		w.Header().Set("X-Myc-Backend", "vllm")
		w.Header().Set("X-Myc-Decision", "placed_on_warm_instance")
		w.Header().Set("X-Myc-Tools-Requested", "true")
		w.Header().Set("X-Myc-Tool-Choice", "function:echo")
		w.Header().Set("X-Myc-VLLM-Tool-Parser-Configured", "true")
		w.Header().Set("X-Myc-VLLM-Auto-Tool-Choice-Configured", "true")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"echo","arguments":""}}]},"finish_reason":null}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"value\":\"ok\"}"}}]},"finish_reason":"tool_calls"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":21,"completion_tokens":4,"total_tokens":25}}`,
			`data: [DONE]`,
			``,
		}, "\n\n")))
	}))

	outDir := filepath.Join(t.TempDir(), "stream-preflight")
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "tool-model-8192",
			"--project", "tool-project",
			"--tool-name", "echo",
			"--tool-choice", "function",
			"--stream",
			"--expect-node", "spark",
			"--require-tool-call",
			"--reject-textual-tool-call",
			"--min-prompt-tokens", "20",
			"--out", outDir,
		}, client)
	})
	if err != nil {
		t.Fatalf("stream gateway preflight: %v\n%s", err, output)
	}
	for _, want := range []string{
		"header\tX-Myc-Node\tspark",
		"finish_reason\ttool_calls",
		"tool_calls\tcount=1\tnames=echo",
		"stream\tdone=true\tusage_available=true",
		"usage\tprompt=21\tcompletion=4\ttotal=25",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("stream preflight output missing %q:\n%s", want, output)
		}
	}
	data, err := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if err != nil {
		t.Fatalf("read stream preflight evidence: %v", err)
	}
	if strings.Contains(string(data), "stream-secret") || strings.Contains(string(data), "Call") || strings.Contains(string(data), `{"value":"ok"}`) {
		t.Fatalf("stream evidence leaked secret/prompt/content: %s", data)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode stream evidence: %v", err)
	}
	if !evidence.Streaming || !evidence.StreamDone || !evidence.UsageAvailable || evidence.Usage.TotalTokens != 25 || evidence.ToolCallCount != 1 || !reflect.DeepEqual(evidence.ToolCallNames, []string{"echo"}) {
		t.Fatalf("stream evidence = %+v", evidence)
	}
	if !evidence.Request.Streaming || evidence.Request.StructuredOutput != "none" || !evidence.Request.ToolRequested || evidence.Request.ToolName != "echo" || evidence.Request.ToolChoice != "function" {
		t.Fatalf("stream request evidence = %+v", evidence.Request)
	}
	if evidence.Expectations == nil || evidence.Expectations.ExpectNode != "spark" || !evidence.Expectations.RequireToolCall || !evidence.Expectations.RejectTextualToolCall || evidence.Expectations.MinPromptTokens != 20 {
		t.Fatalf("stream expectation evidence = %+v", evidence.Expectations)
	}
}

func TestRunGatewayPreflightStreamMakesMissingUsageExplicit(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "stream-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Myc-Node", "spark")
		w.Header().Set("X-Myc-Tool-Choice", "function:echo")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"echo","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
			`data: [DONE]`,
			``,
		}, "\n\n")))
	}))
	outDir := filepath.Join(t.TempDir(), "stream-no-usage")
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "spark-qwen9b-tools-8192",
			"--tool-name", "echo",
			"--tool-choice", "function",
			"--stream",
			"--require-tool-call",
			"--out", outDir,
		}, client)
	})
	if err != nil {
		t.Fatalf("stream no-usage preflight: %v\n%s", err, output)
	}
	if !strings.Contains(output, "stream\tdone=true\tusage_available=false") || !strings.Contains(output, "usage\tunavailable") {
		t.Fatalf("missing usage not explicit:\n%s", output)
	}
	data, err := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if err != nil {
		t.Fatalf("read stream no-usage evidence: %v", err)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode stream no-usage evidence: %v", err)
	}
	if !evidence.Streaming || !evidence.StreamDone || evidence.UsageAvailable || evidence.Usage.TotalTokens != 0 {
		t.Fatalf("stream no-usage evidence = %+v", evidence)
	}
	if !evidence.Request.Streaming || !evidence.Request.ToolRequested || evidence.Request.ToolChoice != "function" {
		t.Fatalf("stream no-usage request evidence = %+v", evidence.Request)
	}
}

func TestRunGatewayPreflightStreamRequiresTerminalDoneAfterWritingEvidence(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "stream-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Myc-Node", "spark")
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}` + "\n\n"))
	}))
	outDir := filepath.Join(t.TempDir(), "stream-missing-done")
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "spark-qwen9b-tools-8192",
			"--stream",
			"--out", outDir,
		}, client)
	})
	if err == nil || !strings.Contains(err.Error(), "expected terminal streaming [DONE] event") || !strings.Contains(err.Error(), "preflight.json") {
		t.Fatalf("missing done err = %v\n%s", err, output)
	}
	if !strings.Contains(output, "stream\tdone=false\tusage_available=false") {
		t.Fatalf("missing done output = %s", output)
	}
	var evidence gatewayPreflightEvidence
	data, readErr := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if readErr != nil {
		t.Fatalf("read missing done evidence: %v", readErr)
	}
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode missing done evidence: %v", err)
	}
	if !evidence.Streaming || evidence.StreamDone {
		t.Fatalf("missing done evidence = %+v", evidence)
	}
	if !evidence.Request.Streaming || evidence.Request.StructuredOutput != "json_schema" {
		t.Fatalf("missing done request evidence = %+v", evidence.Request)
	}
}

func TestRunGatewayPreflightExpectationsFailAfterWritingEvidence(t *testing.T) {
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "tool-secret")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Myc-Job", "job-auto")
		w.Header().Set("X-Myc-Node", "spark")
		w.Header().Set("X-Myc-Backend", "vllm")
		w.Header().Set("X-Myc-Tools-Requested", "true")
		w.Header().Set("X-Myc-Tool-Choice", "auto")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]string{"content": "<tool_call><function=echo></function></tool_call>", "reasoning_content": "hidden"},
				"finish_reason": "stop",
			}},
			"usage": map[string]int{"prompt_tokens": 64, "completion_tokens": 8, "total_tokens": 72},
		})
	}))
	outDir := filepath.Join(t.TempDir(), "failed-preflight")
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "spark-qwen9b",
			"--context-cap", "8192",
			"--tool-name", "echo",
			"--tool-choice", "auto",
			"--require-tool-call",
			"--reject-textual-tool-call",
			"--min-prompt-tokens", "1000",
			"--min-context-cap", "32768",
			"--expect-finish-reason", "tool_calls",
			"--min-content-length", "100",
			"--max-reasoning-length", "0",
			"--out", outDir,
		}, client)
	})
	if err == nil || !strings.Contains(err.Error(), "expected finish_reason=tool_calls got stop") || !strings.Contains(err.Error(), "expected content length >= 100") || !strings.Contains(err.Error(), "expected reasoning length <= 0 got 6") || !strings.Contains(err.Error(), "expected at least one structured tool call") || !strings.Contains(err.Error(), "expected no textual <tool_call>") || !strings.Contains(err.Error(), "expected prompt_tokens >= 1000") || !strings.Contains(err.Error(), "expected context cap >= 32768 got 8192") || !strings.Contains(err.Error(), "preflight.json") {
		t.Fatalf("expectation err = %v\noutput=%s", err, output)
	}
	if !strings.Contains(output, "tool_calls\tcount=0") || !strings.Contains(output, "textual_tool_call\ttrue") {
		t.Fatalf("failed preflight output = %s", output)
	}
	data, readErr := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if readErr != nil {
		t.Fatalf("read failed evidence: %v", readErr)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode failed evidence: %v", err)
	}
	if evidence.ToolCallCount != 0 || !evidence.TextualToolCall || evidence.Usage.PromptTokens != 64 || evidence.ReasoningLength != 6 {
		t.Fatalf("failed evidence = %+v", evidence)
	}
	if evidence.Request.ContextCap != 8192 || evidence.Request.ToolChoice != "auto" || evidence.Request.ToolName != "echo" || evidence.Expectations == nil || !evidence.Expectations.RequireToolCall || !evidence.Expectations.RejectTextualToolCall || evidence.Expectations.MinPromptTokens != 1000 || evidence.Expectations.MinContextCap != 32768 || evidence.Expectations.ExpectFinishReason != "tool_calls" || evidence.Expectations.MinContentLength != 100 || evidence.Expectations.MaxReasoningLength == nil || *evidence.Expectations.MaxReasoningLength != 0 {
		t.Fatalf("failed request/expectation evidence = request:%+v expectations:%+v", evidence.Request, evidence.Expectations)
	}
	optimization := summarizeJobOptimizationEvidence(jobDebugReport{
		Job:       domain.Job{ID: "job-auto"},
		Preflight: &evidence,
		Diagnosis: &jobDebugDiagnosis{Class: "owner_completed_warm"},
	})
	if optimization.ContextCap != 8192 || optimization.MinContextCap != 32768 || optimization.ContentLength == 0 || optimization.ReasoningLength != 6 || optimization.FinishReason != "stop" || optimization.ContextAdequate || optimization.FailureLayer != "consumer_contract" || optimization.FailureClass != "consumer_contract_context_too_small" {
		t.Fatalf("context optimization evidence = %+v", optimization)
	}
}

func TestRunGatewayPreflightUsesTokenFileAndClassifiesMissingAuth(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "gateway.token")
	if err := os.WriteFile(tokenFile, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	sawFileToken := false
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawFileToken = r.Header.Get("Authorization") == "Bearer file-secret"
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]string{"content": `{"ok":true}`},
				"finish_reason": "stop",
			}},
		})
	}))
	if err := RunWithClient(context.Background(), []string{
		"gateway", "preflight",
		"--url", "http://gateway.test",
		"--model", "tiny",
		"--gateway-token-file", tokenFile,
	}, client); err != nil {
		t.Fatalf("token-file preflight: %v", err)
	}
	if !sawFileToken {
		t.Fatal("gateway preflight did not use token file")
	}

	configPath := filepath.Join(t.TempDir(), "peer.json")
	if err := os.WriteFile(configPath, []byte(`{"gateway_token":"config-secret"}`), 0o600); err != nil {
		t.Fatalf("write peer config: %v", err)
	}
	sawConfigToken := false
	configClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawConfigToken = r.Header.Get("Authorization") == "Bearer config-secret"
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]string{"content": `{"ok":true}`},
				"finish_reason": "stop",
			}},
		})
	}))
	if err := RunWithClient(context.Background(), []string{
		"gateway", "preflight",
		"--url", "http://gateway.test",
		"--model", "tiny",
		"--config", configPath,
	}, configClient); err != nil {
		t.Fatalf("config-token preflight: %v", err)
	}
	if !sawConfigToken {
		t.Fatal("gateway preflight did not use config token")
	}
	missingConfigTokenPath := filepath.Join(t.TempDir(), "peer-no-token.json")
	if err := os.WriteFile(missingConfigTokenPath, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write missing token config: %v", err)
	}
	if _, _, err := resolveGatewayPreflightToken("http://gateway.test", "", "", "", missingConfigTokenPath); err == nil || !strings.Contains(err.Error(), "has no gateway_token") {
		t.Fatalf("missing config token err = %v", err)
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCELIUM_GATEWAY_TOKEN", "")
	t.Setenv("MYCELIUM_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	if err := RunWithClient(context.Background(), []string{
		"gateway", "preflight",
		"--url", "http://gateway.test",
		"--model", "tiny",
	}, client); err == nil || !strings.Contains(err.Error(), "MYCELIUM_API_KEY") || !strings.Contains(err.Error(), "--gateway-token-env OPENAI_API_KEY") {
		t.Fatalf("missing auth err = %v", err)
	}
}

func TestRunGatewayPreflightFailureContracts(t *testing.T) {
	if err := RunWithClient(context.Background(), []string{"gateway"}, directHTTPClient(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))); err == nil || !strings.Contains(err.Error(), "usage: myce gateway") {
		t.Fatalf("gateway usage err = %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"gateway", "bogus"}, directHTTPClient(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))); err == nil || !strings.Contains(err.Error(), "usage: myce gateway") {
		t.Fatalf("gateway bogus err = %v", err)
	}
	emptyTokenFile := filepath.Join(t.TempDir(), "empty.token")
	if err := os.WriteFile(emptyTokenFile, []byte("\n"), 0o600); err != nil {
		t.Fatalf("write empty token file: %v", err)
	}
	if _, _, err := resolveGatewayPreflightToken("http://gateway.test", "", "", emptyTokenFile, ""); err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("empty token file err = %v", err)
	}

	unauthorizedClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "gateway token required", http.StatusUnauthorized)
	}))
	err := RunWithClient(context.Background(), []string{
		"gateway", "preflight",
		"--url", "http://gateway.test",
		"--model", "tiny",
		"--gateway-token", "secret",
	}, unauthorizedClient)
	if err == nil || !strings.Contains(err.Error(), "gateway token required") {
		t.Fatalf("unauthorized preflight err = %v", err)
	}

	malformedClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	err = RunWithClient(context.Background(), []string{
		"gateway", "preflight",
		"--url", "http://gateway.test",
		"--model", "tiny",
		"--gateway-token", "secret",
	}, malformedClient)
	if err == nil || !strings.Contains(err.Error(), "decode gateway preflight response") {
		t.Fatalf("malformed preflight err = %v", err)
	}

	noChoicesClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{}})
	}))
	err = RunWithClient(context.Background(), []string{
		"gateway", "preflight",
		"--url", "http://gateway.test",
		"--model", "tiny",
		"--gateway-token", "secret",
	}, noChoicesClient)
	if err == nil || !strings.Contains(err.Error(), "no choices") {
		t.Fatalf("no choices preflight err = %v", err)
	}
}

func TestRunGatewayPreflightWritesAvailabilityFailureEvidence(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "availability-preflight")
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`job "job-queued" queued: no instance available`))
	}))
	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{
			"gateway", "preflight",
			"--url", "http://gateway.test",
			"--model", "tool-model-large",
			"--project", "tool-canary",
			"--gateway-token", "secret",
			"--tool-name", "echo",
			"--tool-choice", "function",
			"--context-cap", "32768",
			"--min-context-cap", "32768",
			"--out", outDir,
		}, client)
	})
	if err == nil || !strings.Contains(err.Error(), "queued: no instance available") || !strings.Contains(err.Error(), "preflight.json") {
		t.Fatalf("availability preflight err = %v\noutput=%s", err, output)
	}
	if !strings.Contains(output, "evidence\t"+filepath.Join(outDir, "preflight.json")) {
		t.Fatalf("availability preflight output = %s", output)
	}
	data, readErr := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if readErr != nil {
		t.Fatalf("read availability preflight: %v", readErr)
	}
	if strings.Contains(string(data), "secret") {
		t.Fatalf("availability preflight leaked token: %s", data)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("decode availability preflight: %v", err)
	}
	if evidence.StatusCode != http.StatusBadGateway || !strings.Contains(evidence.Error, "queued: no instance available") || evidence.Request.ContextCap != 32768 || evidence.Expectations == nil || evidence.Expectations.MinContextCap != 32768 {
		t.Fatalf("availability preflight evidence = %+v", evidence)
	}

	configPath := filepath.Join(t.TempDir(), "peer.json")
	cfg := operatorPeerConfig{
		ID:        "gateway-peer",
		Listen:    "http://gateway.test",
		StorePath: filepath.Join(t.TempDir(), "control.db"),
		Projects: []domain.Project{
			{ID: "tool-canary", DefaultModel: "tool-model-large", ContextCap: 32768},
		},
		Presets: []domain.Preset{
			{
				ID:              "tool-model-large-vllm",
				ModelRef:        "tool-model",
				Aliases:         []string{"tool-model-large"},
				Backend:         domain.BackendVLLM,
				ContextLength:   32768,
				NodeID:          "node-tool",
				ProviderProfile: "openai-compatible",
			},
		},
	}
	configData, marshalErr := json.Marshal(cfg)
	if marshalErr != nil {
		t.Fatalf("marshal route config: %v", marshalErr)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatalf("write route config: %v", err)
	}
	store, openErr := storesqlite.Open(cfg.StorePath)
	if openErr != nil {
		t.Fatalf("open route store: %v", openErr)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-tool", Status: domain.NodeReady}); err != nil {
		t.Fatalf("save route node: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{ID: "job-queued", Model: "tool-model-large", PresetID: "tool-model-large-vllm", Status: domain.JobQueued}); err != nil {
		t.Fatalf("save route job: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close route store: %v", err)
	}

	debugOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "job", "--preflight", filepath.Join(outDir, "preflight.json"), "--config", configPath, "--consumer-status", "error", "--consumer-error", "no_instance"})
	})
	if err != nil {
		t.Fatalf("debug availability preflight: %v\n%s", err, debugOutput)
	}
	for _, want := range []string{
		"preflight\tstatus=502\tmodel=tool-model-large\tproject=tool-canary",
		"preflight-error\tgateway preflight returned 502: job \"job-queued\" queued: no instance available",
		"route-availability\tmodel=tool-model-large\tproject=tool-canary\tpreset=tool-model-large-vllm\tnode=node-tool\tbackend=vllm\tprofile=openai-compatible\tcontext=32768\tproject_cap=32768\teffective_context=32768\tstatus=no_matching_instance\tstartability=operator_action_required\tnode_known=true\tnode_status=ready\twarm_instances=0\tloading_instances=0\tunfinished_jobs=1",
		"diagnosis\troute_no_instance_available\tgateway preflight failed before owner telemetry was recoverable",
		"optimization\tjob=-\tproject=tool-canary\tconsumer_status=error\tconsumer_result=-\tquality_gate=-\tnode=-\tpreset=-\tmodel=tool-model-large\tattempts=0\tqueue_ms=0\tcontext_cap=32768\tmin_context=32768\tcontext_ok=true\tfinish=-\tcontent_len=0\treasoning_len=0\tjson=false\tstream=false\ttool=true\tusage=0/0/0\tlive_model=false/-\tload_ms=0\tttft_ms=0\tupstream_ms=0\tfailure_layer=availability\tfailure_class=route_no_instance_available",
	} {
		if !strings.Contains(debugOutput, want) {
			t.Fatalf("availability debug output missing %q:\n%s", want, debugOutput)
		}
	}

	summaryOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", outDir, "--failure-layer", "availability"})
	})
	if err != nil {
		t.Fatalf("debug summary availability: %v\n%s", err, summaryOutput)
	}
	if !strings.Contains(summaryOutput, "failure_layer=availability\tfailure_class=route_no_instance_available\thttp_status=502") || !strings.Contains(summaryOutput, "gateway_error=gateway preflight returned 502: job \"job-queued\" queued: no instance available") {
		t.Fatalf("availability summary output = %s", summaryOutput)
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
		{"doctor", "--bad"},
		{"status", "--bad"},
		{"debug", "job", "--bad"},
		{"debug", "bundle", "--bad"},
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

func TestRunOperatorDoctorStatusAndDebugJob(t *testing.T) {
	dbPath, configPath := writeOperatorDebugFixture(t)
	client := operatorSnapshotClient(t)

	var err error
	doctorOutput := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"doctor", "--config", configPath, "--db", dbPath, "--url", "http://peer.test", "--rpc-token", "rpc-secret"}, client)
	})
	if err != nil {
		t.Fatalf("doctor: %v\n%s", err, doctorOutput)
	}
	for _, want := range []string{
		"check\tconfig\tok",
		"check\tstore-health\tok\tjournal_mode=wal busy_timeout_ms=5000 max_open_connections=1",
		"check\tpeer-snapshot\tok\tnode-a instances=1",
		"check\tpresets\tok\t1 configured",
	} {
		if !strings.Contains(doctorOutput, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, doctorOutput)
		}
	}

	statusOutput := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"status", "--config", configPath, "--db", dbPath, "--url", "http://peer.test", "--rpc-token", "rpc-secret"}, client)
	})
	if err != nil {
		t.Fatalf("status: %v\n%s", err, statusOutput)
	}
	for _, want := range []string{
		"peer\tpeer-a\tlisten=http://peer.test\tcompute=true\tgateway_auth=true",
		"store-health\tjournal_mode=wal\tbusy_timeout_ms=5000\tforeign_keys=true\tmax_open_connections=1",
		"queue\tqueued=0 placing=0 loading=0 running=1 done=1 preempted=0 failed=0",
		"snapshot\tok\tnode-a\tinstances=1",
	} {
		if !strings.Contains(statusOutput, want) {
			t.Fatalf("status output missing %q:\n%s", want, statusOutput)
		}
	}

	debugOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "job", "job-a", "--db", dbPath})
	})
	if err != nil {
		t.Fatalf("debug job: %v\n%s", err, debugOutput)
	}
	for _, want := range []string{
		"job\tjob-a\trunning\tmodel=tiny\tproject=project-a\towner=node-a",
		"payload\tstate=registry_payload_replayable\treplayable=true\tredacted=false",
		"registry\trunning\tcoordinator=peer-a\tassigned=node-a",
		"timeline\t2026-06-09T12:00:02Z\tsample\terror",
		"hint\trequest exceeded the selected model context",
	} {
		if !strings.Contains(debugOutput, want) {
			t.Fatalf("debug output missing %q:\n%s", want, debugOutput)
		}
	}
	configDefaultOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "job", "job-a", "--config", configPath})
	})
	if err != nil {
		t.Fatalf("debug job config store default: %v\n%s", err, configDefaultOutput)
	}
	if !strings.Contains(configDefaultOutput, "job\tjob-a\trunning\tmodel=tiny\tproject=project-a\towner=node-a") {
		t.Fatalf("debug config store output = %s", configDefaultOutput)
	}

	jsonOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "job", "job-a", "--json", "--db", dbPath})
	})
	if err != nil {
		t.Fatalf("debug job json: %v\n%s", err, jsonOutput)
	}
	var report jobDebugReport
	if err := json.Unmarshal([]byte(jsonOutput), &report); err != nil {
		t.Fatalf("decode debug json: %v\n%s", err, jsonOutput)
	}
	if report.Job.ID != "job-a" || len(report.SessionTimeline) != 1 || len(report.Hints) != 1 || report.Payload.State != "registry_payload_replayable" || !report.Payload.Replayable {
		t.Fatalf("debug report = %+v", report)
	}

	preflightPath := writeGatewayPreflightEvidenceFixture(t, "job-a")
	ownerClient := operatorOwnerTelemetryClient(t)
	preflightOutput := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"debug", "job", "--preflight", preflightPath, "--owner-telemetry", "--rpc-token", "rpc-secret", "--consumer-run", "strict-json-batch-7", "--consumer-item", "row-006", "--consumer-status", "timeout", "--consumer-elapsed-ms", "180000", "--consumer-error", "client_deadline", "--consumer-result", "fail", "--consumer-quality-gate", "strict-json-validation", "--db", dbPath}, ownerClient)
	})
	if err != nil {
		t.Fatalf("debug job with preflight: %v\n%s", err, preflightOutput)
	}
	for _, want := range []string{
		"job\tjob-a\trunning\tmodel=tiny\tproject=project-a\towner=node-a",
		"payload\tstate=registry_payload_replayable\treplayable=true\tredacted=false",
		"preflight\tstatus=200\tmodel=strict-json-model\tproject=strict-json-batch",
		"preflight-request\tmax_tokens=32\tstream=false\tstructured=none\ttool_requested=true\ttool_name=echo\ttool_choice=auto",
		"preflight-expectations\texpect_node=owner-node\trequire_json=false\trequire_tool_call=true\treject_textual_tool_call=true\trequire_live_model=false\tmin_prompt_tokens=16\tmin_context=0\texpect_finish=-\tmin_content=0\tmax_reasoning=-",
		"preflight-route\tnode=owner-node\tbackend=vllm\tinstance=inst_2\tdecision=placed_on_warm_instance",
		"preflight-tools\trequested=true\tchoice=auto\tvllm_parser=true\tvllm_auto=false",
		"preflight-tool-result\tcount=0\tnames=-\ttextual=true",
		"preflight-usage\tprompt=16\tcompletion=10\ttotal=26",
		"owner-telemetry\tok\tnode=node-a\taddress=http://peer.test\tmetrics=1\tsamples=1\tttft_ms=34\tload_ms=0\ttps=12.50\tcontext=26\tphases=complete",
		"consumer\trun=strict-json-batch-7\titem=row-006\tstatus=timeout\telapsed_ms=180000\terror=client_deadline\tresult=fail\tquality_gate=strict-json-validation",
		"diagnosis\towner_completed_warm\towner node-a completed warm inference; load_ms=0; ttft_ms=34; consumer_status=timeout; upstream_ms=512; structured=json_schema",
		"optimization\tjob=job-a\tproject=strict-json-batch\tconsumer_status=timeout\tconsumer_result=fail\tquality_gate=strict-json-validation\tnode=owner-node\tpreset=tiny\tmodel=strict-json-model\tattempts=1\tqueue_ms=7\tcontext_cap=0\tmin_context=0\tcontext_ok=false\tfinish=stop\tcontent_len=16\treasoning_len=0\tjson=true\tstream=false\ttool=true\tusage=16/10/26\tlive_model=false/-\tload_ms=0\tttft_ms=34\tupstream_ms=512\tfailure_layer=quality\tfailure_class=consumer_quality_failed",
	} {
		if !strings.Contains(preflightOutput, want) {
			t.Fatalf("preflight debug output missing %q:\n%s", want, preflightOutput)
		}
	}

	preflightJSONOutput := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"debug", "job", "--preflight", preflightPath, "--owner-telemetry", "--rpc-token", "rpc-secret", "--json", "--db", dbPath}, ownerClient)
	})
	if err != nil {
		t.Fatalf("debug job preflight json: %v\n%s", err, preflightJSONOutput)
	}
	var preflightReport jobDebugReport
	if err := json.Unmarshal([]byte(preflightJSONOutput), &preflightReport); err != nil {
		t.Fatalf("decode preflight debug json: %v\n%s", err, preflightJSONOutput)
	}
	if preflightReport.Preflight == nil || preflightReport.Preflight.Headers["X-Myc-Node"] != "owner-node" || preflightReport.Preflight.ContentLength != 16 || !preflightReport.Preflight.TextualToolCall || preflightReport.OwnerTelemetry == nil || len(preflightReport.OwnerTelemetry.RunMetrics) != 1 || len(preflightReport.OwnerTelemetry.SessionTimeline) != 1 || preflightReport.Diagnosis == nil || preflightReport.Diagnosis.Class != "owner_completed_warm" {
		t.Fatalf("preflight debug report = %+v", preflightReport)
	}
	if preflightReport.Optimization == nil || preflightReport.Optimization.Project != "strict-json-batch" || preflightReport.Optimization.Model != "strict-json-model" || preflightReport.Optimization.NodeID != "owner-node" || preflightReport.Optimization.PresetID != "tiny" || preflightReport.Optimization.Request.ToolName != "echo" || preflightReport.Optimization.UsageTotalTokens != 26 || preflightReport.Optimization.TTFTMS != 34 || preflightReport.Optimization.UpstreamWallMS != 512 || preflightReport.Optimization.FailureLayer != "none" {
		t.Fatalf("preflight optimization report = %+v", preflightReport.Optimization)
	}
	if preflightReport.Consumer != nil {
		t.Fatalf("unexpected consumer evidence without flags = %+v", preflightReport.Consumer)
	}
	labeledPreflightPath := writeGatewayPreflightEvidenceFixtureWithConsumer(t, "job-a", consumerEvidence{
		RunID:     "tool-run-hold",
		ItemID:    "fixture-compact",
		Status:    "partial",
		ElapsedMS: 42000,
		Error:     "client_timeout",
		Result:    "unknown",
	})
	labeledJSONOutput := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"debug", "job", "--preflight", labeledPreflightPath, "--json", "--db", dbPath}, ownerClient)
	})
	if err != nil {
		t.Fatalf("debug job labeled preflight json: %v\n%s", err, labeledJSONOutput)
	}
	var labeledReport jobDebugReport
	if err := json.Unmarshal([]byte(labeledJSONOutput), &labeledReport); err != nil {
		t.Fatalf("decode labeled preflight debug json: %v\n%s", err, labeledJSONOutput)
	}
	if labeledReport.Consumer == nil || labeledReport.Consumer.RunID != "tool-run-hold" || labeledReport.Consumer.ItemID != "fixture-compact" || labeledReport.Consumer.Status != "partial" || labeledReport.Consumer.ElapsedMS != 42000 || labeledReport.Consumer.Error != "client_timeout" || labeledReport.Consumer.Result != "unknown" {
		t.Fatalf("labeled preflight consumer = %+v", labeledReport.Consumer)
	}
	if err := Run(context.Background(), []string{"debug", "job", "job-other", "--preflight", preflightPath, "--db", dbPath}); err == nil || !strings.Contains(err.Error(), `does not match requested job "job-other"`) {
		t.Fatalf("mismatched preflight error = %v", err)
	}
}

func TestRunDebugJobPrintsPreflightOnlyLiveCatalogFailure(t *testing.T) {
	preflightPath := writeGatewayPreflightOnlyLiveCatalogFailureFixture(t, "error", "gateway models returned 404: not found")
	var err error
	output := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "job", "--preflight", preflightPath, "--consumer-status", "error", "--consumer-error", "stale_catalog"})
	})
	if err != nil {
		t.Fatalf("debug preflight-only: %v\n%s", err, output)
	}
	for _, want := range []string{
		"job\t-\tpreflight_only\tmodel=tool-model-large\tproject=tool-canary\towner=-",
		"preflight\tstatus=0\tmodel=tool-model-large\tproject=tool-canary",
		"preflight-live-model\tstatus=error\tavailable=false\tmatch=-\tmodels=0\terror=gateway models returned 404: not found",
		"consumer\trun=-\titem=-\tstatus=error\telapsed_ms=0\terror=stale_catalog\tresult=-\tquality_gate=-",
		"diagnosis\tgateway_catalog_unavailable\tlive gateway catalog check failed for model tool-model-large: status=error error=gateway models returned 404: not found",
		"optimization\tjob=-\tproject=tool-canary\tconsumer_status=error\tconsumer_result=-\tquality_gate=-\tnode=-\tpreset=-\tmodel=tool-model-large\tattempts=0\tqueue_ms=0\tcontext_cap=32768\tmin_context=32768\tcontext_ok=true\tfinish=-\tcontent_len=0\treasoning_len=0\tjson=false\tstream=false\ttool=true\tusage=0/0/0\tlive_model=false/error\tload_ms=0\tttft_ms=0\tupstream_ms=0\tfailure_layer=catalog\tfailure_class=gateway_catalog_unavailable",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("preflight-only output missing %q:\n%s", want, output)
		}
	}

	jsonOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "job", "--preflight", preflightPath, "--json"})
	})
	if err != nil {
		t.Fatalf("debug preflight-only json: %v\n%s", err, jsonOutput)
	}
	var report jobDebugReport
	if err := json.Unmarshal([]byte(jsonOutput), &report); err != nil {
		t.Fatalf("decode preflight-only json: %v\n%s", err, jsonOutput)
	}
	if report.Optimization == nil || report.Optimization.FailureLayer != "catalog" || report.Optimization.FailureClass != "gateway_catalog_unavailable" || report.Optimization.LiveModelStatus != "error" || report.Optimization.LiveModelAvailable {
		t.Fatalf("preflight-only optimization = %+v", report.Optimization)
	}

	missingPath := writeGatewayPreflightOnlyLiveCatalogFailureFixture(t, "missing", "")
	missingEvidence, readErr := readGatewayPreflightEvidence(missingPath)
	if readErr != nil {
		t.Fatalf("read missing live catalog evidence: %v", readErr)
	}
	missingOptimization := summarizeJobOptimizationEvidence(jobDebugReport{Preflight: &missingEvidence})
	if missingOptimization.FailureLayer != "catalog" || missingOptimization.FailureClass != "live_catalog_model_missing" {
		t.Fatalf("missing live catalog optimization = %+v", missingOptimization)
	}

	err = Run(context.Background(), []string{"debug", "job", "--preflight", preflightPath, "--owner-telemetry"})
	if err == nil || !strings.Contains(err.Error(), "--owner-telemetry requires preflight evidence with X-Myc-Job") {
		t.Fatalf("preflight-only owner telemetry err = %v", err)
	}
}

func TestOperatorStatusAndDebugRejectBadUsage(t *testing.T) {
	if err := RunWithClient(context.Background(), []string{"status", "--watch", "--interval", "0"}, operatorSnapshotClient(t)); err == nil || !strings.Contains(err.Error(), "--interval must be positive") {
		t.Fatalf("status watch error = %v", err)
	}
	for _, args := range [][]string{
		{"debug"},
		{"debug", "unknown"},
		{"debug", "job"},
		{"debug", "job", "job-a", "job-b"},
		{"debug", "job", "job-a", "--db"},
		{"debug", "job", "job-a", "--consumer-elapsed-ms", "-1"},
		{"debug", "job", "job-a", "--consumer-status", "mystery"},
		{"debug", "bundle"},
		{"debug", "summary"},
	} {
		if err := Run(context.Background(), args); err == nil {
			t.Fatalf("Run(%v) expected error", args)
		}
	}
	if err := Run(context.Background(), []string{"debug", "summary", "--dir", t.TempDir()}); err == nil || !strings.Contains(err.Error(), "no preflight.json or optimization.json artifacts") {
		t.Fatalf("empty summary err = %v", err)
	}
}

func TestOperatorStatusReportsConfigAndSnapshotFailures(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{ID: "failed", Status: domain.JobFailed}); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	report, err := buildStatusReport(context.Background(), statusRequest{
		ConfigPath: filepath.Join(dir, "missing-peer.json"),
		DBPath:     dbPath,
		URL:        "http://peer.test",
		Client: directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("rpc token required"))
		})),
	})
	if err != nil {
		t.Fatalf("buildStatusReport: %v\n%s", err, report)
	}
	for _, want := range []string{
		"config\t" + filepath.Join(dir, "missing-peer.json") + "\tfail",
		"store\t" + dbPath + "\tok",
		"store-health\tjournal_mode=wal\tbusy_timeout_ms=5000\tforeign_keys=true\tmax_open_connections=1",
		"queue\tqueued=0 placing=0 loading=0 running=0 done=0 preempted=0 failed=1",
		"snapshot\tfail\tsnapshot http://peer.test: rpc token required",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("status report missing %q:\n%s", want, report)
		}
	}
}

func TestOperatorDebugReportIncludesParentChildrenAndMetricOwner(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	if err := store.SaveJob(context.Background(), domain.Job{ID: "parent", Status: domain.JobDone}); err != nil {
		t.Fatalf("SaveJob parent: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{
		ID:       "job-child",
		ParentID: "parent",
		Status:   domain.JobFailed,
		Error:    "health timeout while loading",
		Progress: []domain.JobProgress{{Stage: "loading", At: now}},
		Result: &domain.JobResult{
			ResultArtifact:  "artifact://job-child",
			ResultMediaType: "application/json",
			ResultBytes:     42,
			ResultSHA256:    strings.Repeat("a", 64),
			ResultRedacted:  true,
			Terminal:        true,
		},
	}); err != nil {
		t.Fatalf("SaveJob child: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{ID: "grandchild", ParentID: "job-child", TaskType: "retry", Status: domain.JobQueued}); err != nil {
		t.Fatalf("SaveJob grandchild: %v", err)
	}
	if err := store.Record(context.Background(), domain.RunMetric{JobID: "job-child", NodeID: "node-metric", InstanceID: "inst-a", PresetID: "tiny", TokensPerSec: 7, TTFTms: 9, ContextUsed: 11, At: now.Add(time.Second)}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := store.SaveRecommendation(context.Background(), domain.RecommendationRecord{ID: "rec-child", ProjectID: "", Type: optimizer.RecommendationContextCap, CreatedAt: now}); err != nil {
		t.Fatalf("SaveRecommendation: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	report, err := buildJobDebugReport(context.Background(), dbPath, "job-child")
	if err != nil {
		t.Fatalf("buildJobDebugReport: %v", err)
	}
	if report.Parent == nil || report.Parent.ID != "parent" || len(report.Children) != 1 || len(report.RunMetrics) != 1 || len(report.Recommendations) != 1 {
		t.Fatalf("debug report = %+v", report)
	}
	output := captureStdout(t, func() {
		printJobDebugReport(os.Stdout, report)
	})
	for _, want := range []string{
		"job\tjob-child\tfailed\tmodel=-\tproject=-\towner=node-metric",
		"result\tterminal=true\tredacted=true\tartifact=artifact://job-child\tmedia_type=application/json\tbytes=42\tsha256=" + strings.Repeat("a", 64),
		"parent\tparent\tdone",
		"child\tgrandchild\tretry\tqueued",
		"timeline\t2026-06-27T10:00:00Z\tprogress\tloading\t-",
		"hint\tbackend did not become healthy before the load timeout",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("debug output missing %q:\n%s", want, output)
		}
	}
	optimization := summarizeJobOptimizationEvidence(report)
	if !optimization.ResultArtifactPresent || optimization.ResultMediaType != "application/json" || optimization.ResultBytes != 42 || !optimization.ResultSHA256Present {
		t.Fatalf("optimization result manifest = %+v", optimization)
	}
	if _, err := buildJobDebugReport(context.Background(), dbPath, "missing"); err == nil || !strings.Contains(err.Error(), `job "missing" not found`) {
		t.Fatalf("missing job error = %v", err)
	}
}

func TestDebugBundleRecordsConfigAndStoreIssues(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "bundle")
	err := writeDebugBundle(context.Background(), debugBundleRequest{
		ConfigPath: filepath.Join(t.TempDir(), "missing-peer.json"),
		DBPath:     t.TempDir(),
		OutDir:     outDir,
	})
	if err != nil {
		t.Fatalf("writeDebugBundle: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest debugBundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode manifest: %v\n%s", err, data)
	}
	if len(manifest.Issues) < 2 || !containsString(manifest.Files, "status.txt") {
		t.Fatalf("manifest = %+v", manifest)
	}
	status, err := os.ReadFile(filepath.Join(outDir, "status.txt"))
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if !strings.Contains(string(status), "config\t") || !strings.Contains(string(status), "\tfail\t") {
		t.Fatalf("status artifact = %s", status)
	}
}

func TestOperatorListenAndFormattingHelpers(t *testing.T) {
	tests := map[string]string{
		"":                        "",
		"localhost:55291":         "localhost",
		"127.0.0.1:55291":         "127.0.0.1",
		"[::1]:55291":             "::1",
		"https://peer.example:99": "peer.example",
		"peer.example":            "peer.example",
		"bad:addr:shape":          "",
	}
	for input, want := range tests {
		if got := listenHost(input); got != want {
			t.Fatalf("listenHost(%q)=%q want %q", input, got, want)
		}
	}
	if !isLoopbackListen("") || !isLoopbackListen("localhost:55291") || !isLoopbackListen("http://127.0.0.1:55291") {
		t.Fatalf("loopback detection rejected loopback listeners")
	}
	if isLoopbackListen("http://198.51.100.143:55291") {
		t.Fatalf("loopback detection accepted LAN listener")
	}
	if got := formatTimeDash(time.Time{}); got != "-" {
		t.Fatalf("zero time = %q", got)
	}
}

func TestRunDebugBundleWritesRedactedArtifacts(t *testing.T) {
	dbPath, configPath := writeOperatorDebugFixture(t)
	client := operatorSnapshotClient(t)
	outDir := filepath.Join(t.TempDir(), "bundle")
	preflightPath := writeGatewayPreflightEvidenceFixture(t, "job-a")

	var err error
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"debug", "bundle", "--config", configPath, "--db", dbPath, "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--preflight", preflightPath, "--owner-telemetry", "--consumer-run", "tool-prod-smoke", "--consumer-item", "item-42", "--consumer-status", "error", "--consumer-elapsed-ms", "3210", "--consumer-error", "response_parse_error", "--consumer-result", "fail", "--consumer-quality-gate", "response-parser", "--out", outDir}, client)
	})
	if err != nil {
		t.Fatalf("debug bundle: %v\n%s", err, output)
	}
	if !strings.Contains(output, "debug-bundle\t"+outDir) {
		t.Fatalf("bundle output = %q", output)
	}
	for _, name := range []string{"manifest.json", "config.redacted.json", "preflight.json", "job_debug.json", "optimization.json", "jobs.json", "registry.json", "run_metrics.json", "session_metrics.json", "nodes.json", "instances.json", "recommendations.json", "peer_snapshot.json", "status.txt"} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Fatalf("missing bundle file %s: %v", name, err)
		}
	}
	redacted, err := os.ReadFile(filepath.Join(outDir, "config.redacted.json"))
	if err != nil {
		t.Fatalf("read redacted config: %v", err)
	}
	if strings.Contains(string(redacted), "rpc-secret") || strings.Contains(string(redacted), "gateway-secret") || !strings.Contains(string(redacted), "[REDACTED]") {
		t.Fatalf("redacted config = %s", redacted)
	}
	jobsData, err := os.ReadFile(filepath.Join(outDir, "jobs.json"))
	if err != nil {
		t.Fatalf("read jobs: %v", err)
	}
	if !strings.Contains(string(jobsData), "job-a") || strings.Contains(string(jobsData), "job-other") {
		t.Fatalf("jobs artifact = %s", jobsData)
	}
	preflightData, err := os.ReadFile(filepath.Join(outDir, "preflight.json"))
	if err != nil {
		t.Fatalf("read bundled preflight: %v", err)
	}
	if strings.Contains(string(preflightData), "Bearer") || strings.Contains(string(preflightData), "Return") || !strings.Contains(string(preflightData), `"X-Myc-Job": "job-a"`) {
		t.Fatalf("bundled preflight = %s", preflightData)
	}
	manifestData, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !strings.Contains(string(manifestData), `"job_id": "job-a"`) || !strings.Contains(string(manifestData), `"preflight.json"`) || !strings.Contains(string(manifestData), `"job_debug.json"`) {
		t.Fatalf("manifest = %s", manifestData)
	}
	jobDebugData, err := os.ReadFile(filepath.Join(outDir, "job_debug.json"))
	if err != nil {
		t.Fatalf("read job debug: %v", err)
	}
	if strings.Contains(string(jobDebugData), "rpc-secret") || !strings.Contains(string(jobDebugData), `"owner_telemetry"`) || !strings.Contains(string(jobDebugData), `"diagnosis"`) || !strings.Contains(string(jobDebugData), `"route_availability"`) || !strings.Contains(string(jobDebugData), `"route_status": "warm_instance_available"`) || !strings.Contains(string(jobDebugData), `"job_id": "job-a"`) || !strings.Contains(string(jobDebugData), `"consumer"`) || !strings.Contains(string(jobDebugData), `"run_id": "tool-prod-smoke"`) || !strings.Contains(string(jobDebugData), `"status": "error"`) {
		t.Fatalf("job debug artifact = %s", jobDebugData)
	}
	optimizationData, err := os.ReadFile(filepath.Join(outDir, "optimization.json"))
	if err != nil {
		t.Fatalf("read optimization artifact: %v", err)
	}
	if strings.Contains(string(optimizationData), "rpc-secret") || !strings.Contains(string(optimizationData), `"project": "strict-json-batch"`) || !strings.Contains(string(optimizationData), `"route_availability"`) || !strings.Contains(string(optimizationData), `"route_status": "warm_instance_available"`) || !strings.Contains(string(optimizationData), `"startability": "ready"`) || !strings.Contains(string(optimizationData), `"consumer_status": "error"`) || !strings.Contains(string(optimizationData), `"consumer_result": "fail"`) || !strings.Contains(string(optimizationData), `"consumer_quality_gate": "response-parser"`) || !strings.Contains(string(optimizationData), `"content_length": 16`) || !strings.Contains(string(optimizationData), `"reasoning_length": 0`) || !strings.Contains(string(optimizationData), `"json_content": true`) || !strings.Contains(string(optimizationData), `"tool_call_count": 0`) || !strings.Contains(string(optimizationData), `"textual_tool_call": true`) || !strings.Contains(string(optimizationData), `"failure_layer": "quality"`) || !strings.Contains(string(optimizationData), `"usage_total_tokens": 26`) {
		t.Fatalf("optimization artifact = %s", optimizationData)
	}

	inheritedOut := filepath.Join(t.TempDir(), "bundle-inherited")
	labeledPreflightPath := writeGatewayPreflightEvidenceFixtureWithConsumer(t, "job-a", consumerEvidence{
		RunID:  "strict-json-batch",
		ItemID: "row-004",
		Status: "ok",
	})
	output = captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"debug", "bundle", "--config", configPath, "--db", dbPath, "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--preflight", labeledPreflightPath, "--owner-telemetry", "--out", inheritedOut}, client)
	})
	if err != nil {
		t.Fatalf("debug inherited bundle: %v\n%s", err, output)
	}
	inheritedJobDebugData, err := os.ReadFile(filepath.Join(inheritedOut, "job_debug.json"))
	if err != nil {
		t.Fatalf("read inherited job debug: %v", err)
	}
	if !strings.Contains(string(inheritedJobDebugData), `"run_id": "strict-json-batch"`) || !strings.Contains(string(inheritedJobDebugData), `"item_id": "row-004"`) || !strings.Contains(string(inheritedJobDebugData), `"status": "ok"`) {
		t.Fatalf("inherited job debug artifact = %s", inheritedJobDebugData)
	}
}

func TestRunDebugSummaryJoinsBundleAndPreflightArtifacts(t *testing.T) {
	dbPath, configPath := writeOperatorDebugFixture(t)
	root := filepath.Join(t.TempDir(), "evidence")
	bundleDir := filepath.Join(root, "bundle-a")
	preflightOnlyDir := filepath.Join(root, "preflight-only")
	if err := os.MkdirAll(preflightOnlyDir, 0o700); err != nil {
		t.Fatalf("mkdir preflight-only: %v", err)
	}
	preflightPath := writeGatewayPreflightEvidenceFixture(t, "job-a")
	client := operatorSnapshotClient(t)
	var err error
	_ = captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"debug", "bundle", "--config", configPath, "--db", dbPath, "--url", "http://peer.test", "--rpc-token", "rpc-secret", "--preflight", preflightPath, "--owner-telemetry", "--consumer-run", "strict-json-run", "--consumer-item", "item-001", "--consumer-status", "ok", "--consumer-result", "pass", "--consumer-quality-gate", "schema", "--out", bundleDir}, client)
	})
	if err != nil {
		t.Fatalf("debug bundle for summary: %v", err)
	}
	preflightOnlyPath := writeGatewayPreflightOnlyLiveCatalogFailureFixture(t, "missing", "")
	if err := copyPreflightEvidenceFile(preflightOnlyPath, filepath.Join(preflightOnlyDir, "preflight.json")); err != nil {
		t.Fatalf("copy preflight-only evidence: %v", err)
	}

	output := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root})
	})
	if err != nil {
		t.Fatalf("debug summary: %v\n%s", err, output)
	}
	for _, want := range []string{
		"evidence\tkind=optimization\tjob=job-a\tproject=strict-json-batch\trun=strict-json-run\titem=item-001\tstatus=ok\tresult=pass\tquality_gate=schema\tmodel=strict-json-model\tnode=owner-node\tbackend=vllm\tpreset=tiny\tattempts=1\troute_status=warm_instance_available\tstartability=ready\twarm_instances=1\tloading_instances=0\tunfinished_jobs=0\tcontext=0/0",
		"tool=true\ttool_choice=auto\tfinish=stop\tcontent=16\treasoning=0\tjson=true\ttextual_tool=true\tusage_available=true\tusage=16/10/26",
		"owner_metrics=true\tlive_model=false/-\tfailure_layer=none\tfailure_class=owner_completed_warm",
		"evidence\tkind=preflight\tjob=-\tproject=tool-canary\trun=-\titem=-\tstatus=-\tresult=-\tquality_gate=-\tmodel=tool-model-large\tnode=-\tbackend=-\tpreset=-\tattempts=0\troute_status=-\tstartability=-\twarm_instances=0\tloading_instances=0\tunfinished_jobs=0\tcontext=32768/32768\tcontext_ok=true",
		"live_model=false/missing\tfailure_layer=catalog\tfailure_class=live_catalog_model_missing",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("summary output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, bundleDir) || strings.Contains(output, preflightOnlyDir) || strings.Contains(output, "rpc-secret") || strings.Contains(output, "gateway-secret") {
		t.Fatalf("summary output leaked path or secret: %s", output)
	}

	jsonOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--json"})
	})
	if err != nil {
		t.Fatalf("debug summary json: %v\n%s", err, jsonOutput)
	}
	var rows []debugSummaryRow
	if err := json.Unmarshal([]byte(jsonOutput), &rows); err != nil {
		t.Fatalf("decode summary json: %v\n%s", err, jsonOutput)
	}
	if len(rows) != 2 || rows[0].Kind != "optimization" || rows[0].ConsumerRun != "strict-json-run" || rows[0].ConsumerQualityGate != "schema" || rows[1].Kind != "preflight" || rows[1].FailureClass != "live_catalog_model_missing" {
		t.Fatalf("summary rows = %+v", rows)
	}
	if rows[0].RouteStatus != "warm_instance_available" || rows[0].Startability != "ready" || rows[0].WarmInstances != 1 {
		t.Fatalf("summary route availability row = %+v", rows[0])
	}
	if strings.Contains(jsonOutput, bundleDir) || strings.Contains(jsonOutput, preflightOnlyDir) || strings.Contains(jsonOutput, "rpc-secret") || strings.Contains(jsonOutput, "gateway-secret") {
		t.Fatalf("summary json leaked path or secret: %s", jsonOutput)
	}

	filteredOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--failure-layer", "catalog", "--live-model", "missing", "--context", "adequate", "--usage", "missing", "--cold", "unknown"})
	})
	if err != nil {
		t.Fatalf("filtered debug summary: %v\n%s", err, filteredOutput)
	}
	if strings.Count(filteredOutput, "evidence\t") != 1 || !strings.Contains(filteredOutput, "failure_class=live_catalog_model_missing") || strings.Contains(filteredOutput, "job=job-a") {
		t.Fatalf("filtered summary output = %s", filteredOutput)
	}
	routeFilteredOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--route-status", "warm_instance_available", "--startability", "ready"})
	})
	if err != nil {
		t.Fatalf("route filtered debug summary: %v\n%s", err, routeFilteredOutput)
	}
	if strings.Count(routeFilteredOutput, "evidence\t") != 1 || !strings.Contains(routeFilteredOutput, "route_status=warm_instance_available\tstartability=ready") || strings.Contains(routeFilteredOutput, "failure_class=live_catalog_model_missing") {
		t.Fatalf("route filtered summary output = %s", routeFilteredOutput)
	}
	qualityFilteredOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--quality-gate", "schema"})
	})
	if err != nil {
		t.Fatalf("quality filtered debug summary: %v\n%s", err, qualityFilteredOutput)
	}
	if strings.Count(qualityFilteredOutput, "evidence\t") != 1 || !strings.Contains(qualityFilteredOutput, "quality_gate=schema") || strings.Contains(qualityFilteredOutput, "failure_class=live_catalog_model_missing") {
		t.Fatalf("quality filtered summary output = %s", qualityFilteredOutput)
	}

	groupedOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--group-by", "failure-layer"})
	})
	if err != nil {
		t.Fatalf("grouped debug summary: %v\n%s", err, groupedOutput)
	}
	for _, want := range []string{
		"group\tby=failure-layer\tkey=catalog\tcount=1",
		"group\tby=failure-layer\tkey=none\tcount=1",
	} {
		if !strings.Contains(groupedOutput, want) {
			t.Fatalf("grouped summary missing %q:\n%s", want, groupedOutput)
		}
	}
	routeGroupedOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--group-by", "startability"})
	})
	if err != nil {
		t.Fatalf("route grouped debug summary: %v\n%s", err, routeGroupedOutput)
	}
	for _, want := range []string{
		"group\tby=startability\tkey=ready\tcount=1",
		"group\tby=startability\tkey=unknown\tcount=1",
	} {
		if !strings.Contains(routeGroupedOutput, want) {
			t.Fatalf("route grouped summary missing %q:\n%s", want, routeGroupedOutput)
		}
	}
	qualityGroupedOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--group-by", "quality-gate"})
	})
	if err != nil {
		t.Fatalf("quality grouped debug summary: %v\n%s", err, qualityGroupedOutput)
	}
	for _, want := range []string{
		"group\tby=quality-gate\tkey=schema\tcount=1",
		"group\tby=quality-gate\tkey=unknown\tcount=1",
	} {
		if !strings.Contains(qualityGroupedOutput, want) {
			t.Fatalf("quality grouped summary missing %q:\n%s", want, qualityGroupedOutput)
		}
	}
	routeKey := "model=strict-json-model,node=owner-node,backend=vllm,preset=tiny"
	routeKeyMissing := "model=tool-model-large,node=unknown,backend=unknown,preset=unknown"
	routeContractGroupedOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--group-by", "route"})
	})
	if err != nil {
		t.Fatalf("route contract grouped debug summary: %v\n%s", err, routeContractGroupedOutput)
	}
	for _, want := range []string{
		"group\tby=route\tkey=" + routeKey + "\tcount=1",
		"group\tby=route\tkey=" + routeKeyMissing + "\tcount=1",
	} {
		if !strings.Contains(routeContractGroupedOutput, want) {
			t.Fatalf("route contract grouped summary missing %q:\n%s", want, routeContractGroupedOutput)
		}
	}
	rollupOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--rollup-by", "failure-layer"})
	})
	if err != nil {
		t.Fatalf("rollup debug summary: %v\n%s", err, rollupOutput)
	}
	for _, want := range []string{
		"rollup\tby=failure-layer\tkey=catalog\tcount=1\tsuccess=0\tfailure=1\tunknown=0",
		"catalog_failures=1",
		"live_catalog_available=0\tlive_catalog_missing_or_error=1",
		"rollup\tby=failure-layer\tkey=none\tcount=1\tsuccess=1\tfailure=0\tunknown=0",
		"usage_available=1\tusage_missing=0\ttotal_tokens=26",
		"tool_requested=1\ttextual_tool_calls=1\tjson_content=1",
		"attempt_samples=1\ttotal_attempts=1\tmax_attempts=1\tqueue_samples=1\tavg_queue_ms=7\tmax_queue_ms=7",
		"latency_samples=1\tavg_latency_ms=553\tmax_latency_ms=553\twarm_instances=1",
	} {
		if !strings.Contains(rollupOutput, want) {
			t.Fatalf("rollup summary missing %q:\n%s", want, rollupOutput)
		}
	}
	rollupJSON := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--rollup-by", "route-status", "--json"})
	})
	if err != nil {
		t.Fatalf("rollup debug summary json: %v\n%s", err, rollupJSON)
	}
	var rollups []debugSummaryRollup
	if err := json.Unmarshal([]byte(rollupJSON), &rollups); err != nil {
		t.Fatalf("decode rollup summary json: %v\n%s", err, rollupJSON)
	}
	if len(rollups) != 2 || rollups[0].Key != "unknown" || rollups[0].CatalogFailures != 1 || rollups[1].Key != "warm_instance_available" || rollups[1].Successes != 1 || rollups[1].TotalTokens != 26 || rollups[1].AverageLatencyMS != 553 || rollups[1].AverageQueueWaitMS != 7 || rollups[1].MaxAttempts != 1 {
		t.Fatalf("rollups = %+v", rollups)
	}
	qualityRollupOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--rollup-by", "quality-gate"})
	})
	if err != nil {
		t.Fatalf("quality rollup debug summary: %v\n%s", err, qualityRollupOutput)
	}
	if !strings.Contains(qualityRollupOutput, "rollup\tby=quality-gate\tkey=schema\tcount=1\tsuccess=1") || !strings.Contains(qualityRollupOutput, "rollup\tby=quality-gate\tkey=unknown\tcount=1\tsuccess=0\tfailure=1") {
		t.Fatalf("quality rollup output = %s", qualityRollupOutput)
	}
	recommendOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--recommend", "--recommend-by", "model"})
	})
	if err != nil {
		t.Fatalf("recommend debug summary: %v\n%s", err, recommendOutput)
	}
	for _, want := range []string{
		"recommendation\tby=model\tkey=tool-model-large\trecommendation=catalog_stale\tseverity=blocker\tconfidence=high",
		"primary_layer=catalog\tprimary_class=live_catalog_model_missing",
		"recommendation\tby=model\tkey=strict-json-model\trecommendation=tool_call_route_unsuitable\tseverity=blocker\tconfidence=high",
		"quality_gate=schema",
		"textual_tool_calls=1",
	} {
		if !strings.Contains(recommendOutput, want) {
			t.Fatalf("recommend summary missing %q:\n%s", want, recommendOutput)
		}
	}
	recommendJSON := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--recommend", "--recommend-by", "model", "--json"})
	})
	if err != nil {
		t.Fatalf("recommend debug summary json: %v\n%s", err, recommendJSON)
	}
	var recommendations []debugSummaryRecommendation
	if err := json.Unmarshal([]byte(recommendJSON), &recommendations); err != nil {
		t.Fatalf("decode recommendations json: %v\n%s", err, recommendJSON)
	}
	if len(recommendations) != 2 || recommendations[0].Recommendation != "catalog_stale" || recommendations[1].Recommendation != "tool_call_route_unsuitable" || recommendations[1].QualityGate != "schema" {
		t.Fatalf("recommendations = %+v", recommendations)
	}
	defaultRecommendOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--recommend"})
	})
	if err != nil {
		t.Fatalf("default recommend debug summary: %v\n%s", err, defaultRecommendOutput)
	}
	if !strings.Contains(defaultRecommendOutput, "recommendation\tby=route\tkey="+routeKeyMissing+"\trecommendation=catalog_stale") || !strings.Contains(defaultRecommendOutput, "recommendation\tby=route\tkey="+routeKey+"\trecommendation=tool_call_route_unsuitable") {
		t.Fatalf("default recommend output = %s", defaultRecommendOutput)
	}
	trainingOutput := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--training-jsonl"})
	})
	if err != nil {
		t.Fatalf("training jsonl: %v\n%s", err, trainingOutput)
	}
	if strings.Contains(trainingOutput, bundleDir) || strings.Contains(trainingOutput, preflightOnlyDir) || strings.Contains(trainingOutput, "rpc-secret") || strings.Contains(trainingOutput, "gateway-secret") {
		t.Fatalf("training jsonl leaked path or secret: %s", trainingOutput)
	}
	lines := strings.Split(strings.TrimSpace(trainingOutput), "\n")
	if len(lines) != 2 {
		t.Fatalf("training jsonl line count = %d output=%s", len(lines), trainingOutput)
	}
	var trainingRows []debugSummaryTrainingRow
	for _, line := range lines {
		var trainingRow debugSummaryTrainingRow
		if err := json.Unmarshal([]byte(line), &trainingRow); err != nil {
			t.Fatalf("decode training row: %v\n%s", err, line)
		}
		trainingRows = append(trainingRows, trainingRow)
	}
	if trainingRows[0].SchemaVersion != "mycelium.debug_training.v1" || trainingRows[0].RouteKey != routeKey || trainingRows[0].Recommendation != "tool_call_route_unsuitable" || trainingRows[0].QualitySignalSource != "consumer_gate" || trainingRows[0].DeterministicQualityPass == nil || !*trainingRows[0].DeterministicQualityPass || trainingRows[0].SemanticQualityLabel != "" || trainingRows[0].TotalLatencyMS != 553 || trainingRows[0].PayloadState != "registry_payload_replayable" || !trainingRows[0].PayloadReplayable {
		t.Fatalf("training optimization row = %+v", trainingRows[0])
	}
	if trainingRows[1].RouteKey != routeKeyMissing || trainingRows[1].Recommendation != "catalog_stale" || trainingRows[1].QualitySignalSource != "not_collected" || trainingRows[1].DeterministicQualityPass != nil {
		t.Fatalf("training preflight row = %+v", trainingRows[1])
	}
	if err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--training-jsonl", "--json"}); err == nil || !strings.Contains(err.Error(), "--training-jsonl") {
		t.Fatalf("training/json conflict err = %v", err)
	}

	sortedJSON := captureStdout(t, func() {
		err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--sort", "context", "--json"})
	})
	if err != nil {
		t.Fatalf("sorted debug summary json: %v\n%s", err, sortedJSON)
	}
	var sortedRows []debugSummaryRow
	if err := json.Unmarshal([]byte(sortedJSON), &sortedRows); err != nil {
		t.Fatalf("decode sorted summary json: %v\n%s", err, sortedJSON)
	}
	if len(sortedRows) != 2 || sortedRows[0].Model != "tool-model-large" || sortedRows[0].ContextCap != 32768 {
		t.Fatalf("sorted rows = %+v", sortedRows)
	}

	if err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--failure-layer", "provider"}); err == nil || !strings.Contains(err.Error(), "no debug summary rows matched filters") {
		t.Fatalf("empty filtered summary err = %v", err)
	}
	if err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--context", "tiny"}); err == nil || !strings.Contains(err.Error(), "--context") {
		t.Fatalf("bad context filter err = %v", err)
	}
	if err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--sort", "mystery"}); err == nil || !strings.Contains(err.Error(), "--sort") {
		t.Fatalf("bad sort err = %v", err)
	}
	if err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--group-by", "mystery"}); err == nil || !strings.Contains(err.Error(), "--group-by") {
		t.Fatalf("bad group err = %v", err)
	}
	if err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--rollup-by", "mystery"}); err == nil || !strings.Contains(err.Error(), "--rollup-by") {
		t.Fatalf("bad rollup err = %v", err)
	}
	if err = Run(context.Background(), []string{"debug", "summary", "--dir", root, "--recommend", "--recommend-by", "mystery"}); err == nil || !strings.Contains(err.Error(), "--recommend-by") {
		t.Fatalf("bad recommend err = %v", err)
	}
}

func TestDebugSummaryFilterAndGroupingContracts(t *testing.T) {
	cold := true
	warm := false
	rows := []debugSummaryRow{
		{Model: "model-a", NodeID: "node-a", PresetID: "preset-a", ConsumerQualityGate: "schema", Attempts: 2, RouteStatus: "warm_instance_available", Startability: "ready", WarmInstances: 1, FailureLayer: "none", ContextCap: 65536, MinContextCap: 32768, ContextAdequate: true, LiveModelChecked: true, LiveModelAvailable: true, LiveModelStatus: "ok", UsageAvailable: true, UsageTotalTokens: 100, ColdLoad: &cold, QueueWaitMS: 25, LoadWallMS: 400, TTFTMS: 30, UpstreamWallMS: 200},
		{Model: "model-b", NodeID: "node-b", PresetID: "preset-b", ConsumerQualityGate: "parser", RouteStatus: "no_matching_instance", Startability: "operator_action_required", FailureLayer: "consumer_contract", ContextCap: 8192, MinContextCap: 32768, ContextAdequate: false, LiveModelChecked: true, LiveModelStatus: "missing", UsageAvailable: false, ColdLoad: &warm, UpstreamWallMS: 50},
		{Model: "model-c", FailureLayer: "catalog", LiveModelChecked: true, LiveModelStatus: "error"},
		{Model: "model-d"},
	}

	if got := filterDebugSummaryRows(append([]debugSummaryRow(nil), rows...), debugSummaryFilter{Context: "adequate", LiveModel: "available", Usage: "available", Cold: "cold"}); len(got) != 1 || got[0].Model != "model-a" {
		t.Fatalf("adequate/available/cold filter = %+v", got)
	}
	if got := filterDebugSummaryRows(append([]debugSummaryRow(nil), rows...), debugSummaryFilter{Context: "undersized", LiveModel: "missing", Usage: "missing", Cold: "warm"}); len(got) != 1 || got[0].Model != "model-b" {
		t.Fatalf("undersized/missing/warm filter = %+v", got)
	}
	if got := filterDebugSummaryRows(append([]debugSummaryRow(nil), rows...), debugSummaryFilter{Context: "unknown", LiveModel: "unchecked", Usage: "missing", Cold: "unknown"}); len(got) != 1 || got[0].Model != "model-d" {
		t.Fatalf("unknown filter = %+v", got)
	}
	if got := filterDebugSummaryRows(append([]debugSummaryRow(nil), rows...), debugSummaryFilter{LiveModel: "error"}); len(got) != 1 || got[0].Model != "model-c" {
		t.Fatalf("live error filter = %+v", got)
	}
	if got := filterDebugSummaryRows(append([]debugSummaryRow(nil), rows...), debugSummaryFilter{RouteStatus: "no_matching_instance", Startability: "operator_action_required"}); len(got) != 1 || got[0].Model != "model-b" {
		t.Fatalf("route availability filter = %+v", got)
	}
	if got := filterDebugSummaryRows(append([]debugSummaryRow(nil), rows...), debugSummaryFilter{QualityGate: "parser"}); len(got) != 1 || got[0].Model != "model-b" {
		t.Fatalf("quality gate filter = %+v", got)
	}

	latencyRows := append([]debugSummaryRow(nil), rows...)
	if err := sortDebugSummaryRows(latencyRows, "latency"); err != nil {
		t.Fatalf("sort latency: %v", err)
	}
	if latencyRows[0].Model != "model-a" || debugSummaryLatencyMS(latencyRows[0]) != 655 {
		t.Fatalf("latency sort rows = %+v", latencyRows)
	}
	usageRows := append([]debugSummaryRow(nil), rows...)
	if err := sortDebugSummaryRows(usageRows, "usage"); err != nil {
		t.Fatalf("sort usage: %v", err)
	}
	if usageRows[0].Model != "model-a" {
		t.Fatalf("usage sort rows = %+v", usageRows)
	}
	for _, sortBy := range []string{"input", "failure", "node", "model", "context"} {
		if err := sortDebugSummaryRows(append([]debugSummaryRow(nil), rows...), sortBy); err != nil {
			t.Fatalf("sort %s: %v", sortBy, err)
		}
	}

	for _, groupBy := range []string{"failure-layer", "failure-class", "quality-gate", "route", "node", "model", "preset", "route-status", "startability", "context", "live-model", "usage", "cold"} {
		groups, err := groupDebugSummaryRows(rows, groupBy)
		if err != nil {
			t.Fatalf("group %s: %v", groupBy, err)
		}
		if len(groups) == 0 || groups[0].By != groupBy {
			t.Fatalf("group %s rows = %+v", groupBy, groups)
		}
	}
	if _, err := groupDebugSummaryRows(rows, "bad"); err == nil || !strings.Contains(err.Error(), "--group-by") {
		t.Fatalf("bad group err = %v", err)
	}

	rollups, err := rollupDebugSummaryRows(rows, "model")
	if err != nil {
		t.Fatalf("rollup model: %v", err)
	}
	if len(rollups) != 4 || rollups[0].Key != "model-b" || rollups[0].Failures != 1 || rollups[0].ConsumerContractFailures != 1 || rollups[0].UsageMissing != 1 || rollups[0].ContextUndersized != 1 || rollups[0].WarmLoads != 1 {
		t.Fatalf("model rollups = %+v", rollups)
	}
	if rollups[1].Key != "model-c" || rollups[1].CatalogFailures != 1 || rollups[1].LiveCatalogMissingOrError != 1 {
		t.Fatalf("catalog rollup = %+v", rollups)
	}
	if rollups[2].Key != "model-a" || rollups[2].UnknownOutcomes != 1 || rollups[2].UsageAvailable != 1 || rollups[2].TotalTokens != 100 || rollups[2].ContextAdequate != 1 || rollups[2].ToolRequested != 0 || rollups[2].ColdLoads != 1 || rollups[2].AttemptSamples != 1 || rollups[2].TotalAttempts != 2 || rollups[2].AverageQueueWaitMS != 25 || rollups[2].MaxQueueWaitMS != 25 || rollups[2].AverageLatencyMS != 655 || rollups[2].MaxLatencyMS != 655 || rollups[2].WarmInstances != 1 {
		t.Fatalf("success-ish rollup = %+v", rollups)
	}
	if _, err := rollupDebugSummaryRows(rows, "bad"); err == nil || !strings.Contains(err.Error(), "--rollup-by") {
		t.Fatalf("bad rollup err = %v", err)
	}
}

func TestDebugSummaryRecommendationContracts(t *testing.T) {
	rows := []debugSummaryRow{
		{Model: "healthy", ConsumerResult: "pass", UsageAvailable: true, UsageTotalTokens: 40, ContextCap: 32768, MinContextCap: 8192, ContextAdequate: true, LiveModelChecked: true, LiveModelAvailable: true, UpstreamWallMS: 100, ResultArtifactPresent: true, ResultMediaType: "application/json", ResultBytes: 42, ResultSHA256Present: true},
		{Model: "catalog", FailureLayer: "catalog", FailureClass: "live_catalog_model_missing", LiveModelChecked: true, LiveModelStatus: "missing"},
		{Model: "saturated", FailureLayer: "availability", FailureClass: "route_no_instance_available", RouteStatus: "no_matching_instance", UnfinishedJobs: 2, LoadingInstances: 1},
		{Model: "no-owner", FailureLayer: "availability", FailureClass: "route_no_instance_available", RouteStatus: "no_matching_instance"},
		{Model: "small-context", FailureLayer: "consumer_contract", FailureClass: "consumer_contract_context_too_small", ContextCap: 8192, MinContextCap: 32768, ContextAdequate: false},
		{Model: "textual-tool", ToolRequested: true, TextualToolCall: true},
		{Model: "final-content", FinishReason: "length", ContentLength: 0, ReasoningLength: 1024},
		{Model: "quality", ConsumerResult: "fail", ConsumerQualityGate: "schema", FailureLayer: "quality", FailureClass: "consumer_quality_failed"},
		{Model: "provider", FailureLayer: "provider", FailureClass: "provider_timeout"},
		{Model: "unknown"},
	}
	recommendations, err := recommendDebugSummaryRows(rows, "model")
	if err != nil {
		t.Fatalf("recommend rows: %v", err)
	}
	byModel := map[string]debugSummaryRecommendation{}
	for _, recommendation := range recommendations {
		byModel[recommendation.Key] = recommendation
	}
	for model, want := range map[string]string{
		"healthy":       "route_healthy",
		"catalog":       "catalog_stale",
		"saturated":     "saturated_queue",
		"no-owner":      "no_instance_start_owner",
		"small-context": "context_too_small",
		"textual-tool":  "tool_call_route_unsuitable",
		"final-content": "final_content_route_unsuitable",
		"quality":       "quality_gate_failing",
		"provider":      "provider_failure",
		"unknown":       "insufficient_evidence",
	} {
		if got := byModel[model].Recommendation; got != want {
			t.Fatalf("recommendation for %s = %s, want %s; all=%+v", model, got, want, recommendations)
		}
	}
	if byModel["quality"].QualityGate != "schema" || byModel["final-content"].FinalContentIssues != 1 || byModel["healthy"].AverageLatencyMS != 100 || byModel["saturated"].UnfinishedJobs != 2 {
		t.Fatalf("recommendation evidence = %+v", byModel)
	}
	if _, err := recommendDebugSummaryRows(rows, "bad"); err == nil || !strings.Contains(err.Error(), "--recommend-by") {
		t.Fatalf("bad recommend err = %v", err)
	}

	trainingRows, err := buildDebugSummaryTrainingRows(rows)
	if err != nil {
		t.Fatalf("training rows: %v", err)
	}
	if len(trainingRows) != len(rows) {
		t.Fatalf("training row count = %d want %d", len(trainingRows), len(rows))
	}
	trainingByModel := map[string]debugSummaryTrainingRow{}
	for _, row := range trainingRows {
		trainingByModel[row.Model] = row
	}
	if trainingByModel["quality"].QualitySignalSource != "consumer_gate" || trainingByModel["quality"].DeterministicQualityPass == nil || *trainingByModel["quality"].DeterministicQualityPass || trainingByModel["quality"].Recommendation != "quality_gate_failing" {
		t.Fatalf("quality training row = %+v", trainingByModel["quality"])
	}
	if !trainingByModel["healthy"].ResultArtifactPresent || trainingByModel["healthy"].ResultMediaType != "application/json" || trainingByModel["healthy"].ResultBytes != 42 || !trainingByModel["healthy"].ResultSHA256Present {
		t.Fatalf("healthy training result manifest = %+v", trainingByModel["healthy"])
	}
	if trainingByModel["unknown"].QualitySignalSource != "not_collected" || trainingByModel["unknown"].DeterministicQualityPass != nil || trainingByModel["unknown"].Recommendation != "insufficient_evidence" {
		t.Fatalf("unknown training row = %+v", trainingByModel["unknown"])
	}
}

func TestRunDebugSelectRanksTrainingRows(t *testing.T) {
	pass := true
	rows := []debugSummaryTrainingRow{
		{
			SchemaVersion:            "mycelium.debug_training.v1",
			EvidenceKind:             "optimization",
			RouteKey:                 "model=large,node=node-a,backend=vllm,preset=preset-large",
			Model:                    "large",
			NodeID:                   "node-a",
			Backend:                  "vllm",
			PresetID:                 "preset-large",
			ContextCap:               65536,
			MinContextCap:            32768,
			ContextAdequate:          true,
			ToolRequested:            true,
			JSONContent:              true,
			UsageAvailable:           true,
			UsageTotalTokens:         2400,
			StatusCode:               200,
			FinishReason:             "stop",
			ContentLength:            42,
			TotalLatencyMS:           800,
			QualitySignalSource:      "consumer_gate",
			DeterministicQualityPass: &pass,
			Recommendation:           "route_healthy",
			RecommendationSeverity:   "ok",
			RecommendationNext:       "keep collecting evidence",
		},
		{
			SchemaVersion:   "mycelium.debug_training.v1",
			EvidenceKind:    "preflight",
			RouteKey:        "model=small,node=node-b,backend=vllm,preset=preset-small",
			Model:           "small",
			NodeID:          "node-b",
			Backend:         "vllm",
			PresetID:        "preset-small",
			ContextCap:      8192,
			MinContextCap:   32768,
			ContextAdequate: false,
			Recommendation:  "context_too_small",
		},
		{
			SchemaVersion:   "mycelium.debug_training.v1",
			EvidenceKind:    "optimization",
			RouteKey:        "model=tool-text,node=node-c,backend=vllm,preset=preset-tool",
			Model:           "tool-text",
			NodeID:          "node-c",
			Backend:         "vllm",
			PresetID:        "preset-tool",
			ContextCap:      65536,
			MinContextCap:   32768,
			ContextAdequate: true,
			ToolRequested:   true,
			TextualToolCall: true,
			StatusCode:      200,
			FinishReason:    "stop",
			ContentLength:   80,
			Recommendation:  "tool_call_route_unsuitable",
		},
	}
	path := filepath.Join(t.TempDir(), "training.jsonl")
	var data strings.Builder
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("marshal row: %v", err)
		}
		data.Write(encoded)
		data.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(data.String()), 0o600); err != nil {
		t.Fatalf("write training JSONL: %v", err)
	}

	output := captureStdout(t, func() {
		err := Run(context.Background(), []string{"debug", "select", "--training-jsonl", path, "--min-context-cap", "32768", "--require-tools", "--require-json", "--require-final-content", "--require-usage"})
		if err != nil {
			t.Fatalf("debug select: %v", err)
		}
	})
	for _, want := range []string{
		"route-selection\tdecision=selected\tkey=model=large,node=node-a,backend=vllm,preset=preset-large",
		"recommendation=route_healthy",
		"route-selection\tdecision=refused\tkey=model=small,node=node-b,backend=vllm,preset=preset-small",
		"reason=context cap is below the requested minimum",
		"route-selection\tdecision=refused\tkey=model=tool-text,node=node-c,backend=vllm,preset=preset-tool",
		"reason=saved evidence includes textual tool calls",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("debug select output missing %q:\n%s", want, output)
		}
	}

	jsonOutput := captureStdout(t, func() {
		err := Run(context.Background(), []string{"debug", "select", "--training-jsonl", path, "--json", "--min-context-cap", "32768", "--require-tools"})
		if err != nil {
			t.Fatalf("debug select json: %v", err)
		}
	})
	var selections []debugRouteSelection
	if err := json.Unmarshal([]byte(jsonOutput), &selections); err != nil {
		t.Fatalf("decode select JSON: %v\n%s", err, jsonOutput)
	}
	if len(selections) != 3 || selections[0].Decision != "selected" || selections[0].RouteKey != rows[0].RouteKey || selections[1].Decision != "refused" || selections[2].Decision != "refused" {
		t.Fatalf("selections = %+v", selections)
	}

	badPath := filepath.Join(t.TempDir(), "bad.jsonl")
	if err := os.WriteFile(badPath, []byte(`{"schema_version":"wrong","route_key":"route","evidence_kind":"optimization"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write bad JSONL: %v", err)
	}
	if err := Run(context.Background(), []string{"debug", "select", "--training-jsonl", badPath}); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("bad schema err = %v", err)
	}
	if err := os.WriteFile(badPath, []byte(`{"schema_version":"mycelium.debug_training.v1","evidence_kind":"mystery","route_key":"route"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write bad evidence kind JSONL: %v", err)
	}
	if err := Run(context.Background(), []string{"debug", "select", "--training-jsonl", badPath}); err == nil || !strings.Contains(err.Error(), "evidence_kind") {
		t.Fatalf("bad evidence kind err = %v", err)
	}
	if err := os.WriteFile(badPath, []byte(`{"schema_version":"mycelium.debug_training.v1","evidence_kind":"preflight","route_key":"route","owner_telemetry_available":true}`+"\n"), 0o600); err != nil {
		t.Fatalf("write bad preflight telemetry JSONL: %v", err)
	}
	if err := Run(context.Background(), []string{"debug", "select", "--training-jsonl", badPath}); err == nil || !strings.Contains(err.Error(), "preflight-only evidence as owner telemetry") {
		t.Fatalf("bad preflight telemetry err = %v", err)
	}
	if err := os.WriteFile(badPath, []byte(`{"schema_version":"mycelium.debug_training.v1","evidence_kind":"optimization","route_key":"route","deterministic_quality_pass":true}`+"\n"), 0o600); err != nil {
		t.Fatalf("write bad quality JSONL: %v", err)
	}
	if err := Run(context.Background(), []string{"debug", "select", "--training-jsonl", badPath}); err == nil || !strings.Contains(err.Error(), "deterministic quality") {
		t.Fatalf("bad quality err = %v", err)
	}
	if err := Run(context.Background(), []string{"debug", "select"}); err == nil || !strings.Contains(err.Error(), "--training-jsonl") {
		t.Fatalf("missing training err = %v", err)
	}
}

func TestRunDebugProposePolicyBuildsApprovalReport(t *testing.T) {
	pass := true
	rows := []debugSummaryTrainingRow{
		{
			SchemaVersion:            "mycelium.debug_training.v1",
			EvidenceKind:             "optimization",
			RouteKey:                 "model=large,node=node-a,backend=vllm,preset=preset-large",
			Model:                    "large",
			NodeID:                   "node-a",
			Backend:                  "vllm",
			PresetID:                 "preset-large",
			ContextCap:               65536,
			MinContextCap:            32768,
			ContextAdequate:          true,
			ToolRequested:            true,
			JSONContent:              true,
			UsageAvailable:           true,
			UsageTotalTokens:         2400,
			StatusCode:               200,
			FinishReason:             "stop",
			ContentLength:            42,
			TotalLatencyMS:           800,
			OwnerTelemetryAvailable:  true,
			LiveModelChecked:         true,
			LiveModelAvailable:       true,
			QualitySignalSource:      "consumer_gate",
			DeterministicQualityPass: &pass,
			Recommendation:           "route_healthy",
			RecommendationSeverity:   "ok",
			RecommendationConfidence: "high",
		},
		{
			SchemaVersion:            "mycelium.debug_training.v1",
			EvidenceKind:             "optimization",
			RouteKey:                 "model=large,node=node-a,backend=vllm,preset=preset-large",
			Model:                    "large",
			NodeID:                   "node-a",
			Backend:                  "vllm",
			PresetID:                 "preset-large",
			ContextCap:               65536,
			MinContextCap:            32768,
			ContextAdequate:          true,
			ToolRequested:            true,
			JSONContent:              true,
			UsageAvailable:           true,
			UsageTotalTokens:         2200,
			StatusCode:               200,
			FinishReason:             "stop",
			ContentLength:            40,
			TotalLatencyMS:           700,
			OwnerTelemetryAvailable:  true,
			LiveModelChecked:         true,
			LiveModelAvailable:       true,
			QualitySignalSource:      "consumer_gate",
			DeterministicQualityPass: &pass,
			Recommendation:           "route_healthy",
			RecommendationSeverity:   "ok",
			RecommendationConfidence: "high",
		},
		{
			SchemaVersion:   "mycelium.debug_training.v1",
			EvidenceKind:    "preflight",
			RouteKey:        "model=small,node=node-b,backend=vllm,preset=preset-small",
			Model:           "small",
			NodeID:          "node-b",
			Backend:         "vllm",
			PresetID:        "preset-small",
			ContextCap:      65536,
			MinContextCap:   32768,
			ContextAdequate: true,
			ToolRequested:   true,
			JSONContent:     true,
			UsageAvailable:  true,
			StatusCode:      200,
			FinishReason:    "stop",
			ContentLength:   24,
			Recommendation:  "route_healthy",
		},
		{
			SchemaVersion:   "mycelium.debug_training.v1",
			EvidenceKind:    "optimization",
			RouteKey:        "model=tool-text,node=node-c,backend=vllm,preset=preset-tool",
			Model:           "tool-text",
			NodeID:          "node-c",
			Backend:         "vllm",
			PresetID:        "preset-tool",
			ContextCap:      65536,
			MinContextCap:   32768,
			ContextAdequate: true,
			ToolRequested:   true,
			TextualToolCall: true,
			StatusCode:      200,
			FinishReason:    "stop",
			ContentLength:   80,
			Recommendation:  "tool_call_route_unsuitable",
		},
	}
	path := filepath.Join(t.TempDir(), "training.jsonl")
	var data strings.Builder
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("marshal row: %v", err)
		}
		data.Write(encoded)
		data.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(data.String()), 0o600); err != nil {
		t.Fatalf("write training JSONL: %v", err)
	}

	output := captureStdout(t, func() {
		err := Run(context.Background(), []string{
			"debug", "propose-policy",
			"--training-jsonl", path,
			"--min-context-cap", "32768",
			"--require-tools",
			"--require-json",
			"--require-final-content",
			"--require-usage",
			"--min-successes", "2",
			"--require-owner-telemetry",
			"--require-live-model",
			"--require-quality-pass",
		})
		if err != nil {
			t.Fatalf("debug propose-policy: %v", err)
		}
	})
	for _, want := range []string{
		"policy-proposal\tschema=mycelium.route_policy_proposal.v1\tmode=report_only\tproposed=1\tcandidates=0\trefused=1\tmissing_evidence=1",
		"policy-route\taction=propose_policy_change\tdecision=selected\tkey=model=large,node=node-a,backend=vllm,preset=preset-large",
		"evidence=owner_telemetry",
		"policy-route\taction=collect_more_evidence\tdecision=candidate\tkey=model=small,node=node-b,backend=vllm,preset=preset-small",
		"missing=need at least 2 successful saved rows,need owner-side run/session telemetry,need live catalog availability evidence,need deterministic consumer quality pass evidence",
		"policy-route\taction=refuse_policy_change\tdecision=refused\tkey=model=tool-text,node=node-c,backend=vllm,preset=preset-tool",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("policy proposal output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, path) || strings.Contains(output, "Bearer") {
		t.Fatalf("policy proposal leaked local path or token material: %s", output)
	}

	jsonOutput := captureStdout(t, func() {
		err := Run(context.Background(), []string{"debug", "propose-policy", "--training-jsonl", path, "--json", "--min-successes", "2", "--require-owner-telemetry"})
		if err != nil {
			t.Fatalf("debug propose-policy json: %v", err)
		}
	})
	var report debugRoutePolicyProposalReport
	if err := json.Unmarshal([]byte(jsonOutput), &report); err != nil {
		t.Fatalf("decode policy proposal JSON: %v\n%s", err, jsonOutput)
	}
	if report.SchemaVersion != "mycelium.route_policy_proposal.v1" || report.Mode != "report_only" || report.ProposedPolicyChanges != 1 || !strings.Contains(report.ApprovalBoundary, "operator approval") {
		t.Fatalf("policy report = %+v", report)
	}
	if len(report.Proposals) != 3 || report.Proposals[0].Action != "propose_policy_change" || !report.Proposals[0].ApprovalRequired || len(report.Proposals[0].RollbackNotes) == 0 {
		t.Fatalf("policy proposals = %+v", report.Proposals)
	}

	if err := Run(context.Background(), []string{"debug", "propose-policy"}); err == nil || !strings.Contains(err.Error(), "--training-jsonl") {
		t.Fatalf("missing training err = %v", err)
	}
}

func TestRunDebugVerifyPreflightValidatesSavedEvidence(t *testing.T) {
	preflightPath := writeGatewayPreflightEvidenceFixture(t, "job-a")

	output := captureStdout(t, func() {
		err := Run(context.Background(), []string{"debug", "verify-preflight", "--preflight", preflightPath})
		if err == nil || !strings.Contains(err.Error(), "structured tool call") || !strings.Contains(err.Error(), "textual <tool_call>") {
			t.Fatalf("debug verify-preflight err = %v", err)
		}
	})
	if !strings.Contains(output, "preflight-verification\tresult=fail") || !strings.Contains(output, "preflight-verification-failure\texpected at least one structured tool call") || strings.Contains(output, preflightPath) || strings.Contains(output, "Bearer") {
		t.Fatalf("failed verifier output = %q", output)
	}

	evidence, err := readGatewayPreflightEvidence(preflightPath)
	if err != nil {
		t.Fatalf("read preflight evidence: %v", err)
	}
	evidence.TextualToolCall = false
	evidence.ToolCallCount = 1
	evidence.ToolCallNames = []string{"echo"}
	evidence.Request.ContextCap = 32768
	evidence.LiveModel = &gatewayPreflightLiveModelEvidence{
		Checked:        true,
		Model:          evidence.Model,
		Status:         "ok",
		ModelAvailable: true,
		ModelMatch:     evidence.Model,
	}
	passPath := filepath.Join(t.TempDir(), "preflight-pass.json")
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal passing preflight: %v", err)
	}
	if err := os.WriteFile(passPath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write passing preflight: %v", err)
	}

	output = captureStdout(t, func() {
		err := Run(context.Background(), []string{
			"debug", "verify-preflight",
			"--preflight", passPath,
			"--require-live-model",
			"--require-json",
			"--require-tool-call",
			"--reject-textual-tool-call",
			"--min-context-cap", "32768",
			"--min-content-length", "1",
			"--max-reasoning-length", "0",
		})
		if err != nil {
			t.Fatalf("debug verify-preflight pass: %v", err)
		}
	})
	if !strings.Contains(output, "preflight-verification\tresult=pass") || !strings.Contains(output, "tool_calls=1") || strings.Contains(output, passPath) || strings.Contains(output, "Bearer") {
		t.Fatalf("passing verifier output = %q", output)
	}

	jsonOutput := captureStdout(t, func() {
		err := Run(context.Background(), []string{"debug", "verify-preflight", "--preflight", passPath, "--json", "--min-content-length", "99"})
		if err == nil || !strings.Contains(err.Error(), "content length") {
			t.Fatalf("debug verify-preflight json err = %v", err)
		}
	})
	var verification gatewayPreflightVerification
	if err := json.Unmarshal([]byte(jsonOutput), &verification); err != nil {
		t.Fatalf("decode verification JSON: %v\n%s", err, jsonOutput)
	}
	if verification.Passed || verification.Result != "fail" || len(verification.Failures) != 1 || !strings.Contains(verification.Failures[0], "content length") {
		t.Fatalf("verification JSON = %+v", verification)
	}

	if err := Run(context.Background(), []string{"debug", "verify-preflight"}); err == nil || !strings.Contains(err.Error(), "--preflight") {
		t.Fatalf("missing preflight err = %v", err)
	}
	if err := Run(context.Background(), []string{"debug", "verify-preflight", "--preflight", passPath, "--expect-node", "other-node"}); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting expectation err = %v", err)
	}
}

func TestRunRouteStatusVerifiesLatestContractEvidence(t *testing.T) {
	root := t.TempDir()
	contractsPath := filepath.Join(t.TempDir(), "route-contracts.json")
	writeRouteContractsFixture(t, contractsPath, routeContractFile{
		SchemaVersion: routeContractSchemaVersion,
		EvidenceRoot:  root,
		Contracts: []routeCertificationContract{
			{
				Name:                  "strict-json-general",
				Model:                 "strict-json-model",
				Project:               "strict-json-batch",
				ContextCap:            32768,
				MinContextCap:         32768,
				ExpectNode:            "owner-node",
				RequireJSON:           true,
				RequireLiveModel:      true,
				ExpectFinishReason:    "stop",
				MinContentLength:      1,
				MaxReasoningLength:    intPtr(0),
				ConsumerQualityGate:   "schema-valid",
				RejectTextualToolCall: true,
			},
			{
				Name:            "tool-agent-general",
				Model:           "tool-model-large",
				MinContextCap:   65536,
				RequireToolCall: true,
			},
		},
	})
	evidence := routeCertifiedPreflightFixture("job-a")
	evidenceDir := filepath.Join(root, "strict-json-general", "20260701T120000Z")
	if err := os.MkdirAll(evidenceDir, 0700); err != nil {
		t.Fatalf("create evidence dir: %v", err)
	}
	writeGatewayPreflightEvidenceValue(t, filepath.Join(evidenceDir, "preflight.json"), evidence)

	var runErr error
	output := captureStdout(t, func() {
		runErr = Run(context.Background(), []string{"route", "status", "--contracts", contractsPath, "--json"})
	})
	if runErr == nil || !strings.Contains(runErr.Error(), "route certification incomplete") {
		t.Fatalf("route status err = %v", runErr)
	}
	if strings.Contains(output, "Return") || strings.Contains(output, "Bearer") {
		t.Fatalf("route status leaked private material: %s", output)
	}
	var report routeCertificationStatusReport
	if err := json.Unmarshal([]byte(output), &report); err != nil {
		t.Fatalf("decode route status JSON: %v\n%s", err, output)
	}
	if len(report.Contracts) != 2 {
		t.Fatalf("route status contracts = %+v", report.Contracts)
	}
	if report.Contracts[0].Name != "strict-json-general" || report.Contracts[0].Result != "certified" || report.Contracts[0].Verification == nil || !report.Contracts[0].Verification.Passed {
		t.Fatalf("strict JSON status = %+v", report.Contracts[0])
	}
	if report.Contracts[1].Name != "tool-agent-general" || report.Contracts[1].Result != "missing_evidence" || !strings.Contains(report.Contracts[1].NextOperatorCheck, "myce route certify tool-agent-general") {
		t.Fatalf("tool status = %+v", report.Contracts[1])
	}

	textOutput := captureStdout(t, func() {
		runErr = Run(context.Background(), []string{"route", "status", "--contracts", contractsPath})
	})
	if runErr == nil || !strings.Contains(runErr.Error(), "route certification incomplete") {
		t.Fatalf("route status text err = %v", runErr)
	}
	for _, want := range []string{
		"route-status\tcontracts=" + contractsPath,
		"route\tname=strict-json-general\tresult=certified",
		"route\tname=tool-agent-general\tresult=missing_evidence",
		"next=myce route certify tool-agent-general --contracts " + contractsPath,
	} {
		if !strings.Contains(textOutput, want) {
			t.Fatalf("route status output missing %q:\n%s", want, textOutput)
		}
	}
}

func TestRunRouteCertifyRunsNamedPreflightIntoEvidenceHome(t *testing.T) {
	root := t.TempDir()
	contractsPath := filepath.Join(t.TempDir(), "route-contracts.json")
	writeRouteContractsFixture(t, contractsPath, routeContractFile{
		SchemaVersion: routeContractSchemaVersion,
		Contracts: []routeCertificationContract{{
			Name:               "strict-json-general",
			URL:                "http://gateway.test",
			Model:              "strict-json-model",
			Project:            "strict-json-cert",
			Prompt:             `Return {"ok":true}.`,
			MaxTokens:          16,
			ContextCap:         32768,
			MinContextCap:      32768,
			ExpectNode:         "owner-node",
			RequireJSON:        true,
			RequireLiveModel:   true,
			ExpectFinishReason: "stop",
			MinContentLength:   1,
			MaxReasoningLength: intPtr(0),
			ConsumerRun:        "certification-run",
			ConsumerItem:       "contract",
			ConsumerStatus:     "ok",
			ConsumerResult:     "pass",
		}},
	})
	tokenFile := filepath.Join(t.TempDir(), "gateway-token")
	if err := os.WriteFile(tokenFile, []byte("route-secret\n"), 0600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer route-secret" {
			t.Fatalf("auth = %q", got)
		}
		switch r.URL.Path {
		case "/v1/models":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]string{{"id": "strict-json-model"}},
			})
		case "/v1/chat/completions":
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			if req["model"] != "strict-json-model" || req["stream"] == true {
				t.Fatalf("request = %#v", req)
			}
			w.Header().Set("X-Myc-Job", "job-cert")
			w.Header().Set("X-Myc-Node", "owner-node")
			w.Header().Set("X-Myc-Backend", "vllm")
			w.Header().Set("X-Myc-Decision", "placed_on_warm_instance")
			w.Header().Set("X-Myc-Structured-Output", "json_schema")
			w.Header().Set("X-Myc-Guided-Decoding-Requested", "true")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"message":       map[string]string{"content": `{"ok":true}`},
					"finish_reason": "stop",
				}},
				"usage": map[string]int{"prompt_tokens": 6, "completion_tokens": 3, "total_tokens": 9},
			})
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))

	var runErr error
	output := captureStdout(t, func() {
		runErr = RunWithClient(context.Background(), []string{
			"route", "certify",
			"strict-json-general",
			"--contracts", contractsPath,
			"--evidence-root", root,
			"--gateway-token-file", tokenFile,
		}, client)
	})
	if runErr != nil {
		t.Fatalf("route certify: %v\n%s", runErr, output)
	}
	if !strings.Contains(output, "route-certify\tcontract=strict-json-general") || !strings.Contains(output, "gateway\thttp://gateway.test\tok") || strings.Contains(output, "route-secret") {
		t.Fatalf("route certify output = %s", output)
	}
	matches, err := filepath.Glob(filepath.Join(root, "strict-json-general", "*", "preflight.json"))
	if err != nil {
		t.Fatalf("glob evidence: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("evidence matches = %v", matches)
	}
	evidenceData, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read evidence: %v", err)
	}
	if strings.Contains(string(evidenceData), "route-secret") || strings.Contains(string(evidenceData), "Return") || strings.Contains(string(evidenceData), `{"ok":true}`) {
		t.Fatalf("route certify evidence leaked private material: %s", evidenceData)
	}
}

func TestRunRouteStatusReportsContractEvidenceMismatch(t *testing.T) {
	root := t.TempDir()
	contractsPath := filepath.Join(t.TempDir(), "route-contracts.json")
	writeRouteContractsFixture(t, contractsPath, routeContractFile{
		SchemaVersion: routeContractSchemaVersion,
		EvidenceRoot:  root,
		Contracts: []routeCertificationContract{{
			Name:            "tool-agent-high-context",
			Model:           "tool-model-large",
			Project:         "tool-project",
			ContextCap:      65536,
			MinContextCap:   65536,
			ToolName:        "echo",
			ToolChoice:      "function",
			Stream:          true,
			RequireToolCall: true,
		}},
	})
	evidenceDir := filepath.Join(root, "tool-agent-high-context", "20260701T120000Z")
	if err := os.MkdirAll(evidenceDir, 0700); err != nil {
		t.Fatalf("create evidence dir: %v", err)
	}
	writeGatewayPreflightEvidenceValue(t, filepath.Join(evidenceDir, "preflight.json"), routeCertifiedPreflightFixture("job-a"))

	output := captureStdout(t, func() {
		err := Run(context.Background(), []string{"route", "status", "--contracts", contractsPath, "--json"})
		if err == nil || !strings.Contains(err.Error(), "route certification incomplete") {
			t.Fatalf("route status mismatch err = %v", err)
		}
	})
	var report routeCertificationStatusReport
	if err := json.Unmarshal([]byte(output), &report); err != nil {
		t.Fatalf("decode status JSON: %v\n%s", err, output)
	}
	if len(report.Contracts) != 1 || report.Contracts[0].Result != "failed" {
		t.Fatalf("mismatch status = %+v", report.Contracts)
	}
	failures := strings.Join(report.Contracts[0].Failures, "\n")
	for _, want := range []string{
		"expected at least one structured tool call",
		"expected context cap >= 65536 got 32768",
		"expected model=tool-model-large got strict-json-model",
		"expected project=tool-project got strict-json-batch",
		"expected streaming=true got false",
		"expected context_cap=65536 got 32768",
		"expected tool_name=echo got -",
		"expected tool_choice=function got -",
	} {
		if !strings.Contains(failures, want) {
			t.Fatalf("failures missing %q:\n%s", want, failures)
		}
	}
}

func TestRouteStatusMarksStaleCertificationEvidence(t *testing.T) {
	root := t.TempDir()
	contractsPath := filepath.Join(t.TempDir(), "route-contracts.json")
	writeRouteContractsFixture(t, contractsPath, routeContractFile{
		SchemaVersion: routeContractSchemaVersion,
		EvidenceRoot:  root,
		Contracts: []routeCertificationContract{{
			Name:          "strict-json-general",
			Model:         "strict-json-model",
			RetentionDays: 1,
		}},
	})
	evidencePath := filepath.Join(root, "strict-json-general", "20260701T120000Z", "preflight.json")
	if err := os.MkdirAll(filepath.Dir(evidencePath), 0700); err != nil {
		t.Fatalf("create evidence dir: %v", err)
	}
	writeGatewayPreflightEvidenceValue(t, evidencePath, routeCertifiedPreflightFixture("job-a"))
	old := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(evidencePath, old, old); err != nil {
		t.Fatalf("age evidence: %v", err)
	}

	report, err := buildRouteCertificationStatus(contractsPath, "", old.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("build route status: %v", err)
	}
	if len(report.Contracts) != 1 || report.Contracts[0].Result != "stale_evidence" || !strings.Contains(strings.Join(report.Contracts[0].Failures, "\n"), "retention_days=1") {
		t.Fatalf("stale status = %+v", report.Contracts)
	}
	if err := routeCertificationStatusError(report); err == nil || !strings.Contains(err.Error(), "route certification incomplete") {
		t.Fatalf("stale status error = %v", err)
	}
}

func TestRouteContractValidationAndUsageErrors(t *testing.T) {
	if err := Run(context.Background(), []string{"route"}); err == nil || !strings.Contains(err.Error(), "usage: myce route") {
		t.Fatalf("route usage err = %v", err)
	}
	if err := Run(context.Background(), []string{"route", "bogus"}); err == nil || !strings.Contains(err.Error(), "usage: myce route") {
		t.Fatalf("route bogus err = %v", err)
	}
	if err := Run(context.Background(), []string{"route", "certify", "--contracts", filepath.Join(t.TempDir(), "missing.json"), "missing"}); err == nil || !strings.Contains(err.Error(), "read route contracts") {
		t.Fatalf("missing contracts err = %v", err)
	}
	badJSONPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badJSONPath, []byte(`{bad json`), 0600); err != nil {
		t.Fatalf("write bad json: %v", err)
	}
	if err := Run(context.Background(), []string{"route", "status", "--contracts", badJSONPath}); err == nil || !strings.Contains(err.Error(), "decode route contracts") {
		t.Fatalf("bad json err = %v", err)
	}
	for name, contracts := range map[string]routeContractFile{
		"schema": {
			SchemaVersion: "future",
			Contracts:     []routeCertificationContract{{Name: "strict-json-general", Model: "strict-json-model"}},
		},
		"duplicate": {
			SchemaVersion: routeContractSchemaVersion,
			Contracts: []routeCertificationContract{
				{Name: "strict-json-general", Model: "strict-json-model"},
				{Name: "strict-json-general", Model: "other-model"},
			},
		},
		"missing-model": {
			SchemaVersion: routeContractSchemaVersion,
			Contracts:     []routeCertificationContract{{Name: "strict-json-general"}},
		},
		"negative": {
			SchemaVersion: routeContractSchemaVersion,
			Contracts:     []routeCertificationContract{{Name: "strict-json-general", Model: "strict-json-model", MinContextCap: -1}},
		},
		"negative-reasoning": {
			SchemaVersion: routeContractSchemaVersion,
			Contracts:     []routeCertificationContract{{Name: "strict-json-general", Model: "strict-json-model", MaxReasoningLength: intPtr(-1)}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "contracts.json")
			writeRouteContractsFixture(t, path, contracts)
			if _, err := loadRouteContracts(path); err == nil {
				t.Fatalf("loadRouteContracts(%s) succeeded", name)
			}
		})
	}

	home := t.TempDir()
	t.Setenv("HOME", home)
	if got := defaultRouteContractsPath(); got != filepath.Join(home, ".mycelium", "route-contracts.json") {
		t.Fatalf("default route contracts path = %s", got)
	}
	if got := defaultRouteEvidenceRoot(); got != filepath.Join(home, ".mycelium", "evidence") {
		t.Fatalf("default route evidence root = %s", got)
	}
	if got := expandHomePath("~/custom-evidence"); got != filepath.Join(home, "custom-evidence") {
		t.Fatalf("expanded home path = %s", got)
	}
}

func TestRunCalibratePlanWritesSimulationReadyBenchmarkConfig(t *testing.T) {
	dbPath, configPath := writeCalibrationInventoryFixture(t)
	outDir := filepath.Join(t.TempDir(), "calibration-plan")

	var runErr error
	output := captureStdout(t, func() {
		runErr = Run(context.Background(), []string{"calibrate", "plan", "--config", configPath, "--db", dbPath, "--out", outDir})
	})
	if runErr != nil {
		t.Fatalf("calibrate plan: %v\n%s", runErr, output)
	}
	for _, want := range []string{
		"calibrate-plan\tprofile=conservative\tproject=calibration-project",
		"models=2\tnodes=3",
		"calibrate-proof\tcold placement observed",
		"calibrate-proof\twarm reuse observed",
		"calibrate-proof\thard preemption/reallocation pressure observed",
		"calibrate-next\tmyce calibrate run --plan " + outDir + " --simulate --out " + filepath.Join(outDir, "runs"),
		"calibrate-local-proof-next\tmake smoke-workload-executor",
		"calibrate-live-next\tmyce benchmark fleet --config " + filepath.Join(outDir, "benchmark-config.json") + " --profile conservative --out " + filepath.Join(outDir, "live-runs"),
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("calibrate plan output missing %q:\n%s", want, output)
		}
	}
	for _, name := range []string{"benchmark-config.json", "calibration-plan.json"} {
		info, err := os.Stat(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("%s mode = %o", name, info.Mode().Perm())
		}
	}
	benchData, err := os.ReadFile(filepath.Join(outDir, "benchmark-config.json"))
	if err != nil {
		t.Fatalf("read benchmark config: %v", err)
	}
	if strings.Contains(string(benchData), "rpc-secret") || strings.Contains(string(benchData), "gateway-secret") {
		t.Fatalf("calibration benchmark config leaked token material: %s", benchData)
	}
	cfg, err := bench.LoadFleetConfig(filepath.Join(outDir, "benchmark-config.json"))
	if err != nil {
		t.Fatalf("load generated benchmark config: %v", err)
	}
	if cfg.Project != "calibration-project" || len(cfg.Models) != 2 || len(cfg.Waves) != 3 || cfg.GatewayToken != "" || cfg.RPCToken != "" {
		t.Fatalf("generated benchmark config = %+v", cfg)
	}
	if _, err := bench.SimulateFleet(context.Background(), cfg, bench.FleetProfileConservative, clock.System{}); err != nil {
		t.Fatalf("generated benchmark config did not simulate: %v", err)
	}

	planData, err := os.ReadFile(filepath.Join(outDir, "calibration-plan.json"))
	if err != nil {
		t.Fatalf("read calibration plan: %v", err)
	}
	if strings.Contains(string(planData), "rpc-secret") || strings.Contains(string(planData), "gateway-secret") {
		t.Fatalf("calibration plan leaked token material: %s", planData)
	}
	var artifact calibrationPlanArtifact
	if err := json.Unmarshal(planData, &artifact); err != nil {
		t.Fatalf("decode calibration plan: %v", err)
	}
	if artifact.SchemaVersion != calibrationPlanSchemaVersion || !artifact.Preflight.Passed || len(artifact.Sweeps) != 3 || !reflect.DeepEqual(artifact.LocalProofCommand, []string{"make", "smoke-workload-executor"}) || !reflect.DeepEqual(artifact.LiveRunCommand, []string{"myce", "benchmark", "fleet", "--config", filepath.Join(outDir, "benchmark-config.json"), "--profile", "conservative", "--out", filepath.Join(outDir, "live-runs")}) {
		t.Fatalf("calibration artifact = %+v", artifact)
	}
}

func TestRunCalibrateRunSimulatesGeneratedPlan(t *testing.T) {
	dbPath, configPath := writeCalibrationInventoryFixture(t)
	planDir := filepath.Join(t.TempDir(), "calibration-plan")
	if err := Run(context.Background(), []string{"calibrate", "plan", "--config", configPath, "--db", dbPath, "--out", planDir}); err != nil {
		t.Fatalf("calibrate plan: %v", err)
	}
	runOut := filepath.Join(t.TempDir(), "calibration-run")

	var runErr error
	output := captureStdout(t, func() {
		runErr = Run(context.Background(), []string{"calibrate", "run", "--plan", planDir, "--simulate", "--out", runOut})
	})
	if runErr != nil {
		t.Fatalf("calibrate run: %v\n%s", runErr, output)
	}
	if !strings.Contains(output, "calibrate-run\t") || !strings.Contains(output, "\tsimulated\t") || !strings.Contains(output, "proofs=") {
		t.Fatalf("calibrate run output = %s", output)
	}
	matches, err := filepath.Glob(filepath.Join(runOut, "calibration-*", "results.json"))
	if err != nil {
		t.Fatalf("glob run results: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("run results matches = %v", matches)
	}

	if err := Run(context.Background(), []string{"calibrate", "run", "--plan", planDir, "--out", runOut}); err == nil || !strings.Contains(err.Error(), "--simulate") {
		t.Fatalf("live calibrate run boundary err = %v", err)
	}
}

func TestCalibrationPlanFiltersUnavailablePinnedPresetsAndResetsWarmForColdProof(t *testing.T) {
	cfg := operatorPeerConfig{ID: "gateway-peer", Listen: "http://gateway.test"}
	nodes := []domain.Node{
		{
			ID:               "b70",
			Address:          "http://b70.test",
			Status:           domain.NodeReady,
			MaxUtil:          0.85,
			DiskTotalMB:      1_000_000,
			DiskFreeMB:       800_000,
			DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
			OOMSeverity:      domain.OOMSoft,
			Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 32_000}},
			SpeedClass:       domain.SpeedClass{TokensPerSecRef: 75},
		},
		{
			ID:               "spark",
			Address:          "http://spark.test",
			Status:           domain.NodeReady,
			MaxUtil:          0.85,
			DiskTotalMB:      4_000_000,
			DiskFreeMB:       3_000_000,
			DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
			OOMSeverity:      domain.OOMCatastrophic,
			Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 124_000}},
			SpeedClass:       domain.SpeedClass{TokensPerSecRef: 150},
		},
	}
	presets := []domain.Preset{
		{ID: "unavailable-small", NodeID: "offline-node", Backend: domain.BackendLlamaCpp, ContextLength: 512, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 64, ArtifactSizeMB: 64, KVPerTokenMB: 0.01},
		{ID: "warm-small", NodeID: "b70", Backend: domain.BackendVLLM, LaunchArgs: []string{"--gpu-memory-utilization", "0.75"}, ContextLength: 2048, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 18_000, ArtifactSizeMB: 18_000, KVPerTokenMB: 0.02},
		{ID: "cold-small", NodeID: "b70", Backend: domain.BackendVLLM, LaunchArgs: []string{"--gpu-memory-utilization", "0.75"}, ContextLength: 4096, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 19_000, ArtifactSizeMB: 19_000, KVPerTokenMB: 0.02},
		{ID: "large-pressure", NodeID: "spark", Backend: domain.BackendVLLM, LaunchArgs: []string{"--gpu-memory-utilization", "0.70"}, ContextLength: 8192, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 86_000, ArtifactSizeMB: 75_000, KVPerTokenMB: 0.02},
	}
	instances := []domain.ModelInstance{{
		ID:             "warm-inst",
		PresetID:       "warm-small",
		NodeID:         "b70",
		AcceleratorSet: []int{0},
		Claim:          domain.Claim{WeightsMB: 56_000},
		State:          domain.InstReady,
		Priority:       domain.PriorityNormal,
	}, {
		ID:             "spark-background",
		PresetID:       "cold-small",
		NodeID:         "spark",
		AcceleratorSet: []int{0},
		Claim:          domain.Claim{WeightsMB: 56_000},
		State:          domain.InstReady,
		Priority:       domain.PriorityBackground,
	}}

	benchCfg, err := calibrationBenchmarkConfig(cfg, nodes, presets, instances, bench.FleetProfileConservative, "calibration-project", 2)
	if err != nil {
		t.Fatalf("calibrationBenchmarkConfig: %v", err)
	}
	if len(benchCfg.Models) != 2 || benchCfg.Models[0].PresetID != "warm-small" || benchCfg.Models[1].PresetID != "large-pressure" {
		t.Fatalf("selected models = %+v", benchCfg.Models)
	}
	if !benchCfg.Safety.ResetBenchmarkInstances {
		t.Fatalf("conservative calibration should reset selected benchmark instances")
	}
	for _, model := range benchCfg.Models {
		if model.PresetID == "unavailable-small" {
			t.Fatalf("unavailable pinned preset selected: %+v", benchCfg.Models)
		}
	}
	var hasDiskGuard bool
	for _, node := range benchCfg.Simulation.Nodes {
		if node.ID == "simulation-disk-headroom-guard" {
			hasDiskGuard = true
		}
	}
	if !hasDiskGuard {
		t.Fatalf("simulation nodes missing disk-headroom guard: %+v", benchCfg.Simulation.Nodes)
	}
	for _, inst := range benchCfg.Simulation.Instances {
		if inst.PresetID == "warm-small" {
			t.Fatalf("simulation retained resettable benchmark instance: %+v", benchCfg.Simulation.Instances)
		}
	}
	preflight, err := bench.SimulateFleet(context.Background(), benchCfg, bench.FleetProfileConservative, clock.System{})
	if err != nil {
		t.Fatalf("generated live-like calibration config did not simulate: %v", err)
	}
	for _, want := range []string{"cold placement", "warm reuse", "disk-headroom"} {
		if !proofsContain(preflight.Proofs, want) {
			t.Fatalf("proofs missing %q: %+v", want, preflight.Proofs)
		}
	}
}

func TestRunCalibrateStatusAndReportSummarizeSavedEvidence(t *testing.T) {
	dbPath, configPath := writeCalibrationInventoryFixture(t)
	planDir := filepath.Join(t.TempDir(), "calibration-plan")
	if err := Run(context.Background(), []string{"calibrate", "plan", "--config", configPath, "--db", dbPath, "--out", planDir}); err != nil {
		t.Fatalf("calibrate plan: %v", err)
	}

	var statusErr error
	planOnly := captureStdout(t, func() {
		statusErr = Run(context.Background(), []string{"calibrate", "status", "--plan", planDir})
	})
	if statusErr != nil {
		t.Fatalf("calibrate status plan-only: %v", statusErr)
	}
	if !strings.Contains(planOnly, "readiness=plan_ready") || !strings.Contains(planOnly, "no calibration run artifact supplied") {
		t.Fatalf("plan-only status = %s", planOnly)
	}

	runOut := filepath.Join(t.TempDir(), "calibration-run")
	if err := Run(context.Background(), []string{"calibrate", "run", "--plan", planDir, "--simulate", "--out", runOut}); err != nil {
		t.Fatalf("calibrate run: %v", err)
	}
	statusWithRun := captureStdout(t, func() {
		statusErr = Run(context.Background(), []string{"calibrate", "status", "--plan", planDir, "--run", runOut})
	})
	if statusErr != nil {
		t.Fatalf("calibrate status with run: %v", statusErr)
	}
	for _, want := range []string{
		"readiness=simulation_proven",
		"calibrate-run-status\tcalibration-",
		"simulated=true",
		"live calibration has not run and no optimizer defaults were applied",
		"calibrate-next\tmake smoke-workload-executor",
		"calibrate-next\tmyce benchmark fleet --config " + filepath.Join(planDir, "benchmark-config.json") + " --profile conservative --out " + filepath.Join(planDir, "live-runs"),
	} {
		if !strings.Contains(statusWithRun, want) {
			t.Fatalf("status with run missing %q:\n%s", want, statusWithRun)
		}
	}

	reportPath := filepath.Join(t.TempDir(), "calibration-report.json")
	reportOut := captureStdout(t, func() {
		statusErr = Run(context.Background(), []string{"calibrate", "report", "--plan", planDir, "--run", runOut, "--out", reportPath})
	})
	if statusErr != nil {
		t.Fatalf("calibrate report: %v", statusErr)
	}
	if !strings.Contains(reportOut, "readiness=simulation_proven") {
		t.Fatalf("report output = %s", reportOut)
	}
	info, err := os.Stat(reportPath)
	if err != nil {
		t.Fatalf("missing report: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("report mode = %o", info.Mode().Perm())
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if strings.Contains(string(data), "rpc-secret") || strings.Contains(string(data), "gateway-secret") {
		t.Fatalf("calibration report leaked token material: %s", data)
	}
	var report calibrationReportArtifact
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.SchemaVersion != calibrationReportSchemaVersion || report.Readiness != "simulation_proven" || report.Run == nil || report.Run.MetricRows != 0 || !reflect.DeepEqual(report.Plan.LocalProofCommand, []string{"make", "smoke-workload-executor"}) || !reflect.DeepEqual(report.Plan.LiveRunCommand, []string{"myce", "benchmark", "fleet", "--config", filepath.Join(planDir, "benchmark-config.json"), "--profile", "conservative", "--out", filepath.Join(planDir, "live-runs")}) {
		t.Fatalf("report = %+v", report)
	}
}

func TestRunCalibrateApplyPriorsImportsOnlyLiveMetrics(t *testing.T) {
	dbPath, configPath := writeCalibrationInventoryFixture(t)
	planDir := filepath.Join(t.TempDir(), "calibration-plan")
	if err := Run(context.Background(), []string{"calibrate", "plan", "--config", configPath, "--db", dbPath, "--out", planDir}); err != nil {
		t.Fatalf("calibrate plan: %v", err)
	}
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	liveRunDir := filepath.Join(t.TempDir(), "live-run")
	liveResult := bench.FleetRunResult{
		RunID:     "calibration-live",
		Profile:   bench.FleetProfileConservative,
		OutputDir: liveRunDir,
		Manifest:  bench.FleetManifest{RunID: "calibration-live", StartedAt: now, FinishedAt: now.Add(time.Minute)},
		Preflight: bench.FleetPreflight{Passed: true, Proofs: []string{"live owner telemetry collected"}},
		Results:   []bench.FleetJobResult{{JobID: "job-b70", NodeID: "b70"}},
		Metrics: []domain.RunMetric{
			{JobID: "job-b70-a", NodeID: "b70", PresetID: "preset-9b", Backend: domain.BackendLlamaCpp, Project: "calibration", TokensPerSec: 100, At: now},
			{JobID: "job-b70-b", NodeID: "b70", PresetID: "preset-9b", Backend: domain.BackendLlamaCpp, Project: "calibration", TokensPerSec: 120, At: now.Add(time.Second)},
		},
	}
	if err := os.MkdirAll(liveRunDir, 0o700); err != nil {
		t.Fatalf("mkdir live run: %v", err)
	}
	writeJSONFixture(t, filepath.Join(liveRunDir, "results.json"), liveResult)

	report, err := buildCalibrationReport(planDir, liveRunDir)
	if err != nil {
		t.Fatalf("live calibration report: %v", err)
	}
	if report.Readiness != "live_evidence_present" || report.Run == nil || report.Run.MetricRows != 2 || !strings.Contains(strings.Join(report.Next, "\n"), "apply-priors") {
		t.Fatalf("live calibration report = %+v", report)
	}

	var runErr error
	output := captureStdout(t, func() {
		runErr = Run(context.Background(), []string{"calibrate", "apply-priors", "--plan", planDir, "--run", liveRunDir})
	})
	if runErr != nil {
		t.Fatalf("apply priors: %v\n%s", runErr, output)
	}
	if !strings.Contains(output, "calibrate-priors\trun=calibration-live\tmetrics=2\tnodes=1") || !strings.Contains(output, "calibrate-prior-node\tb70\ttokens_per_sec=110.00\tsource=telemetry-calibrated") {
		t.Fatalf("apply priors output = %s", output)
	}
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store after apply: %v", err)
	}
	defer store.Close()
	nodes, err := store.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	var b70 domain.Node
	for _, node := range nodes {
		if node.ID == "b70" {
			b70 = node
		}
	}
	if b70.SpeedClass.TokensPerSecRef != 110 || b70.SpeedClass.Source != "telemetry-calibrated" || b70.SpeedClass.ProbedAt.IsZero() {
		t.Fatalf("b70 speed class = %+v", b70.SpeedClass)
	}

	simRunDir := filepath.Join(t.TempDir(), "sim-run")
	simResult := liveResult
	simResult.RunID = "calibration-sim"
	simResult.Simulated = true
	if err := os.MkdirAll(simRunDir, 0o700); err != nil {
		t.Fatalf("mkdir sim run: %v", err)
	}
	writeJSONFixture(t, filepath.Join(simRunDir, "results.json"), simResult)
	if err := Run(context.Background(), []string{"calibrate", "apply-priors", "--plan", planDir, "--run", simRunDir}); err == nil || !strings.Contains(err.Error(), "simulated") {
		t.Fatalf("simulated apply priors err = %v", err)
	}

	noMetricRunDir := filepath.Join(t.TempDir(), "no-metric-run")
	noMetricResult := liveResult
	noMetricResult.RunID = "calibration-no-metrics"
	noMetricResult.Metrics = nil
	if err := os.MkdirAll(noMetricRunDir, 0o700); err != nil {
		t.Fatalf("mkdir no metric run: %v", err)
	}
	writeJSONFixture(t, filepath.Join(noMetricRunDir, "results.json"), noMetricResult)
	report, err = buildCalibrationReport(planDir, noMetricRunDir)
	if err != nil {
		t.Fatalf("no-metric report: %v", err)
	}
	if report.Readiness != "live_evidence_missing_metrics" {
		t.Fatalf("no-metric readiness = %+v", report)
	}
	if err := Run(context.Background(), []string{"calibrate", "apply-priors", "--plan", planDir, "--run", noMetricRunDir}); err == nil || !strings.Contains(err.Error(), "no live metric rows") {
		t.Fatalf("no metric apply priors err = %v", err)
	}
}

func TestCalibratePlanFailsWithoutAllocatorPressureInventory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.SaveProject(context.Background(), domain.Project{ID: "calibration-project"}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-a", Status: domain.NodeReady, MaxUtil: 0.9, OOMSeverity: domain.OOMSoft, DiskTotalMB: 1000, DiskFreeMB: 900, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 24000}}}); err != nil {
		t.Fatalf("save node: %v", err)
	}
	if err := store.SavePreset(context.Background(), domain.Preset{ID: "small", ModelRef: "small", Backend: domain.BackendLlamaCpp, ContextLength: 4096, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 4000, KVPerTokenMB: 0.01}); err != nil {
		t.Fatalf("save preset: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	configPath := filepath.Join(t.TempDir(), "peer.json")
	if err := os.WriteFile(configPath, []byte(`{"id":"gateway-peer","listen":"http://gateway.test","store_path":`+quoteJSON(dbPath)+`}`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	err = Run(context.Background(), []string{"calibrate", "plan", "--config", configPath, "--out", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "requires at least two ready nodes") {
		t.Fatalf("weak calibration inventory err = %v", err)
	}
}

func TestCalibrateUsageErrors(t *testing.T) {
	if err := Run(context.Background(), []string{"calibrate"}); err == nil || !strings.Contains(err.Error(), "usage: myce calibrate") {
		t.Fatalf("calibrate usage err = %v", err)
	}
	if err := Run(context.Background(), []string{"calibrate", "bogus"}); err == nil || !strings.Contains(err.Error(), "usage: myce calibrate") {
		t.Fatalf("calibrate bogus err = %v", err)
	}
	if err := Run(context.Background(), []string{"calibrate", "plan"}); err == nil || !strings.Contains(err.Error(), "--out") {
		t.Fatalf("calibrate plan missing out err = %v", err)
	}
	if err := Run(context.Background(), []string{"calibrate", "run", "--simulate"}); err == nil || !strings.Contains(err.Error(), "--plan") {
		t.Fatalf("calibrate run missing plan err = %v", err)
	}
	if err := Run(context.Background(), []string{"calibrate", "status"}); err == nil || !strings.Contains(err.Error(), "--plan") {
		t.Fatalf("calibrate status missing plan err = %v", err)
	}
	if err := Run(context.Background(), []string{"calibrate", "report", "--plan", t.TempDir()}); err == nil || !strings.Contains(err.Error(), "--out") {
		t.Fatalf("calibrate report missing out err = %v", err)
	}
}

func TestConsumerEvidenceFromFlags(t *testing.T) {
	evidence, err := consumerEvidenceFromFlags(" run ", " item ", "TIMEOUT", 42, " deadline ", "FAIL", " schema ")
	if err != nil {
		t.Fatalf("consumerEvidenceFromFlags: %v", err)
	}
	if evidence.RunID != "run" || evidence.ItemID != "item" || evidence.Status != "timeout" || evidence.ElapsedMS != 42 || evidence.Error != "deadline" || evidence.Result != "fail" || evidence.QualityGate != "schema" {
		t.Fatalf("consumer evidence = %+v", evidence)
	}
	empty, err := consumerEvidenceFromFlags("", "", "", 0, "", "", "")
	if err != nil {
		t.Fatalf("empty consumer evidence: %v", err)
	}
	if empty != nil {
		t.Fatalf("empty consumer evidence = %+v", empty)
	}
	if _, err := consumerEvidenceFromFlags("", "", "lost", 0, "", "", ""); err == nil || !strings.Contains(err.Error(), "--consumer-status") {
		t.Fatalf("bad status err = %v", err)
	}
	if _, err := consumerEvidenceFromFlags("", "", "", -1, "", "", ""); err == nil || !strings.Contains(err.Error(), "--consumer-elapsed-ms") {
		t.Fatalf("bad elapsed err = %v", err)
	}
	if _, err := consumerEvidenceFromFlags("", "", "", 0, "", "maybe", ""); err == nil || !strings.Contains(err.Error(), "--consumer-result") {
		t.Fatalf("bad result err = %v", err)
	}
}

func TestGatewayPreflightEvidenceHelpers(t *testing.T) {
	path := writeGatewayPreflightEvidenceFixture(t, "job-a")
	evidence, err := readGatewayPreflightEvidence(path)
	if err != nil {
		t.Fatalf("readGatewayPreflightEvidence: %v", err)
	}
	if got := preflightEvidenceJobID(evidence); got != "job-a" {
		t.Fatalf("preflightEvidenceJobID=%q", got)
	}

	evidence.Headers = map[string]string{"x-myc-job": "job-lower"}
	if got := preflightEvidenceJobID(evidence); got != "job-lower" {
		t.Fatalf("case-insensitive preflightEvidenceJobID=%q", got)
	}
	evidence.Headers = nil
	if got := preflightEvidenceJobID(evidence); got != "" {
		t.Fatalf("empty preflightEvidenceJobID=%q", got)
	}

	copyPath := filepath.Join(t.TempDir(), "preflight-copy.json")
	if err := copyPreflightEvidenceFile(path, copyPath); err != nil {
		t.Fatalf("copyPreflightEvidenceFile: %v", err)
	}
	copied, err := os.ReadFile(copyPath)
	if err != nil {
		t.Fatalf("read copied preflight: %v", err)
	}
	if !strings.Contains(string(copied), `"X-Myc-Job": "job-a"`) || strings.Contains(string(copied), "Bearer") {
		t.Fatalf("copied preflight = %s", copied)
	}

	badPath := filepath.Join(t.TempDir(), "bad-preflight.json")
	if err := os.WriteFile(badPath, []byte(`{bad json`), 0600); err != nil {
		t.Fatalf("write bad preflight: %v", err)
	}
	if _, err := readGatewayPreflightEvidence(badPath); err == nil || !strings.Contains(err.Error(), "decode gateway preflight evidence") {
		t.Fatalf("bad preflight err = %v", err)
	}
	if _, err := readGatewayPreflightEvidence(filepath.Join(t.TempDir(), "missing.json")); err == nil || !strings.Contains(err.Error(), "read gateway preflight evidence") {
		t.Fatalf("missing preflight err = %v", err)
	}

	evidence.Streaming = true
	evidence.StreamDone = true
	evidence.UsageAvailable = false
	evidence.Usage = api.OpenAIUsage{}
	evidence.LiveModel = &gatewayPreflightLiveModelEvidence{
		Checked:        true,
		Status:         "missing",
		ModelAvailable: false,
		ModelCount:     3,
		Error:          "gateway models returned 404",
	}
	var summary strings.Builder
	printPreflightEvidenceSummary(&summary, evidence)
	if !strings.Contains(summary.String(), "preflight-stream\tdone=true\tusage_available=false") || !strings.Contains(summary.String(), "preflight-usage\tunavailable") || !strings.Contains(summary.String(), "preflight-live-model\tstatus=missing\tavailable=false\tmatch=-\tmodels=3\terror=gateway models returned 404") {
		t.Fatalf("streaming preflight summary = %s", summary.String())
	}
}

func TestOwnerTelemetryFetchFailureModes(t *testing.T) {
	dbPath, _ := writeOperatorDebugFixture(t)
	baseReport, err := buildJobDebugReport(context.Background(), dbPath, "job-a")
	if err != nil {
		t.Fatalf("buildJobDebugReport: %v", err)
	}

	noOwner := fetchJobOwnerTelemetry(context.Background(), dbPath, jobDebugReport{Job: domain.Job{ID: "job-missing-owner"}}, "rpc-secret", operatorOwnerTelemetryClient(t))
	if noOwner.Error != "owner node is not known" {
		t.Fatalf("no owner telemetry = %+v", noOwner)
	}
	noToken := fetchJobOwnerTelemetry(context.Background(), dbPath, baseReport, "", operatorOwnerTelemetryClient(t))
	if noToken.Error != "rpc token is required for owner telemetry" {
		t.Fatalf("no token telemetry = %+v", noToken)
	}

	missingAddressDir := t.TempDir()
	missingAddressDB := filepath.Join(missingAddressDir, "control.db")
	store, err := storesqlite.Open(missingAddressDB)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{ID: "node-a", Status: domain.NodeReady}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	noAddress := fetchJobOwnerTelemetry(context.Background(), missingAddressDB, baseReport, "rpc-secret", operatorOwnerTelemetryClient(t))
	if !strings.Contains(noAddress.Error, `owner node "node-a" has no known address`) {
		t.Fatalf("no address telemetry = %+v", noAddress)
	}

	unauthorized := fetchJobOwnerTelemetry(context.Background(), dbPath, baseReport, "bad-token", operatorOwnerTelemetryClient(t))
	if !strings.Contains(unauthorized.Error, "rpc token required") {
		t.Fatalf("unauthorized telemetry = %+v", unauthorized)
	}

	badJSONClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"rpc token required"}`))
			return
		}
		_, _ = w.Write([]byte(`{bad json`))
	}))
	badJSON := fetchJobOwnerTelemetry(context.Background(), dbPath, baseReport, "rpc-secret", badJSONClient)
	if !strings.Contains(badJSON.Error, "decode owner telemetry") {
		t.Fatalf("bad json telemetry = %+v", badJSON)
	}

	preflight := gatewayPreflightEvidence{Headers: map[string]string{"x-myc-node": "node-a"}}
	if got := jobOwnerNode(jobDebugReport{Job: domain.Job{ID: "job-a"}, Preflight: &preflight}); got != "node-a" {
		t.Fatalf("jobOwnerNode from preflight = %q", got)
	}
	if phases := ownerTelemetryPhases([]domain.SessionMetric{{Phase: domain.TelemetryPhasePlaced}, {Phase: domain.TelemetryPhaseComplete}}); phases != "placed,complete" {
		t.Fatalf("ownerTelemetryPhases = %q", phases)
	}
	if clientOrDefault(nil) != http.DefaultClient {
		t.Fatalf("clientOrDefault(nil) did not return default client")
	}
	limited := limitTail([]int{1, 2, 3}, 0)
	if len(limited) != 3 {
		t.Fatalf("limitTail zero limit = %v", limited)
	}
}

func TestJobDebugDiagnosisClassifiesConsumerEdges(t *testing.T) {
	completeCold := diagnoseJobDebugReport(jobDebugReport{
		Job: domain.Job{ID: "job-cold", Status: domain.JobDone},
		Preflight: &gatewayPreflightEvidence{Headers: map[string]string{
			"X-Myc-Node":              "spark",
			"X-Myc-Cold-Load":         "true",
			"X-Myc-Upstream-Wall-Ms":  "1759",
			"X-Myc-Structured-Output": "json_schema",
		}},
		OwnerTelemetry: &ownerTelemetryReport{
			RunMetrics: []domain.RunMetric{{NodeID: "spark", LoadWallClockMS: 158807, TTFTms: 1759}},
			SessionTimeline: []domain.SessionMetric{
				{Phase: domain.TelemetryPhasePlaced},
				{Phase: domain.TelemetryPhaseLoadReady},
				{Phase: domain.TelemetryPhaseUpstreamStart},
				{Phase: domain.TelemetryPhaseComplete},
			},
		},
	})
	if completeCold.Class != "owner_completed_cold_load" || !strings.Contains(completeCold.Summary, "load_ms=158807") || !strings.Contains(completeCold.Next, "consumer timed out") {
		t.Fatalf("complete cold diagnosis = %+v", completeCold)
	}

	telemetryMissing := diagnoseJobDebugReport(jobDebugReport{OwnerTelemetry: &ownerTelemetryReport{Error: "rpc token required"}})
	if telemetryMissing.Class != "owner_telemetry_unavailable" {
		t.Fatalf("telemetry missing diagnosis = %+v", telemetryMissing)
	}

	cleanupRequired := diagnoseJobDebugReport(jobDebugReport{Registry: &domain.JobRecord{CleanupRequired: true}})
	if cleanupRequired.Class != "cleanup_required" {
		t.Fatalf("cleanup diagnosis = %+v", cleanupRequired)
	}

	failed := diagnoseJobDebugReport(jobDebugReport{
		Job:            domain.Job{ID: "job-failed", Status: domain.JobFailed},
		OwnerTelemetry: &ownerTelemetryReport{SessionTimeline: []domain.SessionMetric{{Phase: domain.TelemetryPhaseError, Error: "context overflow"}}},
	})
	if failed.Class != "owner_failed" || !strings.Contains(failed.Summary, "context overflow") {
		t.Fatalf("failed diagnosis = %+v", failed)
	}

	inflight := diagnoseJobDebugReport(jobDebugReport{Job: domain.Job{ID: "job-loading", Status: domain.JobLoading}})
	if inflight.Class != "inflight_or_stale" {
		t.Fatalf("inflight diagnosis = %+v", inflight)
	}

	doneNoOwner := diagnoseJobDebugReport(jobDebugReport{Job: domain.Job{ID: "job-done", Status: domain.JobDone}})
	if doneNoOwner.Class != "job_done_without_owner_complete" {
		t.Fatalf("done no owner diagnosis = %+v", doneNoOwner)
	}

	registryFailure := jobFailureText(jobDebugReport{Registry: &domain.JobRecord{CleanupError: "cleanup failed"}})
	if registryFailure != "cleanup failed" {
		t.Fatalf("registry cleanup failure text = %q", registryFailure)
	}
	recoveryFailure := jobFailureText(jobDebugReport{Registry: &domain.JobRecord{RecoveryNote: "recovery note"}})
	if recoveryFailure != "recovery note" {
		t.Fatalf("registry recovery failure text = %q", recoveryFailure)
	}
	localSampleFailure := jobFailureText(jobDebugReport{SessionTimeline: []domain.SessionMetric{{Error: "local sample error"}}})
	if localSampleFailure != "local sample error" {
		t.Fatalf("local sample failure text = %q", localSampleFailure)
	}
	headers := jobDebugReport{Preflight: &gatewayPreflightEvidence{Headers: map[string]string{"x-myc-cold-load": "true"}}}
	if got := preflightHeader(headers, "X-Myc-Cold-Load"); got != "true" {
		t.Fatalf("case-insensitive preflightHeader = %q", got)
	}
	if got := preflightHeader(jobDebugReport{}, "X-Myc-Cold-Load"); got != "" {
		t.Fatalf("empty preflightHeader = %q", got)
	}
}

func TestRunDoctorFailsMissingNonLoopbackTokens(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	configPath := filepath.Join(dir, "peer.json")
	if err := os.WriteFile(configPath, []byte(`{"id":"peer-a","listen":"http://peer.test","store_path":`+quoteJSON(dbPath)+`}`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	output := captureStdout(t, func() {
		err = RunWithClient(context.Background(), []string{"doctor", "--config", configPath, "--db", dbPath}, operatorSnapshotClient(t))
	})
	if err == nil {
		t.Fatalf("doctor succeeded unexpectedly:\n%s", output)
	}
	if !strings.Contains(output, "check\trpc-token\tfail") || !strings.Contains(output, "check\tgateway-token\tfail") {
		t.Fatalf("doctor output = %s", output)
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

	noContent := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeBenchmarkOpenAIStream(w, nil, api.OpenAIUsage{TotalTokens: 1})
	}))
	if _, err := (benchmarkGatewayClient{BaseURL: "http://gateway-no-content.test", Client: noContent}).Complete(ctx, "tiny", "prompt"); err == nil || !strings.Contains(err.Error(), "no assistant content") {
		t.Fatalf("no content err = %v", err)
	}
}

func TestReadBenchmarkStreamRecordsTTFTAndTokensPerSecond(t *testing.T) {
	start := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	times := []time.Time{
		start.Add(50 * time.Millisecond),
		start.Add(150 * time.Millisecond),
		start.Add(350 * time.Millisecond),
		start.Add(650 * time.Millisecond),
		start.Add(850 * time.Millisecond),
	}
	now := func() time.Time {
		if len(times) == 0 {
			t.Fatal("unexpected clock read")
		}
		next := times[0]
		times = times[1:]
		return next
	}
	stream := strings.NewReader(
		"event: loading\n" +
			"data: {\"action\":\"cold-load\"}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"alpha \"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"beta\"}}]}\n\n" +
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"total_tokens\":7}}\n\n" +
			"data: [DONE]\n\n",
	)
	completion, err := readBenchmarkStream(stream, start, now)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if completion.Text != "alpha beta" {
		t.Fatalf("text = %q", completion.Text)
	}
	if completion.TTFTms != 150 {
		t.Fatalf("ttft = %d", completion.TTFTms)
	}
	if completion.ContextTokens != 7 {
		t.Fatalf("context tokens = %d", completion.ContextTokens)
	}
	if completion.TokensPerSec < 5.71 || completion.TokensPerSec > 5.72 {
		t.Fatalf("tokens/sec = %.4f", completion.TokensPerSec)
	}
}

func writeBenchmarkOpenAIStream(w http.ResponseWriter, parts []string, usage api.OpenAIUsage) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, part := range parts {
		encoded, _ := json.Marshal(part)
		_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%s}}]}\n\n", encoded)
	}
	_, _ = fmt.Fprintf(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":%d,\"completion_tokens\":%d,\"total_tokens\":%d}}\n\n",
		usage.PromptTokens,
		usage.CompletionTokens,
		usage.TotalTokens,
	)
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
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

func TestRunDebugJobCanIncludeReconciliationPlan(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	decision := domain.PlacementDecision{
		JobID:  "job-reconcile",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
		Trace: []domain.TraceStep{{
			Step: "filter",
			Data: map[string]any{"dropped": map[string]string{
				"node-engine": "label." + domain.LabelPeerBackends,
			}},
		}},
	}
	if err := store.SaveJob(context.Background(), domain.Job{
		ID:     "job-reconcile",
		Model:  "model-a",
		Status: domain.JobQueued,
		Result: &domain.JobResult{Decision: decision},
	}); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var errRun error
	output := captureStdout(t, func() {
		errRun = Run(context.Background(), []string{"debug", "job", "job-reconcile", "--db", dbPath, "--reconcile"})
	})
	if errRun != nil {
		t.Fatalf("debug job reconcile: %v\n%s", errRun, output)
	}
	for _, want := range []string{
		"reconcile\tjob=job-reconcile\tmodel=model-a\tpreset=preset-a\taction=queued",
		"reconcile-task\tkind=verify_engine_readiness\tnode=node-engine",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("debug reconcile output missing %q:\n%s", want, output)
		}
	}

	jsonOutput := captureStdout(t, func() {
		errRun = Run(context.Background(), []string{"debug", "job", "job-reconcile", "--db", dbPath, "--reconcile", "--json"})
	})
	if errRun != nil {
		t.Fatalf("debug job reconcile json: %v\n%s", errRun, jsonOutput)
	}
	var report jobDebugReport
	if err := json.Unmarshal([]byte(jsonOutput), &report); err != nil {
		t.Fatalf("decode debug reconcile json: %v\n%s", err, jsonOutput)
	}
	if report.Reconciliation == nil || len(report.Reconciliation.Tasks) != 1 || report.Reconciliation.Tasks[0].Kind != "verify_engine_readiness" {
		t.Fatalf("reconciliation report = %+v", report.Reconciliation)
	}
}

func writeOperatorDebugFixture(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	if err := store.SaveNode(context.Background(), domain.Node{
		ID:           "node-a",
		Name:         "Node A",
		Address:      "http://peer.test",
		Status:       domain.NodeReady,
		HeartbeatAt:  now,
		Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 24576}},
	}); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.SaveNode(context.Background(), domain.Node{
		ID:           "owner-node",
		Name:         "Owner B",
		Address:      "http://owner.test",
		Status:       domain.NodeReady,
		HeartbeatAt:  now,
		Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 24576}},
	}); err != nil {
		t.Fatalf("SaveNode owner: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPreset("tiny")); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveInstance(context.Background(), domain.ModelInstance{ID: "inst-a", NodeID: "node-a", PresetID: "tiny", State: domain.InstReady}); err != nil {
		t.Fatalf("SaveInstance: %v", err)
	}
	if err := store.SaveInstance(context.Background(), domain.ModelInstance{ID: "inst_2", NodeID: "owner-node", PresetID: "tiny", State: domain.InstReady}); err != nil {
		t.Fatalf("SaveInstance owner: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{
		ID:       "job-a",
		TaskType: "chat",
		Model:    "tiny",
		Project:  "project-a",
		Status:   domain.JobRunning,
		Progress: []domain.JobProgress{{Stage: "placed", Message: "node-a", At: now}},
	}); err != nil {
		t.Fatalf("SaveJob job-a: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{ID: "job-other", TaskType: "chat", Model: "tiny", Project: "project-b", Status: domain.JobDone}); err != nil {
		t.Fatalf("SaveJob job-other: %v", err)
	}
	request, err := peercoord.EncodeRescuePayload(domain.Job{ID: "job-a", TaskType: "chat", Model: "tiny", Project: "project-a"}, []byte(`{"model":"tiny"}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	if err := store.Put(context.Background(), domain.JobRecord{
		JobID:        "job-a",
		Coordinator:  "peer-a",
		AssignedNode: "node-a",
		Status:       domain.JobRunning,
		Request:      request,
		Fence:        3,
		UpdatedAt:    now.Add(time.Second),
	}); err != nil {
		t.Fatalf("Put registry: %v", err)
	}
	if err := store.Record(context.Background(), domain.RunMetric{
		JobID:        "job-a",
		InstanceID:   "inst-a",
		NodeID:       "node-a",
		PresetID:     "tiny",
		Backend:      domain.BackendLlamaCpp,
		Project:      "project-a",
		TokensPerSec: 42,
		TTFTms:       12,
		ContextUsed:  4096,
		At:           now.Add(time.Second),
	}); err != nil {
		t.Fatalf("Record metric: %v", err)
	}
	if err := store.RecordSample(context.Background(), domain.SessionMetric{
		SessionID:  "session-a",
		Sequence:   1,
		JobID:      "job-a",
		Phase:      domain.TelemetryPhaseError,
		InstanceID: "inst-a",
		NodeID:     "node-a",
		Project:    "project-a",
		PresetID:   "tiny",
		Error:      "context overflow",
		At:         now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("RecordSample: %v", err)
	}
	if err := store.SaveRecommendation(context.Background(), domain.RecommendationRecord{
		ID:               "rec-a",
		ProjectID:        "project-a",
		Type:             optimizer.RecommendationContextCap,
		RecommendedValue: 4096,
		CreatedAt:        now,
	}); err != nil {
		t.Fatalf("SaveRecommendation: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	configPath := filepath.Join(dir, "peer.json")
	config := `{
  "id": "peer-a",
  "listen": "http://peer.test",
  "store_path": ` + quoteJSON(dbPath) + `,
  "compute": true,
  "rpc_token": "rpc-secret",
  "gateway_token": "gateway-secret",
  "projects": [{"id": "strict-json-batch", "default_model": "strict-json-model", "context_cap": 4096}],
  "presets": [{"id": "tiny", "model_ref": "strict-json-model", "aliases": ["strict-json-model"], "backend": "vllm", "context_length": 4096, "node_id": "owner-node", "provider_profile": "openai-compatible"}]
}`
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return dbPath, configPath
}

func writeGatewayPreflightEvidenceFixture(t *testing.T, jobID string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "preflight.json")
	evidence := gatewayPreflightEvidence{
		Gateway:    "http://gateway.test",
		Model:      "strict-json-model",
		Project:    "strict-json-batch",
		AuthSource: "MYCELIUM_API_KEY",
		Request: gatewayPreflightRequestEvidence{
			MaxTokens:        32,
			StructuredOutput: "none",
			ToolRequested:    true,
			ToolName:         "echo",
			ToolChoice:       "auto",
		},
		Expectations: &gatewayPreflightExpectationEvidence{
			ExpectNode:            "owner-node",
			RequireToolCall:       true,
			RejectTextualToolCall: true,
			MinPromptTokens:       16,
		},
		StatusCode:      http.StatusOK,
		FinishReason:    "stop",
		ContentLength:   16,
		ReasoningLength: 0,
		TextualToolCall: true,
		JSONContent:     true,
		UsageAvailable:  true,
		Usage: api.OpenAIUsage{
			PromptTokens:     16,
			CompletionTokens: 10,
			TotalTokens:      26,
		},
		Headers: map[string]string{
			"X-Myc-Job":                              jobID,
			"X-Myc-Node":                             "owner-node",
			"X-Myc-Instance":                         "inst_2",
			"X-Myc-Backend":                          "vllm",
			"X-Myc-Decision":                         "placed_on_warm_instance",
			"X-Myc-Attempts":                         "1",
			"X-Myc-Cold-Load":                        "false",
			"X-Myc-Queue-Wait-Ms":                    "7",
			"X-Myc-Upstream-Wall-Ms":                 "512",
			"X-Myc-Structured-Output":                "json_schema",
			"X-Myc-Guided-Decoding-Requested":        "true",
			"X-Myc-Tools-Requested":                  "true",
			"X-Myc-Tool-Choice":                      "auto",
			"X-Myc-VLLM-Tool-Parser-Configured":      "true",
			"X-Myc-VLLM-Auto-Tool-Choice-Configured": "false",
		},
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal preflight evidence: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatalf("write preflight evidence: %v", err)
	}
	return path
}

func routeCertifiedPreflightFixture(jobID string) gatewayPreflightEvidence {
	return gatewayPreflightEvidence{
		Gateway:    "http://gateway.test",
		Model:      "strict-json-model",
		Project:    "strict-json-batch",
		AuthSource: "test-token-source",
		Request: gatewayPreflightRequestEvidence{
			MaxTokens:        32,
			ContextCap:       32768,
			StructuredOutput: "json_schema",
		},
		Expectations: &gatewayPreflightExpectationEvidence{
			ExpectNode:            "owner-node",
			RequireJSON:           true,
			RejectTextualToolCall: true,
			RequireLiveModel:      true,
			MinContextCap:         32768,
			ExpectFinishReason:    "stop",
			MinContentLength:      1,
			MaxReasoningLength:    intPtr(0),
		},
		StatusCode:      http.StatusOK,
		FinishReason:    "stop",
		ContentLength:   11,
		ReasoningLength: 0,
		JSONContent:     true,
		UsageAvailable:  true,
		Usage: api.OpenAIUsage{
			PromptTokens:     16,
			CompletionTokens: 3,
			TotalTokens:      19,
		},
		LiveModel: &gatewayPreflightLiveModelEvidence{
			Checked:        true,
			Gateway:        "http://gateway.test",
			Model:          "strict-json-model",
			Status:         "ok",
			ModelAvailable: true,
			ModelMatch:     "strict-json-model",
			ModelCount:     1,
		},
		Headers: map[string]string{
			"X-Myc-Job":                       jobID,
			"X-Myc-Node":                      "owner-node",
			"X-Myc-Backend":                   "vllm",
			"X-Myc-Decision":                  "placed_on_warm_instance",
			"X-Myc-Structured-Output":         "json_schema",
			"X-Myc-Guided-Decoding-Requested": "true",
		},
	}
}

func writeRouteContractsFixture(t *testing.T, path string, contracts routeContractFile) {
	t.Helper()
	writeJSONFixture(t, path, contracts)
}

func writeJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal JSON fixture: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatalf("write JSON fixture: %v", err)
	}
}

func writeGatewayPreflightEvidenceValue(t *testing.T, path string, evidence gatewayPreflightEvidence) {
	t.Helper()
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal preflight evidence: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatalf("write preflight evidence: %v", err)
	}
}

func intPtr(value int) *int {
	return &value
}

func writeGatewayPreflightOnlyLiveCatalogFailureFixture(t *testing.T, status, errorText string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "preflight.json")
	evidence := gatewayPreflightEvidence{
		Gateway:    "http://gateway.test",
		Model:      "tool-model-large",
		Project:    "tool-canary",
		AuthSource: "test-token-source",
		Request: gatewayPreflightRequestEvidence{
			MaxTokens:     256,
			ContextCap:    32768,
			ToolRequested: true,
			ToolName:      "echo",
			ToolChoice:    "function",
		},
		Expectations: &gatewayPreflightExpectationEvidence{
			RequireLiveModel: true,
			RequireToolCall:  true,
			MinContextCap:    32768,
		},
		StatusCode: 0,
		LiveModel: &gatewayPreflightLiveModelEvidence{
			Checked:        true,
			Gateway:        "http://gateway.test",
			Model:          "tool-model-large",
			Status:         status,
			ModelAvailable: false,
			Error:          errorText,
		},
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal preflight-only evidence: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatalf("write preflight-only evidence: %v", err)
	}
	return path
}

func writeGatewayPreflightEvidenceFixtureWithConsumer(t *testing.T, jobID string, consumer consumerEvidence) string {
	t.Helper()
	path := writeGatewayPreflightEvidenceFixture(t, jobID)
	evidence, err := readGatewayPreflightEvidence(path)
	if err != nil {
		t.Fatalf("read preflight evidence fixture: %v", err)
	}
	evidence.Consumer = &consumer
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal labeled preflight evidence: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatalf("write labeled preflight evidence: %v", err)
	}
	return path
}

func operatorSnapshotClient(t *testing.T) *http.Client {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"rpc token required"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{
			Node:      domain.Node{ID: "node-a", Address: "http://peer.test", Status: domain.NodeReady},
			Instances: []domain.ModelInstance{{ID: "inst-a", NodeID: "node-a", PresetID: "tiny", State: domain.InstReady}},
		})
	})
	mountOperatorTelemetryHandlers(t, mux)
	return directHTTPClient(mux)
}

func operatorOwnerTelemetryClient(t *testing.T) *http.Client {
	t.Helper()
	mux := http.NewServeMux()
	mountOperatorTelemetryHandlers(t, mux)
	return directHTTPClient(mux)
}

func mountOperatorTelemetryHandlers(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	checkAuth := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"rpc token required"}`))
			return false
		}
		return true
	}
	mux.HandleFunc("/telemetry/metrics", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		_ = json.NewEncoder(w).Encode([]domain.RunMetric{{
			JobID:        "job-a",
			InstanceID:   "inst-a",
			NodeID:       "node-a",
			PresetID:     "tiny",
			Backend:      domain.BackendVLLM,
			Project:      "project-a",
			TokensPerSec: 12.5,
			TTFTms:       34,
			ContextUsed:  26,
			At:           time.Date(2026, 6, 29, 13, 30, 0, 0, time.UTC),
		}})
	})
	mux.HandleFunc("/telemetry/samples", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		_ = json.NewEncoder(w).Encode([]domain.SessionMetric{{
			SessionID: "session-owner",
			Sequence:  1,
			JobID:     "job-a",
			Phase:     domain.TelemetryPhaseComplete,
			NodeID:    "node-a",
			Project:   "project-a",
			PresetID:  "tiny",
			At:        time.Date(2026, 6, 29, 13, 30, 1, 0, time.UTC),
		}})
	})
}

func quoteJSON(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func proofsContain(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func writeCalibrationInventoryFixture(t *testing.T) (string, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	cfg := controlFleetConfig()
	if err := store.SaveProject(context.Background(), domain.Project{ID: "calibration-project"}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	for _, node := range cfg.Simulation.Nodes {
		node.Address = "http://" + node.ID + ".test"
		if err := store.SaveNode(context.Background(), node); err != nil {
			t.Fatalf("save node %s: %v", node.ID, err)
		}
	}
	for _, preset := range cfg.Simulation.Presets {
		if err := store.SavePreset(context.Background(), preset); err != nil {
			t.Fatalf("save preset %s: %v", preset.ID, err)
		}
	}
	for _, inst := range cfg.Simulation.Instances {
		if err := store.SaveInstance(context.Background(), inst); err != nil {
			t.Fatalf("save instance %s: %v", inst.ID, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	configPath := filepath.Join(t.TempDir(), "peer.json")
	if err := os.WriteFile(configPath, []byte(`{
  "id": "gateway-peer",
  "listen": "http://gateway.test",
  "store_path": `+quoteJSON(dbPath)+`,
  "rpc_token": "rpc-secret",
  "gateway_token": "gateway-secret"
}`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return dbPath, configPath
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
