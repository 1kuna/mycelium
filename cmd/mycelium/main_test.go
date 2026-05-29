package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/optimizer"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
)

func TestRunDispatchesKnownCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "run", args: []string{"run"}, want: "read peer config"},
		{name: "ctl", args: []string{"ctl"}, want: "usage: myce <add-model|models|nodes|projects|jobs|recommendations>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run(%v) err = %v", tt.args, err)
			}
		})
	}
}

func TestRunControlAddModel(t *testing.T) {
	store := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "control.db")
	model := filepath.Join(t.TempDir(), "tiny.gguf")
	if err := os.WriteFile(model, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	err := runControl(context.Background(), []string{"add-model", "--store", store, "--db", dbPath, "--id", "tiny", "--model", "tiny-model", model})
	if err != nil {
		t.Fatalf("runControl add-model: %v", err)
	}
	preset, err := catalog.ReadPreset(store, "tiny")
	if err != nil {
		t.Fatalf("ReadPreset: %v", err)
	}
	if preset.ModelRef == model || !strings.Contains(preset.ModelRef, "tiny-tiny.gguf") {
		t.Fatalf("preset = %+v", preset)
	}
	if strings.Join(preset.Aliases, ",") != "tiny-model" {
		t.Fatalf("preset aliases = %+v", preset.Aliases)
	}
	control, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open control store: %v", err)
	}
	defer control.Close()
	if got, err := control.Preset(context.Background(), "tiny"); err != nil || got.ID != "tiny" || strings.Join(got.Aliases, ",") != "tiny-model" {
		t.Fatalf("control preset = %+v, %v", got, err)
	}
}

func TestRunControlListCommandsAndProjectSet(t *testing.T) {
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
		{"projects", "set", "--db", dbPath, "--id", "project-b", "--default-model", "preset-b", "--priority", "background", "--speed-pref", "latency", "--context-cap", "4096", "--preemption", "hard", "--auto-apply"},
	}
	for _, args := range commands {
		if err := runControl(context.Background(), args); err != nil {
			t.Fatalf("runControl(%v): %v", args, err)
		}
	}
	for _, args := range [][]string{
		{"models", "bad"},
		{"nodes", "bad"},
		{"projects", "bad"},
		{"jobs", "bad"},
		{"recommendations", "bad"},
		{"recommendations", "generate", "--db", dbPath},
		{"recommendations", "apply", "--db", dbPath},
	} {
		if err := runControl(context.Background(), args); err == nil {
			t.Fatalf("runControl(%v) expected error", args)
		}
	}
}

func TestLoadConfigsAndDefaultHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if got := defaultMyceliumHome(); got != filepath.Join(home, ".mycelium") {
		t.Fatalf("home = %s", got)
	}
	peerCfg, err := loadPeerConfig("")
	if err == nil || !strings.Contains(err.Error(), "read peer config") || peerCfg.Listen != "" {
		t.Fatalf("empty peer config = %+v %v", peerCfg, err)
	}
	peerPath := filepath.Join(t.TempDir(), "peer.json")
	if err := os.WriteFile(peerPath, []byte(`{}`), 0644); err != nil {
		t.Fatalf("write peer config: %v", err)
	}
	peerCfg, err = loadPeerConfig(peerPath)
	if err != nil {
		t.Fatalf("loadPeerConfig: %v", err)
	}
	if peerCfg.QueueDrainMS != 1000 || peerCfg.QueueDrainLimit != 1 || peerCfg.OptimizerEvalMS != 60000 {
		t.Fatalf("peer drain defaults = %+v", peerCfg)
	}
	if peerCfg.ComputeConfig.ID != "peer_local" || peerCfg.ComputeConfig.BackendListen != "127.0.0.1:51848" {
		t.Fatalf("compute defaults = %+v", peerCfg.ComputeConfig)
	}
	computePath := filepath.Join(t.TempDir(), "compute-peer.json")
	computeRaw := `{"compute":true,"compute_config":{"backend_listen":"127.0.0.1:8","id":"peer-json","name":"Peer JSON","backend":"mlx","backend_binary":"/bin/mlx","llama_server":"/bin/echo","vram_mb":1234,"max_util":0.7,"gguf_parser":"parser"}}`
	if err := os.WriteFile(computePath, []byte(computeRaw), 0644); err != nil {
		t.Fatalf("write compute peer config: %v", err)
	}
	computeCfg, err := loadPeerConfig(computePath)
	if err != nil {
		t.Fatalf("loadPeerConfig compute: %v", err)
	}
	if !computeCfg.Compute || computeCfg.ComputeConfig.ID != "peer-json" || computeCfg.ComputeConfig.VRAMMB != 1234 || computeCfg.ComputeConfig.GGUFParser != "parser" || computeCfg.ComputeConfig.Backend != domain.BackendMLX || computeCfg.ComputeConfig.BackendBinary != "/bin/mlx" {
		t.Fatalf("compute peer config = %+v", computeCfg)
	}
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte(`{`), 0644); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := loadPeerConfig(badPath); err == nil {
		t.Fatal("expected bad peer config error")
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

	if err := runControl(context.Background(), []string{"recommendations", "generate", "--db", dbPath, "--project", project.ID}); err != nil {
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

	if err := runControl(context.Background(), []string{"recommendations", "apply", "--db", dbPath, "--id", recs[0].ID}); err != nil {
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

	if err := runControl(context.Background(), []string{"recommendations", "apply", "--db", dbPath, "--id", rec.ID}); err != nil {
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

func TestBuildPeerGatewayWithJoinToken(t *testing.T) {
	preset := testPreset("tiny")
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.SaveLease(context.Background(), domain.Lease{ID: "expired", ExpiresAt: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatalf("SaveLease: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	configPath := writePeerConfig(t, PeerConfig{
		Listen:       "127.0.0.1:0",
		StorePath:    dbPath,
		JoinToken:    "secret",
		Presets:      []domain.Preset{preset},
		Reservations: []domain.Reservation{{ID: "pin-a", Kind: domain.ReservationPinned, NodeID: "node-a", PresetID: preset.ID}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, handler, err := buildPeerGateway(ctx, []string{"--config", configPath})
	if err != nil {
		t.Fatalf("buildPeerGateway: %v", err)
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nodes", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "[]\n" {
		t.Fatalf("nodes status/body = %d %q", rec.Code, rec.Body.String())
	}
	reopened, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if leases, err := reopened.ListLeases(context.Background()); err != nil || len(leases) != 0 {
		t.Fatalf("leases after boot = %+v %v", leases, err)
	}
}

func TestAllocatorFromReservationsReservesHeadroomAndPinnedPresets(t *testing.T) {
	preset := testPreset("tiny")
	preset.EstWeightsMB = 100
	preset.ContextLength = 1000
	preset.KVPerTokenMB = 0.5
	allocator := allocatorFromReservations([]domain.Reservation{
		{ID: "headroom", Kind: domain.ReservationHeadroom, NodeID: "node-a", Headroom: domain.Claim{WeightsMB: 10}},
		{ID: "pinned", Kind: domain.ReservationPinned, NodeID: "node-b", PresetID: preset.ID},
	}, presetMap([]domain.Preset{preset}))
	nodeA := domain.Node{ID: "node-a", MaxUtil: 1, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 100}}}
	if allocator.Fits(nodeA, []int{0}, nil, domain.Claim{WeightsMB: 95}) {
		t.Fatal("headroom reservation was not enforced")
	}
	nodeB := domain.Node{ID: "node-b", MaxUtil: 1, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 700}}}
	if allocator.Fits(nodeB, []int{0}, nil, domain.Claim{WeightsMB: 101}) {
		t.Fatal("pinned preset reservation was not enforced")
	}
}

func TestProjectMapIndexesByID(t *testing.T) {
	projects := projectMap([]domain.Project{{ID: "proj-a", ContextCap: 4096}})
	if projects["proj-a"].ContextCap != 4096 {
		t.Fatalf("projects = %+v", projects)
	}
}

func TestRestoreQueuedJobs(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.SaveJob(context.Background(), domain.Job{ID: "queued", Model: "tiny", Status: domain.JobQueued}); err != nil {
		t.Fatalf("SaveJob queued: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{ID: "done", Model: "tiny", Status: domain.JobDone}); err != nil {
		t.Fatalf("SaveJob done: %v", err)
	}
	queue := scheduler.NewQueue(clock.System{})
	if err := restoreQueuedJobs(context.Background(), store, queue); err != nil {
		t.Fatalf("restoreQueuedJobs: %v", err)
	}
	if queue.Len() != 1 {
		t.Fatalf("queue len = %d", queue.Len())
	}
	job, ok := queue.Dequeue()
	if !ok || job.ID != "queued" {
		t.Fatalf("dequeue = %+v %v", job, ok)
	}
}

func TestRunOptimizerEvaluationPersistsRecommendationsAndCalibratesSpeed(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	project := domain.Project{ID: "project-a", ContextCap: 16000, AutoApply: true}
	node := domain.Node{ID: "node-a", Name: "Node A", Status: domain.NodeReady}
	if err := store.SaveProject(context.Background(), project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SaveNode(context.Background(), node); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPresetWithContext("small", 6000)); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := store.SavePreset(context.Background(), testPresetWithContext("large", 16000)); err != nil {
		t.Fatalf("SavePreset large: %v", err)
	}
	for _, metric := range []domain.RunMetric{
		{JobID: "job-a", NodeID: node.ID, Project: project.ID, ContextUsed: 3500, TokensPerSec: 10, At: now},
		{JobID: "job-b", NodeID: node.ID, Project: project.ID, ContextUsed: 4000, TokensPerSec: 20, At: now.Add(time.Second)},
	} {
		if err := store.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	if err := runOptimizerEvaluation(context.Background(), store, clock.System{}); err != nil {
		t.Fatalf("runOptimizerEvaluation: %v", err)
	}
	appliedProject, err := store.Project(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if appliedProject.ContextCap != 6000 {
		t.Fatalf("project = %+v", appliedProject)
	}
	recs, err := store.ListRecommendations(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("ListRecommendations: %v", err)
	}
	if len(recs) != 1 || recs[0].Observed["avg_tokens"] != 3750 {
		t.Fatalf("recommendations = %+v", recs)
	}
	calibrated, err := store.Node(context.Background(), node.ID)
	if err != nil {
		t.Fatalf("Node: %v", err)
	}
	if calibrated.SpeedClass.TokensPerSecRef != 15 || calibrated.SpeedClass.Source != "telemetry-calibrated" {
		t.Fatalf("node = %+v", calibrated)
	}
}

func TestPeerEstimatorUsesGGUFParserWhenConfigured(t *testing.T) {
	if _, ok := peerEstimator(PeerConfig{}, nil).(*estimate.InMemoryEstimator); !ok {
		t.Fatal("default estimator should use preset estimates")
	}
	if _, ok := peerEstimator(PeerConfig{GGUFParser: "gguf-parser"}, nil).(*estimate.GGUFEstimator); !ok {
		t.Fatal("configured gguf parser should use GGUF estimator")
	}
}

func TestRunNodeAndPeerExitOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	configPath := writePeerConfig(t, PeerConfig{
		Listen:    "127.0.0.1:0",
		StorePath: filepath.Join(t.TempDir(), "control.db"),
		JoinToken: "secret",
		Presets:   []domain.Preset{testPreset("tiny")},
	})
	if err := runPeer(ctx, []string{"--config", configPath}); err != nil {
		t.Fatalf("runPeer canceled: %v", err)
	}
}

func TestBuildPeerGatewayWithLocalCompute(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	configPath := writePeerConfig(t, PeerConfig{
		Listen:    "127.0.0.1:0",
		StorePath: dbPath,
		Compute:   true,
		ComputeConfig: ComputeConfig{
			ID:            "peer-a",
			Name:          "Peer A",
			BackendListen: "127.0.0.1:51848",
			LlamaServer:   "/bin/echo",
			VRAMMB:        1024,
		},
		Presets: []domain.Preset{testPreset("tiny")},
	})
	addr, handler, err := buildPeerGateway(context.Background(), []string{"--config", configPath})
	if err != nil {
		t.Fatalf("buildPeerGateway: %v", err)
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/snapshot", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("snapshot status/body = %d %q", rec.Code, rec.Body.String())
	}
	var snap domain.NodeSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("snapshot json: %v", err)
	}
	if snap.Node.ID != "peer-a" || snap.Node.Labels[LabelPeerBackend] != string(domain.BackendLlamaCpp) {
		t.Fatalf("snapshot = %+v", snap)
	}
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if got, err := store.Node(context.Background(), "peer-a"); err != nil || got.ID != "peer-a" {
		t.Fatalf("stored node = %+v %v", got, err)
	}
}

func TestBuildComputeRuntimeSelectsConfiguredBackends(t *testing.T) {
	for _, tt := range []struct {
		backend domain.Backend
		name    string
	}{
		{backend: domain.BackendMLX, name: "mlx"},
		{backend: domain.BackendVLLM, name: "vllm"},
	} {
		t.Run(string(tt.backend), func(t *testing.T) {
			store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer store.Close()
			runtime, err := buildComputeRuntime(context.Background(), PeerConfig{
				Listen: "127.0.0.1:0",
				ComputeConfig: defaultedComputeConfig(ComputeConfig{
					ID:            "peer-a",
					Name:          "Peer A",
					Backend:       tt.backend,
					BackendBinary: "/bin/echo",
					VRAMMB:        1024,
				}),
			}, store)
			if err != nil {
				t.Fatalf("buildComputeRuntime: %v", err)
			}
			if runtime.node.Labels[LabelPeerBackend] != string(tt.backend) {
				t.Fatalf("node labels = %+v", runtime.node.Labels)
			}
			adapter, err := computeBackendAdapter(ComputeConfig{Backend: tt.backend, BackendBinary: "/bin/echo"}, nodeagent.StoreProcessRegistry{})
			if err != nil {
				t.Fatalf("computeBackendAdapter: %v", err)
			}
			if adapter.Name() != tt.name {
				t.Fatalf("adapter name = %s", adapter.Name())
			}
		})
	}
}

func TestBuildComputeRuntimeRejectsUnknownBackend(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	_, err = buildComputeRuntime(context.Background(), PeerConfig{
		Listen: "127.0.0.1:0",
		ComputeConfig: defaultedComputeConfig(ComputeConfig{
			Backend: domain.Backend("unknown"),
			VRAMMB:  1024,
		}),
	}, store)
	if err == nil || !strings.Contains(err.Error(), "unknown compute backend") {
		t.Fatalf("unknown backend err = %v", err)
	}
	if got := computeBackendBinary(ComputeConfig{Backend: domain.BackendLlamaCpp, BackendBinary: "/bin/custom", LlamaServer: "/bin/llama"}, "fallback"); got != "/bin/custom" {
		t.Fatalf("backend binary = %s", got)
	}
	if got := computeBackendBinary(ComputeConfig{Backend: domain.BackendLlamaCpp, LlamaServer: "/bin/llama"}, "fallback"); got != "/bin/llama" {
		t.Fatalf("llama binary = %s", got)
	}
	if got := computeBackendBinary(ComputeConfig{Backend: domain.BackendMLX}, "mlx_lm.server"); got != "mlx_lm.server" {
		t.Fatalf("mlx binary = %s", got)
	}
}

func TestRunRejectsMissingAndUnknownCommand(t *testing.T) {
	for _, args := range [][]string{[]string{"bogus"}, []string{"server"}, []string{"node"}} {
		err := run(context.Background(), args)
		if err == nil {
			t.Fatalf("run(%v) expected error", args)
		}
	}
}

func writePeerConfig(t *testing.T, cfg PeerConfig) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
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
