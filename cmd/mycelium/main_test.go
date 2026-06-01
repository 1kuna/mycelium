package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/optimizer"
	peercoord "mycelium/internal/peer"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/test/mocks"
)

func TestRunDispatchesKnownCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "run", args: []string{"run"}, want: "read peer config"},
		{name: "ctl", args: []string{"ctl"}, want: "usage: myce <add-model|models|nodes|projects|jobs|recommendations|benchmark>"},
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
	if peerCfg.QueueDrainMS != 1000 || peerCfg.QueueDrainLimit != 1 || peerCfg.OptimizerEvalMS != 60000 || peerCfg.RegistrySyncMS != 1000 || peerCfg.DiscoveryScanMS != 250 || peerCfg.DiscoveryAdvertiseMS != 5000 {
		t.Fatalf("peer drain defaults = %+v", peerCfg)
	}
	if peerCfg.ComputeConfig.ID != "peer_local" || peerCfg.ComputeConfig.BackendListen != "127.0.0.1:51848" {
		t.Fatalf("compute defaults = %+v", peerCfg.ComputeConfig)
	}
	computePath := filepath.Join(t.TempDir(), "compute-peer.json")
	computeRaw := `{"compute":true,"overlay":true,"overlay_listen_addrs":["/ip4/127.0.0.1/tcp/0"],"overlay_bootstrap":["/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWFake"],"private_storage_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","submitter_policy":{"submitter-a":{"max_priority":"interactive","allow_private":true}},"compute_config":{"backend_listen":"127.0.0.1:8","id":"peer-json","name":"Peer JSON","backend":"mlx","backend_binary":"/bin/mlx","llama_server":"/bin/echo","vram_mb":1234,"max_util":0.7,"gguf_parser":"parser"}}`
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
	if !computeCfg.Overlay || len(computeCfg.OverlayListenAddrs) != 1 || len(computeCfg.OverlayBootstrap) != 1 {
		t.Fatalf("overlay config = %+v", computeCfg)
	}
	if computeCfg.PrivateStorageKey == "" || !computeCfg.SubmitterPolicy["submitter-a"].AllowPrivate {
		t.Fatalf("private config = %+v", computeCfg)
	}
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte(`{`), 0644); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := loadPeerConfig(badPath); err == nil {
		t.Fatal("expected bad peer config error")
	}
}

func TestPrivateStorageKeyValidation(t *testing.T) {
	key, err := privateStorageKey("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil || string(key) != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("privateStorageKey = %q %v", key, err)
	}
	if key, err := privateStorageKey(""); err != nil || key != nil {
		t.Fatalf("empty privateStorageKey = %q %v", key, err)
	}
	if _, err := privateStorageKey("short"); err == nil {
		t.Fatal("short private storage key accepted")
	}
	if got := privateLocalNodeID(PeerConfig{}); got != "" {
		t.Fatalf("non-compute private node = %q", got)
	}
	if got := privateLocalNodeID(PeerConfig{Compute: true, ComputeConfig: ComputeConfig{ID: "peer-a"}}); got != "peer-a" {
		t.Fatalf("private node = %q", got)
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
		RPCToken:     "rpc-secret",
		Presets:      []domain.Preset{preset},
		Reservations: []domain.Reservation{{ID: "pin-a", Kind: domain.ReservationPinned, NodeID: "node-a", PresetID: preset.ID}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, handler, cleanup, err := buildPeerGateway(ctx, []string{"--config", configPath})
	if err != nil {
		t.Fatalf("buildPeerGateway: %v", err)
	}
	if cleanup != nil {
		t.Fatal("gateway-only peer unexpectedly returned compute cleanup")
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/peer/health", nil)
	req.Header.Set("X-Myc-Join-Token", "secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("peer health status/body = %d %q", rec.Code, rec.Body.String())
	}
	var peer domain.Peer
	if err := json.Unmarshal(rec.Body.Bytes(), &peer); err != nil {
		t.Fatalf("decode peer health: %v", err)
	}
	if peer.ID == "" || len(peer.Addresses) != 1 || peer.Addresses[0] != "127.0.0.1:0" {
		t.Fatalf("peer health = %+v", peer)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/peer/health", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("peer health without join token status/body = %d %q", rec.Code, rec.Body.String())
	}
	reopened, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if leases, err := reopened.ListLeases(context.Background()); err != nil || len(leases) != 0 {
		t.Fatalf("leases after boot = %+v %v", leases, err)
	}
	tokens, err := reopened.ListJoinTokens(context.Background())
	if err != nil {
		t.Fatalf("ListJoinTokens: %v", err)
	}
	if len(tokens) != 1 || !tokens[0].Active || !tokens[0].Current {
		t.Fatalf("tokens after boot = %+v", tokens)
	}
}

func TestParseJoinFlag(t *testing.T) {
	if join, err := parseJoinFlag("secret"); err != nil || join.Token != "secret" || join.RPCToken != "" {
		t.Fatalf("raw join = %+v %v", join, err)
	}
	joinURI, err := membership.BuildJoinTokenWithRPC("secret", "rpc-secret")
	if err != nil {
		t.Fatalf("BuildJoinToken: %v", err)
	}
	if join, err := parseJoinFlag(joinURI); err != nil || join.Token != "secret" || join.RPCToken != "rpc-secret" {
		t.Fatalf("join uri = %+v %v", join, err)
	}
	if join, err := parseJoinFlag("mycjoin://127.0.0.1:51846?token=secret&rpc_token=rpc-secret"); err != nil || join.Address != "127.0.0.1:51846" {
		t.Fatalf("seed join uri = %+v %v", join, err)
	}
	if _, err := parseJoinFlag(""); err == nil {
		t.Fatal("empty join token accepted")
	}
}

func TestBuildPeerGatewayRequiresRPCTokenForJoin(t *testing.T) {
	configPath := writePeerConfig(t, PeerConfig{
		Listen:    "127.0.0.1:0",
		StorePath: filepath.Join(t.TempDir(), "control.db"),
		JoinToken: "secret",
		Presets:   []domain.Preset{testPreset("tiny")},
	})
	if _, _, _, err := buildPeerGateway(context.Background(), []string{"--config", configPath}); err == nil || !strings.Contains(err.Error(), "rpc_token") {
		t.Fatalf("missing rpc token err = %v", err)
	}
}

func TestPeerHealthProbeAndDeadMarker(t *testing.T) {
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"http://placeholder"}, Compute: true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/peer/health" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		peer.Addresses[0] = serverURL(r)
		_ = json.NewEncoder(w).Encode(peer)
	}))
	defer server.Close()
	peer.Addresses[0] = server.URL

	if err := probePeerHealth(context.Background(), peer); err != nil {
		t.Fatalf("probePeerHealth: %v", err)
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(domain.Peer{ID: "other"})
	}))
	defer bad.Close()
	if err := probePeerHealth(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{bad.URL}}); err == nil || !strings.Contains(err.Error(), "other") {
		t.Fatalf("bad peer err = %v", err)
	}
	if err := probePeerHealth(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("unreachable err = %v", err)
	}

	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := markDeadPeer(store)(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}); err != nil {
		t.Fatalf("markDeadPeer: %v", err)
	}
	node, err := store.Node(context.Background(), "peer-a")
	if err != nil {
		t.Fatalf("Node: %v", err)
	}
	if node.Status != domain.NodeUnreachable {
		t.Fatalf("node = %+v", node)
	}
}

func TestRegistryRPCRequiresAuthAndMergesRecords(t *testing.T) {
	ctx := context.Background()
	store := peerTestRegistry(t)
	local := domain.JobRecord{
		JobID:       "job-local",
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     []byte(`{"job":"local"}`),
		UpdatedAt:   time.Unix(20, 0).UTC(),
	}
	if err := store.Put(ctx, local); err != nil {
		t.Fatalf("Put local: %v", err)
	}
	mux := http.NewServeMux()
	mountRegistryHTTP(mux, store, "rpc-secret")
	server := httptest.NewServer(mux)
	defer server.Close()
	peer := domain.Peer{ID: "peer-a", Addresses: []string{server.URL}}

	if _, err := (registryHTTPClient{}).Snapshot(ctx, peer); err == nil || !strings.Contains(err.Error(), "rpc token") {
		t.Fatalf("unauthorized snapshot err = %v", err)
	}
	client := registryHTTPClient{AuthToken: "rpc-secret"}
	records, err := client.Snapshot(ctx, peer)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(records) != 1 || records[0].JobID != local.JobID {
		t.Fatalf("snapshot = %+v", records)
	}
	remote := domain.JobRecord{
		JobID:       "job-remote",
		Coordinator: "peer-b",
		Status:      domain.JobQueued,
		Request:     []byte(`{"job":"remote"}`),
		UpdatedAt:   time.Unix(21, 0).UTC(),
	}
	if err := client.Push(ctx, peer, []domain.JobRecord{remote}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	records, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot local: %v", err)
	}
	if len(records) != 2 || records[1].JobID != "job-remote" {
		t.Fatalf("local registry = %+v", records)
	}
}

func TestTelemetryRPCRequiresAuthAndMergesMetricsAndRecommendations(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	localMetric := domain.RunMetric{JobID: "job-local", NodeID: "peer-a", Project: "project-a", TokensPerSec: 12, At: time.Unix(20, 0).UTC()}
	if err := store.Record(ctx, localMetric); err != nil {
		t.Fatalf("Record local: %v", err)
	}
	mux := http.NewServeMux()
	mountTelemetryHTTP(mux, store, "rpc-secret")
	server := httptest.NewServer(mux)
	defer server.Close()
	peer := domain.Peer{ID: "peer-a", Addresses: []string{server.URL}}

	if _, err := (telemetryHTTPClient{}).Metrics(ctx, peer); err == nil || !strings.Contains(err.Error(), "rpc token") {
		t.Fatalf("unauthorized metrics err = %v", err)
	}
	client := telemetryHTTPClient{AuthToken: "rpc-secret"}
	metrics, err := client.Metrics(ctx, peer)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if len(metrics) != 1 || metrics[0].JobID != localMetric.JobID {
		t.Fatalf("metrics = %+v", metrics)
	}
	remoteMetric := domain.RunMetric{JobID: "job-remote", NodeID: "peer-b", Project: "project-a", TokensPerSec: 18, At: time.Unix(21, 0).UTC()}
	if err := client.PushMetrics(ctx, peer, []domain.RunMetric{remoteMetric}); err != nil {
		t.Fatalf("PushMetrics: %v", err)
	}
	metrics, err = store.Metrics(ctx, "")
	if err != nil {
		t.Fatalf("Metrics local: %v", err)
	}
	if len(metrics) != 2 || metrics[1].JobID != remoteMetric.JobID {
		t.Fatalf("local metrics = %+v", metrics)
	}
	rec := domain.RecommendationRecord{ID: "rec-a", Type: optimizer.RecommendationContextCap, ProjectID: "project-a", CreatedAt: time.Unix(22, 0).UTC()}
	if err := client.PushRecommendations(ctx, peer, []domain.RecommendationRecord{rec}); err != nil {
		t.Fatalf("PushRecommendations: %v", err)
	}
	recs, err := client.Recommendations(ctx, peer)
	if err != nil {
		t.Fatalf("Recommendations: %v", err)
	}
	if len(recs) != 1 || recs[0].ID != rec.ID {
		t.Fatalf("recommendations = %+v", recs)
	}
}

func TestMountNodeHTTPIncludesAdmissionRuntimeRoutes(t *testing.T) {
	mux := http.NewServeMux()
	seen := map[string]bool{}
	mountNodeHTTP(mux, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.URL.Path] = true
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, path := range []string{
		"/admission/bind-instance",
		"/admission/lease-by-instance",
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		if rec.Code != http.StatusNoContent || !seen[path] {
			t.Fatalf("%s status=%d seen=%+v", path, rec.Code, seen)
		}
	}
}

func TestRescueRecoveredJobDecodesPayloadAndSubmits(t *testing.T) {
	job := domain.Job{ID: "job-a", Model: "tiny", Priority: domain.PriorityInteractive}
	payload, err := peercoord.EncodeRescuePayload(job, []byte(`{"model":"tiny"}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	runtime := &recordingRescueRuntime{}
	rescue := rescueRecoveredJob(runtime)

	if err := rescue(context.Background(), domain.JobRecord{JobID: job.ID, Request: payload}); err != nil {
		t.Fatalf("rescue: %v", err)
	}
	if runtime.job.ID != job.ID || string(runtime.payload) != `{"model":"tiny"}` {
		t.Fatalf("runtime job=%+v payload=%s", runtime.job, runtime.payload)
	}

	badPayload, err := peercoord.EncodeRescuePayload(domain.Job{ID: "other"}, []byte(`{}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload bad: %v", err)
	}
	if err := rescue(context.Background(), domain.JobRecord{JobID: job.ID, Request: badPayload}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatch rescue err = %v", err)
	}
	if err := rescue(context.Background(), domain.JobRecord{JobID: job.ID, Request: []byte(`{}`)}); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("bad rescue err = %v", err)
	}
}

func TestSeedPeerProbeRemembersReachablePeer(t *testing.T) {
	seed := domain.Peer{ID: "seed-peer", Addresses: []string{"127.0.0.1:0"}, Compute: true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Myc-Join-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(seed)
	}))
	defer server.Close()
	cache := membership.NewCachedPeerDiscovery(&mocks.PeerDiscovery{}, mocks.NewFakeClock(time.Unix(1, 0).UTC()), time.Minute)

	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	probeSeedPeers(context.Background(), cache, []string{server.URL}, "secret", manager)
	peers, err := cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != "seed-peer" || peers[0].Addresses[0] != strings.TrimPrefix(server.URL, "http://") {
		t.Fatalf("seed peers = %+v", peers)
	}
	if _, err := fetchPeerHealth(context.Background(), server.URL, "wrong"); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("wrong join token err = %v", err)
	}
	if err := manager.Revoke("secret"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	cache = membership.NewCachedPeerDiscovery(&mocks.PeerDiscovery{}, mocks.NewFakeClock(time.Unix(1, 0).UTC()), time.Minute)
	probeSeedPeers(context.Background(), cache, []string{server.URL}, "secret", manager)
	peers, err = cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers after revoke: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("revoked token seed peers = %+v", peers)
	}
	if got := seedPeerProbeInterval(time.Millisecond); got != 5*time.Second {
		t.Fatalf("seed interval = %s", got)
	}
}

func TestPeerHealthUsesPersistentTokenManager(t *testing.T) {
	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	mux := http.NewServeMux()
	mountPeerHTTP(mux, domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}, manager)

	req := httptest.NewRequest(http.MethodGet, "/peer/health", nil)
	req.Header.Set("X-Myc-Join-Token", "secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health before revoke = %d %s", rec.Code, rec.Body.String())
	}

	if err := manager.Revoke("secret"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/peer/health", nil)
	req.Header.Set("X-Myc-Join-Token", "secret")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("health after revoke = %d %s", rec.Code, rec.Body.String())
	}
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}

func peerTestRegistry(t *testing.T) *storesqlite.Store {
	t.Helper()
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open registry: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

type recordingRescueRuntime struct {
	job     domain.Job
	payload []byte
}

func (r *recordingRescueRuntime) SubmitWithPayload(_ context.Context, job domain.Job, payload []byte, _ ...scheduler.SubmitHooks) (scheduler.Result, error) {
	r.job = job
	r.payload = append([]byte(nil), payload...)
	return scheduler.Result{}, nil
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

func TestComputeAdmissionAllocatorUsesStoreReservations(t *testing.T) {
	ctx := context.Background()
	store := peerTestRegistry(t)
	preset := testPreset("tiny")
	preset.EstWeightsMB = 100
	preset.ContextLength = 1000
	preset.KVPerTokenMB = 0.5
	if err := store.SavePreset(ctx, preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveReservation(ctx, domain.Reservation{ID: "headroom", Kind: domain.ReservationHeadroom, NodeID: "node-a", Headroom: domain.Claim{WeightsMB: 10}}); err != nil {
		t.Fatalf("SaveReservation headroom: %v", err)
	}
	if err := store.SaveReservation(ctx, domain.Reservation{ID: "pinned", Kind: domain.ReservationPinned, NodeID: "node-a", PresetID: preset.ID}); err != nil {
		t.Fatalf("SaveReservation pinned: %v", err)
	}
	allocator, pinned, err := computeAdmissionAllocator(ctx, store, "node-a")
	if err != nil {
		t.Fatalf("computeAdmissionAllocator: %v", err)
	}
	node := domain.Node{ID: "node-a", MaxUtil: 1, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 700}}}
	if allocator.Fits(node, []int{0}, nil, domain.Claim{WeightsMB: 101}) {
		t.Fatal("store-backed reservations were not enforced")
	}
	if !reflect.DeepEqual(pinned, []string{"pinned"}) {
		t.Fatalf("pinned ids = %+v", pinned)
	}
}

func TestLoadPinnedReservationsWarmsAndProtectsPreset(t *testing.T) {
	ctx := context.Background()
	store := peerTestRegistry(t)
	preset := testPreset("tiny")
	preset.EstWeightsMB = 10
	preset.KVPerTokenMB = 0.01
	if err := store.SavePreset(ctx, preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveReservation(ctx, domain.Reservation{ID: "pin-a", Kind: domain.ReservationPinned, NodeID: "node-a", PresetID: preset.ID}); err != nil {
		t.Fatalf("SaveReservation: %v", err)
	}
	node := domain.Node{ID: "node-a", MaxUtil: 1, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 100}}}
	backend := mocks.NewBackendAdapter()
	agent := nodeagent.NewAgent(node, backend, mocks.NewFakeClock(time.Unix(1, 0).UTC()), nodeagent.WithAllocator(lease.NewAllocator()))

	if err := loadPinnedReservations(ctx, agent, store, node.ID); err != nil {
		t.Fatalf("loadPinnedReservations: %v", err)
	}
	snap, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Instances) != 1 || snap.Instances[0].PresetID != preset.ID || !snap.Instances[0].Pinned || snap.Instances[0].ReservationID != "pin-a" {
		t.Fatalf("instances = %+v", snap.Instances)
	}
	if len(backend.Calls) == 0 || backend.Calls[0].Preset.ID != preset.ID {
		t.Fatalf("backend calls = %+v", backend.Calls)
	}
}

func TestLoadPinnedReservationsFailsLoudly(t *testing.T) {
	ctx := context.Background()
	store := peerTestRegistry(t)
	node := domain.Node{ID: "node-a", MaxUtil: 1, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 100}}}
	agent := nodeagent.NewAgent(node, mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Unix(1, 0).UTC()), nodeagent.WithAllocator(lease.NewAllocator()))
	if err := store.SaveReservation(ctx, domain.Reservation{ID: "pin-missing", Kind: domain.ReservationPinned, NodeID: node.ID}); err != nil {
		t.Fatalf("SaveReservation missing: %v", err)
	}
	if err := loadPinnedReservations(ctx, agent, store, node.ID); err == nil || !strings.Contains(err.Error(), "preset id") {
		t.Fatalf("missing preset err = %v", err)
	}

	store = peerTestRegistry(t)
	if err := store.SaveReservation(ctx, domain.Reservation{ID: "pin-unknown", Kind: domain.ReservationPinned, NodeID: node.ID, PresetID: "unknown"}); err != nil {
		t.Fatalf("SaveReservation unknown: %v", err)
	}
	if err := loadPinnedReservations(ctx, agent, store, node.ID); err == nil {
		t.Fatal("unknown preset accepted")
	}
}

func TestSubmitterPolicyFromConfig(t *testing.T) {
	policy := submitterPolicyFromConfig(map[string]SubmitterPolicyRule{
		"":      {MaxPriority: domain.PriorityInteractive, AllowPrivate: true},
		"guest": {MaxPriority: domain.PriorityBackground},
		"submitter-a":  {MaxPriority: domain.PriorityInteractive, AllowPrivate: true},
	})
	if len(policy.Rules) != 2 || policy.Rules["submitter-a"].MaxPriority != domain.PriorityInteractive || !policy.Rules["submitter-a"].AllowPrivate || policy.Rules["guest"].AllowPrivate {
		t.Fatalf("policy = %+v", policy)
	}
	if empty := submitterPolicyFromConfig(nil); len(empty.Rules) != 0 {
		t.Fatalf("empty policy = %+v", empty)
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

	if _, err := runOptimizerEvaluation(context.Background(), store, clock.System{}, telemetrySyncConfig{}); err != nil {
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

func TestRunOptimizerEvaluationIncludesRemoteTelemetryAndPushesRecommendations(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	project := domain.Project{ID: "project-a", ContextCap: 16000, AutoApply: false}
	if err := store.SaveProject(ctx, project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SavePreset(ctx, testPresetWithContext("small", 6000)); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := store.SavePreset(ctx, testPresetWithContext("large", 16000)); err != nil {
		t.Fatalf("SavePreset large: %v", err)
	}
	remotePeer := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	client := &mocks.TelemetryPeerClient{
		MetricsByPeer: map[string][]domain.RunMetric{
			remotePeer.ID: []domain.RunMetric{
				{JobID: "remote-a", NodeID: remotePeer.ID, Project: project.ID, ContextUsed: 3500, TokensPerSec: 10, At: time.Unix(30, 0).UTC()},
				{JobID: "remote-b", NodeID: remotePeer.ID, Project: project.ID, ContextUsed: 4000, TokensPerSec: 20, At: time.Unix(31, 0).UTC()},
			},
		},
	}

	result, err := runOptimizerEvaluation(ctx, store, clock.System{}, telemetrySyncConfig{
		SelfID: "peer-a",
		Peers:  &mocks.PeerDiscovery{PeersVal: []domain.Peer{remotePeer}},
		Client: client,
	})
	if err != nil {
		t.Fatalf("runOptimizerEvaluation: %v", err)
	}
	if result.ImportedMetrics != 2 || result.PushedRecommendations != 1 || len(result.SkippedPeers) != 0 {
		t.Fatalf("sync result = %+v", result)
	}
	recs, err := store.ListRecommendations(ctx, project.ID)
	if err != nil {
		t.Fatalf("ListRecommendations: %v", err)
	}
	if len(recs) != 1 || recs[0].Observed["avg_tokens"] != 3750 {
		t.Fatalf("recommendations = %+v", recs)
	}
	appliedProject, err := store.Project(ctx, project.ID)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if appliedProject.ContextCap != project.ContextCap {
		t.Fatalf("auto_apply=false project changed: %+v", appliedProject)
	}
	if pushed := client.PushedRecommendations[remotePeer.ID]; len(pushed) != 1 || pushed[0].ProjectID != project.ID {
		t.Fatalf("pushed recommendations = %+v", pushed)
	}
}

func TestRunOptimizerEvaluationSkipsUnreachableTelemetryPeersWithEvidence(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.SaveProject(ctx, domain.Project{ID: "project-a", ContextCap: 16000}); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SavePreset(ctx, testPresetWithContext("small", 6000)); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	remotePeer := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	result, err := runOptimizerEvaluation(ctx, store, clock.System{}, telemetrySyncConfig{
		SelfID: "peer-a",
		Peers:  &mocks.PeerDiscovery{PeersVal: []domain.Peer{remotePeer}},
		Client: &mocks.TelemetryPeerClient{MetricsErr: errors.New("dial refused")},
	})
	if err != nil {
		t.Fatalf("runOptimizerEvaluation: %v", err)
	}
	if len(result.SkippedPeers) != 1 || !strings.Contains(result.SkippedPeers[0], "peer-b metrics: dial refused") {
		t.Fatalf("skipped peers = %+v", result.SkippedPeers)
	}
}

func TestShouldRunGroupOptimizerSelectsOneReadyComputePeer(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(0, 0).UTC())
	fleet := optimizerFleet{snap: domain.FleetSnapshot{Nodes: []domain.Node{
		{ID: "node-c", Status: domain.NodeReady},
		{ID: "node-a", Status: domain.NodeReady},
		{ID: "node-b", Status: domain.NodeUnreachable},
	}}}
	ok, err := shouldRunGroupOptimizer(ctx, fleet, "node-a", clk, time.Minute)
	if err != nil || !ok {
		t.Fatalf("node-a first slot ok=%v err=%v", ok, err)
	}
	ok, err = shouldRunGroupOptimizer(ctx, fleet, "node-c", clk, time.Minute)
	if err != nil || ok {
		t.Fatalf("node-c first slot ok=%v err=%v", ok, err)
	}
	clk.Advance(time.Minute)
	ok, err = shouldRunGroupOptimizer(ctx, fleet, "node-c", clk, time.Minute)
	if err != nil || !ok {
		t.Fatalf("node-c second slot ok=%v err=%v", ok, err)
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
	addr, handler, cleanup, err := buildPeerGateway(context.Background(), []string{"--config", configPath})
	if err != nil {
		t.Fatalf("buildPeerGateway: %v", err)
	}
	if cleanup == nil {
		t.Fatal("local compute peer did not return cleanup")
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
	server := httptest.NewServer(handler)
	defer server.Close()
	client := nodeagent.NewHTTPClient(server.URL)
	job := domain.Job{ID: "job-a", Priority: domain.PriorityInteractive}
	offer, err := client.Offer(context.Background(), domain.AdmissionRequest{Job: job, Claim: domain.Claim{WeightsMB: 1}})
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}
	lease, err := client.Commit(context.Background(), offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := client.BindInstance(context.Background(), lease.ID, "inst-a"); err != nil {
		t.Fatalf("BindInstance: %v", err)
	}
	gotLease, found, err := client.LeaseForInstance(context.Background(), "inst-a")
	if err != nil || !found || gotLease.ID != lease.ID {
		t.Fatalf("LeaseForInstance = %+v found=%v err=%v", gotLease, found, err)
	}
}

func TestNewLocalPeerAgentRequiresAdmissionExtensions(t *testing.T) {
	agent := mocks.NewNodeAgent(domain.Node{ID: "peer-a"})
	if got, err := newLocalPeerAgent(agent, &mocks.AdmissionController{}); err != nil || got.LeaseBinder == nil || got.LeaseInspector == nil {
		t.Fatalf("newLocalPeerAgent = %+v err=%v", got, err)
	}
	if _, err := newLocalPeerAgent(agent, localAdmissionOnly{}); err == nil || !strings.Contains(err.Error(), "lease inspection") {
		t.Fatalf("missing extension err = %v", err)
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

func TestComputeBackendAdapterLaunchesCustomProcessWithRenderedArgs(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	out := filepath.Join(t.TempDir(), "custom-args.txt")
	registry := nodeagent.StoreProcessRegistry{Store: store, NodeID: "peer-a"}
	adapter, err := computeBackendAdapter(ComputeConfig{
		Backend:       domain.BackendCustom,
		BackendBinary: "/bin/sh",
		CustomArgs: []string{
			"-c",
			"printf '%s\n%s\n%s\n' \"$1\" \"$2\" \"$4\" > \"$3\"; sleep 30",
			"sh",
			"{model}|{preset}|{host}|{port}|{addr}",
			"base={preset}:{port}",
			out,
		},
		StopGraceMS: 25,
	}, registry)
	if err != nil {
		t.Fatalf("computeBackendAdapter: %v", err)
	}
	if adapter.Name() != "custom" {
		t.Fatalf("adapter name = %s", adapter.Name())
	}
	preset := testPreset("custom-preset")
	preset.ModelRef = "model.gguf"
	preset.LaunchArgs = []string{"launch={preset}:{addr}"}
	handle, err := adapter.Launch(ctx, preset, "127.0.0.1:54321")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	defer func() { _ = adapter.Stop(context.Background(), handle) }()

	data := waitForFile(t, out)
	want := "model.gguf|custom-preset|127.0.0.1|54321|127.0.0.1:54321\nbase=custom-preset:54321\nlaunch=custom-preset:127.0.0.1:54321\n"
	if string(data) != want {
		t.Fatalf("custom args = %q want %q", data, want)
	}
	refs, err := store.ProcessRefs(ctx, "peer-a")
	if err != nil {
		t.Fatalf("ProcessRefs: %v", err)
	}
	if len(refs) != 1 || refs[0].PID != handle.PID {
		t.Fatalf("refs = %+v handle=%+v", refs, handle)
	}
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	refs, err = store.ProcessRefs(ctx, "peer-a")
	if err != nil {
		t.Fatalf("ProcessRefs after stop: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("refs after stop = %+v", refs)
	}
}

func TestComputeBackendAdapterRequiresCustomBinary(t *testing.T) {
	if _, err := computeBackendAdapter(ComputeConfig{Backend: domain.BackendCustom}, nodeagent.StoreProcessRegistry{}); err == nil || !strings.Contains(err.Error(), "custom compute backend binary") {
		t.Fatalf("missing custom binary err = %v", err)
	}
}

type localAdmissionOnly struct{}

func (localAdmissionOnly) Offer(context.Context, domain.AdmissionRequest) (domain.LeaseOffer, error) {
	return domain.LeaseOffer{}, nil
}

func (localAdmissionOnly) Commit(context.Context, string, uint64) (domain.Lease, error) {
	return domain.Lease{}, nil
}

func (localAdmissionOnly) Release(context.Context, string) error {
	return nil
}

func (localAdmissionOnly) Preempt(context.Context, string, string) error {
	return nil
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

func waitForFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			return data
		}
		if time.Now().After(deadline) {
			t.Fatalf("read %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type optimizerFleet struct {
	snap domain.FleetSnapshot
	err  error
}

func (f optimizerFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	return f.snap, f.err
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
