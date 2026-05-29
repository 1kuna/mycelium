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
	"mycelium/internal/domain"
	storesqlite "mycelium/internal/store/sqlite"
)

func TestRunDispatchesKnownCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "server", args: []string{"server"}, want: "read server config"},
		{name: "myce", args: []string{"myce"}, want: "usage: myce <add-model|models|nodes|projects|jobs|recommendations>"},
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
	control, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open control store: %v", err)
	}
	defer control.Close()
	if got, err := control.Preset(context.Background(), "tiny"); err != nil || got.ID != "tiny" {
		t.Fatalf("control preset = %+v, %v", got, err)
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

func TestBuildGatewayServerWithJoinToken(t *testing.T) {
	configPath := writeServerConfig(t, ServerConfig{
		Listen:    "127.0.0.1:0",
		StorePath: filepath.Join(t.TempDir(), "control.db"),
		JoinToken: "secret",
		Presets:   []domain.Preset{testPreset("tiny")},
	})
	addr, handler, err := buildGatewayServer(context.Background(), []string{"--config", configPath})
	if err != nil {
		t.Fatalf("buildGatewayServer: %v", err)
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nodes", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "[]\n" {
		t.Fatalf("nodes status/body = %d %q", rec.Code, rec.Body.String())
	}
}

func TestRunNodeAndServerExitOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runNode(ctx, []string{"--listen", "127.0.0.1:0", "--backend-listen", "127.0.0.1:0"}); err != nil {
		t.Fatalf("runNode canceled: %v", err)
	}
	configPath := writeServerConfig(t, ServerConfig{
		Listen:    "127.0.0.1:0",
		StorePath: filepath.Join(t.TempDir(), "control.db"),
		JoinToken: "secret",
		Presets:   []domain.Preset{testPreset("tiny")},
	})
	if err := runServer(ctx, []string{"--config", configPath}); err != nil {
		t.Fatalf("runServer canceled: %v", err)
	}
}

func TestBuildNodeServer(t *testing.T) {
	addr, handler, err := buildNodeServer([]string{"--listen", "127.0.0.1:0", "--id", "node-a", "--name", "Node A", "--llama-server", "/bin/echo", "--vram-mb", "1024"})
	if err != nil {
		t.Fatalf("buildNodeServer: %v", err)
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}
}

func TestJoinedBackendAddrRewritesLoopbackToAdvertisedLANHost(t *testing.T) {
	got := joinedBackendAddr("127.0.0.1:51848", "192.0.2.63:51847")
	if got != "192.0.2.63:51848" {
		t.Fatalf("joinedBackendAddr = %s", got)
	}
	if got := joinedBackendAddr("10.0.0.5:6000", "192.0.2.63:51847"); got != "10.0.0.5:6000" {
		t.Fatalf("explicit backend changed to %s", got)
	}
}

func TestEffectiveAdvertiseAddrUsesActualPortForZeroListen(t *testing.T) {
	got := effectiveAdvertiseAddr("0.0.0.0:0", "127.0.0.1:60000")
	if got != "0.0.0.0:60000" {
		t.Fatalf("effectiveAdvertiseAddr = %s", got)
	}
}

func TestRunRejectsMissingAndUnknownCommand(t *testing.T) {
	for _, args := range [][]string{nil, []string{"bogus"}} {
		err := run(context.Background(), args)
		if err == nil {
			t.Fatalf("run(%v) expected error", args)
		}
	}
}

func writeServerConfig(t *testing.T, cfg ServerConfig) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "server.json")
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
