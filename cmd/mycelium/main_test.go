package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	osuser "os/user"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"mycelium/internal/backends/processadapter"
	"mycelium/internal/catalog"
	"mycelium/internal/domain"
	"mycelium/internal/engine"
	"mycelium/internal/enginecompat"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/gateway/translate"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/optimizer"
	peercoord "mycelium/internal/peer"
	"mycelium/internal/ports"
	"mycelium/internal/reconcile"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/internal/telemetry"
	"mycelium/internal/workload"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"

	_ "modernc.org/sqlite"
)

func TestRunDispatchesKnownCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "run", args: []string{"run"}, want: "read peer config"},
		{name: "ctl gateway preflight", args: []string{"ctl", "gateway", "preflight"}, want: "--url is required"},
		{name: "ctl route", args: []string{"ctl", "route"}, want: "usage: myce route <certify|status>"},
		{name: "ctl", args: []string{"ctl"}, want: "usage: myce <add-model|models|nodes|projects|jobs|telemetry|recommendations|benchmark|calibrate|gateway|route|reconcile|pool|doctor|debug|status>"},
		{name: "server removed", args: []string{"server"}, want: "peer-native"},
		{name: "node removed", args: []string{"node"}, want: "peer-native"},
		{name: "unknown", args: []string{"wat"}, want: "unknown command"},
		{name: "flag passthrough", args: []string{"--config", filepath.Join(t.TempDir(), "missing.json")}, want: "open"},
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

func TestMainExitCodeReportsErrorsAndSuccess(t *testing.T) {
	var stderr bytes.Buffer
	if code := mainExitCode(context.Background(), []string{"bogus"}, &stderr); code != 1 || !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("error exit code=%d stderr=%q", code, stderr.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stderr.Reset()
	if code := mainExitCode(ctx, nil, &stderr); code != 0 || stderr.Len() != 0 {
		t.Fatalf("success exit code=%d stderr=%q", code, stderr.String())
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
	defaultCfg := applyPeerConfigDefaults(PeerConfig{
		Listen:        "127.0.0.1:5555",
		JoinToken:     "join-secret",
		RPCToken:      "rpc-secret",
		GatewayToken:  "gateway-secret",
		ComputeConfig: ComputeConfig{ID: "peer-default"},
	})
	if err := savePeerConfig("", defaultCfg); err != nil {
		t.Fatalf("save default peer config: %v", err)
	}
	savedDefault, err := loadPeerConfig("")
	if err != nil || savedDefault.Listen != "127.0.0.1:5555" || savedDefault.GatewayToken != "gateway-secret" {
		t.Fatalf("saved default peer config = %+v err=%v", savedDefault, err)
	}
	if err := savePeerConfig(filepath.Join(t.TempDir(), "unsafe.json"), PeerConfig{Listen: "peer.example:5555"}); err == nil || !strings.Contains(err.Error(), "rpc_token") {
		t.Fatalf("unsafe save peer config err = %v", err)
	}
	relDir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(cwd); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})
	if err := os.Chdir(relDir); err != nil {
		t.Fatalf("chdir relative config dir: %v", err)
	}
	if err := savePeerConfig("relative-peer.json", defaultCfg); err != nil {
		t.Fatalf("save relative peer config: %v", err)
	}
	if _, err := os.Stat(filepath.Join(relDir, "relative-peer.json")); err != nil {
		t.Fatalf("relative peer config missing: %v", err)
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
	if peerCfg.ComputeConfig.ID != "peer_local" || peerCfg.ComputeConfig.BackendListen != "127.0.0.1:51848" || peerCfg.ComputeConfig.DiskMinFreeRatio != domain.DefaultDiskMinFreeRatio || peerCfg.ComputeConfig.LoadTimeoutMS != 300000 {
		t.Fatalf("compute defaults = %+v", peerCfg.ComputeConfig)
	}
	computePath := filepath.Join(t.TempDir(), "compute-peer.json")
	computeRaw := `{"compute":true,"compute_config":{"backend_listen":"127.0.0.1:8","id":"peer-json","name":"Peer JSON","backend":"mlx","backend_binary":"/bin/mlx","llama_server":"/bin/echo","vram_mb":1234,"max_util":0.7,"disk_min_free_ratio":0.33,"load_timeout_ms":1200000,"gguf_parser":"parser"}}`
	if err := os.WriteFile(computePath, []byte(computeRaw), 0644); err != nil {
		t.Fatalf("write compute peer config: %v", err)
	}
	computeCfg, err := loadPeerConfig(computePath)
	if err != nil {
		t.Fatalf("loadPeerConfig compute: %v", err)
	}
	if !computeCfg.Compute || computeCfg.ComputeConfig.ID != "peer-json" || computeCfg.ComputeConfig.VRAMMB != 1234 || computeCfg.ComputeConfig.GGUFParser != "parser" || computeCfg.ComputeConfig.Backend != domain.BackendMLX || computeCfg.ComputeConfig.BackendBinary != "/bin/mlx" || computeCfg.ComputeConfig.DiskMinFreeRatio != 0.33 || computeCfg.ComputeConfig.LoadTimeoutMS != 1200000 {
		t.Fatalf("compute peer config = %+v", computeCfg)
	}
	multiPath := filepath.Join(t.TempDir(), "multi-peer.json")
	multiRaw := `{"compute":true,"compute_config":{"backend_listen":"127.0.0.1:0","id":"peer-json","backends":[{"backend":"vllm","backend_binary":"/bin/vllm","custom_args":["--gpu-memory-utilization","0.70"]},{"backend":"llamacpp","llama_server":"/bin/llama","custom_args":["--n-gpu-layers","99"]}]}}`
	if err := os.WriteFile(multiPath, []byte(multiRaw), 0644); err != nil {
		t.Fatalf("write multi peer config: %v", err)
	}
	multiCfg, err := loadPeerConfig(multiPath)
	if err != nil {
		t.Fatalf("loadPeerConfig multi: %v", err)
	}
	runtimes := computeBackendRuntimes(multiCfg.ComputeConfig)
	if len(runtimes) != 2 || runtimes[0].Backend != domain.BackendVLLM || runtimes[0].BackendBinary != "/bin/vllm" || runtimes[1].Backend != domain.BackendLlamaCpp || runtimes[1].LlamaServer != "/bin/llama" {
		t.Fatalf("runtime configs = %+v", runtimes)
	}
	duplicatePath := filepath.Join(t.TempDir(), "duplicate-peer.json")
	duplicateRaw := `{"compute_config":{"backends":[{"backend":"vllm"},{"backend":"vllm"}]}}`
	if err := os.WriteFile(duplicatePath, []byte(duplicateRaw), 0644); err != nil {
		t.Fatalf("write duplicate peer config: %v", err)
	}
	if _, err := loadPeerConfig(duplicatePath); err == nil || !strings.Contains(err.Error(), "duplicate compute backend") {
		t.Fatalf("duplicate runtime err = %v", err)
	}
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte(`{`), 0644); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := loadPeerConfig(badPath); err == nil {
		t.Fatal("expected bad peer config error")
	}
}

func TestSavePeerConfigAtomicBacksUpAndRestoresOnValidationFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peer.json")
	original := applyPeerConfigDefaults(PeerConfig{ID: "peer-a", Listen: "127.0.0.1:1111", ComputeConfig: ComputeConfig{ID: "peer-a"}})
	if err := savePeerConfig(path, original); err != nil {
		t.Fatalf("save original: %v", err)
	}
	if err := os.WriteFile(path+".backup.20260627T120000Z", []byte("older backup"), 0600); err != nil {
		t.Fatalf("write existing backup: %v", err)
	}
	next := original
	next.Listen = "127.0.0.1:2222"
	backup, err := savePeerConfigAtomic(path, next, "2026/06/27T12:00:00Z", nil)
	if err != nil {
		t.Fatalf("savePeerConfigAtomic: %v", err)
	}
	if !strings.HasSuffix(backup, ".backup.2026-06-27T12-00-00Z") {
		t.Fatalf("backup path = %s", backup)
	}
	loaded, err := loadPeerConfig(path)
	if err != nil || loaded.Listen != next.Listen {
		t.Fatalf("loaded atomic config = %+v err=%v", loaded, err)
	}
	backupData, err := os.ReadFile(backup)
	if err != nil || !strings.Contains(string(backupData), `"listen": "127.0.0.1:1111"`) {
		t.Fatalf("backup data = %q err=%v", backupData, err)
	}

	failing := loaded
	failing.Listen = "127.0.0.1:3333"
	_, err = savePeerConfigAtomic(path, failing, "restore", func(string) error {
		return errors.New("synthetic validation")
	})
	if err == nil || !strings.Contains(err.Error(), "post-write peer config validation failed") {
		t.Fatalf("validation err = %v", err)
	}
	restored, err := loadPeerConfig(path)
	if err != nil || restored.Listen != loaded.Listen {
		t.Fatalf("restored config = %+v err=%v", restored, err)
	}

	newPath := filepath.Join(dir, "new-peer.json")
	_, err = savePeerConfigAtomic(newPath, applyPeerConfigDefaults(PeerConfig{ID: "peer-new", Listen: "127.0.0.1:4444", ComputeConfig: ComputeConfig{ID: "peer-new"}}), "new", func(string) error {
		return errors.New("new config invalid")
	})
	if err == nil || !strings.Contains(err.Error(), "post-write peer config validation failed") {
		t.Fatalf("new validation err = %v", err)
	}
	if _, err := os.Stat(newPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new config was not removed after failed validation: %v", err)
	}
	if got := backupStampForPlan(domain.BootstrapPlan{}); got != "unknown" {
		t.Fatalf("zero backup stamp = %s", got)
	}
}

func TestConfigInitGeneratesSafeComputeConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configPath := filepath.Join(home, ".mycelium", "peer.json")
	nextHex := 0
	cfg, err := generatePeerConfig(context.Background(), configInitOptions{
		Path:    configPath,
		Compute: "auto",
		Listen:  "loopback",
		Backend: "auto",
		GOOS:    "linux",
		GOARCH:  "arm64",
		RandomHex: func(bytesLen int) (string, error) {
			nextHex++
			return strings.Repeat(strconv.Itoa(nextHex), bytesLen*2), nil
		},
		Detect: func(_ context.Context, seed domain.Node) (domain.Node, error) {
			seed.OS = "linux"
			seed.Status = domain.NodeReady
			seed.OOMSeverity = domain.OOMCatastrophic
			seed.DiskTotalMB = 1000
			seed.DiskFreeMB = 400
			seed.Accelerators = []domain.Accelerator{{Index: 0, Vendor: "nvidia", Kind: "gb10", VRAMTotalMB: 131072, UnifiedMemory: true}}
			return seed, nil
		},
	})
	if err != nil {
		t.Fatalf("generatePeerConfig: %v", err)
	}
	if !cfg.Compute || cfg.Listen != "127.0.0.1:51846" || cfg.ComputeConfig.BackendListen != "127.0.0.1:51848" {
		t.Fatalf("generated config = %+v", cfg)
	}
	if cfg.ComputeConfig.Backend != domain.BackendVLLM || cfg.ComputeConfig.VRAMMB != 131072 || cfg.ComputeConfig.DiskMinFreeRatio != domain.DefaultDiskMinFreeRatio {
		t.Fatalf("compute config = %+v", cfg.ComputeConfig)
	}
	if !reflect.DeepEqual(cfg.ComputeConfig.CustomArgs, []string{"--gpu-memory-utilization", "0.85"}) {
		t.Fatalf("vllm args = %+v", cfg.ComputeConfig.CustomArgs)
	}
	if cfg.ID == "" || cfg.JoinToken == "" || cfg.RPCToken == "" || cfg.GatewayToken == "" {
		t.Fatalf("missing generated identity/tokens = %+v", cfg)
	}
	if err := runConfig(context.Background(), []string{"init", "--config", configPath, "--compute", "off", "--listen", "loopback"}); err != nil {
		t.Fatalf("runConfig init: %v", err)
	}
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat generated config: %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("generated config mode = %o", got)
	}
	if runtime.GOOS != "windows" {
		dirInfo, err := os.Stat(filepath.Dir(configPath))
		if err != nil {
			t.Fatalf("stat config dir: %v", err)
		}
		if got := dirInfo.Mode().Perm(); got != 0700 {
			t.Fatalf("generated config dir mode = %o", got)
		}
	}
	loaded, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load generated config: %v", err)
	}
	if loaded.Compute {
		t.Fatalf("compute off config = %+v", loaded)
	}
	explicitPath := filepath.Join(home, ".mycelium", "explicit-peer.json")
	if err := runConfig(context.Background(), []string{
		"init",
		"--config", explicitPath,
		"--compute", "on",
		"--listen", "127.0.0.1:5566",
		"--backend", "mlx",
		"--max-util", "0.75",
		"--disk-min-free-ratio", "0.30",
	}); err != nil {
		t.Fatalf("runConfig explicit init: %v", err)
	}
	explicit, err := loadPeerConfig(explicitPath)
	if err != nil {
		t.Fatalf("load explicit config: %v", err)
	}
	if !explicit.Compute || explicit.Listen != "127.0.0.1:5566" || explicit.ComputeConfig.Backend != domain.BackendMLX || explicit.ComputeConfig.MaxUtil != 0.75 || explicit.ComputeConfig.DiskMinFreeRatio != 0.30 {
		t.Fatalf("explicit config = %+v", explicit)
	}
}

func TestPeerConfigValidationRejectsUnsafeValues(t *testing.T) {
	validate := func(cfg PeerConfig) error {
		return validatePeerConfig(applyPeerConfigDefaults(cfg))
	}
	if err := validate(PeerConfig{ComputeConfig: ComputeConfig{MaxUtil: 1.2}}); err == nil || !strings.Contains(err.Error(), "max_util") {
		t.Fatalf("max util err = %v", err)
	}
	if err := validate(PeerConfig{ComputeConfig: ComputeConfig{DiskMinFreeRatio: 1}}); err == nil || !strings.Contains(err.Error(), "disk_min_free_ratio") {
		t.Fatalf("disk floor err = %v", err)
	}
	err := validate(PeerConfig{ComputeConfig: ComputeConfig{
		Backend:    domain.BackendVLLM,
		CustomArgs: []string{"--gpu-memory-utilization", "0.90"},
	}})
	if err != nil {
		t.Fatalf("ordinary vllm config should not enforce catastrophic cap globally: %v", err)
	}
	if err := validate(PeerConfig{Listen: "peer.example:51846"}); err == nil || !strings.Contains(err.Error(), "rpc_token") {
		t.Fatalf("lan auth err = %v", err)
	}
	if err := validate(PeerConfig{Listen: "peer.example:51846", RPCToken: "rpc-secret"}); err == nil || !strings.Contains(err.Error(), "gateway_token") {
		t.Fatalf("lan gateway auth err = %v", err)
	}
	if err := validate(PeerConfig{Listen: "peer.example:51846", RPCToken: "rpc-secret", GatewayToken: "gateway-secret"}); err != nil {
		t.Fatalf("lan auth config = %v", err)
	}
	if err := validate(PeerConfig{
		Listen:               "peer.example:51846",
		RPCToken:             "rpc-secret",
		DefaultProject:       "project-a",
		Projects:             []domain.Project{{ID: "project-a"}},
		GatewayProjectTokens: []GatewayProjectToken{{Project: "project-a", Token: "project-token-a"}},
	}); err != nil {
		t.Fatalf("lan project token auth config = %v", err)
	}
	if err := validate(PeerConfig{Overlay: true}); err == nil || !strings.Contains(err.Error(), "overlay") {
		t.Fatalf("overlay err = %v", err)
	}
	if err := validate(PeerConfig{PrivateStorageKey: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}); err == nil || !strings.Contains(err.Error(), "private_storage_key") {
		t.Fatalf("private key err = %v", err)
	}
	if err := validate(PeerConfig{SubmitterPolicy: map[string]SubmitterPolicyRule{"submitter-a": {AllowPrivate: true}}}); err == nil || !strings.Contains(err.Error(), "submitter_policy") {
		t.Fatalf("submitter policy err = %v", err)
	}
	validProject := domain.Project{ID: "project-a", Priority: domain.PriorityNormal, SpeedPref: domain.SpeedAuto, Preemption: domain.PreemptSoft}
	if err := validate(PeerConfig{DefaultProject: "project-a", Projects: []domain.Project{validProject}}); err != nil {
		t.Fatalf("valid project config = %v", err)
	}
	for _, tt := range []struct {
		name    string
		project domain.Project
		want    string
	}{
		{name: "missing id", project: domain.Project{}, want: "project id"},
		{name: "bad priority", project: domain.Project{ID: "project-a", Priority: "urgent"}, want: "priority"},
		{name: "bad speed", project: domain.Project{ID: "project-a", SpeedPref: "fastest"}, want: "speed_pref"},
		{name: "bad preemption", project: domain.Project{ID: "project-a", Preemption: "takeover"}, want: "preemption"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := validate(PeerConfig{Projects: []domain.Project{tt.project}}); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("project err = %v want %q", err, tt.want)
			}
		})
	}
	if err := validate(PeerConfig{Projects: []domain.Project{validProject, validProject}}); err == nil || !strings.Contains(err.Error(), "duplicate project") {
		t.Fatalf("duplicate project err = %v", err)
	}
	if err := validate(PeerConfig{DefaultProject: "missing", Projects: []domain.Project{validProject}}); err == nil || !strings.Contains(err.Error(), "default_project") {
		t.Fatalf("missing default project err = %v", err)
	}
	if err := validate(PeerConfig{Projects: []domain.Project{validProject}, GatewayProjectTokens: []GatewayProjectToken{{Project: "missing", Token: "project-token-a"}}}); err == nil || !strings.Contains(err.Error(), "gateway_project_tokens project") {
		t.Fatalf("missing gateway project token project err = %v", err)
	}
	if err := validate(PeerConfig{Projects: []domain.Project{validProject}, GatewayToken: "token-a", GatewayProjectTokens: []GatewayProjectToken{{Project: validProject.ID, Token: "token-a"}}}); err == nil || !strings.Contains(err.Error(), "duplicate gateway token") {
		t.Fatalf("duplicate gateway token err = %v", err)
	}
	if _, _, err := vllmGPUUtilization([]string{"--gpu-memory-utilization"}); err == nil {
		t.Fatal("missing vllm utilization value accepted")
	}
}

func TestConfigInitHelperBranches(t *testing.T) {
	if err := runConfig(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("empty config command err = %v", err)
	}
	if _, err := prefixedRandomID("peer", func(int) (string, error) { return "", errors.New("entropy unavailable") }); err == nil || !strings.Contains(err.Error(), "entropy unavailable") {
		t.Fatalf("prefixed random id err = %v", err)
	}
	if err := runConfig(context.Background(), []string{"bogus"}); err == nil || !strings.Contains(err.Error(), "unknown config") {
		t.Fatalf("unknown config command err = %v", err)
	}
	for _, raw := range []string{"loopback", "127.0.0.1:1234", "lan"} {
		if got, err := resolveListen(raw); err != nil || got == "" {
			t.Fatalf("resolveListen(%q) = %q %v", raw, got, err)
		}
	}
	if _, err := resolveListen("missing-port"); err == nil {
		t.Fatal("bad listen accepted")
	}
	if got, err := resolveBackendListen("0.0.0.0:51846"); err != nil || got != "127.0.0.1:51848" {
		t.Fatalf("backend listen = %q %v", got, err)
	}
	if _, err := resolveBackendListen("bad"); err == nil {
		t.Fatal("bad backend listen accepted")
	}
	for _, raw := range []string{"llama.cpp", "llamacpp", "mlx", "vllm", "custom", "auto"} {
		if _, err := normalizeBackend(raw); err != nil {
			t.Fatalf("normalizeBackend(%q): %v", raw, err)
		}
	}
	if backend, err := normalizeBackend("llama.cpp"); err != nil || backend != domain.BackendLlamaCpp {
		t.Fatalf("normalize llama.cpp = %q %v", backend, err)
	}
	if _, err := normalizeBackend("bad"); err == nil {
		t.Fatal("bad backend accepted")
	}
	var vram optionalIntFlag
	if err := vram.Set("0"); err != nil {
		t.Fatalf("set zero vram: %v", err)
	}
	if !vram.set || vram.value != 0 || vram.String() != "0" {
		t.Fatalf("zero vram flag = %+v string=%q", vram, vram.String())
	}
	if err := vram.Set("bad"); err == nil {
		t.Fatal("bad vram value accepted")
	}
	if got := defaultBackendForHost(configInitOptions{GOOS: "darwin", GOARCH: "arm64"}, domain.Node{}); got != domain.BackendLlamaCpp {
		t.Fatalf("darwin backend = %s", got)
	}
	if got := defaultBackendForHost(configInitOptions{GOOS: "linux", GOARCH: "arm64"}, domain.Node{Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}); got != domain.BackendVLLM {
		t.Fatalf("linux nvidia backend = %s", got)
	}
	if got := defaultBackendForHost(configInitOptions{GOOS: "linux", GOARCH: "amd64"}, domain.Node{}); got != domain.BackendLlamaCpp {
		t.Fatalf("linux default backend = %s", got)
	}
	if _, err := prefixedRandomID("peer", func(int) (string, error) { return "", errors.New("random") }); err == nil {
		t.Fatal("random id error swallowed")
	}
	if node, ok, err := detectConfigNode(context.Background(), configInitOptions{}, PeerConfig{}); err != nil || ok || node.ID != "" {
		t.Fatalf("nil detector = %+v %v %v", node, ok, err)
	}
	if _, err := generatePeerConfig(context.Background(), configInitOptions{Compute: "maybe", RandomHex: func(int) (string, error) { return "abcd", nil }}); err == nil {
		t.Fatal("bad compute accepted")
	}
	if _, err := generatePeerConfig(context.Background(), configInitOptions{Compute: "off", Backend: "bad", RandomHex: func(int) (string, error) { return "abcd", nil }}); err == nil {
		t.Fatal("bad backend accepted")
	}
	if _, err := generatePeerConfig(context.Background(), configInitOptions{
		Compute: "on",
		RandomHex: func(int) (string, error) {
			return "abcd", nil
		},
		Detect: func(context.Context, domain.Node) (domain.Node, error) {
			return domain.Node{}, errors.New("detect")
		},
	}); err == nil || !strings.Contains(err.Error(), "detect") {
		t.Fatalf("compute-on detect err = %v", err)
	}
	if _, _, err := parseGPUUtilization("not-number"); err == nil {
		t.Fatal("bad gpu utilization accepted")
	}
	if _, _, err := parseGPUUtilization("1.5"); err == nil {
		t.Fatal("out-of-range gpu utilization accepted")
	}
	if err := validatePeerConfig(applyPeerConfigDefaults(PeerConfig{ComputeConfig: ComputeConfig{Backend: domain.Backend("wat")}})); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown backend validate err = %v", err)
	}
}

func TestValidateRuntimeComputeSafety(t *testing.T) {
	catastrophic := domain.Node{OOMSeverity: domain.OOMCatastrophic}
	if err := validateRuntimeComputeSafety(ComputeConfig{Backend: domain.BackendLlamaCpp}, catastrophic); err != nil {
		t.Fatalf("non-vllm safety err = %v", err)
	}
	if err := validateRuntimeComputeSafety(ComputeConfig{Backend: domain.BackendVLLM}, catastrophic); err == nil || !strings.Contains(err.Error(), "gpu-memory-utilization") {
		t.Fatalf("missing cap err = %v", err)
	}
	if err := validateRuntimeComputeSafety(ComputeConfig{Backend: domain.BackendVLLM, CustomArgs: []string{"--gpu-memory-utilization", "0.86"}}, catastrophic); err == nil || !strings.Contains(err.Error(), "0.85") {
		t.Fatalf("too-high cap err = %v", err)
	}
	if err := validateRuntimeComputeSafety(ComputeConfig{Backend: domain.BackendVLLM, CustomArgs: []string{"--gpu-memory-utilization=0.85"}}, catastrophic); err != nil {
		t.Fatalf("safe cap err = %v", err)
	}
}

func TestApplyExplicitVRAMPreservesDetectedHardwareFacts(t *testing.T) {
	intel := domain.Node{
		ID:          "b70",
		OS:          "linux",
		OOMSeverity: domain.OOMSoft,
		Labels:      map[string]string{"gpu.vendor": "intel"},
		Accelerators: []domain.Accelerator{{
			Index:       0,
			Vendor:      "intel",
			Kind:        "arc-pro-b70",
			VRAMTotalMB: 24576,
			VRAMUsedMB:  33048,
		}},
	}
	overridden, err := applyExplicitVRAM(intel, 49152)
	if err != nil {
		t.Fatalf("applyExplicitVRAM: %v", err)
	}
	if overridden.OS != "linux" || overridden.OOMSeverity != domain.OOMSoft || overridden.Labels["gpu.vendor"] != "intel" {
		t.Fatalf("node facts were rewritten = %+v", overridden)
	}
	if overridden.Accelerators[0].Vendor != "intel" || overridden.Accelerators[0].Kind != "arc-pro-b70" || overridden.Accelerators[0].VRAMTotalMB != 49152 || overridden.Accelerators[0].VRAMUsedMB != 0 {
		t.Fatalf("accelerator facts = %+v", overridden.Accelerators[0])
	}
	if intel.Accelerators[0].VRAMTotalMB != 24576 || intel.Accelerators[0].VRAMUsedMB != 33048 {
		t.Fatalf("source node was mutated = %+v", intel.Accelerators[0])
	}
	if _, err := applyExplicitVRAM(domain.Node{ID: "cpu-only"}, 1024); err == nil || !strings.Contains(err.Error(), "no detected accelerator") {
		t.Fatalf("cpu-only override err = %v", err)
	}
}

func TestServiceSpecAndRenderers(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configPath := filepath.Join(home, ".mycelium", "peer.json")
	spec, err := parseServiceSpec("install", []string{"--config", configPath, "--name", "fleet one!"}, "darwin")
	if err != nil {
		t.Fatalf("parseServiceSpec darwin: %v", err)
	}
	if spec.Name != "fleetone" || spec.Label != "com.mycelium.fleetone" || spec.Scope != serviceScopeUser {
		t.Fatalf("darwin spec = %+v", spec)
	}
	if !strings.HasPrefix(spec.UnitPath, filepath.Join(home, "Library", "LaunchAgents")) {
		t.Fatalf("darwin unit path = %s", spec.UnitPath)
	}
	plist := renderLaunchdPlist(spec)
	if !strings.Contains(plist, "<string>run</string>") || !strings.Contains(plist, xmlEscape(configPath)) {
		t.Fatalf("plist = %s", plist)
	}
	if !strings.Contains(plist, "<key>NetworkState</key>") || !strings.Contains(plist, "<key>WorkingDirectory</key>") {
		t.Fatalf("plist missing launchd network/working directory settings: %s", plist)
	}
	if strings.Contains(plist, "join-secret") || strings.Contains(plist, "rpc-secret") {
		t.Fatalf("plist leaked secret: %s", plist)
	}
	systemSpec, err := parseServiceSpec("install", []string{"--config", configPath, "--system", "--name", "spark"}, "linux")
	if err != nil {
		t.Fatalf("parseServiceSpec linux: %v", err)
	}
	if systemSpec.Scope != serviceScopeSystem || systemSpec.UnitPath != "/etc/systemd/system/mycelium-spark.service" {
		t.Fatalf("linux system spec = %+v", systemSpec)
	}
	unit := renderSystemdUnit(systemSpec)
	if !strings.Contains(unit, "ExecStart=") || !strings.Contains(unit, " run --config ") || strings.Contains(unit, "join-secret") {
		t.Fatalf("unit = %s", unit)
	}
	darwinSystemSpec, err := parseServiceSpec("plan", []string{"--config", configPath, "--binary", "/opt/my celium/bin/mycelium", "--system", "--name", "real fleet"}, "darwin")
	if err != nil {
		t.Fatalf("parseServiceSpec darwin system: %v", err)
	}
	if darwinSystemSpec.Scope != serviceScopeSystem || darwinSystemSpec.UnitPath != "/Library/LaunchDaemons/com.mycelium.realfleet.plist" {
		t.Fatalf("darwin system spec = %+v", darwinSystemSpec)
	}
	if darwinSystemSpec.BinaryPath != "/opt/my celium/bin/mycelium" {
		t.Fatalf("darwin system binary = %s", darwinSystemSpec.BinaryPath)
	}
	if darwinSystemSpec.RunUser == "" {
		t.Fatalf("darwin system spec did not select a run user: %+v", darwinSystemSpec)
	}
	plan := renderServicePlan(darwinSystemSpec, "darwin")
	for _, want := range []string{
		"scope\tsystem",
		"run_user\t" + darwinSystemSpec.RunUser,
		"sudo ",
		"sudo '/opt/my celium/bin/mycelium' service install",
		"launchctl bootstrap system /Library/LaunchDaemons/com.mycelium.realfleet.plist",
		"launchctl kickstart -k system/com.mycelium.realfleet",
		"<key>UserName</key>",
		"<string>" + darwinSystemSpec.RunUser + "</string>",
		"<string>run</string>",
	} {
		if !strings.Contains(plan, want) {
			t.Fatalf("plan missing %q:\n%s", want, plan)
		}
	}
	if strings.Contains(plan, "join-secret") || strings.Contains(plan, "rpc-secret") {
		t.Fatalf("plan leaked secret: %s", plan)
	}
	t.Setenv("SUDO_USER", darwinSystemSpec.RunUser)
	sudoSpec, err := parseServiceSpec("plan", []string{"--config", configPath, "--binary", "/opt/mycelium", "--system"}, "darwin")
	if err != nil {
		t.Fatalf("parseServiceSpec darwin sudo system: %v", err)
	}
	if sudoSpec.Home != darwinSystemSpec.Home || sudoSpec.LogDir != filepath.Join(darwinSystemSpec.Home, ".mycelium", "logs") {
		t.Fatalf("sudo system home/logs = %s %s want %s", sudoSpec.Home, sudoSpec.LogDir, darwinSystemSpec.Home)
	}
	linuxUserSpec, err := parseServiceSpec("plan", []string{"--config", configPath, "--binary", "/usr/local/bin/mycelium", "--user", "--name", "dev"}, "linux")
	if err != nil {
		t.Fatalf("parseServiceSpec linux user: %v", err)
	}
	linuxUserPlan := renderServicePlan(linuxUserSpec, "linux")
	for _, want := range []string{
		"scope\tuser",
		"systemctl --user restart mycelium-dev.service",
		"ExecStart=\"/usr/local/bin/mycelium\" run --config ",
	} {
		if !strings.Contains(linuxUserPlan, want) {
			t.Fatalf("linux user plan missing %q:\n%s", want, linuxUserPlan)
		}
	}
	linuxSystemSpec, err := parseServiceSpec("plan", []string{"--config", configPath, "--binary", "/usr/local/bin/mycelium", "--system", "--name", "prod"}, "linux")
	if err != nil {
		t.Fatalf("parseServiceSpec linux system: %v", err)
	}
	linuxSystemPlan := renderServicePlan(linuxSystemSpec, "linux")
	if !strings.Contains(linuxSystemPlan, "sudo systemctl restart mycelium-prod.service") || strings.Contains(linuxSystemPlan, "systemctl --user") {
		t.Fatalf("linux system plan =\n%s", linuxSystemPlan)
	}
	if unsupported := renderServicePlan(linuxSystemSpec, "windows"); !strings.Contains(unsupported, "unsupported\twindows") {
		t.Fatalf("unsupported plan =\n%s", unsupported)
	}
	quotedSpec := darwinSystemSpec
	quotedSpec.BinaryPath = "/tmp/my celium"
	quotedPlan := renderServicePlan(quotedSpec, "darwin")
	if !strings.Contains(quotedPlan, "sudo '/tmp/my celium' service install") {
		t.Fatalf("plan did not shell-quote binary with spaces:\n%s", quotedPlan)
	}
	stableBinary := filepath.Join(home, "mycelium-stable")
	targetBinary := filepath.Join(home, "mycelium target")
	if err := os.WriteFile(stableBinary, []byte("stable-binary"), 0755); err != nil {
		t.Fatalf("write stable binary: %v", err)
	}
	if err := os.WriteFile(targetBinary, []byte("target-binary"), 0755); err != nil {
		t.Fatalf("write target binary: %v", err)
	}
	upgradeSpec, err := parseServiceSpec("upgrade", []string{
		"--config", configPath,
		"--binary", stableBinary,
		"--target-binary", targetBinary,
		"--backup", filepath.Join(home, "stable.backup"),
		"--name", "fleet one",
		"--user",
		"--dry-run",
	}, "darwin")
	if err != nil {
		t.Fatalf("parseServiceSpec upgrade: %v", err)
	}
	upgradePlan, err := buildServiceUpgradePlan(upgradeSpec, "darwin")
	if err != nil {
		t.Fatalf("buildServiceUpgradePlan: %v", err)
	}
	renderedUpgrade := renderServiceUpgradePlan(upgradePlan)
	for _, want := range []string{
		"upgrade_dry_run\ttrue",
		"stable_binary\t" + stableBinary,
		"target_binary\t" + targetBinary,
		"current_sha256\t",
		"target_sha256\t",
		"preflight_status:",
		"backup: cp ",
		"replace: install -m 0755 ",
		"restart: launchctl kickstart -k gui/",
		"wait: sleep 35",
		"verify:",
		"rollback:",
		"no live change was made",
	} {
		if !strings.Contains(renderedUpgrade, want) {
			t.Fatalf("upgrade plan missing %q:\n%s", want, renderedUpgrade)
		}
	}
	if strings.Contains(renderedUpgrade, "join-secret") || strings.Contains(renderedUpgrade, "rpc-secret") {
		t.Fatalf("upgrade plan leaked secret: %s", renderedUpgrade)
	}
	if _, err := buildServiceUpgradePlan(serviceSpec{BinaryPath: stableBinary, TargetPath: targetBinary}, "darwin"); err == nil || !strings.Contains(err.Error(), "dry-run") {
		t.Fatalf("non-dry-run upgrade err = %v", err)
	}
	if _, err := buildServiceUpgradePlan(serviceSpec{BinaryPath: stableBinary, DryRun: true}, "darwin"); err == nil || !strings.Contains(err.Error(), "--target-binary") {
		t.Fatalf("missing target upgrade err = %v", err)
	}
	if _, err := buildServiceUpgradePlan(serviceSpec{BinaryPath: stableBinary, TargetPath: filepath.Join(home, "missing"), DryRun: true}, "darwin"); err == nil || !strings.Contains(err.Error(), "target service binary") {
		t.Fatalf("missing target binary err = %v", err)
	}
	oldStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	os.Stdout = writePipe
	runErr := runServiceWithManager(context.Background(), []string{
		"upgrade",
		"--config", configPath,
		"--binary", stableBinary,
		"--target-binary", targetBinary,
		"--name", "fleet one",
		"--user",
		"--dry-run",
	}, nil, "darwin")
	_ = writePipe.Close()
	os.Stdout = oldStdout
	runOut, readErr := io.ReadAll(readPipe)
	if readErr != nil {
		t.Fatalf("read service upgrade stdout: %v", readErr)
	}
	if runErr != nil {
		t.Fatalf("runServiceWithManager upgrade: %v", runErr)
	}
	if got := string(runOut); !strings.Contains(got, "upgrade_dry_run\ttrue") || !strings.Contains(got, "replace: install -m 0755") {
		t.Fatalf("run service upgrade output = %s", got)
	}
	oldStdout = os.Stdout
	readPipe, writePipe, err = os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe json: %v", err)
	}
	os.Stdout = writePipe
	runErr = runServiceWithManager(context.Background(), []string{
		"upgrade",
		"--config", configPath,
		"--binary", stableBinary,
		"--target-binary", targetBinary,
		"--name", "fleet one",
		"--user",
		"--dry-run",
		"--json",
	}, nil, "darwin")
	_ = writePipe.Close()
	os.Stdout = oldStdout
	runOut, readErr = io.ReadAll(readPipe)
	if readErr != nil {
		t.Fatalf("read service upgrade json stdout: %v", readErr)
	}
	if runErr != nil {
		t.Fatalf("runServiceWithManager upgrade json: %v", runErr)
	}
	var upgradeJSON serviceUpgradePlanArtifact
	if err := json.Unmarshal(runOut, &upgradeJSON); err != nil {
		t.Fatalf("decode upgrade json: %v\n%s", err, runOut)
	}
	if !upgradeJSON.DryRun || upgradeJSON.Service != "com.mycelium.fleetone" || upgradeJSON.Commands["replace"] == "" || upgradeJSON.Commands["rollback"] == "" || upgradeJSON.Commands["verify"] == "" || upgradeJSON.Commands["wait"] != "sleep 35" {
		t.Fatalf("upgrade json = %+v", upgradeJSON)
	}
	if !strings.Contains(upgradeJSON.Commands["rollback"], "sleep 35") {
		t.Fatalf("rollback should wait before status verification: %+v", upgradeJSON.Commands)
	}
	if strings.Contains(string(runOut), "join-secret") || strings.Contains(string(runOut), "rpc-secret") {
		t.Fatalf("upgrade json leaked secret: %s", runOut)
	}
	if got := serviceRestartCommand(linuxUserSpec, "linux"); got != "systemctl --user restart mycelium-dev.service" {
		t.Fatalf("linux user restart = %q", got)
	}
	if got := serviceRestartCommand(linuxSystemSpec, "linux"); got != "sudo systemctl restart mycelium-prod.service" {
		t.Fatalf("linux system restart = %q", got)
	}
	if got := serviceRestartCommand(linuxSystemSpec, "windows"); !strings.Contains(got, "unsupported service restart") {
		t.Fatalf("unsupported restart = %q", got)
	}
	if got := serviceStatusCommand(darwinSystemSpec); !strings.HasPrefix(got, "sudo ") || !strings.Contains(got, " service status ") {
		t.Fatalf("system status command = %q", got)
	}
	if got := servicePrivilegedCommand(darwinSystemSpec, "cp", "/from path", "/to path"); got != "sudo cp '/from path' '/to path'" {
		t.Fatalf("system privileged command = %q", got)
	}
	if _, err := parseServiceSpec("install", nil, "darwin"); err == nil || !strings.Contains(err.Error(), "--config") {
		t.Fatalf("missing config err = %v", err)
	}
	if _, err := parseServiceSpec("install", []string{"--config", configPath, "--user", "--system"}, "darwin"); err == nil || !strings.Contains(err.Error(), "mutually") {
		t.Fatalf("scope err = %v", err)
	}
	if _, err := parseServiceSpec("install", []string{"--config", configPath}, "windows"); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported err = %v", err)
	}
}

func TestServiceManagersUseExpectedCommands(t *testing.T) {
	ctx := context.Background()
	spec := serviceSpec{
		Name:       "default",
		Label:      "com.mycelium.default",
		Scope:      serviceScopeUser,
		ConfigPath: filepath.Join(t.TempDir(), "peer.json"),
		BinaryPath: "/bin/mycelium",
		Home:       t.TempDir(),
		LogDir:     filepath.Join(t.TempDir(), "logs"),
		UnitPath:   filepath.Join(t.TempDir(), "com.mycelium.default.plist"),
	}
	rec := &serviceCommandRecorder{}
	launchd := launchdManager{Run: rec.Run}
	if err := launchd.Install(ctx, spec); err != nil {
		t.Fatalf("launchd Install: %v", err)
	}
	if _, err := os.Stat(spec.UnitPath); err != nil {
		t.Fatalf("launchd unit not written: %v", err)
	}
	if got := strings.Join(rec.Commands, "\n"); !strings.Contains(got, "launchctl bootstrap") || !strings.Contains(got, "launchctl kickstart") {
		t.Fatalf("launchd commands = %s", got)
	}
	if status, err := launchd.Status(ctx, spec); err != nil || status != "active" {
		t.Fatalf("launchd status = %q %v", status, err)
	}
	if err := launchd.Uninstall(ctx, spec); err != nil {
		t.Fatalf("launchd Uninstall: %v", err)
	}
	if _, err := os.Stat(spec.UnitPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("launchd unit after uninstall err = %v", err)
	}

	systemSpec := spec
	systemSpec.UnitPath = filepath.Join(t.TempDir(), "mycelium-default.service")
	rec = &serviceCommandRecorder{}
	systemd := systemdManager{Run: rec.Run}
	if err := systemd.Install(ctx, systemSpec); err != nil {
		t.Fatalf("systemd Install: %v", err)
	}
	if got := strings.Join(rec.Commands, "\n"); !strings.Contains(got, "systemctl --user daemon-reload") || !strings.Contains(got, "systemctl --user enable --now mycelium-default.service") {
		t.Fatalf("systemd commands = %s", got)
	}
	if status, err := systemd.Status(ctx, systemSpec); err != nil || status != "active" {
		t.Fatalf("systemd status = %q %v", status, err)
	}
	if err := systemd.Uninstall(ctx, systemSpec); err != nil {
		t.Fatalf("systemd Uninstall: %v", err)
	}

	systemSpec.Scope = serviceScopeSystem
	systemSpec.UnitPath = filepath.Join(t.TempDir(), "mycelium-system.service")
	rec = &serviceCommandRecorder{}
	systemd = systemdManager{Run: rec.Run}
	if err := systemd.Install(ctx, systemSpec); err != nil {
		t.Fatalf("systemd system Install: %v", err)
	}
	if got := strings.Join(rec.Commands, "\n"); strings.Contains(got, "systemctl --user") || !strings.Contains(got, "systemctl daemon-reload") {
		t.Fatalf("systemd system commands = %s", got)
	}
}

func TestRunServiceStatusChecksPeerHealth(t *testing.T) {
	var sawJoinToken bool
	var sawRPCAuth bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/peer/health":
			sawJoinToken = r.Header.Get("X-Myc-Join-Token") == "join-secret"
			w.WriteHeader(http.StatusNoContent)
		case "/peer/diagnostics":
			sawRPCAuth = r.Header.Get("Authorization") == "Bearer rpc-secret"
			_ = json.NewEncoder(w).Encode(peerDiagnosticsReport{Ready: true, Seeds: []peerSeedDiagnostic{{Address: "127.0.0.1:1", Ready: true, PeerID: "seed"}}})
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	home := t.TempDir()
	t.Setenv("HOME", home)
	configPath := writePeerConfig(t, PeerConfig{
		Listen:    strings.TrimPrefix(server.URL, "http://"),
		JoinToken: "join-secret",
		RPCToken:  "rpc-secret",
		SeedPeers: []string{"127.0.0.1:1"},
	})
	if err := runServiceWithManager(context.Background(), []string{"status", "--config", configPath}, fakeServiceManager{status: "active"}, "darwin"); err != nil {
		t.Fatalf("service status: %v", err)
	}
	if !sawJoinToken {
		t.Fatal("service health did not send join token")
	}
	if !sawRPCAuth {
		t.Fatal("service diagnostics did not send RPC token")
	}
	if err := runServiceWithManager(context.Background(), []string{"install", "--config", configPath}, fakeServiceManager{}, "darwin"); err != nil {
		t.Fatalf("service install: %v", err)
	}
	if err := runServiceWithManager(context.Background(), []string{"uninstall", "--config", configPath}, fakeServiceManager{}, "darwin"); err != nil {
		t.Fatalf("service uninstall: %v", err)
	}
	if err := runServiceWithManager(context.Background(), []string{"bogus", "--config", configPath}, fakeServiceManager{}, "darwin"); err == nil || !strings.Contains(err.Error(), "unknown service") {
		t.Fatalf("unknown service err = %v", err)
	}
	if _, err := serviceManagerForGOOS("plan9", nil); err == nil {
		t.Fatal("unsupported service manager accepted")
	}
	if got, err := servicePeerBaseURL("0.0.0.0:51846"); err != nil || got != "http://127.0.0.1:51846" {
		t.Fatalf("service base URL = %q %v", got, err)
	}
	failedPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failedPeer.Close()
	failedConfig := writePeerConfig(t, PeerConfig{Listen: strings.TrimPrefix(failedPeer.URL, "http://")})
	if _, err := servicePeerHealth(context.Background(), failedConfig); err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("failed service health err = %v", err)
	}
	refusedPeer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	refusedURL := refusedPeer.URL
	refusedPeer.Close()
	refusedConfig := writePeerConfig(t, PeerConfig{Listen: strings.TrimPrefix(refusedURL, "http://")})
	if _, err := servicePeerHealth(context.Background(), refusedConfig); err == nil || !strings.Contains(err.Error(), "peer health failed") {
		t.Fatalf("refused service health err = %v", err)
	}
	failedDiagnostics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(peerDiagnosticsReport{Ready: false, Seeds: []peerSeedDiagnostic{{Address: "seed", Error: "no route"}}})
	}))
	defer failedDiagnostics.Close()
	failedDiagnosticsConfig := writePeerConfig(t, PeerConfig{Listen: strings.TrimPrefix(failedDiagnostics.URL, "http://"), RPCToken: "rpc-secret", SeedPeers: []string{"seed"}})
	if _, err := servicePeerDiagnostics(context.Background(), failedDiagnosticsConfig); err == nil || !strings.Contains(err.Error(), "peer diagnostics") {
		t.Fatalf("failed service diagnostics err = %v", err)
	}
	refusedDiagnostics := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	refusedDiagnosticsURL := refusedDiagnostics.URL
	refusedDiagnostics.Close()
	refusedDiagnosticsConfig := writePeerConfig(t, PeerConfig{Listen: strings.TrimPrefix(refusedDiagnosticsURL, "http://"), SeedPeers: []string{"seed"}})
	if _, err := servicePeerDiagnostics(context.Background(), refusedDiagnosticsConfig); err == nil || !strings.Contains(err.Error(), "peer diagnostics failed") {
		t.Fatalf("refused service diagnostics err = %v", err)
	}

	noSeedConfig := writePeerConfig(t, PeerConfig{Listen: "127.0.0.1:1"})
	if got, err := servicePeerDiagnostics(context.Background(), noSeedConfig); err != nil || got != "" {
		t.Fatalf("no seed diagnostics = %q %v", got, err)
	}

	seedFailure := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(peerDiagnosticsReport{Ready: true, Seeds: []peerSeedDiagnostic{{Address: "127.0.0.1:2", Ready: false, Error: "connection refused"}}})
	}))
	defer seedFailure.Close()
	seedFailureConfig := writePeerConfig(t, PeerConfig{Listen: strings.TrimPrefix(seedFailure.URL, "http://"), SeedPeers: []string{"127.0.0.1:2"}})
	if _, err := servicePeerDiagnostics(context.Background(), seedFailureConfig); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("seed failure diagnostics err = %v", err)
	}

	badDiagnostics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ready":true}{}`))
	}))
	defer badDiagnostics.Close()
	badDiagnosticsConfig := writePeerConfig(t, PeerConfig{Listen: strings.TrimPrefix(badDiagnostics.URL, "http://"), SeedPeers: []string{"127.0.0.1:2"}})
	if _, err := servicePeerDiagnostics(context.Background(), badDiagnosticsConfig); err == nil || !strings.Contains(err.Error(), "after top-level value") {
		t.Fatalf("bad diagnostics err = %v", err)
	}
}

func TestServiceManagersPropagateCommandFailures(t *testing.T) {
	ctx := context.Background()
	spec := serviceSpec{
		Name:       "real-fleet",
		Label:      "com.mycelium.real-fleet",
		Scope:      serviceScopeUser,
		ConfigPath: "/tmp/peer.json",
		BinaryPath: "/usr/local/bin/mycelium",
		Home:       t.TempDir(),
		LogDir:     filepath.Join(t.TempDir(), "logs"),
		UnitPath:   filepath.Join(t.TempDir(), "com.mycelium.real-fleet.plist"),
	}
	rec := &serviceCommandRecorder{Err: errors.New("launchctl denied")}
	launchd := launchdManager{Run: rec.Run}
	if err := launchd.Install(ctx, spec); err == nil || !strings.Contains(err.Error(), "launchctl denied") {
		t.Fatalf("launchd install err = %v", err)
	}
	if len(rec.Commands) < 2 || !strings.Contains(rec.Commands[1], "bootstrap") {
		t.Fatalf("launchd install commands = %+v", rec.Commands)
	}
	if _, err := launchd.Status(ctx, spec); err == nil || !strings.Contains(err.Error(), "launchctl denied") {
		t.Fatalf("launchd status err = %v", err)
	}

	systemSpec := spec
	systemSpec.Scope = serviceScopeSystem
	systemSpec.UnitPath = filepath.Join(t.TempDir(), "mycelium-real-fleet.service")
	rec = &serviceCommandRecorder{Err: errors.New("systemctl denied")}
	systemd := systemdManager{Run: rec.Run}
	if err := systemd.Install(ctx, systemSpec); err == nil || !strings.Contains(err.Error(), "systemctl denied") {
		t.Fatalf("systemd install err = %v", err)
	}
	if _, err := systemd.Status(ctx, systemSpec); err == nil || !strings.Contains(err.Error(), "systemctl denied") {
		t.Fatalf("systemd status err = %v", err)
	}
}

func TestServiceUtilityErrorBranches(t *testing.T) {
	if err := runService(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("runService usage err = %v", err)
	}
	if err := run(context.Background(), []string{"service"}); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("run service dispatch err = %v", err)
	}
	for _, goos := range []string{"darwin", "linux"} {
		if manager, err := serviceManagerForGOOS(goos, func(context.Context, string, ...string) error { return nil }); err != nil || manager == nil {
			t.Fatalf("serviceManagerForGOOS(%s) = %T %v", goos, manager, err)
		}
	}
	home := t.TempDir()
	spec := serviceSpec{Name: "default", Label: "com.mycelium.default", Home: home, LogDir: filepath.Join(home, "logs")}
	if path, err := serviceUnitPath(serviceSpec{Name: "default", Home: home, Scope: serviceScopeUser}, "linux"); err != nil || !strings.Contains(path, ".config/systemd/user/mycelium-default.service") {
		t.Fatalf("linux user unit path = %q %v", path, err)
	}
	if got := launchdDomain(serviceSpec{Scope: serviceScopeSystem}); got != "system" {
		t.Fatalf("system launchd domain = %s", got)
	}
	if current, err := osuser.Current(); err == nil && current.Username != "" {
		if homeDir, err := userHome(current.Username); err != nil || homeDir == "" {
			t.Fatalf("current user home = %q %v", homeDir, err)
		}
	}
	truePath, err := exec.LookPath("true")
	if err == nil {
		if err := runServiceCommand(context.Background(), truePath); err != nil {
			t.Fatalf("runServiceCommand true: %v", err)
		}
	}
	falsePath, err := exec.LookPath("false")
	if err == nil {
		if err := runServiceCommand(context.Background(), falsePath); err == nil {
			t.Fatal("runServiceCommand false succeeded")
		}
	}
	if truePath != "" {
		if err := (launchdManager{}).run(context.Background(), truePath); err != nil {
			t.Fatalf("launchd default runner: %v", err)
		}
	}
	fakeBin := filepath.Join(home, "bin")
	if err := os.MkdirAll(fakeBin, 0755); err != nil {
		t.Fatalf("fake bin: %v", err)
	}
	fakeSystemctl := filepath.Join(fakeBin, "systemctl")
	if err := os.WriteFile(fakeSystemctl, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("fake systemctl: %v", err)
	}
	fakeLaunchctl := filepath.Join(fakeBin, "launchctl")
	if err := os.WriteFile(fakeLaunchctl, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("fake launchctl: %v", err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	defaultLaunchdSpec := serviceSpec{
		Scope:      serviceScopeUser,
		Label:      "com.mycelium.default",
		ConfigPath: filepath.Join(home, "peer.json"),
		BinaryPath: "/bin/mycelium",
		LogDir:     filepath.Join(home, "logs"),
		UnitPath:   filepath.Join(home, "unit.plist"),
	}
	if err := (launchdManager{}).Install(context.Background(), defaultLaunchdSpec); err != nil {
		t.Fatalf("launchd default runner install: %v", err)
	}
	if status, err := (launchdManager{}).Status(context.Background(), defaultLaunchdSpec); err != nil || status != "active" {
		t.Fatalf("launchd default runner status = %q %v", status, err)
	}
	if err := (launchdManager{}).Uninstall(context.Background(), defaultLaunchdSpec); err != nil {
		t.Fatalf("launchd default runner uninstall: %v", err)
	}
	defaultSystemdSpec := serviceSpec{
		Scope:      serviceScopeSystem,
		ConfigPath: filepath.Join(home, "peer.json"),
		BinaryPath: "/bin/mycelium",
		LogDir:     filepath.Join(home, "logs"),
		UnitPath:   filepath.Join(home, "unit.service"),
	}
	if err := (systemdManager{}).Install(context.Background(), defaultSystemdSpec); err != nil {
		t.Fatalf("systemd default runner install: %v", err)
	}
	if status, err := (systemdManager{}).Status(context.Background(), defaultSystemdSpec); err != nil || status != "active" {
		t.Fatalf("systemd default runner status = %q %v", status, err)
	}
	if err := (systemdManager{}).Uninstall(context.Background(), defaultSystemdSpec); err != nil {
		t.Fatalf("systemd default runner uninstall: %v", err)
	}
	defaultSystemdSpec.Scope = serviceScopeUser
	defaultSystemdSpec.UnitPath = filepath.Join(home, "user-unit.service")
	if err := (systemdManager{}).Install(context.Background(), defaultSystemdSpec); err != nil {
		t.Fatalf("systemd user default runner install: %v", err)
	}
	if status, err := (systemdManager{}).Status(context.Background(), defaultSystemdSpec); err != nil || status != "active" {
		t.Fatalf("systemd user default runner status = %q %v", status, err)
	}
	if err := (systemdManager{}).Uninstall(context.Background(), defaultSystemdSpec); err != nil {
		t.Fatalf("systemd user default runner uninstall: %v", err)
	}
	errBoom := errors.New("manager")
	if err := runServiceWithManager(context.Background(), []string{"install", "--config", filepath.Join(home, "peer.json")}, fakeServiceManager{err: errBoom}, "darwin"); !errors.Is(err, errBoom) {
		t.Fatalf("install manager err = %v", err)
	}
	if err := runServiceWithManager(context.Background(), []string{"status", "--config", filepath.Join(home, "peer.json")}, fakeServiceManager{err: errBoom}, "darwin"); !errors.Is(err, errBoom) {
		t.Fatalf("status manager err = %v", err)
	}
	if err := runServiceWithManager(context.Background(), []string{"uninstall", "--config", filepath.Join(home, "peer.json")}, fakeServiceManager{err: errBoom}, "darwin"); !errors.Is(err, errBoom) {
		t.Fatalf("uninstall manager err = %v", err)
	}
	if _, err := servicePeerHealth(context.Background(), filepath.Join(home, "missing.json")); err == nil {
		t.Fatal("missing config health succeeded")
	}
	if _, err := servicePeerBaseURL("bad"); err == nil {
		t.Fatal("bad service base URL accepted")
	}
	spec.UnitPath = filepath.Join(home, "unit.plist")
	if err := writeServiceFile(spec.UnitPath, spec.LogDir, "unit"); err != nil {
		t.Fatalf("writeServiceFile: %v", err)
	}
	if data, err := os.ReadFile(spec.UnitPath); err != nil || string(data) != "unit" {
		t.Fatalf("unit data = %q %v", data, err)
	}
	blocked := filepath.Join(home, "blocked")
	if err := os.WriteFile(blocked, []byte("file"), 0644); err != nil {
		t.Fatalf("blocked file: %v", err)
	}
	if err := writeServiceFile(filepath.Join(blocked, "unit.plist"), spec.LogDir, "unit"); err == nil {
		t.Fatal("writeServiceFile accepted file parent")
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

func TestPeerConfigHelpersSeedMapsAndShutdowns(t *testing.T) {
	if got := defaultedBackendRuntimeConfig(BackendRuntimeConfig{}); got.Backend != domain.BackendLlamaCpp || got.LlamaServer != "llama-server" {
		t.Fatalf("default backend runtime = %+v", got)
	}
	var target string
	overrideString(nil, &target)
	if target != "" {
		t.Fatalf("nil override changed target to %q", target)
	}
	empty := ""
	overrideString(&empty, &target)
	if target != "" {
		t.Fatalf("empty override changed target to %q", target)
	}
	value := "set"
	overrideString(&value, &target)
	if target != "set" {
		t.Fatalf("override target = %q", target)
	}
	labels := withPeerBackendLabel(map[string]string{"existing": "yes"}, domain.BackendVLLM)
	if labels["existing"] != "yes" || labels[LabelPeerBackend] != string(domain.BackendVLLM) {
		t.Fatalf("labels = %+v", labels)
	}
	multiLabels := withPeerBackendLabels(map[string]string{"existing": "yes", LabelPeerBackend: string(domain.BackendVLLM)}, []domain.Backend{domain.BackendVLLM, domain.BackendLlamaCpp})
	if multiLabels["existing"] != "yes" || multiLabels[domain.LabelPeerBackends] != "llamacpp,vllm" || multiLabels[LabelPeerBackend] != "" {
		t.Fatalf("multi labels = %+v", multiLabels)
	}
	if combineShutdowns(nil) != nil {
		t.Fatal("empty shutdown list returned a function")
	}
	boom := errors.New("shutdown boom")
	shutdown := combineShutdowns([]func(context.Context) error{
		func(context.Context) error { return nil },
		func(context.Context) error { return boom },
	})
	if err := shutdown(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("shutdown err = %v", err)
	}

	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "seed.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	preset := testPreset("preset-a")
	preset.ModelRef = "model-a"
	preset.Aliases = []string{"alias-a", ""}
	cfg := PeerConfig{
		Projects:     []domain.Project{{ID: "project-a"}},
		Presets:      []domain.Preset{preset},
		Reservations: []domain.Reservation{{ID: "reservation-a", Kind: domain.ReservationHeadroom, NodeID: "node-a", Headroom: domain.Claim{WeightsMB: 1}}},
	}
	if err := seedControlStore(context.Background(), store, cfg); err != nil {
		t.Fatalf("seedControlStore: %v", err)
	}
	if _, err := store.Project(context.Background(), "project-a"); err != nil {
		t.Fatalf("seeded project: %v", err)
	}
	if got := presetMap([]domain.Preset{preset}); got["model-a"].ID != preset.ID || got["alias-a"].ID != preset.ID {
		t.Fatalf("preset map = %+v", got)
	}

	defaultStore, err := storesqlite.Open(filepath.Join(t.TempDir(), "default-seed.sqlite"))
	if err != nil {
		t.Fatalf("Open default: %v", err)
	}
	defer defaultStore.Close()
	if err := seedControlStore(context.Background(), defaultStore, PeerConfig{}); err != nil {
		t.Fatalf("seedControlStore default: %v", err)
	}
	defaultProject, err := defaultStore.Project(context.Background(), "default")
	if err != nil {
		t.Fatalf("default project: %v", err)
	}
	if defaultProject.Priority != domain.PriorityInteractive || defaultProject.ExpectedConcurrency != 1 || defaultProject.Preemption != domain.PreemptSoft {
		t.Fatalf("default project = %+v", defaultProject)
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
	recordSustainedContextMetrics(t, store, project.ID, "", now)
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
		Listen:          "127.0.0.1:0",
		StorePath:       dbPath,
		JoinToken:       "secret",
		RPCToken:        "rpc-secret",
		GatewayToken:    "gateway-secret",
		DiscoveryListen: "127.0.0.1:0",
		DiscoveryAddr:   "127.0.0.1:9",
		Presets:         []domain.Preset{preset},
		Reservations:    []domain.Reservation{{ID: "pin-a", Kind: domain.ReservationPinned, NodeID: "node-a", PresetID: preset.ID}},
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
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/invite", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("admin invite without rpc status/body = %d %q", rec.Code, rec.Body.String())
	}
	body := `{"model":"tiny","messages":[{"role":"user","content":"hi"}]}`
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("gateway accepted rpc token status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer gateway-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusUnauthorized {
		t.Fatalf("gateway rejected gateway token status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/invite", nil)
	req.Host = "peer-a.local:51846"
	req.Header.Set("Authorization", "Bearer rpc-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin invite status/body = %d %q", rec.Code, rec.Body.String())
	}
	var invite adminInviteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &invite); err != nil {
		t.Fatalf("decode invite: %v", err)
	}
	join, err := membership.ParseJoinToken(invite.Join)
	if err != nil {
		t.Fatalf("parse invite: %v", err)
	}
	if join.Address != "127.0.0.1:0" || join.Token != "secret" {
		t.Fatalf("invite join = %+v uri=%s", join, invite.Join)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/tokens/rotate", strings.NewReader(`{"token":"next-secret"}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate status/body = %d %q", rec.Code, rec.Body.String())
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read rotated config: %v", err)
	}
	var rotated PeerConfig
	if err := json.Unmarshal(data, &rotated); err != nil {
		t.Fatalf("decode rotated config: %v", err)
	}
	if rotated.JoinToken != "next-secret" {
		t.Fatalf("rotated config = %+v", rotated)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/admin/tokens", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("tokens without auth status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/admin/tokens", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("tokens status/body = %d %q", rec.Code, rec.Body.String())
	}
	var listed []domain.JoinTokenRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode tokens: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed tokens = %+v", listed)
	}
	for _, token := range listed {
		if token.Secret != "" {
			t.Fatalf("admin token list exposed secret: %+v", listed)
		}
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/tokens/rotate", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("generated rotate status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/tokens/revoke", strings.NewReader(`{"token":"secret"}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("revoke status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/peer/health", nil)
	req.Header.Set("X-Myc-Join-Token", "secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked peer health status/body = %d %q", rec.Code, rec.Body.String())
	}
	for _, tt := range []struct {
		method string
		path   string
		body   string
		want   int
	}{
		{method: http.MethodGet, path: "/admin/invite", want: http.StatusMethodNotAllowed},
		{method: http.MethodPost, path: "/admin/tokens", want: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/admin/tokens/rotate", want: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/admin/tokens/revoke", want: http.StatusMethodNotAllowed},
		{method: http.MethodPost, path: "/admin/tokens/rotate", body: `{`, want: http.StatusBadRequest},
		{method: http.MethodPost, path: "/admin/tokens/revoke", body: `{`, want: http.StatusBadRequest},
		{method: http.MethodPost, path: "/admin/tokens/revoke", body: `{}`, want: http.StatusBadRequest},
	} {
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
		req.Header.Set("Authorization", "Bearer rpc-secret")
		handler.ServeHTTP(rec, req)
		if rec.Code != tt.want {
			t.Fatalf("%s %s status/body = %d %q want %d", tt.method, tt.path, rec.Code, rec.Body.String(), tt.want)
		}
	}
	if got := adminJoinAddress("0.0.0.0:51846", "peer.local:51846"); got != "peer.local:51846" {
		t.Fatalf("wildcard join address = %s", got)
	}
	if got := adminJoinAddress("bad-listen", "peer.local:51846"); got != "bad-listen" {
		t.Fatalf("bad listen join address = %s", got)
	}
}

func TestBuildPeerGatewayJoinBootstrapsCleanHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	join := "mycjoin://127.0.0.1:1?token=join-secret"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr, handler, cleanup, err := buildPeerGateway(ctx, []string{"--join", join, "--rpc-token", "join-rpc", "--listen", "127.0.0.1:0", "--discovery-listen", "127.0.0.1:0", "--discovery-addr", "127.0.0.1:9"})
	if err != nil {
		t.Fatalf("buildPeerGateway clean join: %v", err)
	}
	if cleanup != nil {
		t.Fatal("thin clean-home peer returned compute cleanup")
	}
	if addr != "127.0.0.1:0" || handler == nil {
		t.Fatalf("addr=%s handler=%v", addr, handler)
	}

	configPath := filepath.Join(home, ".mycelium", "peer.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read bootstrapped config: %v", err)
	}
	var cfg PeerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("decode bootstrapped config: %v", err)
	}
	if cfg.JoinToken != "join-secret" || cfg.RPCToken != "join-rpc" || len(cfg.SeedPeers) != 1 || cfg.SeedPeers[0] != "127.0.0.1:1" || cfg.Compute {
		t.Fatalf("bootstrapped config = %+v", cfg)
	}
	if cfg.ID == "" || cfg.ID == "peer_local" || cfg.ComputeConfig.ID != cfg.ID {
		t.Fatalf("bootstrapped peer id was not unique: %+v", cfg)
	}

	store, err := storesqlite.Open(filepath.Join(home, ".mycelium", "mycelium.db"))
	if err != nil {
		t.Fatalf("open bootstrapped store: %v", err)
	}
	defer store.Close()
	tokens, err := store.ListJoinTokens(context.Background())
	if err != nil {
		t.Fatalf("ListJoinTokens: %v", err)
	}
	if len(tokens) != 1 || !tokens[0].Active || !tokens[0].Current {
		t.Fatalf("tokens = %+v", tokens)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/peer/health", nil)
	req.Header.Set("X-Myc-Join-Token", "join-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("peer health status/body = %d %q", rec.Code, rec.Body.String())
	}
}

func TestRunBootstrapAppliesCleanHomeJoinState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configPath := filepath.Join(home, ".mycelium", "peer.json")
	join := "mycjoin://127.0.0.1:51846?token=join-secret"
	if err := runBootstrap(context.Background(), []string{"--join", join, "--rpc-token", "join-rpc", "--compute", "off", "--config", configPath}); err != nil {
		t.Fatalf("dry-run bootstrap: %v", err)
	}
	if _, err := os.Stat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dry-run wrote config err=%v", err)
	}
	if err := runBootstrap(context.Background(), []string{"--join", join, "--rpc-token", "join-rpc", "--compute", "off", "--config", configPath, "--apply"}); err != nil {
		t.Fatalf("apply bootstrap: %v", err)
	}
	cfg, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load bootstrap config: %v", err)
	}
	if cfg.JoinToken != "join-secret" || cfg.RPCToken != "join-rpc" || len(cfg.SeedPeers) != 1 || cfg.SeedPeers[0] != "127.0.0.1:51846" || cfg.Compute {
		t.Fatalf("bootstrap config = %+v", cfg)
	}
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		t.Fatalf("Open bootstrap store: %v", err)
	}
	defer store.Close()
	tokens, err := store.ListJoinTokens(context.Background())
	if err != nil {
		t.Fatalf("ListJoinTokens: %v", err)
	}
	if len(tokens) != 1 || !tokens[0].Active || !tokens[0].Current {
		t.Fatalf("bootstrap tokens = %+v", tokens)
	}
	if err := runBootstrap(context.Background(), []string{"--join", "mycjoin://127.0.0.1:51846?token=join-secret", "--compute", "off"}); err == nil || !strings.Contains(err.Error(), "--rpc-token") {
		t.Fatalf("missing rpc token err = %v", err)
	}
	serviceConfig := filepath.Join(home, ".mycelium", "service-peer.json")
	if err := runBootstrapWithServiceManager(context.Background(), []string{"--join", join, "--rpc-token", "join-rpc", "--compute", "off", "--config", serviceConfig, "--apply", "--install-service"}, fakeServiceManager{}, "darwin"); err != nil {
		t.Fatalf("bootstrap install service: %v", err)
	}
	if err := runBootstrapWithServiceManager(context.Background(), []string{"--join", join, "--rpc-token", "join-rpc", "--compute", "off", "--config", filepath.Join(home, ".mycelium", "service-peer-err.json"), "--apply", "--install-service"}, fakeServiceManager{err: errors.New("service")}, "darwin"); err == nil || !strings.Contains(err.Error(), "service") {
		t.Fatalf("bootstrap service err = %v", err)
	}
	if err := runBootstrap(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "--join") {
		t.Fatalf("missing join err = %v", err)
	}
	if err := runBootstrap(context.Background(), []string{"--bad-flag"}); err == nil {
		t.Fatal("bad bootstrap flag accepted")
	}
	if err := runBootstrap(context.Background(), []string{"--join", "not://a-join"}); err == nil {
		t.Fatal("bad bootstrap join accepted")
	}
	if err := run(context.Background(), []string{"bootstrap"}); err == nil || !strings.Contains(err.Error(), "--join") {
		t.Fatalf("run bootstrap err = %v", err)
	}
}

func TestAdminHTTPFailureBranches(t *testing.T) {
	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	cfg := PeerConfig{Listen: "127.0.0.1:51846", RPCToken: "rpc-secret"}
	mux := http.NewServeMux()
	mountAdminHTTP(mux, &cfg, filepath.Join(t.TempDir(), "peer.json"), manager, failingTokenStore{}, "rpc-secret")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/invite", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("invite bad cfg status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/admin/tokens", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("tokens bad store status/body = %d %q", rec.Code, rec.Body.String())
	}
	cfg.JoinToken = "secret"
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/tokens/rotate", strings.NewReader(`{"token":"next"}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate status/body = %d %q", rec.Code, rec.Body.String())
	}
}

func TestCatalogStageHTTPMaterializesPresetAndLocality(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	model := filepath.Join(t.TempDir(), "tiny.gguf")
	if err := os.WriteFile(model, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	cfg := applyPeerConfigDefaults(PeerConfig{
		ID:         "peer-a",
		CatalogDir: filepath.Join(t.TempDir(), "catalog"),
		Compute:    true,
		GGUFParser: writeMetadataParser(t),
		ComputeConfig: ComputeConfig{
			ID:      "node-a",
			Backend: domain.BackendLlamaCpp,
		},
	})
	mux := http.NewServeMux()
	mountCatalogHTTP(mux, cfg, store, "rpc-secret", mocks.NewFakeClock(time.Unix(10, 0).UTC()))
	body, err := json.Marshal(catalogStageRequest{
		Preset: domain.Preset{
			ID:             "tiny",
			ModelRef:       model,
			Aliases:        []string{"tiny-model"},
			Backend:        domain.BackendLlamaCpp,
			ContextLength:  2048,
			Quant:          "Q4",
			Capabilities:   []domain.Capability{domain.CapabilityChat},
			ArtifactSizeMB: 1,
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/catalog/stage", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("stage status/body = %d %q", rec.Code, rec.Body.String())
	}
	var response catalogStageResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Locality.ID != "node-a:tiny" || response.Locality.State != domain.ModelLocalityReady || !response.Locality.Managed {
		t.Fatalf("locality response = %+v", response.Locality)
	}
	preset, err := store.Preset(ctx, "tiny")
	if err != nil {
		t.Fatalf("Preset: %v", err)
	}
	if preset.NodeID != "node-a" || !strings.Contains(preset.ModelRef, "tiny-tiny.gguf") {
		t.Fatalf("staged preset = %+v", preset)
	}
	localities, err := store.ListModelLocalities(ctx)
	if err != nil {
		t.Fatalf("ListModelLocalities: %v", err)
	}
	if len(localities) != 1 || localities[0].ID != response.Locality.ID {
		t.Fatalf("localities = %+v", localities)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].TaskType != "catalog_stage" || jobs[0].Status != domain.JobDone || len(jobs[0].Progress) == 0 {
		t.Fatalf("stage jobs = %+v", jobs)
	}
	if _, err := os.Stat(preset.ModelRef); err != nil {
		t.Fatalf("staged model stat: %v", err)
	}

	runtimeBody, err := json.Marshal(catalogStageRequest{
		Preset: domain.Preset{
			ID:             "runtime-hf",
			ModelRef:       model,
			Aliases:        []string{"runtime-model"},
			Backend:        domain.BackendLlamaCpp,
			ContextLength:  2048,
			Quant:          "int4",
			Capabilities:   []domain.Capability{domain.CapabilityChat},
			ArtifactSizeMB: 18432,
			NodeID:         "node-a",
		},
	})
	if err != nil {
		t.Fatalf("marshal runtime request: %v", err)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/catalog/stage", bytes.NewReader(runtimeBody))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runtime stage status/body = %d %q", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode runtime response: %v", err)
	}
	if response.Locality.ID != "node-a:runtime-hf" || response.Locality.Managed || response.Locality.Reason != "runtime source adopted" {
		t.Fatalf("runtime locality = %+v", response.Locality)
	}
	if response.Preset.ModelRef != model || response.Preset.ArtifactSizeMB != 10 || response.Preset.EstWeightsMB != 10 || response.Preset.KVPerTokenMB != 0.5 || response.Preset.ContextLength != 2048 || response.Preset.LayerCount != 12 {
		t.Fatalf("runtime preset did not adopt parsed metadata: %+v", response.Preset)
	}
	runtimePreset, err := store.Preset(ctx, "runtime-hf")
	if err != nil {
		t.Fatalf("runtime Preset: %v", err)
	}
	if runtimePreset.ModelRef != model || runtimePreset.ArtifactSizeMB != 10 || runtimePreset.EstWeightsMB != 10 || runtimePreset.KVPerTokenMB != 0.5 || runtimePreset.LayerCount != 12 {
		t.Fatalf("stored runtime preset = %+v", runtimePreset)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/catalog/stage", strings.NewReader(`{"preset":{"id":"missing-runtime","model_ref":"repo/model","node_id":"node-a"}}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "no such file") {
		t.Fatalf("missing-runtime status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/catalog/stage", strings.NewReader(`{"preset":{"id":"wrong-node","model_ref":"repo/model","node_id":"other-node"}}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "declared local") {
		t.Fatalf("wrong-node status/body = %d %q", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	evictBody, err := json.Marshal(catalogEvictRequest{PresetID: "tiny", NodeID: "node-a"})
	if err != nil {
		t.Fatalf("marshal evict: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/catalog/evict", bytes.NewReader(evictBody))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("evict status/body = %d %q", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(preset.ModelRef); !os.IsNotExist(err) {
		t.Fatalf("evicted model stat err = %v", err)
	}
	localities, err = store.ListModelLocalities(ctx)
	if err != nil || len(localities) != 1 || localities[0].ID != "node-a:runtime-hf" || localities[0].Managed {
		t.Fatalf("localities after evict = %+v %v", localities, err)
	}

	outside := filepath.Join(t.TempDir(), "outside.gguf")
	if err := os.WriteFile(outside, []byte("model"), 0644); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := store.SaveModelLocality(ctx, domain.ModelLocality{ID: "node-a:outside", PresetID: "outside", NodeID: "node-a", State: domain.ModelLocalityReady, ModelRef: outside, Managed: true}); err != nil {
		t.Fatalf("SaveModelLocality outside: %v", err)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/catalog/evict", strings.NewReader(`{"preset_id":"outside","node_id":"node-a"}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "managed catalog") {
		t.Fatalf("outside evict status/body = %d %q", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/catalog/stage", strings.NewReader(`{}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/catalog/stage", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/catalog/stage", strings.NewReader(`{`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/catalog/stage", strings.NewReader(`{"preset":{}}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "preset id") {
		t.Fatalf("missing id status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/catalog/stage", strings.NewReader(`{"preset":{"id":"no-source"}}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "source model ref") {
		t.Fatalf("missing source status/body = %d %q", rec.Code, rec.Body.String())
	}
	if got := firstPresetModelAlias(domain.Preset{ID: "id", ModelRef: "ref"}); got != "id" {
		t.Fatalf("alias fallback id = %s", got)
	}
	if got := firstPresetModelAlias(domain.Preset{ModelRef: "ref"}); got != "ref" {
		t.Fatalf("alias fallback ref = %s", got)
	}
}

func TestShouldAdoptRuntimeSource(t *testing.T) {
	model := filepath.Join(t.TempDir(), "model.gguf")
	if err := os.WriteFile(model, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "repo")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("mkdir repo: %v", err)
	}
	preset := domain.Preset{ID: "runtime", NodeID: "node-a"}
	for _, tt := range []struct {
		name   string
		preset domain.Preset
		source string
		nodeID string
		want   bool
	}{
		{name: "bare repo", preset: preset, source: "owner/repo", nodeID: "node-a"},
		{name: "local directory", preset: preset, source: dir, nodeID: "node-a"},
		{name: "regular file", preset: preset, source: model, nodeID: "node-a", want: true},
		{name: "hf artifact", preset: preset, source: "hf://owner/repo/model.gguf", nodeID: "node-a"},
		{name: "oci artifact", preset: preset, source: "oci://registry/repo:model", nodeID: "node-a"},
		{name: "wrong node", preset: preset, source: "owner/repo", nodeID: "node-b"},
		{name: "portable preset", preset: domain.Preset{ID: "portable"}, source: "owner/repo", nodeID: "node-a"},
	} {
		if got := shouldAdoptRuntimeSource(tt.preset, tt.source, tt.nodeID); got != tt.want {
			t.Fatalf("%s: got %t want %t", tt.name, got, tt.want)
		}
	}
}

func TestCatalogStageRuntimeAdoptionRequiresInspection(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	model := filepath.Join(t.TempDir(), "runtime.gguf")
	if err := os.WriteFile(model, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	cfg := applyPeerConfigDefaults(PeerConfig{
		ID:         "peer-a",
		CatalogDir: filepath.Join(t.TempDir(), "catalog"),
		Compute:    true,
		ComputeConfig: ComputeConfig{
			ID:      "node-a",
			Backend: domain.BackendLlamaCpp,
		},
	})
	mux := http.NewServeMux()
	mountCatalogHTTP(mux, cfg, store, "rpc-secret", mocks.NewFakeClock(time.Unix(10, 0).UTC()))
	body, err := json.Marshal(catalogStageRequest{Preset: domain.Preset{
		ID:       "runtime",
		ModelRef: model,
		Backend:  domain.BackendLlamaCpp,
		NodeID:   "node-a",
		Aliases:  []string{"runtime"},
	}})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/catalog/stage", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "gguf parser") {
		t.Fatalf("runtime adoption without parser status/body = %d %q", rec.Code, rec.Body.String())
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != domain.JobFailed {
		t.Fatalf("jobs = %+v", jobs)
	}
	localities, err := store.ListModelLocalities(ctx)
	if err != nil {
		t.Fatalf("ListModelLocalities: %v", err)
	}
	if len(localities) != 0 {
		t.Fatalf("runtime adoption created locality = %+v", localities)
	}
}

func TestCatalogEvictHTTPProtectionBranches(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	cfg := applyPeerConfigDefaults(PeerConfig{ID: "peer-a", CatalogDir: filepath.Join(t.TempDir(), "catalog"), ComputeConfig: ComputeConfig{ID: "node-a"}})
	modelPath := filepath.Join(cfg.CatalogDir, "models", "model.gguf")
	if err := os.MkdirAll(filepath.Dir(modelPath), 0755); err != nil {
		t.Fatalf("mkdir model dir: %v", err)
	}
	mux := http.NewServeMux()
	mountCatalogHTTP(mux, cfg, store, "rpc-secret", mocks.NewFakeClock(time.Unix(10, 0).UTC()))

	for _, tt := range []struct {
		name     string
		body     string
		method   string
		setup    func()
		wantCode int
		wantBody string
	}{
		{name: "method", method: http.MethodGet, body: `{}`, wantCode: http.StatusMethodNotAllowed},
		{name: "bad json", method: http.MethodPost, body: `{`, wantCode: http.StatusBadRequest},
		{name: "missing fields", method: http.MethodPost, body: `{}`, wantCode: http.StatusBadRequest, wantBody: "preset_id"},
		{name: "missing locality", method: http.MethodPost, body: `{"preset_id":"missing","node_id":"node-a"}`, wantCode: http.StatusBadRequest, wantBody: "not found"},
		{name: "unmanaged", method: http.MethodPost, body: `{"preset_id":"user","node_id":"node-a"}`, setup: func() {
			mustSaveLocality(t, store, domain.ModelLocality{ID: "node-a:user", PresetID: "user", NodeID: "node-a", State: domain.ModelLocalityReady, ModelRef: modelPath})
		}, wantCode: http.StatusConflict, wantBody: "not managed"},
		{name: "pinned", method: http.MethodPost, body: `{"preset_id":"pinned","node_id":"node-a"}`, setup: func() {
			mustSaveLocality(t, store, domain.ModelLocality{ID: "node-a:pinned", PresetID: "pinned", NodeID: "node-a", State: domain.ModelLocalityReady, ModelRef: modelPath, Managed: true, Pinned: true})
		}, wantCode: http.StatusConflict, wantBody: "warm or pinned"},
		{name: "live instance", method: http.MethodPost, body: `{"preset_id":"live","node_id":"node-a"}`, setup: func() {
			mustSaveLocality(t, store, domain.ModelLocality{ID: "node-a:live", PresetID: "live", NodeID: "node-a", State: domain.ModelLocalityReady, ModelRef: modelPath, Managed: true})
			if err := store.SaveInstance(ctx, domain.ModelInstance{ID: "inst-live", PresetID: "live", NodeID: "node-a", State: domain.InstReady}); err != nil {
				t.Fatalf("SaveInstance: %v", err)
			}
		}, wantCode: http.StatusConflict, wantBody: "live instance"},
		{name: "reservation", method: http.MethodPost, body: `{"preset_id":"reserved","node_id":"node-a"}`, setup: func() {
			mustSaveLocality(t, store, domain.ModelLocality{ID: "node-a:reserved", PresetID: "reserved", NodeID: "node-a", State: domain.ModelLocalityReady, ModelRef: modelPath, Managed: true})
			if err := store.SaveReservation(ctx, domain.Reservation{ID: "res-reserved", PresetID: "reserved", NodeID: "node-a"}); err != nil {
				t.Fatalf("SaveReservation: %v", err)
			}
		}, wantCode: http.StatusConflict, wantBody: "reservation"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, "/catalog/evict", strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer rpc-secret")
			mux.ServeHTTP(rec, req)
			if rec.Code != tt.wantCode || (tt.wantBody != "" && !strings.Contains(rec.Body.String(), tt.wantBody)) {
				t.Fatalf("status/body = %d %q", rec.Code, rec.Body.String())
			}
		})
	}
}

func mustSaveLocality(t *testing.T, store *storesqlite.Store, locality domain.ModelLocality) {
	t.Helper()
	if err := store.SaveModelLocality(context.Background(), locality); err != nil {
		t.Fatalf("SaveModelLocality: %v", err)
	}
}

func TestBootstrapEngineHelpers(t *testing.T) {
	configured := domain.EngineProfile{ID: "configured", Backend: domain.BackendVLLM, Ready: true}
	merged := mergeEngineProfiles(configured, []domain.EngineProfile{
		{ID: "llama", Backend: domain.BackendLlamaCpp, Ready: true},
		{ID: "vllm-default", Backend: domain.BackendVLLM, Ready: true},
	})
	if len(merged) != 2 || merged[0].ID != "configured" || merged[1].ID != "llama" {
		t.Fatalf("merged = %+v", merged)
	}
	if chosen, ok := chooseReadyEngine(merged, domain.BackendVLLM); !ok || chosen.ID != "configured" {
		t.Fatalf("chosen preferred = %+v %t", chosen, ok)
	}
	if chosen, ok := chooseReadyEngine([]domain.EngineProfile{{ID: "mlx", Backend: domain.BackendMLX, Ready: true}}, domain.BackendVLLM); !ok || chosen.ID != "mlx" {
		t.Fatalf("chosen fallback = %+v %t", chosen, ok)
	}
	if _, ok := chooseReadyEngine([]domain.EngineProfile{{ID: "bad", Backend: domain.BackendMLX}}, domain.BackendVLLM); ok {
		t.Fatal("unready engine chosen")
	}
	if got := diskSummary(domain.HostFacts{}); got != "disk=unknown" {
		t.Fatalf("empty disk summary = %s", got)
	}
	if got := diskSummary(domain.HostFacts{DiskFreeMB: 25, DiskTotalMB: 100}); got != "disk_free=25MB/100MB" {
		t.Fatalf("disk summary = %s", got)
	}
	cfg := engineConfigFromCompute(ComputeConfig{Backend: domain.BackendCustom, BackendBinary: "/bin/custom", CustomArgs: []string{"--x"}, HealthPath: "/ready", MaxUtil: 0.5, DiskMinFreeRatio: 0.25})
	if cfg.Backend != domain.BackendCustom || cfg.BackendBinary != "/bin/custom" || cfg.HealthPath != "/ready" || cfg.MaxUtil != 0.5 || cfg.DiskMinFreeRatio != 0.25 {
		t.Fatalf("engine config = %+v", cfg)
	}
	if got := engineDefaultBinary(domain.BackendVLLM); got != "vllm" {
		t.Fatalf("engine default = %s", got)
	}
	if got := engineDefaultBinary(domain.BackendMLX); got != "mlx_lm.server" {
		t.Fatalf("mlx default = %s", got)
	}
	if got := engineDefaultBinary(domain.BackendLlamaCpp); got != "llama-server" {
		t.Fatalf("llama default = %s", got)
	}
	if got := engineDefaultBinary(domain.BackendCustom); got != "" {
		t.Fatalf("custom default = %s", got)
	}
}

func TestBootstrapDoctorConfigLoadsOrDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg, err := bootstrapDoctorConfig("")
	if err != nil {
		t.Fatalf("default doctor config: %v", err)
	}
	if cfg.Listen != "127.0.0.1:51846" || cfg.ComputeConfig.ID != "peer_local" {
		t.Fatalf("default doctor config = %+v", cfg)
	}

	configPath := filepath.Join(t.TempDir(), "peer.json")
	if err := savePeerConfig(configPath, PeerConfig{ID: "peer-a", Listen: "127.0.0.1:9", ComputeConfig: ComputeConfig{ID: "peer-a"}}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	cfg, err = bootstrapDoctorConfig(configPath)
	if err != nil {
		t.Fatalf("load doctor config: %v", err)
	}
	if cfg.ID != "peer-a" || cfg.Listen != "127.0.0.1:9" {
		t.Fatalf("loaded doctor config = %+v", cfg)
	}

	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte(`{`), 0600); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := bootstrapDoctorConfig(badPath); err == nil {
		t.Fatal("bad doctor config accepted")
	}
	if err := runBootstrapDoctor(context.Background(), badPath, false); err == nil || !strings.Contains(err.Error(), "parse peer config") {
		t.Fatalf("bad doctor wrapper err = %v", err)
	}
}

func TestBootstrapDoctorAndAdoptionWithFakeDetector(t *testing.T) {
	detector := fakeBootstrapEngineDetector{
		host: domain.HostFacts{NodeID: "peer-a", Platform: "linux/arm64", OOMSeverity: domain.OOMCatastrophic, DiskFreeMB: 10, DiskTotalMB: 20, Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
		profiles: []domain.EngineProfile{
			{ID: "engine-vllm", Backend: domain.BackendVLLM, BinaryPath: "/opt/vllm", Args: []string{"--gpu-memory-utilization", "0.85"}, Ready: true},
			{ID: "engine-mlx", Backend: domain.BackendMLX, Ready: false, UnreadyReason: "unsupported platform"},
		},
		configured: domain.EngineProfile{ID: "configured-vllm", Backend: domain.BackendVLLM, BinaryPath: "/opt/vllm", Args: []string{"--gpu-memory-utilization", "0.85"}, HealthPath: "/health", Ready: true},
	}
	cfg := applyPeerConfigDefaults(PeerConfig{Compute: true, ComputeConfig: ComputeConfig{ID: "peer-a", Backend: domain.BackendVLLM}})
	if err := runBootstrapDoctorWithDetector(context.Background(), cfg, false, detector); err != nil {
		t.Fatalf("doctor text: %v", err)
	}
	if err := runBootstrapDoctorWithDetector(context.Background(), cfg, true, detector); err != nil {
		t.Fatalf("doctor json: %v", err)
	}
	report, err := detectBootstrapEnginesWithDetector(context.Background(), cfg, detector)
	if err != nil {
		t.Fatalf("detect bootstrap engines: %v", err)
	}
	if report.Host.Platform != "linux/arm64" || len(report.Engines) != 2 || report.Engines[0].ID != "configured-vllm" {
		t.Fatalf("report = %+v", report)
	}
	if _, err := detectBootstrapEnginesWithDetector(context.Background(), cfg, fakeBootstrapEngineDetector{host: detector.host, enginesErr: errors.New("engines")}); err == nil {
		t.Fatal("engine detection failure accepted")
	}
	if err := runBootstrapDoctorWithDetector(context.Background(), cfg, false, fakeBootstrapEngineDetector{err: errors.New("host")}); err == nil {
		t.Fatal("doctor host failure accepted")
	}
	if err := adoptBootstrapEngineWithDetector(context.Background(), &cfg, "auto", detector); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if cfg.ComputeConfig.BackendBinary != "/opt/vllm" || !reflect.DeepEqual(cfg.ComputeConfig.CustomArgs, []string{"--gpu-memory-utilization", "0.85"}) || len(cfg.EngineProfiles) != 1 || cfg.EngineProfiles[0].Backend != domain.BackendVLLM {
		t.Fatalf("adopted config = %+v", cfg)
	}
	cfg = applyPeerConfigDefaults(PeerConfig{Compute: true, ComputeConfig: ComputeConfig{ID: "peer-a", Backend: domain.BackendVLLM}})
	if err := adoptBootstrapEngineWithDetector(context.Background(), &cfg, "auto", fakeBootstrapEngineDetector{err: errors.New("detect")}); err != nil || cfg.Compute {
		t.Fatalf("auto detect failure cfg=%+v err=%v", cfg, err)
	}
	cfg = applyPeerConfigDefaults(PeerConfig{Compute: true, ComputeConfig: ComputeConfig{ID: "peer-a", Backend: domain.BackendVLLM}})
	if err := adoptBootstrapEngineWithDetector(context.Background(), &cfg, "on", fakeBootstrapEngineDetector{err: errors.New("detect")}); err == nil {
		t.Fatal("compute-on detect failure accepted")
	}
	cfg = applyPeerConfigDefaults(PeerConfig{Compute: true, ComputeConfig: ComputeConfig{ID: "peer-a", Backend: domain.BackendVLLM}})
	if err := adoptBootstrapEngineWithDetector(context.Background(), &cfg, "on", fakeBootstrapEngineDetector{host: detector.host, configured: domain.EngineProfile{Backend: domain.BackendVLLM}, profiles: []domain.EngineProfile{{Backend: domain.BackendVLLM}}}); err == nil {
		t.Fatal("compute-on no-ready engine accepted")
	}
	report, err = detectBootstrapEngines(context.Background(), applyPeerConfigDefaults(PeerConfig{ComputeConfig: ComputeConfig{ID: "peer-real", Backend: domain.BackendLlamaCpp}}))
	if err != nil {
		t.Fatalf("real bootstrap detector wrapper: %v", err)
	}
	if report.Host.Platform == "" || len(report.Engines) == 0 {
		t.Fatalf("real detector report = %+v", report)
	}
}

func TestBootstrapDoctorPlansModelRootCompatibility(t *testing.T) {
	root := t.TempDir()
	modelDir := filepath.Join(root, "gemma-ov")
	if err := os.Mkdir(modelDir, 0755); err != nil {
		t.Fatalf("mkdir model: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelDir, "openvino_language_model.xml"), []byte("<xml/>"), 0644); err != nil {
		t.Fatalf("write xml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelDir, "openvino_language_model.bin"), []byte("weights"), 0644); err != nil {
		t.Fatalf("write bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelDir, "config.json"), []byte(`{"model_type":"gemma4","architectures":["Gemma4ForConditionalGeneration"],"vision_config":{},"text_config":{"max_position_embeddings":262144}}`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	report, err := detectBootstrapEnginesWithDetectorOptions(context.Background(), applyPeerConfigDefaults(PeerConfig{ComputeConfig: ComputeConfig{ID: "b70", Backend: domain.BackendOpenVINO}}), bootstrapDoctorOptions{
		RequestedEngines: []domain.Backend{domain.BackendOpenVINO},
		ModelRoots:       []string{root},
	}, fakeBootstrapEngineDetector{
		host: domain.HostFacts{NodeID: "b70", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}},
		profiles: []domain.EngineProfile{{
			ID:                 "openvino",
			Backend:            domain.BackendOpenVINO,
			Ready:              true,
			SupportedModels:    []string{catalog.FormatOpenVINOIR},
			SupportedPlatforms: []string{"linux/amd64"},
			ArtifactPlatform:   "linux/amd64",
		}},
	})
	if err != nil {
		t.Fatalf("detect with model root: %v", err)
	}
	if len(report.Plan.ModelCandidates) != 1 || report.Plan.ModelCandidates[0].Name != "gemma-ov" {
		t.Fatalf("model candidates = %+v", report.Plan.ModelCandidates)
	}
	if got := report.Plan.ModelCandidates[0].Backends; len(got) != 1 || got[0].Backend != domain.BackendOpenVINO || !got[0].Ready {
		t.Fatalf("model backend compatibility = %+v", got)
	}
	if engines, err := parseBootstrapEngines("vllm,sglang,openvino"); err != nil || !reflect.DeepEqual(engines, []domain.Backend{domain.BackendVLLM, domain.BackendSGLang, domain.BackendOpenVINO}) {
		t.Fatalf("parse engines = %+v %v", engines, err)
	}
	if _, err := parseBootstrapEngines("bogus"); err == nil {
		t.Fatal("unknown engine accepted")
	}
}

func TestBootstrapDoctorSavePlanAndEnginesCommands(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "control.db")
	cfg := applyPeerConfigDefaults(PeerConfig{StorePath: storePath, ComputeConfig: ComputeConfig{ID: "node-a", Backend: domain.BackendVLLM}})
	detector := fakeBootstrapEngineDetector{
		host: domain.HostFacts{NodeID: "node-a", OS: "linux", Arch: "arm64", Platform: "linux/arm64", OOMSeverity: domain.OOMCatastrophic, Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
		profiles: []domain.EngineProfile{{
			ID:                 "engine-vllm",
			Backend:            domain.BackendVLLM,
			Ready:              true,
			BinaryPath:         "/opt/vllm",
			Args:               []string{"--gpu-memory-utilization", "0.85"},
			SupportedModels:    []string{catalog.FormatHFTransformers},
			SupportedPlatforms: []string{"linux/arm64"},
			ArtifactPlatform:   "linux/arm64",
		}},
		configured: domain.EngineProfile{
			ID:                 "configured-vllm",
			Backend:            domain.BackendVLLM,
			Ready:              true,
			BinaryPath:         "/opt/vllm",
			Args:               []string{"--gpu-memory-utilization", "0.85"},
			SupportedPlatforms: []string{"linux/arm64"},
			ArtifactPlatform:   "linux/arm64",
		},
	}
	if err := runBootstrapDoctorWithDetectorOptions(context.Background(), cfg, bootstrapDoctorOptions{RequestedEngines: []domain.Backend{domain.BackendVLLM}}, detector); err != nil {
		t.Fatalf("dry doctor: %v", err)
	}
	if _, err := os.Stat(storePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dry doctor wrote store err=%v", err)
	}
	if err := runBootstrapDoctorWithDetectorOptions(context.Background(), cfg, bootstrapDoctorOptions{RequestedEngines: []domain.Backend{domain.BackendVLLM}, SavePlan: true}, detector); err != nil {
		t.Fatalf("save doctor: %v", err)
	}
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	profiles, err := store.ListEngineProfiles(context.Background())
	if err != nil || len(profiles) != 1 || profiles[0].Backend != domain.BackendVLLM || !profiles[0].Ready {
		t.Fatalf("engine profiles = %+v %v", profiles, err)
	}
	plans, err := store.ListBootstrapPlans(context.Background())
	if err != nil || len(plans) != 1 || len(plans[0].ResultingProfiles) != 1 {
		t.Fatalf("bootstrap plans = %+v %v", plans, err)
	}
	store.Close()
	if err := runEngines(context.Background(), []string{"list", "--db", storePath}); err != nil {
		t.Fatalf("engines list: %v", err)
	}
	if err := runEngines(context.Background(), []string{"list", "--db", storePath, "--json"}); err != nil {
		t.Fatalf("engines list json: %v", err)
	}
	if err := runEngines(context.Background(), []string{"doctor", "--db", storePath}); err != nil {
		t.Fatalf("engines doctor: %v", err)
	}
	if err := runEngines(context.Background(), []string{"doctor", "--db", storePath, "--json"}); err != nil {
		t.Fatalf("engines doctor json: %v", err)
	}
	if err := run(context.Background(), []string{"engines", "list", "--db", storePath}); err != nil {
		t.Fatalf("run engines list: %v", err)
	}
	if err := runEngines(context.Background(), nil); err == nil {
		t.Fatal("missing engines subcommand accepted")
	}
	if err := runEngines(context.Background(), []string{"bogus"}); err == nil {
		t.Fatal("bad engines subcommand accepted")
	}
	if err := runEngines(context.Background(), []string{"doctor", "--db", storePath, "--peer", "remote"}); err == nil {
		t.Fatal("remote engines doctor accepted")
	}
}

func TestEnginesPreflightUsesSavedReadinessFacts(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	profile := keyedTestProfile(domain.EngineProfile{
		ID:               "engine-openvino",
		Backend:          domain.BackendOpenVINO,
		ManagedBy:        "system",
		DisplayName:      "OpenVINO",
		BinaryPath:       "openvino-genai-openai",
		Ready:            true,
		ArtifactPlatform: "linux/amd64",
	}, host, "")
	if err := store.SaveEngineProfile(context.Background(), profile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	if err := store.SaveBootstrapPlan(context.Background(), domain.BootstrapPlan{
		ID:                "plan-preflight",
		CreatedAt:         time.Unix(9, 0).UTC(),
		Host:              host,
		ResultingProfiles: []domain.EngineProfile{profile},
	}); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}
	store.Close()

	if err := runEngines(context.Background(), []string{"preflight", "--db", storePath, "--backend", "openvino", "--model", "google/gemma-4-31B"}); err != nil {
		t.Fatalf("engines preflight ready: %v", err)
	}
	if err := runEngines(context.Background(), []string{"preflight", "--db", storePath, "--backend", "openvino", "--model", "google/gemma-4-31B", "--json"}); err != nil {
		t.Fatalf("engines preflight json: %v", err)
	}

	emptyPath := filepath.Join(t.TempDir(), "empty.db")
	emptyStore, err := storesqlite.Open(emptyPath)
	if err != nil {
		t.Fatalf("open empty store: %v", err)
	}
	emptyStore.Close()
	if err := runEngines(context.Background(), []string{"preflight", "--db", emptyPath, "--backend", "vllm"}); err != nil {
		t.Fatalf("legacy unverified preflight should be visible but non-fatal: %v", err)
	}
	if err := runEngines(context.Background(), []string{"preflight", "--db", emptyPath, "--backend", "vllm", "--strict"}); err == nil || !strings.Contains(err.Error(), "legacy_config_unverified") {
		t.Fatalf("strict missing preflight err = %v", err)
	}

	unreadyPath := filepath.Join(t.TempDir(), "unready.db")
	unreadyStore, err := storesqlite.Open(unreadyPath)
	if err != nil {
		t.Fatalf("open unready store: %v", err)
	}
	vllmHost := domain.HostFacts{OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	unreadyProfile := keyedTestProfile(domain.EngineProfile{ID: "engine-vllm", Backend: domain.BackendVLLM, ManagedBy: "system", DisplayName: "vLLM", BinaryPath: "vllm", Ready: true}, vllmHost, "")
	if err := unreadyStore.SaveEngineProfile(context.Background(), unreadyProfile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	if err := unreadyStore.SaveBootstrapPlan(context.Background(), domain.BootstrapPlan{ID: "plan-unready", CreatedAt: time.Unix(9, 0).UTC(), Host: vllmHost, ResultingProfiles: []domain.EngineProfile{unreadyProfile}}); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}
	if err := unreadyStore.MarkEngineProfileUnready(context.Background(), "engine-vllm", "driver mismatch"); err != nil {
		t.Fatalf("MarkEngineProfileUnready: %v", err)
	}
	unreadyStore.Close()
	if err := runEngines(context.Background(), []string{"preflight", "--db", unreadyPath, "--backend", "vllm"}); err == nil || !strings.Contains(err.Error(), "driver mismatch") {
		t.Fatalf("unready preflight err = %v", err)
	}
}

func TestModelsCompatUsesSavedReadinessFacts(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	b70 := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	openvino := keyedTestProfile(domain.EngineProfile{
		ID:              "engine-openvino",
		Backend:         domain.BackendOpenVINO,
		DisplayName:     "OpenVINO",
		BinaryPath:      "openvino-server",
		Ready:           true,
		SupportedModels: []string{catalog.FormatOpenVINOIR},
	}, b70, "")
	spark := domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	vllm := keyedTestProfile(domain.EngineProfile{
		ID:              "engine-vllm",
		Backend:         domain.BackendVLLM,
		DisplayName:     "vLLM",
		BinaryPath:      "vllm",
		Ready:           true,
		SupportedModels: []string{catalog.FormatHFTransformers},
	}, spark, "")
	if err := store.SaveEngineProfile(context.Background(), openvino); err != nil {
		t.Fatalf("SaveEngineProfile openvino: %v", err)
	}
	if err := store.SaveEngineProfile(context.Background(), vllm); err != nil {
		t.Fatalf("SaveEngineProfile vllm: %v", err)
	}
	if err := store.SaveBootstrapPlan(context.Background(), domain.BootstrapPlan{ID: "plan-b70", CreatedAt: time.Unix(1, 0).UTC(), Host: b70, ResultingProfiles: []domain.EngineProfile{openvino}}); err != nil {
		t.Fatalf("SaveBootstrapPlan b70: %v", err)
	}
	if err := store.SaveBootstrapPlan(context.Background(), domain.BootstrapPlan{ID: "plan-spark", CreatedAt: time.Unix(2, 0).UTC(), Host: spark, ResultingProfiles: []domain.EngineProfile{vllm}}); err != nil {
		t.Fatalf("SaveBootstrapPlan spark: %v", err)
	}
	if err := store.SavePreset(context.Background(), domain.Preset{ID: "manual", NodeID: "legacy", Backend: domain.BackendLlamaCpp, ModelRef: "/manual/model", ContextLength: 4096, EstWeightsMB: 1, KVPerTokenMB: 0.1}); err != nil {
		t.Fatalf("SavePreset manual: %v", err)
	}
	store.Close()

	args := []string{
		"compat",
		"--db", storePath,
		"--model", "logical/gemma",
		"--artifact", "/models/gemma-ov",
		"--format", catalog.FormatOpenVINOIR,
		"--artifact", "hf://logical/gemma",
		"--format", "safetensors",
	}
	if err := runModels(context.Background(), args); err != nil {
		t.Fatalf("models compat: %v", err)
	}
	if err := runModels(context.Background(), append(args, "--json")); err != nil {
		t.Fatalf("models compat json: %v", err)
	}
	if err := run(context.Background(), append([]string{"models"}, args...)); err != nil {
		t.Fatalf("run models compat: %v", err)
	}
	if err := runModels(context.Background(), []string{"compat", "--db", storePath, "--model", "logical/gemma", "--artifact", "/models/gemma-ov"}); err == nil || !strings.Contains(err.Error(), "one --format") {
		t.Fatalf("models compat missing format err = %v", err)
	}
}

func TestCommandHelperFlagsStoresAndQueries(t *testing.T) {
	var artifacts repeatedString
	if got := artifacts.String(); got != "null" {
		t.Fatalf("empty repeated string = %s", got)
	}
	if err := artifacts.Set("/models/a"); err != nil {
		t.Fatalf("artifact set: %v", err)
	}
	if got := artifacts.String(); !strings.Contains(got, "/models/a") {
		t.Fatalf("artifact string = %s", got)
	}

	var roots stringListFlag
	if got := roots.String(); got != "" {
		t.Fatalf("empty roots = %s", got)
	}
	if err := roots.Set(""); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("empty root err = %v", err)
	}
	if err := roots.Set("/models"); err != nil || roots.String() != "/models" {
		t.Fatalf("roots = %s err=%v", roots.String(), err)
	}

	if err := runModels(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "models subcommand") {
		t.Fatalf("models usage err = %v", err)
	}
	if err := runModels(context.Background(), []string{"unknown"}); err == nil || !strings.Contains(err.Error(), "unknown models") {
		t.Fatalf("models unknown err = %v", err)
	}

	storePath := filepath.Join(t.TempDir(), "control.db")
	store, err := openEngineStore("", storePath)
	if err != nil {
		t.Fatalf("openEngineStore db: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close db store: %v", err)
	}
	configPath := writePeerConfig(t, PeerConfig{StorePath: filepath.Join(t.TempDir(), "from-config.db")})
	store, err = openEngineStore(configPath, "")
	if err != nil {
		t.Fatalf("openEngineStore config: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close config store: %v", err)
	}
	tokenMap := gatewayProjectTokenMap([]GatewayProjectToken{{Project: "project-a", Token: "token-a"}, {Project: "project-b", Token: "token-b"}})
	if tokenMap["token-a"] != "project-a" || tokenMap["token-b"] != "project-b" || gatewayProjectTokenMap(nil) != nil {
		t.Fatalf("gateway token map = %+v", tokenMap)
	}

	since := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	query, err := parseSessionMetricQuery(url.Values{
		"session_id": {"session-a"},
		"project":    {"project-a"},
		"node_id":    {"node-a"},
		"since":      {since.Format(time.RFC3339Nano)},
		"until":      {until.Format(time.RFC3339Nano)},
		"limit":      {"25"},
	})
	if err != nil || query.SessionID != "session-a" || query.Project != "project-a" || query.NodeID != "node-a" || !query.Since.Equal(since) || !query.Until.Equal(until) || query.Limit != 25 {
		t.Fatalf("session query = %+v err=%v", query, err)
	}
	for _, values := range []url.Values{
		{"since": {"not-time"}},
		{"until": {"not-time"}},
		{"limit": {"-1"}},
		{"limit": {"nope"}},
	} {
		if _, err := parseSessionMetricQuery(values); err == nil {
			t.Fatalf("bad session query accepted: %+v", values)
		}
	}
}

func TestCommandSmallContracts(t *testing.T) {
	var compute computeOverrideFlag
	if got := compute.String(); got != "" {
		t.Fatalf("unset compute flag = %q", got)
	}
	if err := compute.Set("yes"); err != nil || compute.String() != "on" || !compute.value {
		t.Fatalf("compute on = %q value=%v err=%v", compute.String(), compute.value, err)
	}
	if err := compute.Set("0"); err != nil || compute.String() != "off" || compute.value {
		t.Fatalf("compute off = %q value=%v err=%v", compute.String(), compute.value, err)
	}
	if !compute.IsBoolFlag() {
		t.Fatal("compute flag no longer behaves as a boolean CLI flag")
	}
	if !gatewayAuthConfigured(PeerConfig{GatewayToken: "token"}) || !gatewayAuthConfigured(PeerConfig{GatewayProjectTokens: []GatewayProjectToken{{Project: "project-a", Token: "token-a"}}}) || gatewayAuthConfigured(PeerConfig{}) {
		t.Fatal("gateway auth configured classification drifted")
	}
	if !peerListenRequiresAuth("0.0.0.0:55291") || !peerListenRequiresAuth("mycelium.local:55291") || peerListenRequiresAuth("127.0.0.1:55291") {
		t.Fatal("peer listen auth classification drifted")
	}
	if _, err := servicePeerBaseURL("not-a-listen-address"); err == nil {
		t.Fatal("invalid peer listen address accepted")
	}
	if got := shellQuote(""); got != "''" {
		t.Fatalf("empty shell quote = %q", got)
	}
	if got := shellQuote("/tmp/my config's/peer.json"); !strings.Contains(got, "'\"'\"'") {
		t.Fatalf("single quote shell escaping = %q", got)
	}
	if got := quoteSystemdArg("/tmp/my config/peer.json"); !strings.Contains(got, "\\\"") && !strings.HasPrefix(got, "\"") {
		t.Fatalf("systemd arg quote = %q", got)
	}

	source := map[string]string{"artifact": "model"}
	cloned := cloneStringMap(source)
	source["artifact"] = "mutated"
	if cloned["artifact"] != "model" || cloneStringMap(nil) != nil {
		t.Fatalf("cloneStringMap = %+v", cloned)
	}

	candidate := domain.BootstrapModelCandidate{Name: "gemma-ov"}
	key := domain.CompatibilityKey{
		OS:                 "linux",
		CPUArch:            "amd64",
		AcceleratorVendor:  "intel",
		AcceleratorRuntime: "level-zero",
		EngineDistribution: "OpenVINO GenAI:openvino-genai-openai",
		Backend:            domain.BackendOpenVINO,
		ModelFormat:        "openvino-ir",
	}
	ready := map[domain.Backend]domain.EngineProfile{
		domain.BackendOpenVINO: {ID: "engine-openvino", Backend: domain.BackendOpenVINO, CompatibilityKey: key},
	}
	profile, ok, reason := readyProfileForCompatibility(domain.BootstrapPlan{}, candidate, domain.BootstrapBackendCompatibility{Backend: domain.BackendOpenVINO, Ready: true, CompatibilityKey: key}, ready)
	if !ok || profile.ID != "engine-openvino" || reason != "" {
		t.Fatalf("ready profile = %+v ok=%v reason=%q", profile, ok, reason)
	}
	if _, ok, reason := readyProfileForCompatibility(domain.BootstrapPlan{}, candidate, domain.BootstrapBackendCompatibility{Backend: domain.BackendOpenVINO}, ready); ok || reason != "backend compatibility is not ready" {
		t.Fatalf("unready compatibility ok=%v reason=%q", ok, reason)
	}
	if _, ok, reason := readyProfileForCompatibility(domain.BootstrapPlan{}, candidate, domain.BootstrapBackendCompatibility{Backend: domain.BackendVLLM, Ready: true}, ready); ok || !strings.Contains(reason, "no saved ready engine profile") {
		t.Fatalf("missing ready profile ok=%v reason=%q", ok, reason)
	}
	if _, ok, reason := readyProfileForCompatibility(domain.BootstrapPlan{}, candidate, domain.BootstrapBackendCompatibility{Backend: domain.BackendOpenVINO, Ready: true, EngineProfileID: "other"}, ready); ok || !strings.Contains(reason, "references engine profile") {
		t.Fatalf("mismatched ready profile ok=%v reason=%q", ok, reason)
	}
}

func TestEnginesInstallPlanIsNonMutatingAndHostAware(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	host := domain.HostFacts{
		NodeID:       "b70",
		OS:           "linux",
		Arch:         "amd64",
		Platform:     "linux/amd64",
		Accelerators: []domain.Accelerator{{Vendor: "intel"}},
	}
	if err := store.SaveBootstrapPlan(context.Background(), domain.BootstrapPlan{ID: "plan-b70", CreatedAt: time.Unix(40, 0).UTC(), Host: host}); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}
	store.Close()

	if err := runEngines(context.Background(), []string{"install-plan", "--db", storePath, "--backend", "openvino", "--host", "b70"}); err != nil {
		t.Fatalf("install-plan saved host: %v", err)
	}
	if err := runEngines(context.Background(), []string{"install-plan", "--db", storePath, "--backend", "openvino", "--host", "b70", "--json"}); err != nil {
		t.Fatalf("install-plan json: %v", err)
	}
	store, err = storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	profiles, err := store.ListEngineProfiles(context.Background())
	if err != nil {
		t.Fatalf("ListEngineProfiles: %v", err)
	}
	plans, err := store.ListBootstrapPlans(context.Background())
	if err != nil {
		t.Fatalf("ListBootstrapPlans: %v", err)
	}
	store.Close()
	if len(profiles) != 0 || len(plans) != 1 {
		t.Fatalf("install-plan mutated store: profiles=%+v plans=%+v", profiles, plans)
	}

	emptyPath := filepath.Join(t.TempDir(), "empty.db")
	emptyStore, err := storesqlite.Open(emptyPath)
	if err != nil {
		t.Fatalf("open empty store: %v", err)
	}
	emptyStore.Close()
	if err := runEngines(context.Background(), []string{"install-plan", "--db", emptyPath, "--backend", "vllm", "--host", "linux/arm64", "--accelerator-vendor", "nvidia"}); err != nil {
		t.Fatalf("install-plan synthetic spark: %v", err)
	}
	if err := runEngines(context.Background(), []string{"install-plan", "--db", emptyPath, "--backend", "mlx", "--host", "darwin/amd64"}); err != nil {
		t.Fatalf("install-plan unsupported still reports: %v", err)
	}
	if err := runEngines(context.Background(), []string{"install-plan", "--db", emptyPath, "--backend", "custom", "--host", "linux/amd64", "--accelerator-vendor", "nvidia"}); err != nil {
		t.Fatalf("install-plan custom manual: %v", err)
	}
	if err := runEngines(context.Background(), []string{"install-plan", "--db", emptyPath, "--backend", "vllm"}); err == nil || !strings.Contains(err.Error(), "--host is required") {
		t.Fatalf("missing host err = %v", err)
	}
}

func TestEnginesApplyMaterializesReadyProfilesAndGeneratedPresets(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "control.db")
	configPath := filepath.Join(dir, "peer.json")
	cfg := applyPeerConfigDefaults(PeerConfig{
		StorePath: storePath,
		Compute:   true,
		ComputeConfig: ComputeConfig{
			ID:      "b70",
			Backend: domain.BackendVLLM,
		},
	})
	if err := savePeerConfig(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	binaryPath := filepath.Join(dir, "openvino-run")
	if err := os.WriteFile(binaryPath, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	ready := keyedTestProfile(domain.EngineProfile{
		ID:                 "engine-openvino",
		Backend:            domain.BackendOpenVINO,
		ManagedBy:          "system",
		DisplayName:        "OpenVINO",
		BinaryPath:         binaryPath,
		Args:               []string{"--device", "GPU"},
		Ready:              true,
		SupportedModels:    []string{catalog.FormatOpenVINOIR},
		SupportedPlatforms: []string{"linux/amd64"},
		ArtifactPlatform:   "linux/amd64",
	}, host, "")
	unready := keyedTestProfile(domain.EngineProfile{
		ID:            "engine-vllm",
		Backend:       domain.BackendVLLM,
		ManagedBy:     "system",
		DisplayName:   "vLLM",
		BinaryPath:    "vllm",
		Ready:         false,
		UnreadyReason: "driver mismatch",
	}, domain.HostFacts{OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}, "")
	plan := domain.BootstrapPlan{
		ID:        "plan-a",
		CreatedAt: time.Unix(10, 0).UTC(),
		Host:      host,
		ResultingProfiles: []domain.EngineProfile{
			ready,
			unready,
		},
		ModelCandidates: []domain.BootstrapModelCandidate{{
			HostID:        "b70",
			Name:          "gemma-ov",
			Path:          "/models/gemma-ov",
			Format:        catalog.FormatOpenVINOIR,
			SizeMB:        53000,
			ContextLength: 262144,
			Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityVision},
			Metadata:      map[string]string{"kv_per_token_mb": "0.25"},
			Backends:      []domain.BootstrapBackendCompatibility{{Backend: domain.BackendOpenVINO, Ready: true}},
		}},
	}
	if err := store.SaveEngineProfile(context.Background(), ready); err != nil {
		t.Fatalf("SaveEngineProfile ready: %v", err)
	}
	if err := store.SaveEngineProfile(context.Background(), unready); err != nil {
		t.Fatalf("SaveEngineProfile unready: %v", err)
	}
	if err := store.SaveBootstrapPlan(context.Background(), plan); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}
	store.Close()

	if err := runEngines(context.Background(), []string{"apply", "--config", configPath, "--db", storePath}); err != nil {
		t.Fatalf("dry apply: %v", err)
	}
	dryConfig, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load dry config: %v", err)
	}
	if len(dryConfig.EngineProfiles) != 0 || len(dryConfig.ComputeConfig.Backends) != 0 {
		t.Fatalf("dry apply mutated config = %+v", dryConfig)
	}
	store, err = storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	if _, err := store.Preset(context.Background(), "gemma-ov"); err == nil {
		t.Fatal("dry apply wrote generated preset")
	}
	store.Close()

	previewStore, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("preview store: %v", err)
	}
	previewPlan, err := previewStore.BootstrapPlan(context.Background(), plan.ID)
	if err != nil {
		t.Fatalf("preview plan: %v", err)
	}
	previewReport, _, err := buildEngineApplyReport(context.Background(), previewStore, dryConfig, configPath, previewPlan, false)
	previewStore.Close()
	if err != nil {
		t.Fatalf("preview report: %v", err)
	}
	if !hasApplyEvidence(previewReport.Verification, "engine_binary", ready.ID, "pass") || !hasApplyEvidence(previewReport.Verification, "store_preset_visibility", "gemma-ov", "unknown") {
		t.Fatalf("preview verification = %+v", previewReport.Verification)
	}

	if err := runEngines(context.Background(), []string{"apply", "--config", configPath, "--db", storePath, "--write"}); err != nil {
		t.Fatalf("write apply: %v", err)
	}
	backups, err := filepath.Glob(configPath + ".backup.*")
	if err != nil {
		t.Fatalf("glob backups: %v", err)
	}
	if len(backups) != 1 {
		t.Fatalf("backups = %+v", backups)
	}
	backupConfig, err := loadPeerConfig(backups[0])
	if err != nil {
		t.Fatalf("load backup config: %v", err)
	}
	if len(backupConfig.EngineProfiles) != 0 || backupConfig.ComputeConfig.Backend != domain.BackendVLLM {
		t.Fatalf("backup config = %+v", backupConfig)
	}
	appliedConfig, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load applied config: %v", err)
	}
	if len(appliedConfig.EngineProfiles) != 1 || appliedConfig.EngineProfiles[0].ID != ready.ID || appliedConfig.EngineProfiles[0].ManagedBy != "bootstrap" {
		t.Fatalf("engine profiles = %+v", appliedConfig.EngineProfiles)
	}
	if len(appliedConfig.ComputeConfig.Backends) != 1 || appliedConfig.ComputeConfig.Backends[0].Backend != domain.BackendOpenVINO || appliedConfig.ComputeConfig.Backends[0].BackendBinary != ready.BinaryPath {
		t.Fatalf("runtime profiles = %+v", appliedConfig.ComputeConfig.Backends)
	}
	if appliedConfig.ComputeConfig.Backend != domain.BackendOpenVINO {
		t.Fatalf("primary backend = %s", appliedConfig.ComputeConfig.Backend)
	}
	if len(appliedConfig.Presets) != 1 || appliedConfig.Presets[0].GeneratedBy != "bootstrap" || appliedConfig.Presets[0].EngineProfileID != ready.ID {
		t.Fatalf("config presets = %+v", appliedConfig.Presets)
	}
	store, err = storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("reopen applied store: %v", err)
	}
	defer store.Close()
	preset, err := store.Preset(context.Background(), "gemma-ov")
	if err != nil {
		t.Fatalf("generated preset not visible to store loader: %v", err)
	}
	if preset.Backend != domain.BackendOpenVINO || preset.ModelRef != "/models/gemma-ov" || preset.ContextLength != 262144 || preset.EstWeightsMB != 53000 || preset.KVPerTokenMB != 0.25 || preset.GeneratedFrom != plan.ID {
		t.Fatalf("generated preset = %+v", preset)
	}
}

func TestEnginesApplyReportsMissingModelMetadataWithoutWritingPreset(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "control.db")
	configPath := filepath.Join(dir, "peer.json")
	cfg := applyPeerConfigDefaults(PeerConfig{StorePath: storePath, Compute: true, ComputeConfig: ComputeConfig{ID: "b70", Backend: domain.BackendOpenVINO}})
	if err := savePeerConfig(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	ready := keyedTestProfile(domain.EngineProfile{ID: "engine-openvino", Backend: domain.BackendOpenVINO, ManagedBy: "system", DisplayName: "OpenVINO", BinaryPath: "/opt/openvino/run", Ready: true}, host, "")
	plan := domain.BootstrapPlan{
		ID:        "plan-missing-metadata",
		CreatedAt: time.Unix(11, 0).UTC(),
		Host:      host,
		ResultingProfiles: []domain.EngineProfile{
			ready,
		},
		ModelCandidates: []domain.BootstrapModelCandidate{{
			HostID:        "b70",
			Name:          "gemma-ov",
			Path:          "/models/gemma-ov",
			Format:        catalog.FormatOpenVINOIR,
			SizeMB:        53000,
			ContextLength: 262144,
			Capabilities:  []domain.Capability{domain.CapabilityChat},
			Backends:      []domain.BootstrapBackendCompatibility{{Backend: domain.BackendOpenVINO, Ready: true}},
		}},
	}
	if err := store.SaveEngineProfile(context.Background(), ready); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	if err := store.SaveBootstrapPlan(context.Background(), plan); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}
	store.Close()

	if err := runEngines(context.Background(), []string{"apply", "--config", configPath, "--db", storePath, "--write"}); err != nil {
		t.Fatalf("write apply: %v", err)
	}
	store, err = storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()
	if _, err := store.Preset(context.Background(), "gemma-ov"); err == nil {
		t.Fatal("apply wrote preset without kv metadata")
	}
	appliedConfig, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(appliedConfig.EngineProfiles) != 1 || len(appliedConfig.Presets) != 0 {
		t.Fatalf("applied config = %+v", appliedConfig)
	}
}

func TestEnginesApplyRejectsCompatibilityKeyMismatch(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "control.db")
	configPath := filepath.Join(dir, "peer.json")
	cfg := applyPeerConfigDefaults(PeerConfig{
		StorePath: storePath,
		Compute:   true,
		ComputeConfig: ComputeConfig{
			ID:      "b70",
			Backend: domain.BackendVLLM,
		},
	})
	if err := savePeerConfig(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	savedHost := domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	targetHost := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	profile := keyedTestProfile(domain.EngineProfile{
		ID:          "engine-vllm",
		Backend:     domain.BackendVLLM,
		ManagedBy:   "system",
		DisplayName: "vLLM",
		BinaryPath:  "vllm",
		Ready:       true,
	}, savedHost, "")
	plan := domain.BootstrapPlan{
		ID:                "plan-mismatch",
		CreatedAt:         time.Unix(13, 0).UTC(),
		Host:              targetHost,
		ResultingProfiles: []domain.EngineProfile{profile},
	}
	if err := store.SaveEngineProfile(context.Background(), profile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	report, _, err := buildEngineApplyReport(context.Background(), store, cfg, configPath, plan, false)
	if err == nil || !strings.Contains(err.Error(), "no saved ready engine profiles") {
		t.Fatalf("apply mismatch err = %v", err)
	}
	if len(report.Blocked) != 1 || !strings.Contains(report.Blocked[0].Reason, "compatibility_key_mismatch") {
		t.Fatalf("blocked = %+v", report.Blocked)
	}
}

func TestBootstrapDoc04EndToEndTempContract(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	storePath := filepath.Join(dir, "control.db")
	configPath := filepath.Join(dir, "peer.json")
	modelDir := filepath.Join(dir, "models", "gemma-ov")
	if err := os.MkdirAll(modelDir, 0755); err != nil {
		t.Fatalf("mkdir model dir: %v", err)
	}
	binaryPath := filepath.Join(dir, "openvino-genai-openai")
	if err := os.WriteFile(binaryPath, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	cfg := applyPeerConfigDefaults(PeerConfig{
		StorePath: storePath,
		Compute:   true,
		ComputeConfig: ComputeConfig{
			ID:      "b70",
			Backend: domain.BackendOpenVINO,
		},
	})
	if err := savePeerConfig(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	host := domain.HostFacts{
		NodeID:       "b70",
		OS:           "linux",
		Arch:         "amd64",
		Platform:     "linux/amd64",
		Accelerators: []domain.Accelerator{{Vendor: "intel", Kind: "arc-pro-b70"}},
	}
	openvino := keyedTestProfile(domain.EngineProfile{
		ID:                 "engine-openvino",
		Backend:            domain.BackendOpenVINO,
		ManagedBy:          "system",
		DisplayName:        "OpenVINO",
		BinaryPath:         binaryPath,
		Ready:              true,
		SupportedModels:    []string{catalog.FormatOpenVINOIR},
		SupportedPlatforms: []string{"linux/amd64"},
		ArtifactPlatform:   "linux/amd64",
	}, host, "")
	vllm := keyedTestProfile(domain.EngineProfile{
		ID:                 "engine-vllm",
		Backend:            domain.BackendVLLM,
		ManagedBy:          "system",
		DisplayName:        "vLLM",
		BinaryPath:         "vllm",
		Ready:              false,
		UnreadyReason:      "driver mismatch",
		SupportedModels:    []string{catalog.FormatHFTransformers},
		SupportedPlatforms: []string{"linux/amd64"},
		ArtifactPlatform:   "linux/amd64",
	}, host, "")
	model := domain.BootstrapModelCandidate{
		HostID:        "b70",
		Name:          "gemma-ov",
		Path:          modelDir,
		Format:        catalog.FormatOpenVINOIR,
		SizeMB:        2,
		ContextLength: 8192,
		Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityVision},
		Metadata:      map[string]string{"kv_per_token_mb": "0.25"},
		Backends:      []domain.BootstrapBackendCompatibility{{Backend: domain.BackendOpenVINO}},
	}

	if err := runBootstrapDoctorWithDetectorOptions(ctx, cfg, bootstrapDoctorOptions{
		ConfigPath:       configPath,
		SavePlan:         true,
		RequestedEngines: []domain.Backend{domain.BackendOpenVINO, domain.BackendVLLM},
		ModelCandidates:  []domain.BootstrapModelCandidate{model},
	}, fakeBootstrapEngineDetector{host: host, profiles: []domain.EngineProfile{openvino, vllm}, configured: openvino}); err != nil {
		t.Fatalf("bootstrap doctor save plan: %v", err)
	}

	if err := runEngines(ctx, []string{"list", "--db", storePath}); err != nil {
		t.Fatalf("engines list: %v", err)
	}
	if err := runEngines(ctx, []string{"doctor", "--db", storePath}); err != nil {
		t.Fatalf("engines doctor: %v", err)
	}
	if err := runEngines(ctx, []string{"preflight", "--db", storePath, "--backend", "openvino", "--model-format", catalog.FormatOpenVINOIR}); err != nil {
		t.Fatalf("matching preflight: %v", err)
	}
	if err := runEngines(ctx, []string{"preflight", "--db", storePath, "--backend", "openvino", "--host-platform", "linux/arm64", "--accelerator-vendor", "nvidia", "--accelerator-runtime", "cuda"}); err == nil || !strings.Contains(err.Error(), "compatibility_key_mismatch") {
		t.Fatalf("mismatch preflight err = %v", err)
	}
	if err := runEngines(ctx, []string{"preflight", "--db", storePath, "--backend", "vllm"}); err == nil || !strings.Contains(err.Error(), "driver mismatch") {
		t.Fatalf("unready preflight err = %v", err)
	}

	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	manual := testPreset("manual-legacy")
	manual.Backend = domain.BackendLlamaCpp
	if err := store.SavePreset(ctx, manual); err != nil {
		t.Fatalf("SavePreset manual: %v", err)
	}
	store.Close()

	if err := runEngines(ctx, []string{"apply", "--config", configPath, "--db", storePath}); err != nil {
		t.Fatalf("apply preview: %v", err)
	}
	previewConfig, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load preview config: %v", err)
	}
	if len(previewConfig.EngineProfiles) != 0 || len(previewConfig.ComputeConfig.Backends) != 0 {
		t.Fatalf("preview mutated config = %+v", previewConfig)
	}
	store, err = storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store after preview: %v", err)
	}
	if _, err := store.Preset(ctx, "gemma-ov"); err == nil {
		t.Fatal("preview materialized generated preset")
	}
	transient, err := loadRuntimePresets(ctx, store, domain.Node{ID: "b70", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}})
	if err != nil {
		t.Fatalf("runtime presets before apply: %v", err)
	}
	if !hasRuntimePreset(transient.Presets, "gemma-ov", domain.EngineReadinessReadyProfile) || !hasRuntimePreset(transient.Presets, "manual-legacy", domain.EngineReadinessLegacyConfigUnverified) {
		t.Fatalf("transient presets = %+v blocked=%+v", transient.Presets, transient.Blocked)
	}
	store.Close()

	if err := runEngines(ctx, []string{"apply", "--config", configPath, "--db", storePath, "--write"}); err != nil {
		t.Fatalf("apply write: %v", err)
	}
	appliedConfig, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load applied config: %v", err)
	}
	if len(appliedConfig.EngineProfiles) != 1 || appliedConfig.EngineProfiles[0].ID != openvino.ID || len(appliedConfig.ComputeConfig.Backends) != 1 {
		t.Fatalf("applied config = %+v", appliedConfig)
	}
	backups, err := filepath.Glob(configPath + ".backup.*")
	if err != nil {
		t.Fatalf("glob backups: %v", err)
	}
	if len(backups) != 1 {
		t.Fatalf("backups = %+v", backups)
	}
	store, err = storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store after write: %v", err)
	}
	defer store.Close()
	generated, err := store.Preset(ctx, "gemma-ov")
	if err != nil {
		t.Fatalf("generated preset: %v", err)
	}
	if generated.EngineProfileID != openvino.ID || generated.GeneratedBy != "bootstrap" || generated.ModelFormat != catalog.FormatOpenVINOIR {
		t.Fatalf("generated preset = %+v", generated)
	}
	loaded, err := loadRuntimePresets(ctx, store, domain.Node{ID: "b70", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}})
	if err != nil {
		t.Fatalf("runtime presets after apply: %v", err)
	}
	if !hasRuntimePreset(loaded.Presets, "gemma-ov", domain.EngineReadinessReadyProfile) || !hasRuntimePreset(loaded.Presets, "manual-legacy", domain.EngineReadinessLegacyConfigUnverified) {
		t.Fatalf("loaded presets = %+v blocked=%+v", loaded.Presets, loaded.Blocked)
	}
}

func TestEnginesApplyRestoresBackupWhenPostWriteValidationFails(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "control.db")
	configPath := filepath.Join(dir, "peer.json")
	cfg := applyPeerConfigDefaults(PeerConfig{
		StorePath: storePath,
		Compute:   true,
		ComputeConfig: ComputeConfig{
			ID:      "b70",
			Backend: domain.BackendVLLM,
		},
	})
	if err := savePeerConfig(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	ready := keyedTestProfile(domain.EngineProfile{ID: "engine-openvino", Backend: domain.BackendOpenVINO, ManagedBy: "system", DisplayName: "OpenVINO", BinaryPath: "openvino-genai-openai", Ready: true}, host, "")
	plan := domain.BootstrapPlan{
		ID:                "plan-restore",
		CreatedAt:         time.Unix(12, 0).UTC(),
		Host:              host,
		ResultingProfiles: []domain.EngineProfile{ready},
	}
	if err := store.SaveEngineProfile(context.Background(), ready); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	if err := store.SaveBootstrapPlan(context.Background(), plan); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}
	store.Close()

	err = runEnginesApplyWithOptions(context.Background(), engineApplyOptions{
		ConfigPath: configPath,
		DBPath:     storePath,
		PlanID:     plan.ID,
		Write:      true,
		PostWriteValidate: func(string) error {
			return errors.New("post validation failed")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "post validation failed") {
		t.Fatalf("apply err = %v", err)
	}
	backups, err := filepath.Glob(configPath + ".backup.*")
	if err != nil {
		t.Fatalf("glob backups: %v", err)
	}
	if len(backups) != 1 {
		t.Fatalf("backups = %+v", backups)
	}
	restored, err := loadPeerConfig(configPath)
	if err != nil {
		t.Fatalf("load restored config: %v", err)
	}
	if len(restored.EngineProfiles) != 0 || restored.ComputeConfig.Backend != domain.BackendVLLM {
		t.Fatalf("restored config = %+v", restored)
	}
}

func TestEnginesApplyVerificationEvidenceReportsPassFailAndUnknown(t *testing.T) {
	dir := t.TempDir()
	exists := filepath.Join(dir, "engine-ok")
	if err := os.WriteFile(exists, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatalf("write engine: %v", err)
	}
	profiles := []domain.EngineProfile{
		{ID: "exists", Backend: domain.BackendOpenVINO, BinaryPath: exists, Ready: true, Version: "1.0"},
		{ID: "missing", Backend: domain.BackendVLLM, BinaryPath: filepath.Join(dir, "missing"), Ready: true},
		{ID: "path-lookup", Backend: domain.BackendLlamaCpp, BinaryPath: "llama-server", Ready: true},
	}
	evidence := verifyEngineProfiles(profiles)
	if !hasApplyEvidence(evidence, "engine_binary", "exists", "pass") {
		t.Fatalf("missing binary pass evidence: %+v", evidence)
	}
	if !hasApplyEvidence(evidence, "engine_binary", "missing", "fail") {
		t.Fatalf("missing binary fail evidence: %+v", evidence)
	}
	if !hasApplyEvidence(evidence, "engine_binary", "path-lookup", "unknown") {
		t.Fatalf("missing binary unknown evidence: %+v", evidence)
	}
	if !hasApplyEvidence(evidence, "engine_version", "exists", "pass") || !hasApplyEvidence(evidence, "engine_version", "missing", "unknown") {
		t.Fatalf("version evidence = %+v", evidence)
	}
}

func TestRuntimePresetLoaderReadsSavedReadyBootstrapFactsWithoutApply(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	profile := keyedTestProfile(domain.EngineProfile{
		ID:              "engine-openvino",
		Backend:         domain.BackendOpenVINO,
		ManagedBy:       "system",
		DisplayName:     "OpenVINO",
		BinaryPath:      "/opt/openvino/run",
		Ready:           true,
		SupportedModels: []string{catalog.FormatOpenVINOIR},
	}, host, "")
	if err := store.SaveEngineProfile(context.Background(), profile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	plan := domain.BootstrapPlan{
		ID:        "plan-runtime",
		CreatedAt: time.Unix(20, 0).UTC(),
		Host:      host,
		ResultingProfiles: []domain.EngineProfile{
			profile,
		},
		ModelCandidates: []domain.BootstrapModelCandidate{{
			HostID:        "b70",
			Name:          "gemma-ov",
			Path:          "/models/gemma-ov",
			Format:        catalog.FormatOpenVINOIR,
			SizeMB:        53000,
			ContextLength: 262144,
			Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityVision},
			Metadata:      map[string]string{"kv_per_token_mb": "0.25"},
			Backends:      []domain.BootstrapBackendCompatibility{{Backend: domain.BackendOpenVINO, Ready: true}},
		}},
	}
	if err := store.SaveBootstrapPlan(context.Background(), plan); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}

	report, err := loadRuntimePresets(context.Background(), store)
	if err != nil {
		t.Fatalf("loadRuntimePresets: %v", err)
	}
	if len(report.Presets) != 1 {
		t.Fatalf("presets = %+v blocked=%+v", report.Presets, report.Blocked)
	}
	preset := report.Presets[0]
	if preset.ID != "gemma-ov" || preset.Backend != domain.BackendOpenVINO || preset.EngineProfileID != profile.ID || preset.EngineReadiness != domain.EngineReadinessReadyProfile {
		t.Fatalf("runtime preset = %+v", preset)
	}
	if _, err := store.Preset(context.Background(), "gemma-ov"); err == nil {
		t.Fatal("runtime loader materialized transient preset into store")
	}
}

func TestEnginesReloadDryRunPreviewsRefreshedRuntimePresets(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	manual := testPreset("manual")
	manual.Backend = domain.BackendLlamaCpp
	if err := store.SavePreset(context.Background(), manual); err != nil {
		t.Fatalf("SavePreset manual: %v", err)
	}
	initial, err := loadRuntimePresets(context.Background(), store)
	if err != nil {
		t.Fatalf("initial loadRuntimePresets: %v", err)
	}
	if !hasRuntimePreset(initial.Presets, "manual", domain.EngineReadinessLegacyConfigUnverified) {
		t.Fatalf("initial presets = %+v", initial.Presets)
	}
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	profile := keyedTestProfile(domain.EngineProfile{ID: "engine-openvino", Backend: domain.BackendOpenVINO, DisplayName: "OpenVINO", BinaryPath: "openvino", Ready: true, SupportedModels: []string{catalog.FormatOpenVINOIR}}, host, "")
	if err := store.SaveEngineProfile(context.Background(), profile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	plan := domain.BootstrapPlan{
		ID:                "plan-runtime-refresh",
		CreatedAt:         time.Unix(30, 0).UTC(),
		Host:              host,
		ResultingProfiles: []domain.EngineProfile{profile},
		ModelCandidates: []domain.BootstrapModelCandidate{{
			HostID:        "b70",
			Name:          "gemma-ov",
			Path:          "/models/gemma-ov",
			Format:        catalog.FormatOpenVINOIR,
			SizeMB:        53000,
			ContextLength: 262144,
			Capabilities:  []domain.Capability{domain.CapabilityChat},
			Metadata:      map[string]string{"kv_per_token_mb": "0.25"},
			Backends:      []domain.BootstrapBackendCompatibility{{Backend: domain.BackendOpenVINO, Ready: true}},
		}},
	}
	if err := store.SaveBootstrapPlan(context.Background(), plan); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}
	refreshed, err := loadRuntimePresets(context.Background(), store)
	if err != nil {
		t.Fatalf("refreshed loadRuntimePresets: %v", err)
	}
	if !hasRuntimePreset(refreshed.Presets, "gemma-ov", domain.EngineReadinessReadyProfile) || !hasRuntimePreset(refreshed.Presets, "manual", domain.EngineReadinessLegacyConfigUnverified) {
		t.Fatalf("refreshed presets = %+v blocked=%+v", refreshed.Presets, refreshed.Blocked)
	}
	store.Close()

	if err := runEngines(context.Background(), []string{"reload", "--db", storePath, "--dry-run"}); err != nil {
		t.Fatalf("engines reload dry-run: %v", err)
	}
	if err := runEngines(context.Background(), []string{"reload", "--db", storePath, "--dry-run", "--json"}); err != nil {
		t.Fatalf("engines reload dry-run json: %v", err)
	}
	if err := runEngines(context.Background(), []string{"reload", "--db", storePath}); err == nil || !strings.Contains(err.Error(), "not live-enabled") {
		t.Fatalf("engines reload live err = %v", err)
	}
}

func TestRuntimePresetLoaderBlocksSavedUnreadyProfilePreset(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	preset := testPreset("qwen")
	preset.Backend = domain.BackendVLLM
	preset.GeneratedBy = "bootstrap"
	preset.EngineProfileID = "engine-vllm"
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	if err := store.SaveEngineProfile(context.Background(), domain.EngineProfile{ID: "engine-vllm", Backend: domain.BackendVLLM, Ready: false, UnreadyReason: "driver mismatch"}); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}

	report, err := loadRuntimePresets(context.Background(), store)
	if err != nil {
		t.Fatalf("loadRuntimePresets: %v", err)
	}
	if len(report.Presets) != 0 {
		t.Fatalf("unready preset loaded = %+v", report.Presets)
	}
	if len(report.Blocked) != 1 || report.Blocked[0].ID != "qwen" || !strings.Contains(report.Blocked[0].Reason, "driver mismatch") {
		t.Fatalf("blocked = %+v", report.Blocked)
	}
}

func TestRuntimePresetLoaderKeepsManualLegacyPresetsVisible(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	preset := testPreset("manual")
	preset.Backend = domain.BackendVLLM
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}

	report, err := loadRuntimePresets(context.Background(), store)
	if err != nil {
		t.Fatalf("loadRuntimePresets: %v", err)
	}
	if len(report.Presets) != 1 || report.Presets[0].ID != "manual" {
		t.Fatalf("presets = %+v", report.Presets)
	}
	if report.Presets[0].EngineReadiness != domain.EngineReadinessLegacyConfigUnverified || !strings.Contains(report.Presets[0].EngineReadinessReason, "legacy_config_unverified") {
		t.Fatalf("legacy readiness = %+v", report.Presets[0])
	}
}

func TestRuntimePresetLoaderBlocksCompatibilityKeyMismatch(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	savedHost := domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	profile := keyedTestProfile(domain.EngineProfile{ID: "engine-vllm", Backend: domain.BackendVLLM, ManagedBy: "system", DisplayName: "vLLM", BinaryPath: "vllm", Ready: true}, savedHost, "")
	if err := store.SaveEngineProfile(context.Background(), profile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	preset := testPreset("qwen")
	preset.Backend = domain.BackendVLLM
	preset.GeneratedBy = "bootstrap"
	preset.EngineProfileID = profile.ID
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	target := domain.Node{ID: "b70", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	report, err := loadRuntimePresets(context.Background(), store, target)
	if err != nil {
		t.Fatalf("loadRuntimePresets: %v", err)
	}
	if len(report.Presets) != 0 || len(report.Blocked) != 1 || !strings.Contains(report.Blocked[0].Reason, "compatibility_key_mismatch") {
		t.Fatalf("report = %+v", report)
	}
}

func TestRuntimePresetLoaderKeepsIncompleteKeyExplicitlyUnverified(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	profile := domain.EngineProfile{ID: "engine-vllm", Backend: domain.BackendVLLM, Ready: true}
	if err := store.SaveEngineProfile(context.Background(), profile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	preset := testPreset("qwen")
	preset.Backend = domain.BackendVLLM
	preset.GeneratedBy = "bootstrap"
	preset.EngineProfileID = profile.ID
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	report, err := loadRuntimePresets(context.Background(), store, domain.Node{OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}})
	if err != nil {
		t.Fatalf("loadRuntimePresets: %v", err)
	}
	if len(report.Presets) != 1 || report.Presets[0].EngineReadiness != domain.EngineReadinessCompatibilityKeyIncomplete || !strings.Contains(report.Presets[0].EngineReadinessReason, "compatibility_key_incomplete") {
		t.Fatalf("presets = %+v blocked=%+v", report.Presets, report.Blocked)
	}
}

func TestRuntimePresetLoaderDoesNotDuplicateAppliedGeneratedPreset(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	profile := keyedTestProfile(domain.EngineProfile{ID: "engine-openvino", Backend: domain.BackendOpenVINO, ManagedBy: "system", DisplayName: "OpenVINO", BinaryPath: "openvino-genai-openai", Ready: true, SupportedModels: []string{catalog.FormatOpenVINOIR}}, host, "")
	if err := store.SaveEngineProfile(context.Background(), profile); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	preset := domain.Preset{
		ID:              "gemma-ov",
		ModelRef:        "/models/gemma-ov",
		Backend:         domain.BackendOpenVINO,
		ContextLength:   262144,
		Capabilities:    []domain.Capability{domain.CapabilityChat},
		EstWeightsMB:    53000,
		KVPerTokenMB:    0.25,
		GeneratedBy:     "bootstrap",
		GeneratedFrom:   "plan-runtime",
		EngineProfileID: profile.ID,
	}
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset: %v", err)
	}
	plan := domain.BootstrapPlan{
		ID:                "plan-runtime",
		CreatedAt:         time.Unix(21, 0).UTC(),
		Host:              host,
		ResultingProfiles: []domain.EngineProfile{profile},
		ModelCandidates: []domain.BootstrapModelCandidate{{
			HostID:        "b70",
			Name:          "gemma-ov",
			Path:          "/models/gemma-ov",
			Format:        catalog.FormatOpenVINOIR,
			SizeMB:        53000,
			ContextLength: 262144,
			Capabilities:  []domain.Capability{domain.CapabilityChat},
			Metadata:      map[string]string{"kv_per_token_mb": "0.25"},
			Backends:      []domain.BootstrapBackendCompatibility{{Backend: domain.BackendOpenVINO, Ready: true}},
		}},
	}
	if err := store.SaveBootstrapPlan(context.Background(), plan); err != nil {
		t.Fatalf("SaveBootstrapPlan: %v", err)
	}

	report, err := loadRuntimePresets(context.Background(), store)
	if err != nil {
		t.Fatalf("loadRuntimePresets: %v", err)
	}
	if len(report.Presets) != 1 || report.Presets[0].ID != "gemma-ov" || report.Presets[0].EngineReadiness != domain.EngineReadinessReadyProfile {
		t.Fatalf("presets = %+v blocked=%+v", report.Presets, report.Blocked)
	}
}

func keyedTestProfile(profile domain.EngineProfile, host domain.HostFacts, modelFormat string) domain.EngineProfile {
	profile.CompatibilityKey = enginecompat.HostProfileKey(host, profile, modelFormat)
	return profile
}

func hasRuntimePreset(presets []domain.Preset, id string, status domain.EngineReadinessStatus) bool {
	for _, preset := range presets {
		if preset.ID == id && preset.EngineReadiness == status {
			return true
		}
	}
	return false
}

func hasApplyEvidence(evidence []engineApplyEvidence, kind, id, status string) bool {
	for _, item := range evidence {
		if item.Kind == kind && item.ID == id && item.Status == status {
			return true
		}
	}
	return false
}

type fakeBootstrapEngineDetector struct {
	host       domain.HostFacts
	profiles   []domain.EngineProfile
	configured domain.EngineProfile
	err        error
	enginesErr error
}

func (f fakeBootstrapEngineDetector) DetectHost(context.Context, domain.Node) (domain.HostFacts, error) {
	if f.err != nil {
		return domain.HostFacts{}, f.err
	}
	return f.host, nil
}

func (f fakeBootstrapEngineDetector) DetectEngines(context.Context, domain.HostFacts) ([]domain.EngineProfile, error) {
	if f.enginesErr != nil {
		return nil, f.enginesErr
	}
	if f.err != nil {
		return nil, f.err
	}
	return append([]domain.EngineProfile(nil), f.profiles...), nil
}

func (f fakeBootstrapEngineDetector) DetectConfiguredEngine(context.Context, domain.HostFacts, engine.Config) domain.EngineProfile {
	return f.configured
}

func TestParseJoinFlag(t *testing.T) {
	if join, err := parseJoinFlag("secret"); err != nil || join.Token != "secret" {
		t.Fatalf("raw join = %+v %v", join, err)
	}
	joinURI, err := membership.BuildJoinToken("secret")
	if err != nil {
		t.Fatalf("BuildJoinToken: %v", err)
	}
	if join, err := parseJoinFlag(joinURI); err != nil || join.Token != "secret" {
		t.Fatalf("join uri = %+v %v", join, err)
	}
	if join, err := parseJoinFlag("mycjoin://127.0.0.1:51846?token=secret"); err != nil || join.Address != "127.0.0.1:51846" {
		t.Fatalf("seed join uri = %+v %v", join, err)
	}
	if _, err := parseJoinFlag("mycjoin://127.0.0.1:51846?token=secret&rpc_token=rpc-secret"); err == nil {
		t.Fatal("join uri with rpc token accepted")
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
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/peer/health" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		peer.Addresses[0] = serverURL(r)
		_ = json.NewEncoder(w).Encode(peer)
	}))
	peer.Addresses[0] = "http://peer-health.test"

	if err := probePeerHealthWithClient(context.Background(), peer, "", client); err != nil {
		t.Fatalf("probePeerHealth: %v", err)
	}
	bad := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(domain.Peer{ID: "other"})
	}))
	if err := probePeerHealthWithClient(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"http://bad-peer-health.test"}}, "", bad); err == nil || !strings.Contains(err.Error(), "other") {
		t.Fatalf("bad peer err = %v", err)
	}
	if err := probePeerHealth(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("unreachable err = %v", err)
	}
	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	serverPeer := domain.Peer{ID: "peer-token", Compute: true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Myc-Join-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		serverPeer.Addresses = []string{serverURL(r)}
		_ = json.NewEncoder(w).Encode(serverPeer)
	}))
	defer server.Close()
	if got, err := fetchPeerHealth(context.Background(), server.URL, "secret"); err != nil || got.ID != "peer-token" {
		t.Fatalf("fetchPeerHealth = %+v %v", got, err)
	}
	if err := probePeerHealthWithToken(context.Background(), domain.Peer{ID: "peer-token", Addresses: []string{server.URL}}, "secret"); err != nil {
		t.Fatalf("probe with token wrapper: %v", err)
	}
	if err := probePeerHealthWithTokenManager(context.Background(), domain.Peer{ID: "peer-token", Addresses: []string{server.URL}}, "secret", manager); err != nil {
		t.Fatalf("probe with token manager wrapper: %v", err)
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
	client := directHTTPClient(mux)
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"http://registry-peer.test"}}

	if _, err := (registryHTTPClient{Client: client}).Snapshot(ctx, peer); err == nil || !strings.Contains(err.Error(), "rpc token") {
		t.Fatalf("unauthorized snapshot err = %v", err)
	}
	registryClient := registryHTTPClient{AuthToken: "rpc-secret", Client: client}
	records, err := registryClient.Snapshot(ctx, peer)
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
	if err := registryClient.Push(ctx, peer, []domain.JobRecord{remote}); err != nil {
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

func TestRegistryRPCSnapshotCompactsTerminalHistory(t *testing.T) {
	ctx := context.Background()
	store := peerTestRegistry(t)
	running := domain.JobRecord{
		JobID:       "job-running",
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     []byte(`{"job":"running"}`),
		UpdatedAt:   time.Unix(20, 0).UTC(),
	}
	done := domain.JobRecord{
		JobID:       "job-done",
		Coordinator: "peer-a",
		Status:      domain.JobDone,
		Request:     []byte(`{"job":"done"}`),
		UpdatedAt:   time.Unix(21, 0).UTC(),
	}
	cleanup := domain.JobRecord{
		JobID:           "job-cleanup",
		Coordinator:     "peer-a",
		Status:          domain.JobDone,
		Request:         []byte(`{"job":"cleanup"}`),
		CleanupRequired: true,
		UpdatedAt:       time.Unix(22, 0).UTC(),
	}
	for _, rec := range []domain.JobRecord{running, done, cleanup} {
		if err := store.Put(ctx, rec); err != nil {
			t.Fatalf("Put %s: %v", rec.JobID, err)
		}
	}
	mux := http.NewServeMux()
	mountRegistryHTTP(mux, store, "rpc-secret")
	client := registryHTTPClient{AuthToken: "rpc-secret", Client: directHTTPClient(mux)}
	records, err := client.Snapshot(ctx, domain.Peer{ID: "peer-a", Addresses: []string{"http://registry-peer.test"}})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(records) != 2 || records[0].JobID != "job-running" || records[1].JobID != "job-cleanup" {
		t.Fatalf("compact registry snapshot = %+v", records)
	}
	localRecords, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatalf("local Snapshot: %v", err)
	}
	if len(localRecords) != 3 {
		t.Fatalf("local registry was compacted: %+v", localRecords)
	}
}

func TestRegistrySnapshotWriteErrorDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	store := peerTestRegistry(t)
	if err := store.Put(ctx, domain.JobRecord{
		JobID:       "job-local",
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     []byte(`{"job":"local"}`),
		UpdatedAt:   time.Unix(20, 0).UTC(),
	}); err != nil {
		t.Fatalf("Put local: %v", err)
	}
	mux := http.NewServeMux()
	mountRegistryHTTP(mux, store, "rpc-secret")
	req := httptest.NewRequest(http.MethodGet, "/registry/snapshot", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("snapshot write error panicked: %v", recovered)
		}
	}()

	mux.ServeHTTP(failingResponseWriter{header: http.Header{}}, req)
}

func TestRegistryRPCRejectsOversizedJSONBody(t *testing.T) {
	mux := http.NewServeMux()
	mountRegistryHTTP(mux, peerTestRegistry(t), "rpc-secret")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/registry/records", io.LimitReader(repeatedByteReader{b: 'x'}, maxPeerRPCJSONBodyBytes+1))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "peer rpc body exceeds") {
		t.Fatalf("oversized registry records status/body = %d %q", rec.Code, rec.Body.String())
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
	localSample := domain.SessionMetric{SessionID: "session-local", Sequence: 1, JobID: "job-local", Phase: domain.TelemetryPhaseComplete, NodeID: "peer-a", Project: "project-a", At: time.Unix(20, 0).UTC()}
	if err := store.RecordSample(ctx, localSample); err != nil {
		t.Fatalf("RecordSample local: %v", err)
	}
	mux := http.NewServeMux()
	mountTelemetryHTTP(mux, store, "rpc-secret")
	client := directHTTPClient(mux)
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"http://telemetry-peer.test"}}

	if _, err := (telemetryHTTPClient{Client: client}).Metrics(ctx, peer); err == nil || !strings.Contains(err.Error(), "rpc token") {
		t.Fatalf("unauthorized metrics err = %v", err)
	}
	telemetryClient := telemetryHTTPClient{AuthToken: "rpc-secret", Client: client}
	metrics, err := telemetryClient.Metrics(ctx, peer)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if len(metrics) != 1 || metrics[0].JobID != localMetric.JobID {
		t.Fatalf("metrics = %+v", metrics)
	}
	samples, err := telemetryClient.Samples(ctx, peer, domain.SessionMetricQuery{Project: "project-a", Limit: 1})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if len(samples) != 1 || samples[0].SessionID != localSample.SessionID {
		t.Fatalf("samples = %+v", samples)
	}
	remoteMetric := domain.RunMetric{JobID: "job-remote", NodeID: "peer-b", Project: "project-a", TokensPerSec: 18, At: time.Unix(21, 0).UTC()}
	if err := telemetryClient.PushMetrics(ctx, peer, []domain.RunMetric{remoteMetric}); err != nil {
		t.Fatalf("PushMetrics: %v", err)
	}
	remoteSample := domain.SessionMetric{SessionID: "session-remote", Sequence: 1, JobID: "job-remote", Phase: domain.TelemetryPhaseComplete, NodeID: "peer-b", Project: "project-a", At: time.Unix(21, 0).UTC()}
	if err := telemetryClient.PushSamples(ctx, peer, []domain.SessionMetric{remoteSample}); err != nil {
		t.Fatalf("PushSamples: %v", err)
	}
	metrics, err = store.Metrics(ctx, "")
	if err != nil {
		t.Fatalf("Metrics local: %v", err)
	}
	if len(metrics) != 2 || metrics[1].JobID != remoteMetric.JobID {
		t.Fatalf("local metrics = %+v", metrics)
	}
	samples, err = store.Samples(ctx, domain.SessionMetricQuery{})
	if err != nil {
		t.Fatalf("Samples local: %v", err)
	}
	if len(samples) != 2 || samples[1].SessionID != remoteSample.SessionID {
		t.Fatalf("local samples = %+v", samples)
	}
	rec := domain.RecommendationRecord{ID: "rec-a", Type: optimizer.RecommendationContextCap, ProjectID: "project-a", CreatedAt: time.Unix(22, 0).UTC()}
	if err := telemetryClient.PushRecommendations(ctx, peer, []domain.RecommendationRecord{rec}); err != nil {
		t.Fatalf("PushRecommendations: %v", err)
	}
	recs, err := telemetryClient.Recommendations(ctx, peer)
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

func TestCombinedFleetAndNodesDelegateAcrossSources(t *testing.T) {
	ctx := context.Background()
	leftAdmission := &mocks.AdmissionController{}
	rightAdmission := &mocks.AdmissionController{}
	leftAgent, err := newLocalPeerAgent(mocks.NewNodeAgent(domain.Node{ID: "left"}), leftAdmission)
	if err != nil {
		t.Fatalf("left local agent: %v", err)
	}
	rightAgent, err := newLocalPeerAgent(mocks.NewNodeAgent(domain.Node{ID: "right"}), rightAdmission)
	if err != nil {
		t.Fatalf("right local agent: %v", err)
	}
	leftDir := gateway.NodeDirectory{Agents: map[string]ports.NodeAgent{"left": leftAgent}}
	rightDir := gateway.NodeDirectory{Agents: map[string]ports.NodeAgent{"right": rightAgent}}

	fleet := combinedFleet{left: leftDir, right: rightDir}
	snap, err := fleet.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Nodes) != 2 {
		t.Fatalf("fleet snapshot = %+v", snap)
	}
	boom := errors.New("boom")
	snap, err = (combinedFleet{left: errorFleetSource{err: boom}, right: rightDir}).Snapshot(ctx)
	if err != nil || len(snap.Nodes) != 1 || snap.Nodes[0].ID != "right" {
		t.Fatalf("left error partial snapshot = %+v err=%v", snap, err)
	}
	snap, err = (combinedFleet{left: leftDir, right: errorFleetSource{err: boom}}).Snapshot(ctx)
	if err != nil || len(snap.Nodes) != 1 || snap.Nodes[0].ID != "left" {
		t.Fatalf("right error partial snapshot = %+v err=%v", snap, err)
	}
	if _, err := (combinedFleet{left: errorFleetSource{err: boom}, right: errorFleetSource{err: boom}}).Snapshot(ctx); !errors.Is(err, boom) {
		t.Fatalf("both error = %v", err)
	}

	nodes := combinedNodes{left: leftDir, right: rightDir}
	if agent, err := nodes.NodeAgent("left"); err != nil || agent == nil {
		t.Fatalf("left NodeAgent = %+v %v", agent, err)
	}
	if agent, err := nodes.NodeAgent("right"); err != nil || agent == nil {
		t.Fatalf("right NodeAgent = %+v %v", agent, err)
	}
	if admission, err := nodes.AdmissionController("right"); err != nil || admission == nil {
		t.Fatalf("right admission = %+v %v", admission, err)
	}
	if inspector, err := nodes.LeaseInspector("right"); err != nil || inspector == nil {
		t.Fatalf("right inspector = %+v %v", inspector, err)
	}
	if inspector, err := nodes.JobStatusInspector("right"); err != nil || inspector == nil {
		t.Fatalf("right job status inspector = %+v %v", inspector, err)
	}
	if _, err := (combinedNodes{left: gateway.NodeDirectory{}, right: plainNodeResolver{}}).AdmissionController("missing"); err == nil {
		t.Fatal("missing right admission exposure accepted")
	}
	if _, err := (combinedNodes{left: gateway.NodeDirectory{}, right: plainNodeResolver{}}).LeaseInspector("missing"); err == nil {
		t.Fatal("missing right lease inspection exposure accepted")
	}
	if _, err := (combinedNodes{left: gateway.NodeDirectory{}, right: plainNodeResolver{}}).JobStatusInspector("missing"); err == nil {
		t.Fatal("missing right job status inspection exposure accepted")
	}
	if got := admissionResolver(plainNodeResolver{}); got != nil {
		t.Fatalf("plain node resolver admission = %+v", got)
	}
}

func TestPeerAddressAuthAndRPCErrorHelpers(t *testing.T) {
	if got := peerHTTPBaseURL(" 127.0.0.1:1/ "); got != "http://127.0.0.1:1" {
		t.Fatalf("base url = %q", got)
	}
	if got, err := reachablePeerAddress("https://example.test:9443/path"); err != nil || got != "example.test:9443" {
		t.Fatalf("reachable url = %q %v", got, err)
	}
	for _, address := range []string{"", "http://"} {
		if _, err := reachablePeerAddress(address); err == nil {
			t.Fatalf("reachablePeerAddress(%q) expected error", address)
		}
	}
	if got := prependReachableAddress([]string{"b", "a"}, "a"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("prepended = %+v", got)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if !peerJoinAuthorized(req, nil) || !peerRPCAuthorized(req, "") {
		t.Fatal("empty auth should allow local peer helpers")
	}
	manager, err := membership.NewTokenManager("join-secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	req.Header.Set("X-Myc-Join-Token", "join-secret")
	if !peerJoinAuthorized(req, manager) {
		t.Fatal("join token was rejected")
	}
	req.Header.Set("Authorization", "Bearer rpc-secret")
	if !peerRPCAuthorized(req, "rpc-secret") {
		t.Fatal("rpc bearer token was rejected")
	}
	req.Header.Set("Authorization", "rpc-secret")
	if peerRPCAuthorized(req, "rpc-secret") {
		t.Fatal("rpc token without bearer prefix was accepted")
	}
	rec := httptest.NewRecorder()
	writePeerRPCError(rec, http.StatusTeapot, errors.New("steep"))
	if rec.Code != http.StatusTeapot || !strings.Contains(rec.Body.String(), "steep") {
		t.Fatalf("rpc error response = %d %q", rec.Code, rec.Body.String())
	}
	if err := manager.Revoke("join-secret"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := authorizedOutboundJoinToken("join-secret", manager); err == nil {
		t.Fatal("revoked outbound join token accepted")
	}
}

func TestRegistryAndTelemetryHTTPErrorBranches(t *testing.T) {
	ctx := context.Background()
	registry := failingRescueSnapshotRegistry{err: errors.New("registry boom")}
	mux := http.NewServeMux()
	mountRegistryHTTP(mux, registry, "rpc-secret")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/registry/snapshot", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "registry boom") {
		t.Fatalf("snapshot error = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/registry/snapshot", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("snapshot method status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/registry/records", strings.NewReader(`{`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad registry records status/body = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/registry/records", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("registry records method status = %d", rec.Code)
	}

	telemetryStore := &recordingTelemetryRPCStore{err: errors.New("telemetry boom")}
	mux = http.NewServeMux()
	mountTelemetryHTTP(mux, telemetryStore, "rpc-secret")
	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/telemetry/metrics", nil),
		httptest.NewRequest(http.MethodPost, "/telemetry/metrics", strings.NewReader(`{`)),
		httptest.NewRequest(http.MethodPut, "/telemetry/metrics", nil),
		httptest.NewRequest(http.MethodGet, "/telemetry/samples", nil),
		httptest.NewRequest(http.MethodGet, "/telemetry/samples?since=bad", nil),
		httptest.NewRequest(http.MethodPost, "/telemetry/samples", strings.NewReader(`{`)),
		httptest.NewRequest(http.MethodPut, "/telemetry/samples", nil),
		httptest.NewRequest(http.MethodGet, "/telemetry/recommendations", nil),
		httptest.NewRequest(http.MethodPost, "/telemetry/recommendations", strings.NewReader(`{`)),
		httptest.NewRequest(http.MethodPut, "/telemetry/recommendations", nil),
	} {
		req = req.WithContext(ctx)
		req.Header.Set("Authorization", "Bearer rpc-secret")
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code < 400 {
			t.Fatalf("%s %s unexpectedly succeeded: %d", req.Method, req.URL.Path, rec.Code)
		}
	}
}

func TestWorkloadHTTPAdmitsQueuedIntentAndReportsStatus(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	runtime := &recordingWorkloadRuntime{store: store, decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionQueued, Preset: domain.Preset{ID: "preset-a"}}}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, runtime, store, "rpc-secret", "peer-a", nil)
	body := `{"job":{"id":"job-a","task_type":"chat","model":"model-a","preset":"preset-a","project":"project-a","priority":"interactive","speed_pref":"latency","context_request":8192,"expected_concurrency":2,"preemption":"soft"},"payload":{"input_bytes":123}}`
	req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("admit status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp workload.AdmissionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode admit response: %v", err)
	}
	if !resp.Queued || resp.Job.Status != domain.JobQueued || resp.Job.ContextRequest != 8192 || resp.PayloadBytes == 0 || !resp.PayloadStored {
		t.Fatalf("admit response = %+v", resp)
	}
	if resp.Execution.State != "queued_not_executed" || resp.Execution.AutomaticProviderOutputCapture || resp.Execution.ResultReplayable {
		t.Fatalf("admit execution evidence = %+v", resp.Execution)
	}
	if strings.Contains(rec.Body.String(), "input_bytes") {
		t.Fatalf("admit response leaked payload: %s", rec.Body.String())
	}
	if string(runtime.payload) != `{"input_bytes":123}` {
		t.Fatalf("runtime payload = %s", runtime.payload)
	}

	req = httptest.NewRequest(http.MethodGet, "/workloads/job-a", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rec.Code, rec.Body.String())
	}
	var status workload.StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if status.Job.ID != "job-a" || status.Job.Project != "project-a" || status.Job.ExpectedConcurrency != 2 {
		t.Fatalf("status response = %+v", status)
	}
}

func TestWorkloadHTTPPersistsReconciliationTasksFromPlacement(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	runtime := &recordingWorkloadRuntime{
		store: store,
		decision: domain.PlacementDecision{
			JobID:  "job-a",
			Action: domain.ActionQueued,
			Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
			Trace: []domain.TraceStep{{
				Step: "filter",
				Data: map[string]any{"dropped": map[string]string{
					"node-a": "fit",
				}},
			}},
		},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, runtime, store, "rpc-secret", "peer-a", nil)
	body := `{"job":{"id":"job-a","task_type":"chat","model":"model-a","preset":"preset-a","project":"project-a"},"payload":{"input_bytes":123}}`
	req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("admit status = %d body=%s", rec.Code, rec.Body.String())
	}
	tasks, err := store.ListReconciliationTasks(context.Background(), "job-a")
	if err != nil {
		t.Fatalf("ListReconciliationTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Kind != string(reconcile.TaskPrewarmReplica) || tasks[0].Status != domain.ReconciliationTaskAllowed || tasks[0].NodeID != "node-a" {
		t.Fatalf("tasks = %+v", tasks)
	}
	if strings.Contains(rec.Body.String(), "reconcile") || strings.Contains(rec.Body.String(), "input_bytes") {
		t.Fatalf("admission response leaked internal task or payload details: %s", rec.Body.String())
	}
}

func TestWorkloadHTTPStatusUsesPerJobRegistryEvidenceWithoutSnapshotBlocking(t *testing.T) {
	job := domain.Job{
		ID:                  "job-a",
		TaskType:            "chat",
		Model:               "model-a",
		PresetID:            "preset-a",
		Project:             "project-a",
		Priority:            domain.PriorityInteractive,
		SpeedPref:           domain.SpeedLatency,
		ContextRequest:      512,
		ExpectedConcurrency: 1,
		Preemption:          domain.PreemptSoft,
		Status:              domain.JobRunning,
		Result: &domain.JobResult{
			Source:         "scheduler",
			Action:         domain.ActionWarmInstance,
			NodeID:         "node-a",
			PresetID:       "preset-a",
			LeaseID:        "lease-a",
			PayloadStored:  true,
			ResultRedacted: true,
		},
	}
	payload := mustJSON(t, workload.ExecutablePayload{
		Kind: workload.ExecutablePayloadKindOpenAIChat,
		Body: json.RawMessage(`{"model":"model-a","messages":[]}`),
	})
	replayable, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	store := &blockingSnapshotWorkloadStore{workloadMemoryStore: &workloadMemoryStore{
		jobs: map[string]domain.Job{job.ID: job},
		records: []domain.JobRecord{{
			JobID:       job.ID,
			Coordinator: "peer-a",
			Status:      domain.JobRunning,
			Request:     replayable,
			UpdatedAt:   time.Unix(50, 0).UTC(),
		}},
	}}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, &recordingWorkloadRuntime{}, store, "rpc-secret", "peer-a", nil)

	req := httptest.NewRequest(http.MethodGet, "/workloads/job-a", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		mux.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("workload status hung on registry snapshot")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rec.Code, rec.Body.String())
	}
	var status workload.StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if status.Payload.State != "registry_payload_replayable" || !status.Payload.Replayable {
		t.Fatalf("payload evidence = %+v", status.Payload)
	}
	if status.Execution.PayloadContract != "registry_executable_gateway_request" || status.Execution.PayloadKind != workload.ExecutablePayloadKindOpenAIChat || !status.Execution.PayloadExecutable {
		t.Fatalf("execution evidence = %+v", status.Execution)
	}
}

func TestWorkloadHTTPStatusUsesSQLitePointLookupWithLargeRegistry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := storesqlite.Open(filepath.Join(dir, "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	job := domain.Job{
		ID:             "target-job",
		TaskType:       "chat",
		Model:          "model-a",
		Project:        "project-a",
		Priority:       domain.PriorityInteractive,
		SpeedPref:      domain.SpeedThroughput,
		ContextRequest: 512,
		Preemption:     domain.PreemptSoft,
		Status:         domain.JobRunning,
		Result: &domain.JobResult{
			Source:         "scheduler",
			Action:         domain.ActionWarmInstance,
			NodeID:         "node-a",
			PresetID:       "preset-a",
			LeaseID:        "lease-a",
			PayloadStored:  true,
			ResultRedacted: true,
		},
	}
	if err := store.SaveJob(ctx, job); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	payload := mustJSON(t, workload.ExecutablePayload{
		Kind: workload.ExecutablePayloadKindOpenAIChat,
		Body: json.RawMessage(`{"model":"model-a","messages":[]}`),
	})
	replayable, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	if err := store.Put(ctx, domain.JobRecord{
		JobID:       job.ID,
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     replayable,
		UpdatedAt:   time.Unix(1000, 0).UTC(),
	}); err != nil {
		t.Fatalf("Put target record: %v", err)
	}
	fatPayload := bytes.Repeat([]byte("x"), 32*1024)
	for i := 0; i < 250; i++ {
		rec := domain.JobRecord{
			JobID:       fmt.Sprintf("noise-%03d", i),
			Coordinator: "peer-a",
			Status:      domain.JobDone,
			Request:     append([]byte(nil), fatPayload...),
			UpdatedAt:   time.Unix(int64(i+1), 0).UTC(),
		}
		if err := store.Put(ctx, rec); err != nil {
			t.Fatalf("Put noise record %d: %v", i, err)
		}
	}

	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, &recordingWorkloadRuntime{}, sqliteWorkloadStatusStoreNoSnapshot{Store: store}, "rpc-secret", "peer-a", nil)
	req := httptest.NewRequest(http.MethodGet, "/workloads/target-job", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		mux.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(750 * time.Millisecond):
		t.Fatal("workload status did not complete within point-lookup budget")
	}
	if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
		t.Fatalf("workload status took %s", elapsed)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rec.Code, rec.Body.String())
	}
	var status workload.StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if status.Payload.State != "registry_payload_replayable" || !status.Payload.Replayable || status.Execution.PayloadContract != "registry_executable_gateway_request" {
		t.Fatalf("status evidence = payload:%+v execution:%+v", status.Payload, status.Execution)
	}
}

func TestWorkloadHTTPAutoDrainsSafeReconciliationTasksFromPlacement(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	runtime := &recordingWorkloadRuntime{
		store: store,
		decision: domain.PlacementDecision{
			JobID:  "job-a",
			Action: domain.ActionQueued,
			Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
			Trace: []domain.TraceStep{{
				Step: "filter",
				Data: map[string]any{"dropped": map[string]string{
					"node-a": "label.gpu.vendor",
				}},
			}},
		},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, runtime, store, "rpc-secret", "peer-a", nil)
	body := `{"job":{"id":"job-a","task_type":"chat","model":"model-a","preset":"preset-a","project":"project-a"},"payload":{"input_bytes":123}}`
	req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("admit status = %d body=%s", rec.Code, rec.Body.String())
	}
	tasks, err := store.ListReconciliationTasks(context.Background(), "job-a")
	if err != nil {
		t.Fatalf("ListReconciliationTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Kind != string(reconcile.TaskRecordIncompatibility) || tasks[0].Status != domain.ReconciliationTaskDone {
		t.Fatalf("tasks = %+v", tasks)
	}
	if len(store.incompat) != 1 || store.incompat[0].TaskID != tasks[0].ID || store.incompat[0].NodeID != "node-a" {
		t.Fatalf("incompatibilities = %+v", store.incompat)
	}
	if strings.Contains(rec.Body.String(), "input_bytes") {
		t.Fatalf("admission response leaked reconciliation or payload details: %s", rec.Body.String())
	}
}

func TestPersistWorkloadReconciliationTasksDoesNotOverwriteExisting(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	decision := domain.PlacementDecision{
		JobID:  "job-a",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
		Trace: []domain.TraceStep{{
			Step: "filter",
			Data: map[string]any{"dropped": map[string]string{
				"node-a": "fit",
			}},
		}},
	}
	plan := reconcile.PlanPlacement(decision, reconcile.DefaultStandingPolicy())
	records := reconcile.RecordsFromPlan(plan, time.Unix(1, 0).UTC())
	if len(records) != 1 {
		t.Fatalf("records = %+v", records)
	}
	existing := records[0]
	existing.Status = domain.ReconciliationTaskDone
	existing.Events = append(existing.Events, domain.ReconciliationTaskEvent{Status: domain.ReconciliationTaskDone, Message: "already drained", At: time.Unix(2, 0).UTC()})
	if err := store.SaveReconciliationTask(context.Background(), existing); err != nil {
		t.Fatalf("SaveReconciliationTask: %v", err)
	}

	err := persistWorkloadReconciliationTasks(context.Background(), store, domain.Job{ID: "job-a", Model: "model-a", PresetID: "preset-a"}, scheduler.Result{Decision: decision})
	if err != nil {
		t.Fatalf("persistWorkloadReconciliationTasks: %v", err)
	}
	tasks, err := store.ListReconciliationTasks(context.Background(), "job-a")
	if err != nil {
		t.Fatalf("ListReconciliationTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Status != domain.ReconciliationTaskDone || len(tasks[0].Events) != len(existing.Events) {
		t.Fatalf("existing task was overwritten: %+v", tasks)
	}
}

func TestPersistWorkloadReconciliationTasksFailureBranches(t *testing.T) {
	decision := domain.PlacementDecision{
		JobID:  "job-a",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
		Trace: []domain.TraceStep{{
			Step: "filter",
			Data: map[string]any{"dropped": map[string]string{
				"node-a": "fit",
			}},
		}},
	}
	job := domain.Job{ID: "job-a", Model: "model-a", PresetID: "preset-a"}
	result := scheduler.Result{Decision: decision}
	if err := persistWorkloadReconciliationTasks(context.Background(), workloadStatusOnlyStore{job: job}, job, result); err != nil {
		t.Fatalf("unsupported store err = %v", err)
	}
	listErr := errors.New("list unavailable")
	if err := persistWorkloadReconciliationTasks(context.Background(), failingReconciliationStore{workloadMemoryStore: &workloadMemoryStore{jobs: map[string]domain.Job{}}, listErr: listErr}, job, result); !errors.Is(err, listErr) {
		t.Fatalf("list err = %v", err)
	}
	saveErr := errors.New("save unavailable")
	if err := persistWorkloadReconciliationTasks(context.Background(), failingReconciliationStore{workloadMemoryStore: &workloadMemoryStore{jobs: map[string]domain.Job{}}, saveErr: saveErr}, job, result); !errors.Is(err, saveErr) {
		t.Fatalf("save err = %v", err)
	}
}

func TestWorkloadHTTPDrainPersistsReconciliationTasksFromPlacement(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{
		"job-drain": {ID: "job-drain", Model: "model-a", PresetID: "preset-a", Project: "project-a", Status: domain.JobQueued},
	}}
	runtime := &recordingWorkloadRuntime{
		store: store,
		drained: []scheduler.Result{{
			Decision: domain.PlacementDecision{
				JobID:  "job-drain",
				Action: domain.ActionQueued,
				Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
				Trace: []domain.TraceStep{{
					Step: "filter",
					Data: map[string]any{"dropped": map[string]string{
						"node-engine": "label." + domain.LabelPeerBackends,
					}},
				}},
			},
		}},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, runtime, store, "rpc-secret", "peer-a", nil)
	req := httptest.NewRequest(http.MethodPost, "/workloads/drain", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("drain status = %d body=%s", rec.Code, rec.Body.String())
	}
	tasks, err := store.ListReconciliationTasks(context.Background(), "job-drain")
	if err != nil {
		t.Fatalf("ListReconciliationTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Kind != string(reconcile.TaskVerifyEngineReadiness) || tasks[0].Status != domain.ReconciliationTaskFailed || tasks[0].NodeID != "node-engine" {
		t.Fatalf("tasks = %+v", tasks)
	}
	if !strings.Contains(tasks[0].Error, "preset") {
		t.Fatalf("task error = %q", tasks[0].Error)
	}
}

func TestWorkloadHTTPRejectsPrivateHandling(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, &recordingWorkloadRuntime{store: store}, store, "", "peer-a", nil)
	req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(`{"job":{"id":"job-a","model":"model-a","project":"project-a","handling":"private"}}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "private workload handling is not enabled") {
		t.Fatalf("private handling response = %d %s", rec.Code, rec.Body.String())
	}
}

func TestWorkloadHTTPErrorBranches(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	if err := store.SaveJob(context.Background(), domain.Job{
		ID:     "job-a",
		Model:  "model-a",
		Status: domain.JobRunning,
		Result: &domain.JobResult{
			Source:  "scheduler",
			NodeID:  "node-a",
			LeaseID: "lease-a",
		},
	}); err != nil {
		t.Fatalf("seed job-a: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{
		ID:     "job-empty",
		Model:  "model-a",
		Status: domain.JobQueued,
	}); err != nil {
		t.Fatalf("seed job-empty: %v", err)
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, &recordingWorkloadRuntime{store: store, err: errors.New("admission boom")}, store, "rpc-secret", "peer-a", nil)
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
		auth   bool
		want   int
	}{
		{name: "unauthorized", method: http.MethodPost, path: "/workloads", body: `{}`, want: http.StatusUnauthorized},
		{name: "post method", method: http.MethodGet, path: "/workloads", auth: true, want: http.StatusMethodNotAllowed},
		{name: "bad json", method: http.MethodPost, path: "/workloads", body: `{`, auth: true, want: http.StatusBadRequest},
		{name: "bad id", method: http.MethodGet, path: "/workloads/", auth: true, want: http.StatusBadRequest},
		{name: "missing status", method: http.MethodGet, path: "/workloads/missing", auth: true, want: http.StatusNotFound},
		{name: "bad status method", method: http.MethodPost, path: "/workloads/job-a", auth: true, want: http.StatusMethodNotAllowed},
		{name: "bad finish method", method: http.MethodGet, path: "/workloads/job-a/finish", auth: true, want: http.StatusMethodNotAllowed},
		{name: "finish without scheduler evidence", method: http.MethodPost, path: "/workloads/job-empty/finish", auth: true, want: http.StatusConflict},
		{name: "missing finish", method: http.MethodPost, path: "/workloads/missing/finish", auth: true, want: http.StatusNotFound},
		{name: "bad cancel method", method: http.MethodGet, path: "/workloads/job-empty/cancel", auth: true, want: http.StatusMethodNotAllowed},
		{name: "bad execute method", method: http.MethodGet, path: "/workloads/job-a/execute", auth: true, want: http.StatusMethodNotAllowed},
		{name: "execute without provider executor", method: http.MethodPost, path: "/workloads/job-a/execute", auth: true, want: http.StatusConflict},
		{name: "bad drain execute flag", method: http.MethodPost, path: "/workloads/drain?execute=maybe", auth: true, want: http.StatusBadRequest},
		{name: "execute without provider executor", method: http.MethodPost, path: "/workloads/drain?execute=true", auth: true, want: http.StatusConflict},
		{name: "runtime error", method: http.MethodPost, path: "/workloads", body: `{"job":{"id":"job-a","model":"model-a","project":"project-a"}}`, auth: true, want: http.StatusConflict},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.auth {
			req.Header.Set("Authorization", "Bearer rpc-secret")
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("%s status = %d body=%s", tc.name, rec.Code, rec.Body.String())
		}
	}
}

func TestWorkloadHTTPCancelTerminalizesJobWithoutLeaseEvidence(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	if err := store.SaveJob(context.Background(), domain.Job{
		ID:      "job-queued",
		Model:   "model-a",
		Project: "project-a",
		Status:  domain.JobQueued,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, &recordingWorkloadRuntime{store: store}, store, "rpc-secret", "peer-a", nil)
	req := httptest.NewRequest(http.MethodPost, "/workloads/job-queued/cancel", strings.NewReader(`{"reason":"stale operator cleanup"}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel status = %d body=%s", rec.Code, rec.Body.String())
	}
	var out workload.FinishResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode cancel response: %v", err)
	}
	if out.Job.Status != domain.JobFailed || out.Job.Error != "stale operator cleanup" || out.Result == nil || out.Result.Source != "operator_cancel" || !out.Result.Terminal {
		t.Fatalf("cancel response = %+v", out)
	}
	latest, err := store.Job(context.Background(), "job-queued")
	if err != nil {
		t.Fatalf("latest job: %v", err)
	}
	if latest.Status != domain.JobFailed || latest.Result == nil || latest.Result.Source != "operator_cancel" {
		t.Fatalf("stored job = %+v", latest)
	}
}

func TestWorkloadHTTPCancelUsesRuntimeFinishWhenLeaseEvidenceExists(t *testing.T) {
	result := &domain.JobResult{
		Source:  "scheduler",
		LeaseID: "lease-a",
		NodeID:  "node-a",
	}
	job := domain.Job{
		ID:      "job-running",
		Model:   "model-a",
		Project: "project-a",
		Status:  domain.JobRunning,
		Result:  result,
	}
	store := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}}
	runtime := &recordingWorkloadRuntime{store: store}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, runtime, store, "rpc-secret", "peer-a", nil)
	req := httptest.NewRequest(http.MethodPost, "/workloads/job-running/cancel", strings.NewReader(`{"reason":"operator release"}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel status = %d body=%s", rec.Code, rec.Body.String())
	}
	latest, err := store.Job(context.Background(), "job-running")
	if err != nil {
		t.Fatalf("latest job: %v", err)
	}
	if latest.Status != domain.JobFailed || latest.Error != "operator release" || latest.Result == nil || latest.Result.LeaseID != "lease-a" || !latest.Result.Terminal {
		t.Fatalf("stored job = %+v", latest)
	}
}

func TestWorkloadHTTPReturnsCreatedForImmediatePlacement(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	runtime := &recordingWorkloadRuntime{
		store:    store,
		decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionLoadedNew, Preset: domain.Preset{ID: "preset-a"}, NodeID: "node-a"},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, runtime, store, "", "peer-a", nil)
	req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(`{"job":{"id":"job-a","model":"model-a","project":"project-a"}}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("created status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestWorkloadHTTPQueueOnlyDefersImmediatePlacement(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	runtime := &recordingWorkloadRuntime{
		store:    store,
		decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionLoadedNew, Preset: domain.Preset{ID: "preset-a"}, NodeID: "node-a"},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, runtime, store, "", "peer-a", nil)
	req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(`{"queue_only":true,"job":{"id":"job-a","model":"model-a","preset":"preset-a","project":"project-a"},"payload":{"input_bytes":123}}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || strings.Contains(rec.Body.String(), "input_bytes") {
		t.Fatalf("queue-only status/body = %d %s", rec.Code, rec.Body.String())
	}
	if len(runtime.hooks) != 1 || !runtime.hooks[0].QueueOnly {
		t.Fatalf("hooks = %+v", runtime.hooks)
	}
	var resp workload.AdmissionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode queue-only response: %v", err)
	}
	if !resp.Queued || resp.Job.Status != domain.JobQueued || resp.Decision.Action != domain.ActionQueued || resp.Execution.State != "queued_not_executed" {
		t.Fatalf("queue-only response = %+v", resp)
	}
}

func TestWorkloadHTTPDrainsQueuedWorkAndReportsTerminalEvidence(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	node.Accelerators = []domain.Accelerator{{Index: 0, Vendor: "test", Kind: "gpu", VRAMTotalMB: 1000}}
	node.MaxUtil = 1
	node.Status = domain.NodeReady
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("model-a"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	placer := &gatedWorkloadPlacer{nodeID: node.ID, preset: preset, claim: domain.Claim{WeightsMB: 100}}
	service := &scheduler.Service{
		Placer:  placer,
		Fleet:   staticWorkloadFleet{node: node},
		Nodes:   workloadNodeResolver{agents: map[string]ports.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners:  workloadNodeResolver{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:   scheduler.NewQueue(mocks.NewFakeClock(time.Unix(50, 0).UTC())),
		Store:   store,
		Clock:   mocks.NewFakeClock(time.Unix(50, 0).UTC()),
		Presets: map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, service, store, "rpc-secret", "peer-a", nil)

	admit := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(`{"job":{"id":"job-a","task_type":"chat","model":"model-a","project":"project-a","priority":"interactive","speed_pref":"throughput","preemption":"soft"},"payload":{"input_bytes":123}}`))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(admit, req)
	if admit.Code != http.StatusAccepted || strings.Contains(admit.Body.String(), "input_bytes") {
		t.Fatalf("admit status/body = %d %s", admit.Code, admit.Body.String())
	}
	var admitted workload.AdmissionResponse
	if err := json.Unmarshal(admit.Body.Bytes(), &admitted); err != nil {
		t.Fatalf("decode admit: %v", err)
	}
	if !admitted.Queued || admitted.Job.Status != domain.JobQueued || admitted.Result == nil || !admitted.Result.PayloadRedacted || admitted.Payload.State != "registry_payload_replayable" || !admitted.Payload.Replayable {
		t.Fatalf("admitted = %+v", admitted)
	}
	status := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/workloads/job-a", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(status, req)
	if status.Code != http.StatusOK || strings.Contains(status.Body.String(), "input_bytes") {
		t.Fatalf("queued status code/body = %d %s", status.Code, status.Body.String())
	}
	var queuedStatus workload.StatusResponse
	if err := json.Unmarshal(status.Body.Bytes(), &queuedStatus); err != nil {
		t.Fatalf("decode queued status: %v", err)
	}
	if queuedStatus.Payload.State != "registry_payload_replayable" || !queuedStatus.Payload.Replayable || queuedStatus.Payload.Bytes == 0 {
		t.Fatalf("queued payload evidence = %+v", queuedStatus.Payload)
	}
	if queuedStatus.Execution.State != "queued_not_executed" || queuedStatus.Execution.AutomaticProviderOutputCapture || queuedStatus.Execution.ResultReplayable {
		t.Fatalf("queued execution evidence = %+v", queuedStatus.Execution)
	}
	if queuedStatus.Execution.PayloadContract != "registry_payload_not_executable" || queuedStatus.Execution.PayloadExecutable || queuedStatus.Execution.PayloadKind != "" {
		t.Fatalf("queued payload execution contract = %+v", queuedStatus.Execution)
	}

	placer.ready = true
	drain := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/workloads/drain?limit=1", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(drain, req)
	if drain.Code != http.StatusOK || strings.Contains(drain.Body.String(), "input_bytes") {
		t.Fatalf("drain status/body = %d %s", drain.Code, drain.Body.String())
	}
	var drained workload.DrainResponse
	if err := json.Unmarshal(drain.Body.Bytes(), &drained); err != nil {
		t.Fatalf("decode drain: %v", err)
	}
	if len(drained.Results) != 1 || drained.Results[0].Job.Status != domain.JobRunning || drained.Results[0].Result == nil || drained.Results[0].Result.NodeID != node.ID || drained.Results[0].Result.LeaseID == "" {
		t.Fatalf("drained = %+v", drained)
	}
	if drained.Results[0].Execution.State != "running_needs_finish" || drained.Results[0].Execution.AutomaticProviderOutputCapture || drained.Results[0].Execution.ResultReplayable {
		t.Fatalf("drained execution evidence = %+v", drained.Results[0].Execution)
	}
	if len(agent.Loaded) != 1 || len(admission.Requests) == 0 {
		t.Fatalf("agent loaded=%+v admission=%+v", agent.Loaded, admission.Requests)
	}

	finish := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/workloads/job-a/finish", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(finish, req)
	if finish.Code != http.StatusOK || strings.Contains(finish.Body.String(), "input_bytes") {
		t.Fatalf("finish status/body = %d %s", finish.Code, finish.Body.String())
	}
	var finished workload.FinishResponse
	if err := json.Unmarshal(finish.Body.Bytes(), &finished); err != nil {
		t.Fatalf("decode finish: %v", err)
	}
	if finished.Job.Status != domain.JobDone || finished.Result == nil || !finished.Result.Terminal {
		t.Fatalf("finished = %+v", finished)
	}
	leases, err := store.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases after finish = %+v", leases)
	}

	status = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/workloads/job-a", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(status, req)
	if status.Code != http.StatusOK || strings.Contains(status.Body.String(), "input_bytes") {
		t.Fatalf("status code/body = %d %s", status.Code, status.Body.String())
	}
	var out workload.StatusResponse
	if err := json.Unmarshal(status.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if out.Job.Status != domain.JobDone || out.Result == nil || !out.Result.Terminal || !out.Result.ResultRedacted || out.Result.PayloadBytes == 0 {
		t.Fatalf("terminal status = %+v", out)
	}
	if out.Execution.State != "terminal_no_result" || out.Execution.AutomaticProviderOutputCapture || out.Execution.ResultReplayable {
		t.Fatalf("terminal execution evidence = %+v", out.Execution)
	}
}

func TestWorkloadHTTPDrainExecuteCapturesProviderResult(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	var providerBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("provider path = %s", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll provider body: %v", err)
		}
		providerBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	node.Accelerators = []domain.Accelerator{{Index: 0, Vendor: "test", Kind: "gpu", VRAMTotalMB: 1000}}
	node.MaxUtil = 1
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("model-a"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	preset.Backend = domain.BackendLlamaCpp
	agent := &addressedWorkloadAgent{node: node, addr: strings.TrimPrefix(upstream.URL, "http://")}
	admission := &mocks.AdmissionController{}
	placer := &gatedWorkloadPlacer{nodeID: node.ID, preset: preset, claim: domain.Claim{WeightsMB: 100}}
	service := &scheduler.Service{
		Placer:  placer,
		Fleet:   staticWorkloadFleet{node: node},
		Nodes:   workloadNodeResolver{agents: map[string]ports.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners:  workloadNodeResolver{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:   scheduler.NewQueue(mocks.NewFakeClock(time.Unix(50, 0).UTC())),
		Store:   store,
		Clock:   mocks.NewFakeClock(time.Unix(50, 0).UTC()),
		Presets: map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
	}
	router := &gateway.Router{
		Fleet:   staticWorkloadFleet{node: node},
		Nodes:   workloadNodeResolver{agents: map[string]ports.NodeAgent{node.ID: agent}},
		Presets: gateway.NewPresetRegistry(preset),
		Runtime: service,
		Clock:   mocks.NewFakeClock(time.Unix(50, 0).UTC()),
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, service, store, "rpc-secret", "peer-a", router)

	payloadBody := json.RawMessage(`{"model":"model-a","messages":[{"role":"user","content":"hi"}]}`)
	payload, err := json.Marshal(workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindOpenAIChat, Body: payloadBody})
	if err != nil {
		t.Fatalf("Marshal payload: %v", err)
	}
	admit := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/workloads", bytes.NewReader(mustJSON(t, workload.AdmissionRequest{
		Job: domain.Job{
			ID:             "job-a",
			TaskType:       "chat",
			Model:          "model-a",
			Project:        "project-a",
			Priority:       domain.PriorityInteractive,
			SpeedPref:      domain.SpeedThroughput,
			Preemption:     domain.PreemptSoft,
			ContextRequest: 1024,
		},
		Payload: payload,
	})))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(admit, req)
	if admit.Code != http.StatusAccepted || strings.Contains(admit.Body.String(), "messages") {
		t.Fatalf("admit status/body = %d %s", admit.Code, admit.Body.String())
	}
	var admitted workload.AdmissionResponse
	if err := json.Unmarshal(admit.Body.Bytes(), &admitted); err != nil {
		t.Fatalf("decode admit: %v", err)
	}
	replayable, err := peercoord.EncodeRescuePayload(admitted.Job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	if err := store.Put(ctx, domain.JobRecord{JobID: "job-a", Coordinator: "peer-a", Status: domain.JobQueued, Request: replayable, UpdatedAt: time.Unix(50, 0).UTC()}); err != nil {
		t.Fatalf("Put registry: %v", err)
	}

	placer.ready = true
	drain := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/workloads/drain?limit=1&execute=true", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(drain, req)
	if drain.Code != http.StatusOK || strings.Contains(drain.Body.String(), "messages") || strings.Contains(drain.Body.String(), "content") {
		t.Fatalf("drain status/body = %d %s", drain.Code, drain.Body.String())
	}
	if providerBody == "" || !strings.Contains(providerBody, `"messages"`) {
		t.Fatalf("provider body = %q", providerBody)
	}
	var drained workload.DrainResponse
	if err := json.Unmarshal(drain.Body.Bytes(), &drained); err != nil {
		t.Fatalf("decode drain: %v", err)
	}
	if len(drained.Results) != 1 || drained.Results[0].Job.Status != domain.JobDone {
		t.Fatalf("drained = %+v", drained)
	}
	exec := drained.Results[0].Execution
	if exec.State != "terminal_result_replayable" || !exec.AutomaticProviderOutputCapture || !exec.ResultReplayable || !exec.ResultBodyStored || !exec.ResultArtifact || exec.ResultBytes == 0 || !exec.ResultSHA256 || !exec.PayloadExecutable {
		t.Fatalf("execution evidence = %+v", exec)
	}
	leases, err := store.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases after execute = %+v", leases)
	}
	resultBody, err := store.WorkloadResultBody(ctx, "job-a")
	if err != nil {
		t.Fatalf("WorkloadResultBody: %v", err)
	}
	if !strings.Contains(string(resultBody.Body), `"choices"`) || resultBody.Bytes != len(resultBody.Body) || resultBody.SHA256 == "" {
		t.Fatalf("result body = %+v body=%s", resultBody, resultBody.Body)
	}
}

func TestWorkloadHTTPExecuteRunningWorkloadCapturesProviderResult(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("provider path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("model-a"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	preset.Backend = domain.BackendLlamaCpp
	agent := &addressedWorkloadAgent{node: node, addr: strings.TrimPrefix(upstream.URL, "http://")}
	admission := &mocks.AdmissionController{}
	lease := domain.Lease{ID: "lease-a", JobID: "job-a", NodeID: node.ID, InstanceID: "inst-a"}
	inst := domain.ModelInstance{ID: "inst-a", PresetID: preset.ID, NodeID: node.ID, State: "ready", Addr: upstream.URL}
	job := domain.Job{
		ID:       "job-a",
		TaskType: "chat",
		Model:    "model-a",
		PresetID: preset.ID,
		Project:  "project-a",
		Status:   domain.JobRunning,
		Result: &domain.JobResult{
			Source:          "scheduler",
			Decision:        domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Action: domain.ActionLoadedNew, Preset: preset},
			Instance:        inst,
			LeaseID:         lease.ID,
			NodeID:          node.ID,
			PresetID:        preset.ID,
			Action:          domain.ActionLoadedNew,
			PayloadBytes:    1,
			PayloadStored:   true,
			PayloadRedacted: true,
			ResultRedacted:  true,
		},
	}
	if err := store.SaveJob(ctx, job); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	if err := store.SaveLease(ctx, lease); err != nil {
		t.Fatalf("SaveLease: %v", err)
	}
	payloadBody := json.RawMessage(`{"model":"model-a","messages":[{"role":"user","content":"hi"}]}`)
	payload, err := json.Marshal(workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindOpenAIChat, Body: payloadBody})
	if err != nil {
		t.Fatalf("Marshal payload: %v", err)
	}
	replayable, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	if err := store.Put(ctx, domain.JobRecord{JobID: "job-a", Coordinator: "peer-a", Status: domain.JobRunning, Request: replayable, UpdatedAt: time.Unix(50, 0).UTC()}); err != nil {
		t.Fatalf("Put registry: %v", err)
	}
	service := &scheduler.Service{
		Placer:  &gatedWorkloadPlacer{ready: true, nodeID: node.ID, preset: preset, claim: domain.Claim{WeightsMB: 100}},
		Fleet:   staticWorkloadFleet{node: node},
		Nodes:   workloadNodeResolver{agents: map[string]ports.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners:  workloadNodeResolver{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:   scheduler.NewQueue(mocks.NewFakeClock(time.Unix(50, 0).UTC())),
		Store:   store,
		Clock:   mocks.NewFakeClock(time.Unix(50, 0).UTC()),
		Presets: map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
	}
	router := &gateway.Router{
		Fleet:   staticWorkloadFleet{node: node},
		Nodes:   workloadNodeResolver{agents: map[string]ports.NodeAgent{node.ID: agent}},
		Presets: gateway.NewPresetRegistry(preset),
		Runtime: service,
		Clock:   mocks.NewFakeClock(time.Unix(50, 0).UTC()),
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, service, store, "rpc-secret", "peer-a", router)

	req := httptest.NewRequest(http.MethodPost, "/workloads/job-a/execute", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "messages") || strings.Contains(rec.Body.String(), "content") {
		t.Fatalf("execute status/body = %d %s", rec.Code, rec.Body.String())
	}
	var finished workload.FinishResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &finished); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}
	if finished.Job.Status != domain.JobDone || finished.Execution.State != "terminal_result_replayable" || !finished.Execution.AutomaticProviderOutputCapture || !finished.Execution.ResultReplayable {
		t.Fatalf("finished = %+v", finished)
	}
	leases, err := store.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases after execute = %+v", leases)
	}
}

func TestWorkloadHTTPDrainsQueuedWorkAcrossOwnersAndReclaimsCapacity(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	nodeA.Accelerators = []domain.Accelerator{{Index: 0, Vendor: "test", Kind: "gpu", VRAMTotalMB: 1000}}
	nodeA.MaxUtil = 1
	nodeA.Status = domain.NodeReady
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	nodeB.Accelerators = []domain.Accelerator{{Index: 0, Vendor: "test", Kind: "gpu", VRAMTotalMB: 1000}}
	nodeB.MaxUtil = 1
	nodeB.Status = domain.NodeReady
	presetA := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("model-a"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	presetB := fixtures.MakePreset(fixtures.WithPresetID("preset-b"), fixtures.WithModelRef("model-b"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	agentA := mocks.NewNodeAgent(nodeA)
	agentB := mocks.NewNodeAgent(nodeB)
	admissionA := &mocks.AdmissionController{}
	admissionB := &mocks.AdmissionController{}
	placer := &scriptedWorkloadPlacer{routes: map[string]workloadRoute{
		"job-a": {nodeID: nodeA.ID, preset: presetA, claim: domain.Claim{WeightsMB: 100}},
		"job-b": {nodeID: nodeB.ID, preset: presetB, claim: domain.Claim{WeightsMB: 100}},
	}}
	service := &scheduler.Service{
		Placer: placer,
		Fleet:  staticWorkloadFleet{nodes: []domain.Node{nodeA, nodeB}},
		Nodes: workloadNodeResolver{
			agents: map[string]ports.NodeAgent{
				nodeA.ID: agentA,
				nodeB.ID: agentB,
			},
			admissions: map[string]ports.AdmissionController{
				nodeA.ID: admissionA,
				nodeB.ID: admissionB,
			},
		},
		Owners: workloadNodeResolver{admissions: map[string]ports.AdmissionController{
			nodeA.ID: admissionA,
			nodeB.ID: admissionB,
		}},
		Queue: scheduler.NewQueue(mocks.NewFakeClock(time.Unix(60, 0).UTC())),
		Store: store,
		Clock: mocks.NewFakeClock(time.Unix(60, 0).UTC()),
		Presets: map[string]domain.Preset{
			presetA.ID:       presetA,
			presetA.ModelRef: presetA,
			presetB.ID:       presetB,
			presetB.ModelRef: presetB,
		},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, service, store, "rpc-secret", "peer-a", nil)

	admitWorkload := func(id, model string) workload.AdmissionResponse {
		t.Helper()
		body := fmt.Sprintf(`{"job":{"id":%q,"task_type":"chat","model":%q,"project":"project-a","priority":"normal","speed_pref":"throughput","preemption":"soft"},"payload":{"input_bytes":123}}`, id, model)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/workloads", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer rpc-secret")
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted || strings.Contains(rec.Body.String(), "input_bytes") {
			t.Fatalf("admit %s status/body = %d %s", id, rec.Code, rec.Body.String())
		}
		var out workload.AdmissionResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode admit %s: %v", id, err)
		}
		if !out.Queued || out.Result == nil || !out.Result.PayloadRedacted || !out.Result.ResultRedacted {
			t.Fatalf("admit %s = %+v", id, out)
		}
		return out
	}
	admitWorkload("job-a", "model-a")
	admitWorkload("job-b", "model-b")

	placer.ready = true
	drain := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/workloads/drain?limit=2", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(drain, req)
	if drain.Code != http.StatusOK || strings.Contains(drain.Body.String(), "input_bytes") {
		t.Fatalf("drain status/body = %d %s", drain.Code, drain.Body.String())
	}
	var drained workload.DrainResponse
	if err := json.Unmarshal(drain.Body.Bytes(), &drained); err != nil {
		t.Fatalf("decode drain: %v", err)
	}
	if len(drained.Results) != 2 {
		t.Fatalf("drained = %+v", drained)
	}
	drainedByJob := map[string]workload.AdmissionResponse{}
	for _, result := range drained.Results {
		drainedByJob[result.Job.ID] = result
	}
	if got := drainedByJob["job-a"]; got.Job.Status != domain.JobRunning || got.Result == nil || got.Result.NodeID != nodeA.ID || got.Result.LeaseID == "" {
		t.Fatalf("drained job-a = %+v", got)
	}
	if got := drainedByJob["job-b"]; got.Job.Status != domain.JobRunning || got.Result == nil || got.Result.NodeID != nodeB.ID || got.Result.LeaseID == "" {
		t.Fatalf("drained job-b = %+v", got)
	}
	leases, err := store.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases after drain: %v", err)
	}
	if len(leases) != 2 {
		t.Fatalf("leases after drain = %+v", leases)
	}
	if len(agentA.Loaded) != 1 || len(agentB.Loaded) != 1 || len(admissionA.Requests) == 0 || len(admissionB.Requests) == 0 {
		t.Fatalf("agent/admission state a=%+v/%+v b=%+v/%+v", agentA.Loaded, admissionA.Requests, agentB.Loaded, admissionB.Requests)
	}

	finishWorkload := func(id string, errorBody string, wantStatus domain.JobStatus) workload.FinishResponse {
		t.Helper()
		body := strings.NewReader(errorBody)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/workloads/"+id+"/finish", body)
		req.Header.Set("Authorization", "Bearer rpc-secret")
		if errorBody != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "input_bytes") {
			t.Fatalf("finish %s status/body = %d %s", id, rec.Code, rec.Body.String())
		}
		var out workload.FinishResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode finish %s: %v", id, err)
		}
		if out.Job.Status != wantStatus || out.Result == nil || !out.Result.Terminal || !out.Result.ResultRedacted {
			t.Fatalf("finish %s = %+v", id, out)
		}
		return out
	}
	digest := strings.Repeat("a", 64)
	finished := finishWorkload("job-a", fmt.Sprintf(`{"result_artifact":"artifact://job-a","result_media_type":"application/json","result_bytes":42,"result_sha256":%q}`, digest), domain.JobDone)
	if finished.Result.ResultArtifact != "artifact://job-a" {
		t.Fatalf("finished result artifact = %q", finished.Result.ResultArtifact)
	}
	if finished.Result.ResultMediaType != "application/json" || finished.Result.ResultBytes != 42 || finished.Result.ResultSHA256 != digest {
		t.Fatalf("finished result manifest = %+v", finished.Result)
	}
	if finished.Execution.State != "terminal_result_manifest_only" || finished.Execution.AutomaticProviderOutputCapture || finished.Execution.ResultReplayable || !finished.Execution.ResultArtifact || finished.Execution.ResultBytes != 42 || finished.Execution.Error == "" {
		t.Fatalf("finished execution evidence = %+v", finished.Execution)
	}
	leases, err = store.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases after first finish: %v", err)
	}
	if len(leases) != 1 || leases[0].JobID != "job-b" {
		t.Fatalf("leases after first finish = %+v", leases)
	}
	failed := finishWorkload("job-b", `{"error":"redacted provider timeout"}`, domain.JobFailed)
	if failed.Job.Error != "redacted provider timeout" {
		t.Fatalf("failed job error = %q", failed.Job.Error)
	}
	leases, err = store.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases after failure: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases after failure = %+v", leases)
	}
}

func TestWorkloadHTTPStoresAndRetrievesExplicitResultBody(t *testing.T) {
	store := &workloadMemoryStore{jobs: map[string]domain.Job{}}
	resultBody := []byte(`{"answer":"ok"}`)
	sum := sha256.Sum256(resultBody)
	digest := hex.EncodeToString(sum[:])
	job := domain.Job{
		ID:     "job-a",
		Model:  "model-a",
		Status: domain.JobRunning,
		Result: &domain.JobResult{
			Source:         "scheduler",
			Action:         domain.ActionLoadedNew,
			NodeID:         "node-a",
			PresetID:       "preset-a",
			LeaseID:        "lease-a",
			ResultRedacted: true,
		},
	}
	if err := store.SaveJob(context.Background(), job); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, &recordingWorkloadRuntime{store: store}, store, "rpc-secret", "peer-a", nil)

	finishBody, err := json.Marshal(workload.FinishRequest{
		ResultArtifact:  "stored://workload/job-a",
		ResultMediaType: "application/json",
		ResultBytes:     len(resultBody),
		ResultSHA256:    digest,
		ResultBody:      resultBody,
	})
	if err != nil {
		t.Fatalf("marshal finish: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/workloads/job-a/finish", bytes.NewReader(finishBody))
	req.Header.Set("Authorization", "Bearer rpc-secret")
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "answer") {
		t.Fatalf("finish status/body = %d %s", rec.Code, rec.Body.String())
	}
	var finished workload.FinishResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &finished); err != nil {
		t.Fatalf("decode finish: %v", err)
	}
	if finished.Result == nil || finished.Result.ResultArtifact != "stored://workload/job-a" || finished.Result.ResultSHA256 != digest {
		t.Fatalf("finished = %+v", finished)
	}
	if finished.Execution.State != "terminal_result_replayable" || finished.Execution.AutomaticProviderOutputCapture || !finished.Execution.ResultReplayable || !finished.Execution.ResultBodyStored {
		t.Fatalf("finished execution evidence = %+v", finished.Execution)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/workloads/job-a", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "answer") {
		t.Fatalf("status leaked result body = %d %s", rec.Code, rec.Body.String())
	}
	var status workload.StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.Execution.State != "terminal_result_replayable" || status.Execution.AutomaticProviderOutputCapture || !status.Execution.ResultReplayable || !status.Execution.ResultBodyStored {
		t.Fatalf("status execution evidence = %+v", status.Execution)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/workloads/job-a/result", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != string(resultBody) {
		t.Fatalf("result status/body = %d %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Myc-Result-SHA256"); got != digest {
		t.Fatalf("result digest header = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("result content type = %q", got)
	}
}

func TestWorkloadHTTPRejectsInvalidResultArtifactManifest(t *testing.T) {
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	job := domain.Job{
		ID:     "job-a",
		Model:  "model-a",
		Status: domain.JobRunning,
		Result: &domain.JobResult{
			Source:         "scheduler",
			Action:         domain.ActionLoadedNew,
			NodeID:         "node-a",
			PresetID:       "preset-a",
			LeaseID:        "lease-a",
			ResultRedacted: true,
		},
	}
	if err := store.SaveJob(context.Background(), job); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	service := &scheduler.Service{Store: store, Queue: scheduler.NewQueue(mocks.NewFakeClock(time.Unix(70, 0).UTC()))}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, service, store, "rpc-secret", "peer-a", nil)

	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "metadata without artifact", body: `{"result_bytes":1}`, want: "result_artifact"},
		{name: "negative bytes", body: `{"result_artifact":"artifact://job-a","result_bytes":-1}`, want: "result_bytes"},
		{name: "bad digest", body: `{"result_artifact":"artifact://job-a","result_sha256":"nope"}`, want: "result_sha256"},
		{name: "body bytes mismatch", body: `{"result_artifact":"stored://workload/job-a","result_body":"eyJhbnN3ZXIiOiJvayJ9","result_bytes":1}`, want: "result_bytes"},
		{name: "body digest mismatch", body: `{"result_artifact":"stored://workload/job-a","result_body":"eyJhbnN3ZXIiOiJvayJ9","result_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, want: "result_sha256"},
		{name: "control character media type", body: "{\"result_artifact\":\"artifact://job-a\",\"result_media_type\":\"application/\\njson\"}", want: "result_media_type"},
	}
	for _, tc := range tests {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/workloads/job-a/finish", strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer rpc-secret")
		req.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), tc.want) {
			t.Fatalf("%s status/body = %d %s", tc.name, rec.Code, rec.Body.String())
		}
	}
}

func TestWorkloadResultBodyRejectsManifestMismatch(t *testing.T) {
	ctx := context.Background()
	store := &workloadMemoryStore{
		jobs: map[string]domain.Job{},
		results: map[string]domain.WorkloadResultBody{
			"job-a": {JobID: "job-a", Bytes: 7, SHA256: strings.Repeat("a", 64)},
		},
	}
	base := func() domain.Job {
		return domain.Job{ID: "job-a", Status: domain.JobDone, Result: &domain.JobResult{Terminal: true, ResultArtifact: "stored://workload/job-a"}}
	}
	if _, err := workloadResultBody(ctx, store, domain.Job{ID: "not-terminal", Status: domain.JobDone, Result: &domain.JobResult{ResultArtifact: "stored://workload/job-a"}}); err == nil || !strings.Contains(err.Error(), "not terminal") {
		t.Fatalf("not-terminal err = %v", err)
	}
	noArtifact := base()
	noArtifact.Result = &domain.JobResult{Terminal: true}
	if _, err := workloadResultBody(ctx, store, noArtifact); err == nil || !strings.Contains(err.Error(), "no result artifact") {
		t.Fatalf("no-artifact err = %v", err)
	}
	bytesMismatch := base()
	bytesMismatch.Result.ResultBytes = 8
	if _, err := workloadResultBody(ctx, store, bytesMismatch); err == nil || !strings.Contains(err.Error(), "manifest bytes") {
		t.Fatalf("bytes mismatch err = %v", err)
	}
	digestMismatch := base()
	digestMismatch.Result = &domain.JobResult{Terminal: true, ResultArtifact: "stored://workload/job-a", ResultSHA256: strings.Repeat("b", 64)}
	if _, err := workloadResultBody(ctx, store, digestMismatch); err == nil || !strings.Contains(err.Error(), "manifest digest") {
		t.Fatalf("digest mismatch err = %v", err)
	}
	ok, err := workloadResultBody(ctx, store, base())
	if err != nil || ok.Bytes != 7 {
		t.Fatalf("result body = %+v err=%v", ok, err)
	}
}

func TestWorkloadPayloadEvidenceClassifiesRegistryStates(t *testing.T) {
	ctx := context.Background()
	job := domain.Job{
		ID:     "job-a",
		Status: domain.JobQueued,
		Result: &domain.JobResult{
			PayloadBytes:  17,
			PayloadStored: true,
		},
	}
	replayable, err := peercoord.EncodeRescuePayload(job, []byte(`{"model":"tiny"}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	other, err := peercoord.EncodeRescuePayload(domain.Job{ID: "other"}, []byte(`{"model":"tiny"}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload other: %v", err)
	}
	tests := []struct {
		name       string
		record     *domain.JobRecord
		wantState  string
		replayable bool
		redacted   bool
		wantError  string
	}{
		{name: "missing record", wantState: "runtime_payload_stored"},
		{name: "replayable", record: &domain.JobRecord{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued, Request: replayable}, wantState: "registry_payload_replayable", replayable: true},
		{name: "redacted", record: &domain.JobRecord{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued, Handling: domain.HandlingPrivate, PayloadRedacted: true}, wantState: "registry_payload_redacted", redacted: true},
		{name: "missing payload", record: &domain.JobRecord{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued}, wantState: "registry_payload_missing"},
		{name: "unreadable", record: &domain.JobRecord{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued, Request: []byte(`{`)}, wantState: "registry_payload_unreadable", wantError: "unexpected"},
		{name: "mismatch", record: &domain.JobRecord{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued, Request: other}, wantState: "registry_payload_mismatch", wantError: "does not match"},
	}
	for _, tc := range tests {
		store := &workloadMemoryStore{jobs: map[string]domain.Job{}, records: nil}
		if err := store.SaveJob(ctx, job); err != nil {
			t.Fatalf("%s SaveJob: %v", tc.name, err)
		}
		if tc.record != nil {
			tc.record.UpdatedAt = time.Unix(80, 0).UTC()
			store.records = []domain.JobRecord{*tc.record}
		}
		got := workloadPayloadEvidence(ctx, store, job)
		if got.State != tc.wantState || got.Replayable != tc.replayable || got.Redacted != tc.redacted || (tc.wantError != "" && !strings.Contains(got.Error, tc.wantError)) {
			t.Fatalf("%s evidence = %+v", tc.name, got)
		}
	}
}

func TestWorkloadJobRecordUsesPointLookup(t *testing.T) {
	ctx := context.Background()
	rec := domain.JobRecord{
		JobID:       "job-a",
		Coordinator: "peer-a",
		Status:      domain.JobQueued,
		Request:     []byte(`{}`),
		UpdatedAt:   time.Unix(1, 0).UTC(),
	}
	directStore := &workloadMemoryStore{records: []domain.JobRecord{rec}}
	got, found, err := workloadJobRecord(ctx, directStore, directStore, "job-a")
	if err != nil || !found || got.JobID != "job-a" {
		t.Fatalf("direct JobRecord = %+v found=%v err=%v", got, found, err)
	}
	fallback := &pointOnlyWorkloadRegistry{records: []domain.JobRecord{rec}}
	got, found, err = workloadJobRecord(ctx, workloadStatusOnlyStore{}, fallback, "job-a")
	if err != nil || !found || got.JobID != "job-a" {
		t.Fatalf("fallback JobRecord = %+v found=%v err=%v", got, found, err)
	}
	_, found, err = workloadJobRecord(ctx, workloadStatusOnlyStore{}, fallback, "missing")
	if err != nil || found {
		t.Fatalf("missing JobRecord found=%v err=%v", found, err)
	}
}

func TestWorkloadExecutionEvidenceClassifiesExecutablePayloadContract(t *testing.T) {
	ctx := context.Background()
	job := domain.Job{
		ID:     "job-a",
		Status: domain.JobQueued,
		Result: &domain.JobResult{
			PayloadBytes:  91,
			PayloadStored: true,
		},
	}
	body := json.RawMessage(`{"model":"model-a","messages":[]}`)
	payload, err := json.Marshal(workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindOpenAIChat, Body: body})
	if err != nil {
		t.Fatalf("Marshal executable payload: %v", err)
	}
	replayable, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	store := &workloadMemoryStore{
		jobs: map[string]domain.Job{job.ID: job},
		records: []domain.JobRecord{{
			JobID:       job.ID,
			Coordinator: "peer-a",
			Status:      domain.JobQueued,
			Request:     replayable,
			UpdatedAt:   time.Unix(90, 0).UTC(),
		}},
	}
	got := workloadExecutionEvidence(ctx, store, job)
	if got.PayloadContract != "registry_executable_gateway_request" || got.PayloadKind != workload.ExecutablePayloadKindOpenAIChat || !got.PayloadExecutable {
		t.Fatalf("execution evidence = %+v", got)
	}
	job.Result = nil
	job.Status = domain.JobFailed
	got = workloadExecutionEvidence(ctx, store, job)
	if got.PayloadContract != "registry_executable_gateway_request" || got.PayloadKind != workload.ExecutablePayloadKindOpenAIChat || !got.PayloadExecutable || got.State != "terminal_no_result" {
		t.Fatalf("failed-before-result evidence = %+v", got)
	}
}

func TestWorkloadSchedulerResultReconstructsLeaseAndPlacementEvidence(t *testing.T) {
	job := domain.Job{
		ID:     "job-a",
		Status: domain.JobRunning,
		Result: &domain.JobResult{
			LeaseID:  "lease-a",
			NodeID:   "owner-a",
			PresetID: "preset-a",
			Action:   domain.ActionLoadedNew,
			Instance: domain.ModelInstance{ID: "inst-a"},
		},
	}
	got, err := workloadSchedulerResult(job)
	if err != nil {
		t.Fatalf("scheduler result: %v", err)
	}
	if got.Lease.ID != "lease-a" || got.Lease.InstanceID != "inst-a" || got.Decision.JobID != "job-a" || got.Decision.NodeID != "owner-a" || got.Decision.Preset.ID != "preset-a" || got.Instance.ID != "inst-a" {
		t.Fatalf("scheduler result = %+v", got)
	}
	job.Result.Decision = domain.PlacementDecision{JobID: "job-a", NodeID: "decision-owner", Preset: domain.Preset{ID: "decision-preset"}, Action: domain.ActionWarmInstance}
	got, err = workloadSchedulerResult(job)
	if err != nil {
		t.Fatalf("scheduler result with decision: %v", err)
	}
	if got.Decision.NodeID != "decision-owner" || got.Decision.Preset.ID != "decision-preset" || got.Decision.Action != domain.ActionWarmInstance {
		t.Fatalf("scheduler decision = %+v", got.Decision)
	}
	for _, tc := range []struct {
		name string
		job  domain.Job
		want string
	}{
		{name: "not running", job: domain.Job{ID: "queued", Status: domain.JobQueued}, want: "execute requires running"},
		{name: "missing result", job: domain.Job{ID: "missing", Status: domain.JobRunning}, want: "no scheduler result evidence"},
		{name: "missing lease", job: domain.Job{ID: "lease", Status: domain.JobRunning, Result: &domain.JobResult{}}, want: "no active lease evidence"},
	} {
		if _, err := workloadSchedulerResult(tc.job); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s err = %v, want %q", tc.name, err, tc.want)
		}
	}
}

func TestExecutableWorkloadIngressRequestClassifiesKindsAndFailures(t *testing.T) {
	ctx := context.Background()
	job := domain.Job{
		ID:     "job-a",
		Model:  "model-a",
		Status: domain.JobQueued,
		Result: &domain.JobResult{PayloadBytes: 1, PayloadStored: true},
	}
	makeRecord := func(t *testing.T, payload []byte) domain.JobRecord {
		t.Helper()
		replayable, err := peercoord.EncodeRescuePayload(job, payload)
		if err != nil {
			t.Fatalf("EncodeRescuePayload: %v", err)
		}
		return domain.JobRecord{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued, Request: replayable, UpdatedAt: time.Unix(90, 0).UTC()}
	}
	tests := []struct {
		name    string
		payload []byte
		want    translate.IngressKind
	}{
		{name: "completion", payload: mustJSON(t, workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindOpenAICompletion, Body: json.RawMessage(`{"model":"model-a","prompt":"hi"}`)}), want: translate.KindOpenAICompletion},
		{name: "anthropic", payload: mustJSON(t, workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindAnthropicMessages, Body: json.RawMessage(`{"model":"model-a","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`)}), want: translate.KindAnthropicMessages},
	}
	for _, tc := range tests {
		store := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}, records: []domain.JobRecord{makeRecord(t, tc.payload)}}
		got, err := executableWorkloadIngressRequest(ctx, store, job)
		if err != nil || got.Kind != tc.want {
			t.Fatalf("%s request = %+v err=%v", tc.name, got, err)
		}
	}
	redacted := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}, records: []domain.JobRecord{{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued, PayloadRedacted: true}}}
	if _, err := executableWorkloadIngressRequest(ctx, redacted, job); err == nil || !strings.Contains(err.Error(), "redacted") {
		t.Fatalf("redacted err = %v", err)
	}
	otherPayload, err := peercoord.EncodeRescuePayload(domain.Job{ID: "other"}, []byte(`{"kind":"openai_chat","body":{"model":"model-a","messages":[]}}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload other: %v", err)
	}
	mismatch := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}, records: []domain.JobRecord{{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobQueued, Request: otherPayload}}}
	if _, err := executableWorkloadIngressRequest(ctx, mismatch, job); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatch err = %v", err)
	}
	unsupported := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}, records: []domain.JobRecord{makeRecord(t, mustJSON(t, workload.ExecutablePayload{Kind: "unknown", Body: json.RawMessage(`{}`)}))}}
	if _, err := executableWorkloadIngressRequest(ctx, unsupported, job); err == nil || !strings.Contains(err.Error(), "unsupported executable payload kind") {
		t.Fatalf("unsupported err = %v", err)
	}
}

func TestPersistWorkloadReplayPayloadRecordsRegistryEvidence(t *testing.T) {
	ctx := context.Background()
	job := domain.Job{
		ID:       "job-a",
		Model:    "model-a",
		Project:  "project-a",
		Status:   domain.JobQueued,
		Handling: domain.HandlingStandard,
		Result:   &domain.JobResult{NodeID: "node-a"},
	}
	if err := persistWorkloadReplayPayload(ctx, &workloadMemoryStore{}, "peer-a", job, nil); err != nil {
		t.Fatalf("empty payload: %v", err)
	}
	if err := persistWorkloadReplayPayload(ctx, workloadStatusOnlyStore{job: job}, "peer-a", job, []byte(`{"kind":"opaque"}`)); err != nil {
		t.Fatalf("non-registry store: %v", err)
	}
	store := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}}
	if err := persistWorkloadReplayPayload(ctx, store, "peer-a", job, []byte(`{"kind":"opaque"}`)); err != nil {
		t.Fatalf("persist replay payload: %v", err)
	}
	if len(store.records) != 1 {
		t.Fatalf("records = %+v", store.records)
	}
	rec := store.records[0]
	if rec.JobID != job.ID || rec.Coordinator != "peer-a" || rec.AssignedNode != "node-a" || rec.Status != domain.JobQueued || len(rec.Request) == 0 {
		t.Fatalf("record = %+v", rec)
	}
	payloadJob, body, err := peercoord.DecodeRescuePayload(rec.Request)
	if err != nil {
		t.Fatalf("DecodeRescuePayload: %v", err)
	}
	if payloadJob.ID != job.ID || string(body) != `{"kind":"opaque"}` {
		t.Fatalf("payload job=%+v body=%s", payloadJob, body)
	}
	anon := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}}
	if err := persistWorkloadReplayPayload(ctx, anon, "", job, []byte(`{"kind":"opaque"}`)); err != nil {
		t.Fatalf("persist anonymous coordinator payload: %v", err)
	}
	if len(anon.records) != 1 || anon.records[0].Coordinator != "workload-rpc" {
		t.Fatalf("anonymous coordinator record = %+v", anon.records)
	}
}

func TestExecuteDrainedWorkloadRejectsStreamingPayload(t *testing.T) {
	ctx := context.Background()
	job := domain.Job{
		ID:     "job-a",
		Model:  "model-a",
		Status: domain.JobRunning,
		Result: &domain.JobResult{PayloadBytes: 1, PayloadStored: true, LeaseID: "lease-a", NodeID: "node-a"},
	}
	payload := mustJSON(t, workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindOpenAIChat, Body: json.RawMessage(`{"model":"model-a","messages":[{"role":"user","content":"hi"}],"stream":true}`)})
	replayable, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	store := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}, records: []domain.JobRecord{{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobRunning, Request: replayable}}}
	result := scheduler.Result{Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionWarmInstance, InstanceID: "inst-a"}, Instance: domain.ModelInstance{ID: "inst-a", NodeID: "node-a"}, Lease: domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: "node-a"}}
	err = executeDrainedWorkload(ctx, nil, &recordingWorkloadRuntime{store: store}, store, job, result)
	if err == nil || !strings.Contains(err.Error(), "streaming") {
		t.Fatalf("streaming err = %v", err)
	}
}

func TestExecuteDrainedWorkloadRequiresSchedulerResultEvidence(t *testing.T) {
	ctx := context.Background()
	job := domain.Job{
		ID:     "job-a",
		Model:  "model-a",
		Status: domain.JobRunning,
	}
	payload := mustJSON(t, workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindOpenAIChat, Body: json.RawMessage(`{"model":"model-a","messages":[{"role":"user","content":"hi"}]}`)})
	replayable, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	store := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}, records: []domain.JobRecord{{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobRunning, Request: replayable}}}
	result := scheduler.Result{Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionWarmInstance, InstanceID: "inst-a"}, Instance: domain.ModelInstance{ID: "inst-a", NodeID: "node-a"}, Lease: domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: "node-a"}}
	err = executeDrainedWorkload(ctx, staticWorkloadProviderExecutor{response: gateway.RouteResponse{Status: http.StatusOK, Body: []byte(`{"ok":true}`), Header: http.Header{"Content-Type": []string{"application/json"}}}}, &recordingWorkloadRuntime{store: store}, store, job, result)
	if err == nil || !strings.Contains(err.Error(), "no scheduler result evidence") {
		t.Fatalf("missing evidence err = %v", err)
	}
}

func TestExecuteDrainedWorkloadPropagatesProviderFailure(t *testing.T) {
	ctx := context.Background()
	job := domain.Job{
		ID:     "job-a",
		Model:  "model-a",
		Status: domain.JobRunning,
		Result: &domain.JobResult{PayloadBytes: 1, PayloadStored: true, LeaseID: "lease-a", NodeID: "node-a"},
	}
	payload := mustJSON(t, workload.ExecutablePayload{Kind: workload.ExecutablePayloadKindOpenAIChat, Body: json.RawMessage(`{"model":"model-a","messages":[{"role":"user","content":"hi"}]}`)})
	replayable, err := peercoord.EncodeRescuePayload(job, payload)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	store := &workloadMemoryStore{jobs: map[string]domain.Job{job.ID: job}, records: []domain.JobRecord{{JobID: job.ID, Coordinator: "peer-a", Status: domain.JobRunning, Request: replayable}}}
	result := scheduler.Result{Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionWarmInstance, InstanceID: "inst-a"}, Instance: domain.ModelInstance{ID: "inst-a", NodeID: "node-a"}, Lease: domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: "node-a"}}
	err = executeDrainedWorkload(ctx, staticWorkloadProviderExecutor{response: gateway.RouteResponse{Status: http.StatusServiceUnavailable, Body: []byte(`{"error":"busy"}`)}}, &recordingWorkloadRuntime{store: store}, store, job, result)
	if err == nil || !strings.Contains(err.Error(), "returned 503") {
		t.Fatalf("provider failure err = %v", err)
	}
}

func TestNormalizeWorkloadJobValidationBranches(t *testing.T) {
	base := domain.Job{ID: "job-a", Model: "model-a", Project: "project-a"}
	normalized, err := normalizeWorkloadJob(base)
	if err != nil {
		t.Fatalf("normalize default: %v", err)
	}
	if normalized.TaskType != "chat" || normalized.Priority != domain.PriorityNormal || normalized.SpeedPref != domain.SpeedThroughput || normalized.Preemption != domain.PreemptInherit || normalized.Status != domain.JobQueued {
		t.Fatalf("normalized defaults = %+v", normalized)
	}
	tests := []struct {
		name string
		job  domain.Job
		want string
	}{
		{name: "missing id", job: domain.Job{Model: "model-a", Project: "project-a"}, want: "job.id"},
		{name: "missing model", job: domain.Job{ID: "job-a", Project: "project-a"}, want: "job.model"},
		{name: "missing project", job: domain.Job{ID: "job-a", Model: "model-a"}, want: "job.project"},
		{name: "bad priority", job: withWorkloadPriority(base, "urgent"), want: "priority"},
		{name: "bad speed", job: withWorkloadSpeed(base, "fast"), want: "speed_pref"},
		{name: "bad preemption", job: withWorkloadPreemption(base, "borrow"), want: "preemption"},
		{name: "bad handling", job: withWorkloadHandling(base, "secret"), want: "handling"},
		{name: "negative context", job: withWorkloadContext(base, -1), want: "context_request"},
		{name: "negative concurrency", job: withWorkloadConcurrency(base, -1), want: "expected_concurrency"},
		{name: "negative deadline", job: withWorkloadDeadline(base, -1), want: "deadline_ms"},
	}
	for _, tc := range tests {
		if _, err := normalizeWorkloadJob(tc.job); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s err = %v", tc.name, err)
		}
	}
}

func TestPeerRPCClientErrorBranches(t *testing.T) {
	ctx := context.Background()
	if err := doPeerRPC(ctx, nil, "", domain.Peer{ID: "peer-a"}, http.MethodGet, "/x", nil, nil); err == nil {
		t.Fatal("peer without address accepted")
	}
	errorClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	if err := doPeerRPC(ctx, errorClient, "", domain.Peer{ID: "peer-a", Addresses: []string{"http://peer-error.test"}}, http.MethodGet, "/x", nil, nil); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("status err = %v", err)
	}
	badJSON := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	var out map[string]string
	if err := doPeerRPC(ctx, badJSON, "", domain.Peer{ID: "peer-a", Addresses: []string{"http://peer-bad-json.test"}}, http.MethodGet, "/x", nil, &out); err == nil {
		t.Fatal("bad JSON peer response accepted")
	}
	okClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rpc-secret" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("headers = %+v", r.Header)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := doPeerRPC(ctx, okClient, "rpc-secret", domain.Peer{ID: "peer-a", Addresses: []string{"http://peer-ok.test"}}, http.MethodPost, "/x", map[string]string{"a": "b"}, nil); err != nil {
		t.Fatalf("ok peer rpc: %v", err)
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
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Myc-Join-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(seed)
	}))
	seedURL := "http://seed-peer.test"
	cache := membership.NewCachedPeerDiscovery(&mocks.PeerDiscovery{}, mocks.NewFakeClock(time.Unix(1, 0).UTC()), time.Minute)

	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	probeSeedPeersWithClient(context.Background(), cache, []string{seedURL}, "secret", manager, client)
	peers, err := cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != "seed-peer" || peers[0].Addresses[0] != strings.TrimPrefix(seedURL, "http://") {
		t.Fatalf("seed peers = %+v", peers)
	}
	if _, err := fetchPeerHealthWithClient(context.Background(), seedURL, "wrong", client); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("wrong join token err = %v", err)
	}
	if err := manager.Revoke("secret"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	cache = membership.NewCachedPeerDiscovery(&mocks.PeerDiscovery{}, mocks.NewFakeClock(time.Unix(1, 0).UTC()), time.Minute)
	probeSeedPeersWithClient(context.Background(), cache, []string{seedURL}, "secret", manager, client)
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
	if got := seedPeerProbeInterval(6 * time.Second); got != 6*time.Second {
		t.Fatalf("seed interval passthrough = %s", got)
	}
}

func TestSeedRefreshingDiscoveryProbesSeedsBeforePeers(t *testing.T) {
	seed := domain.Peer{ID: "seed-peer", Addresses: []string{"127.0.0.1:0"}, Compute: true}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Myc-Join-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(seed)
	}))
	cache := membership.NewCachedPeerDiscovery(&mocks.PeerDiscovery{}, mocks.NewFakeClock(time.Unix(10, 0).UTC()), time.Minute)
	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	discovery := seedRefreshingDiscovery{
		cache:      cache,
		seeds:      []string{"http://seed-refresh.test"},
		joinToken:  "secret",
		joinTokens: manager,
		client:     client,
	}

	peers, err := discovery.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != seed.ID || peers[0].Addresses[0] != "seed-refresh.test" {
		t.Fatalf("seed refreshed peers = %+v", peers)
	}
	if err := discovery.Advertise(context.Background(), domain.Peer{ID: "self"}); err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	watch, err := discovery.WatchPeers(context.Background())
	if err != nil {
		t.Fatalf("WatchPeers: %v", err)
	}
	waitForCondition(t, func() bool {
		select {
		case got := <-watch:
			if got.ID != seed.ID {
				t.Fatalf("watched peer = %+v", got)
			}
			return true
		default:
			return false
		}
	})
	if err := (seedRefreshingDiscovery{}).Advertise(context.Background(), domain.Peer{}); err == nil {
		t.Fatal("nil seed discovery advertise succeeded")
	}
	if _, err := (seedRefreshingDiscovery{}).Peers(context.Background()); err == nil {
		t.Fatal("nil seed discovery peers succeeded")
	}
	if _, err := (seedRefreshingDiscovery{}).WatchPeers(context.Background()); err == nil {
		t.Fatal("nil seed discovery watch succeeded")
	}
}

func TestPeerProbeWrappersValidateBeforeNetwork(t *testing.T) {
	cache := membership.NewCachedPeerDiscovery(&mocks.PeerDiscovery{}, mocks.NewFakeClock(time.Unix(1, 0).UTC()), time.Minute)
	probeSeedPeers(context.Background(), cache, nil, "", nil)
	if _, err := fetchPeerHealth(context.Background(), " ", ""); err == nil || !strings.Contains(err.Error(), "peer address") {
		t.Fatalf("fetchPeerHealth err = %v", err)
	}
	if err := probePeerHealthWithToken(context.Background(), domain.Peer{}, "secret"); err == nil || !strings.Contains(err.Error(), "peer id") {
		t.Fatalf("probePeerHealthWithToken err = %v", err)
	}
}

func TestPeerControlHTTPClientBypassesAmbientProxy(t *testing.T) {
	client := peerControlHTTPClient()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("peer control RPC must not use ambient HTTP proxy settings")
	}
	if !transport.DisableKeepAlives {
		t.Fatal("peer control RPC must not retain idle keep-alive sockets")
	}
	if transport.ResponseHeaderTimeout <= 0 {
		t.Fatal("peer control RPC must bound response header waits")
	}
}

func TestPeerBackgroundHelpersUseFakeClockAndPersistEffects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	discovery := &signalingPeerDiscovery{advertised: make(chan domain.Peer, 4)}
	startPeerAdvertiser(ctx, discovery, domain.Peer{ID: "peer-a"}, clk, time.Second)
	firstAdvertise := <-discovery.advertised
	if firstAdvertise.ID != "peer-a" {
		t.Fatalf("first advertise = %+v", firstAdvertise)
	}
	waitForCondition(t, func() bool {
		clk.Advance(time.Second)
		select {
		case <-discovery.advertised:
			return true
		default:
			return false
		}
	})

	seed := domain.Peer{ID: "seed-peer", Addresses: []string{"127.0.0.1:0"}, Compute: true}
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Myc-Join-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(seed)
	}))
	seedURL := "http://seed-prober.test"
	cache := membership.NewCachedPeerDiscovery(&mocks.PeerDiscovery{}, clk, time.Minute)
	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	startSeedPeerProberWithClient(ctx, cache, []string{seedURL}, "secret", manager, clk, time.Millisecond, client)
	waitForCondition(t, func() bool {
		peers, err := cache.Peers(context.Background())
		return err == nil && len(peers) == 1 && peers[0].ID == seed.ID
	})
	if got := appendSeedPeer([]string{"a"}, " a "); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("duplicate seed append = %+v", got)
	}
	if got := appendSeedPeer([]string{"a"}, "b"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("new seed append = %+v", got)
	}
	if got := peerDiscoveryTTL(2 * time.Second); got != 10*time.Second {
		t.Fatalf("ttl = %s", got)
	}
	if err := probePeerHealthWithTokenManagerAndClient(context.Background(), domain.Peer{ID: seed.ID, Addresses: []string{seedURL}}, "secret", manager, client); err != nil {
		t.Fatalf("probe with manager: %v", err)
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

func TestPeerDiagnosticsChecksSeedsInsidePeerProcess(t *testing.T) {
	manager, err := membership.NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	seedPeer := domain.Peer{ID: "seed-peer", Addresses: []string{"127.0.0.1:0"}, Compute: true}
	seedClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Myc-Join-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(seedPeer)
	}))
	mux := http.NewServeMux()
	mountPeerHTTPWithDiagnostics(mux, domain.Peer{ID: "self", Addresses: []string{"127.0.0.1:1"}}, manager, []string{"http://seed-ok.test"}, "secret", "rpc-secret", seedClient)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/peer/diagnostics", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("diagnostics without auth = %d %s", rec.Code, rec.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/peer/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("diagnostics = %d %s", rec.Code, rec.Body.String())
	}
	var report peerDiagnosticsReport
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatalf("decode diagnostics: %v", err)
	}
	if !report.Ready || len(report.Seeds) != 1 || report.Seeds[0].PeerID != seedPeer.ID || !report.Seeds[0].Compute {
		t.Fatalf("diagnostics report = %+v", report)
	}
	req = httptest.NewRequest(http.MethodPost, "/peer/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("diagnostics method = %d", rec.Code)
	}

	badClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "blocked", http.StatusForbidden)
	}))
	mux = http.NewServeMux()
	mountPeerHTTPWithDiagnostics(mux, domain.Peer{ID: "self"}, manager, []string{"http://seed-bad.test"}, "secret", "rpc-secret", badClient)
	req = httptest.NewRequest(http.MethodGet, "/peer/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer rpc-secret")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "blocked") {
		t.Fatalf("bad diagnostics = %d %s", rec.Code, rec.Body.String())
	}
	if err := manager.Revoke("secret"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	report = buildPeerDiagnostics(context.Background(), domain.Peer{ID: "self"}, []string{"http://seed-revoked.test"}, "secret", manager, seedClient)
	if report.Ready || len(report.Seeds) != 1 || !strings.Contains(report.Seeds[0].Error, "revoked") {
		t.Fatalf("revoked diagnostics = %+v", report)
	}
}

func TestStartRegistryHeartbeatQueueAndOptimizerLoops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := mocks.NewFakeClock(time.Unix(0, 0).UTC())

	registry := &mocks.JobRegistry{WatchCh: make(chan domain.JobRecord)}
	discovery := &signalingPeerDiscovery{peersSeen: make(chan struct{}, 4)}
	startRegistryReplication(ctx, registry, discovery, "peer-a", "", clk, time.Second)
	waitForCondition(t, func() bool {
		select {
		case <-discovery.peersSeen:
			return true
		default:
			return false
		}
	})
	badRegistry := &mocks.JobRegistry{WatchErr: errors.New("watch boom")}
	startRegistryReplication(ctx, badRegistry, discovery, "peer-a", "", clk, time.Second)
	if !containsString(badRegistry.Calls, "watch:") {
		t.Fatalf("bad registry calls = %+v", badRegistry.Calls)
	}

	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	dead := domain.Peer{ID: "dead-peer", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	startPeerHeartbeat(ctx, domain.Peer{ID: "peer-a"}, &mocks.PeerDiscovery{PeersVal: []domain.Peer{dead}}, gateway.NodeDirectory{}, &recordingRescueRuntime{}, store, "", nil, clk)
	waitForCondition(t, func() bool {
		clk.Advance(peercoord.DefaultHeartbeatInterval)
		node, err := store.Node(context.Background(), dead.ID)
		return err == nil && node.Status == domain.NodeUnreachable
	})

	queueStore, err := storesqlite.Open(filepath.Join(t.TempDir(), "queue.sqlite"))
	if err != nil {
		t.Fatalf("Open queue: %v", err)
	}
	defer queueStore.Close()
	if err := queueStore.SaveLease(context.Background(), domain.Lease{ID: "expired", ExpiresAt: time.Unix(-1, 0).UTC()}); err != nil {
		t.Fatalf("SaveLease: %v", err)
	}
	runtime := &scheduler.Service{
		Placer: scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clk),
		Fleet:  gateway.NodeDirectory{Agents: map[string]ports.NodeAgent{}},
		Nodes:  gateway.NodeDirectory{Agents: map[string]ports.NodeAgent{}},
		Queue:  scheduler.NewQueue(clk),
		Store:  queueStore,
		Clock:  clk,
	}
	startQueueDrainer(ctx, runtime, clk, time.Second, 1)
	waitForCondition(t, func() bool {
		clk.Advance(time.Second)
		leases, err := queueStore.ListLeases(context.Background())
		return err == nil && len(leases) == 0
	})

	optimizerStore, err := storesqlite.Open(filepath.Join(t.TempDir(), "optimizer.sqlite"))
	if err != nil {
		t.Fatalf("Open optimizer: %v", err)
	}
	defer optimizerStore.Close()
	project := domain.Project{ID: "project-a", ContextCap: 16000}
	if err := optimizerStore.SaveProject(context.Background(), project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := optimizerStore.SavePreset(context.Background(), testPresetWithContext("small", 6000)); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := optimizerStore.SavePreset(context.Background(), testPresetWithContext("large", 16000)); err != nil {
		t.Fatalf("SavePreset large: %v", err)
	}
	for _, metric := range sustainedContextMetrics(project.ID, "", time.Unix(1, 0).UTC()) {
		if err := optimizerStore.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	startOptimizerEvaluator(ctx, optimizerStore, optimizerFleet{snap: domain.FleetSnapshot{Nodes: []domain.Node{optimizerComputeNode("peer-a")}}}, "peer-a", true, clk, time.Second, telemetrySyncConfig{})
	waitForCondition(t, func() bool {
		clk.Advance(time.Second)
		recs, err := optimizerStore.ListRecommendations(context.Background(), project.ID)
		return err == nil && len(recs) == 1
	})
	cancel()
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}

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

type failingResponseWriter struct {
	header http.Header
}

func (w failingResponseWriter) Header() http.Header {
	return w.header
}

func (failingResponseWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func (failingResponseWriter) WriteHeader(int) {}

type repeatedByteReader struct {
	b byte
}

func (r repeatedByteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
	}
	return len(p), nil
}

type fakeCustomProcessRunner struct {
	next *fakeCustomProcess
}

func (r *fakeCustomProcessRunner) Start(_ context.Context, _ string, args []string) (processadapter.ProcessHandle, error) {
	if r.next == nil {
		r.next = newFakeCustomProcess(9999)
	}
	r.next.startedArgs = append([]string(nil), args...)
	return r.next, nil
}

type fakeCustomProcess struct {
	pid          int
	waitCh       chan error
	done         bool
	exitOnSignal bool
	startedArgs  []string
}

func newFakeCustomProcess(pid int) *fakeCustomProcess {
	return &fakeCustomProcess{pid: pid, waitCh: make(chan error, 1)}
}

func (p *fakeCustomProcess) PID() int {
	return p.pid
}

func (p *fakeCustomProcess) Signal(os.Signal) error {
	if p.exitOnSignal {
		p.finish(nil)
	}
	return nil
}

func (p *fakeCustomProcess) Kill() error {
	p.finish(nil)
	return nil
}

func (p *fakeCustomProcess) Wait() error {
	return <-p.waitCh
}

func (p *fakeCustomProcess) finish(err error) {
	if p.done {
		return
	}
	p.done = true
	p.waitCh <- err
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

type recordingWorkloadRuntime struct {
	store    *workloadMemoryStore
	decision domain.PlacementDecision
	drained  []scheduler.Result
	job      domain.Job
	payload  []byte
	hooks    []scheduler.SubmitHooks
	err      error
}

type staticWorkloadProviderExecutor struct {
	response gateway.RouteResponse
	err      error
}

func (e staticWorkloadProviderExecutor) ExecutePlacedProvider(context.Context, translate.IngressRequest, domain.Job, domain.PlacementDecision, domain.ModelInstance, domain.Lease, int, bool, int) (gateway.RouteResponse, error) {
	return e.response, e.err
}

type workloadStatusOnlyStore struct {
	job domain.Job
}

func (s workloadStatusOnlyStore) Job(context.Context, string) (domain.Job, error) {
	return s.job, nil
}

func (s workloadStatusOnlyStore) SaveJob(context.Context, domain.Job) error {
	return nil
}

func (s workloadStatusOnlyStore) SaveWorkloadResultBody(context.Context, domain.WorkloadResultBody) error {
	return nil
}

func (s workloadStatusOnlyStore) WorkloadResultBody(context.Context, string) (domain.WorkloadResultBody, error) {
	return domain.WorkloadResultBody{}, fmt.Errorf("no result body")
}

func (r *recordingWorkloadRuntime) SubmitWithPayload(_ context.Context, job domain.Job, payload []byte, hooks ...scheduler.SubmitHooks) (scheduler.Result, error) {
	r.job = job
	r.payload = append([]byte(nil), payload...)
	r.hooks = append([]scheduler.SubmitHooks(nil), hooks...)
	if r.err != nil {
		return scheduler.Result{}, r.err
	}
	queueOnly := false
	for _, hook := range hooks {
		queueOnly = queueOnly || hook.QueueOnly
	}
	decision := r.decision
	if queueOnly {
		decision = domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued, Preset: decision.Preset}
	}
	if r.store != nil {
		if decision.Action != "" && decision.Action != domain.ActionQueued {
			job.Status = domain.JobRunning
		}
		job.Result = &domain.JobResult{
			Source:          "scheduler",
			Decision:        decision,
			NodeID:          decision.NodeID,
			PresetID:        decision.Preset.ID,
			Action:          decision.Action,
			PayloadBytes:    len(payload),
			PayloadStored:   len(payload) > 0,
			PayloadRedacted: len(payload) > 0,
			ResultRedacted:  true,
		}
		_ = r.store.SaveJob(context.Background(), job)
	}
	return scheduler.Result{Decision: decision}, nil
}

func (r *recordingWorkloadRuntime) Drain(_ context.Context, _ int) ([]scheduler.Result, error) {
	if r.err != nil {
		return nil, r.err
	}
	return append([]scheduler.Result(nil), r.drained...), nil
}

func (r *recordingWorkloadRuntime) FinishJob(_ context.Context, job domain.Job, lease domain.Lease, cause error) error {
	if r.err != nil {
		return r.err
	}
	if cause == nil {
		job.Status = domain.JobDone
		job.Error = ""
	} else {
		job.Status = domain.JobFailed
		job.Error = cause.Error()
	}
	if job.Result == nil {
		job.Result = &domain.JobResult{Source: "scheduler", ResultRedacted: true}
	}
	job.Result.Terminal = true
	job.Result.LeaseID = lease.ID
	job.Result.NodeID = lease.NodeID
	if r.store != nil {
		_ = r.store.SaveJob(context.Background(), job)
	}
	return nil
}

type gatedWorkloadPlacer struct {
	ready  bool
	nodeID string
	preset domain.Preset
	claim  domain.Claim
}

func (p *gatedWorkloadPlacer) Place(_ context.Context, job domain.Job, _ domain.FleetSnapshot) (domain.PlacementDecision, error) {
	decision := domain.PlacementDecision{
		JobID:            job.ID,
		Preset:           p.preset,
		Claim:            p.claim,
		SpeedPrefApplied: job.SpeedPref,
	}
	if !p.ready {
		decision.Action = domain.ActionQueued
		return decision, nil
	}
	decision.NodeID = p.nodeID
	decision.AcceleratorSet = []int{0}
	decision.Action = domain.ActionLoadedNew
	return decision, nil
}

type workloadRoute struct {
	nodeID string
	preset domain.Preset
	claim  domain.Claim
}

type scriptedWorkloadPlacer struct {
	ready  bool
	routes map[string]workloadRoute
}

func (p *scriptedWorkloadPlacer) Place(_ context.Context, job domain.Job, _ domain.FleetSnapshot) (domain.PlacementDecision, error) {
	route := p.routes[job.ID]
	decision := domain.PlacementDecision{
		JobID:            job.ID,
		Preset:           route.preset,
		Claim:            route.claim,
		SpeedPrefApplied: job.SpeedPref,
	}
	if !p.ready {
		decision.Action = domain.ActionQueued
		return decision, nil
	}
	decision.NodeID = route.nodeID
	decision.AcceleratorSet = []int{0}
	decision.Action = domain.ActionLoadedNew
	return decision, nil
}

type staticWorkloadFleet struct {
	node  domain.Node
	nodes []domain.Node
}

func (f staticWorkloadFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	if len(f.nodes) > 0 {
		return domain.FleetSnapshot{Nodes: append([]domain.Node(nil), f.nodes...)}, nil
	}
	return domain.FleetSnapshot{Nodes: []domain.Node{f.node}}, nil
}

type workloadNodeResolver struct {
	agents     map[string]ports.NodeAgent
	admissions map[string]ports.AdmissionController
}

func (r workloadNodeResolver) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent := r.agents[nodeID]
	if agent == nil {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
}

func (r workloadNodeResolver) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	admission := r.admissions[nodeID]
	if admission == nil {
		return nil, domain.ErrUnreachable
	}
	return admission, nil
}

type addressedWorkloadAgent struct {
	node domain.Node
	addr string
	next int
}

func (a *addressedWorkloadAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: a.node}, nil
}

func (a *addressedWorkloadAgent) Load(_ context.Context, req domain.LoadRequest) (domain.ModelInstance, error) {
	a.next++
	return domain.ModelInstance{
		ID:             fmt.Sprintf("inst_%d", a.next),
		PresetID:       req.Preset.ID,
		NodeID:         a.node.ID,
		AcceleratorSet: append([]int(nil), req.AcceleratorSet...),
		State:          domain.InstReady,
		Claim:          req.Claim,
		Priority:       req.Priority,
		Addr:           a.addr,
	}, nil
}

func (a *addressedWorkloadAgent) Unload(context.Context, string) error {
	return nil
}

func (a *addressedWorkloadAgent) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, nil
}

func (a *addressedWorkloadAgent) BeginRequest(context.Context, string) error {
	return nil
}

func (a *addressedWorkloadAgent) EndRequest(context.Context, string) error {
	return nil
}

type workloadMemoryStore struct {
	jobs           map[string]domain.Job
	results        map[string]domain.WorkloadResultBody
	records        []domain.JobRecord
	reconcileTasks []domain.ReconciliationTaskRecord
	incompat       []domain.IncompatibilityRecord
	presets        map[string]domain.Preset
	nodes          map[string]domain.Node
	localityPlans  []domain.LocalityPlan
	localities     []domain.ModelLocality
	engineProfiles []domain.EngineProfile
}

type failingReconciliationStore struct {
	*workloadMemoryStore
	listErr error
	saveErr error
}

func (s failingReconciliationStore) ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.workloadMemoryStore.ListReconciliationTasks(ctx, jobID)
}

func (s failingReconciliationStore) SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.workloadMemoryStore.SaveReconciliationTask(ctx, task)
}

func (s *workloadMemoryStore) SaveJob(_ context.Context, job domain.Job) error {
	if s.jobs == nil {
		s.jobs = map[string]domain.Job{}
	}
	s.jobs[job.ID] = job
	return nil
}

func (s *workloadMemoryStore) Job(_ context.Context, id string) (domain.Job, error) {
	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, fmt.Errorf("job %q not found", id)
	}
	return job, nil
}

func (s *workloadMemoryStore) SaveWorkloadResultBody(_ context.Context, result domain.WorkloadResultBody) error {
	if s.results == nil {
		s.results = map[string]domain.WorkloadResultBody{}
	}
	s.results[result.JobID] = result
	return nil
}

func (s *workloadMemoryStore) WorkloadResultBody(_ context.Context, jobID string) (domain.WorkloadResultBody, error) {
	result, ok := s.results[jobID]
	if !ok {
		return domain.WorkloadResultBody{}, fmt.Errorf("result body %q not found", jobID)
	}
	return result, nil
}

func (s *workloadMemoryStore) Snapshot(context.Context) ([]domain.JobRecord, error) {
	return append([]domain.JobRecord(nil), s.records...), nil
}

func (s *workloadMemoryStore) JobRecord(_ context.Context, id string) (domain.JobRecord, bool, error) {
	for _, rec := range s.records {
		if rec.JobID == id {
			return rec, true, nil
		}
	}
	return domain.JobRecord{}, false, nil
}

func (s *workloadMemoryStore) Put(_ context.Context, rec domain.JobRecord) error {
	for i, existing := range s.records {
		if existing.JobID == rec.JobID {
			s.records[i] = rec
			return nil
		}
	}
	s.records = append(s.records, rec)
	return nil
}

type blockingSnapshotWorkloadStore struct {
	*workloadMemoryStore
}

func (s *blockingSnapshotWorkloadStore) Snapshot(ctx context.Context) ([]domain.JobRecord, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type pointOnlyWorkloadRegistry struct {
	records []domain.JobRecord
}

func (s *pointOnlyWorkloadRegistry) JobRecord(_ context.Context, id string) (domain.JobRecord, bool, error) {
	for _, rec := range s.records {
		if rec.JobID == id {
			return rec, true, nil
		}
	}
	return domain.JobRecord{}, false, nil
}

type sqliteWorkloadStatusStoreNoSnapshot struct {
	*storesqlite.Store
}

func (s sqliteWorkloadStatusStoreNoSnapshot) Snapshot(context.Context) ([]domain.JobRecord, error) {
	return nil, errors.New("unexpected full registry snapshot")
}

type failingRescueSnapshotRegistry struct {
	err error
}

func (r failingRescueSnapshotRegistry) Put(context.Context, domain.JobRecord) error {
	return r.err
}

func (r failingRescueSnapshotRegistry) Watch(context.Context, string) (<-chan domain.JobRecord, error) {
	return nil, r.err
}

func (r failingRescueSnapshotRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	return nil, r.err
}

func (r failingRescueSnapshotRegistry) RescueSnapshot(context.Context) ([]domain.JobRecord, error) {
	return nil, r.err
}

func (s *workloadMemoryStore) SaveReconciliationTask(_ context.Context, task domain.ReconciliationTaskRecord) error {
	for i := range s.reconcileTasks {
		if s.reconcileTasks[i].ID == task.ID {
			s.reconcileTasks[i] = task
			return nil
		}
	}
	s.reconcileTasks = append(s.reconcileTasks, task)
	return nil
}

func (s *workloadMemoryStore) ListReconciliationTasks(_ context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error) {
	var out []domain.ReconciliationTaskRecord
	for _, task := range s.reconcileTasks {
		if jobID == "" || task.JobID == jobID {
			out = append(out, task)
		}
	}
	return out, nil
}

func (s *workloadMemoryStore) SaveIncompatibility(_ context.Context, rec domain.IncompatibilityRecord) error {
	for i := range s.incompat {
		if s.incompat[i].ID == rec.ID {
			s.incompat[i] = rec
			return nil
		}
	}
	s.incompat = append(s.incompat, rec)
	return nil
}

func (s *workloadMemoryStore) Preset(_ context.Context, id string) (domain.Preset, error) {
	if preset, ok := s.presets[id]; ok {
		return preset, nil
	}
	return domain.Preset{}, fmt.Errorf("preset %q not found", id)
}

func (s *workloadMemoryStore) Node(_ context.Context, id string) (domain.Node, error) {
	if node, ok := s.nodes[id]; ok {
		return node, nil
	}
	return domain.Node{}, fmt.Errorf("node %q not found", id)
}

func (s *workloadMemoryStore) SaveLocalityPlan(_ context.Context, plan domain.LocalityPlan) error {
	for i := range s.localityPlans {
		if s.localityPlans[i].ID == plan.ID {
			s.localityPlans[i] = plan
			return nil
		}
	}
	s.localityPlans = append(s.localityPlans, plan)
	return nil
}

func (s *workloadMemoryStore) SaveModelLocality(_ context.Context, locality domain.ModelLocality) error {
	for i := range s.localities {
		if s.localities[i].ID == locality.ID {
			s.localities[i] = locality
			return nil
		}
	}
	s.localities = append(s.localities, locality)
	return nil
}

func (s *workloadMemoryStore) SaveEngineProfile(_ context.Context, profile domain.EngineProfile) error {
	for i := range s.engineProfiles {
		if s.engineProfiles[i].ID == profile.ID {
			s.engineProfiles[i] = profile
			return nil
		}
	}
	s.engineProfiles = append(s.engineProfiles, profile)
	return nil
}

func (s *workloadMemoryStore) ListEngineProfiles(context.Context) ([]domain.EngineProfile, error) {
	return append([]domain.EngineProfile(nil), s.engineProfiles...), nil
}

func (s *workloadMemoryStore) MarkEngineProfileUnready(_ context.Context, profileID, reason string) error {
	for i := range s.engineProfiles {
		if s.engineProfiles[i].ID == profileID {
			s.engineProfiles[i].Ready = false
			s.engineProfiles[i].UnreadyReason = reason
			return nil
		}
	}
	return fmt.Errorf("engine profile %q not found", profileID)
}

func withWorkloadPriority(job domain.Job, priority domain.Priority) domain.Job {
	job.Priority = priority
	return job
}

func withWorkloadSpeed(job domain.Job, speed domain.SpeedPref) domain.Job {
	job.SpeedPref = speed
	return job
}

func withWorkloadPreemption(job domain.Job, preemption domain.Preemption) domain.Job {
	job.Preemption = preemption
	return job
}

func withWorkloadHandling(job domain.Job, handling domain.HandlingClass) domain.Job {
	job.Handling = handling
	return job
}

func withWorkloadContext(job domain.Job, contextRequest int) domain.Job {
	job.ContextRequest = contextRequest
	return job
}

func withWorkloadConcurrency(job domain.Job, concurrency int) domain.Job {
	job.ExpectedConcurrency = concurrency
	return job
}

func withWorkloadDeadline(job domain.Job, deadlineMS int) domain.Job {
	job.DeadlineMS = deadlineMS
	return job
}

type simpleJobLister struct {
	jobs []domain.Job
	err  error
}

func (l simpleJobLister) ListJobs(context.Context) ([]domain.Job, error) {
	if l.err != nil {
		return nil, l.err
	}
	return append([]domain.Job(nil), l.jobs...), nil
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
	if allocator.Fits(node, []int{0}, nil, domain.Claim{WeightsMB: 95}) {
		t.Fatal("combined store-backed reservations were not enforced")
	}
	if !allocator.Fits(node, []int{0}, nil, domain.Claim{WeightsMB: 90}) {
		t.Fatal("90MB should fit after combined store-backed reservations")
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

	store = peerTestRegistry(t)
	if err := store.SavePreset(ctx, testPreset("tiny")); err != nil {
		t.Fatalf("SavePreset tiny: %v", err)
	}
	if err := store.SaveReservation(ctx, domain.Reservation{ID: "pin-load-fail", Kind: domain.ReservationPinned, NodeID: node.ID, PresetID: "tiny"}); err != nil {
		t.Fatalf("SaveReservation load fail: %v", err)
	}
	backend := mocks.NewBackendAdapter()
	backend.LaunchErr = errors.New("load boom")
	agent = nodeagent.NewAgent(node, backend, mocks.NewFakeClock(time.Unix(1, 0).UTC()), nodeagent.WithAllocator(lease.NewAllocator()))
	if err := loadPinnedReservations(ctx, agent, store, node.ID); err == nil || !strings.Contains(err.Error(), "load boom") {
		t.Fatalf("load failure err = %v", err)
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
	queuedJob := domain.Job{ID: "queued", Model: "tiny", Status: domain.JobQueued}
	if err := store.SaveJob(context.Background(), queuedJob); err != nil {
		t.Fatalf("SaveJob queued: %v", err)
	}
	payload, err := peercoord.EncodeRescuePayload(queuedJob, []byte(`{"job":"queued"}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	if err := store.Put(context.Background(), domain.JobRecord{
		JobID:       queuedJob.ID,
		Coordinator: "peer-a",
		Status:      domain.JobQueued,
		Request:     payload,
		UpdatedAt:   time.Unix(2, 0).UTC(),
	}); err != nil {
		t.Fatalf("Put queued record: %v", err)
	}
	if err := store.SaveJob(context.Background(), domain.Job{ID: "done", Model: "tiny", Status: domain.JobDone}); err != nil {
		t.Fatalf("SaveJob done: %v", err)
	}
	queue := scheduler.NewQueue(mocks.NewFakeClock(time.Unix(1, 0).UTC()))
	if err := restoreQueuedJobs(context.Background(), store, queue); err != nil {
		t.Fatalf("restoreQueuedJobs: %v", err)
	}
	if queue.Len() != 1 {
		t.Fatalf("queue len = %d", queue.Len())
	}
	job, gotPayload, ok := queue.DequeueWithPayload()
	if !ok || job.ID != "queued" || string(gotPayload) != `{"job":"queued"}` {
		t.Fatalf("dequeue = %+v payload=%s %v", job, gotPayload, ok)
	}
}

func TestRestoreQueuedJobsWithoutRegistrySnapshot(t *testing.T) {
	queue := scheduler.NewQueue(mocks.NewFakeClock(time.Unix(1, 0).UTC()))
	store := simpleJobLister{jobs: []domain.Job{{ID: "queued", Model: "tiny", Status: domain.JobQueued}}}
	if err := restoreQueuedJobs(context.Background(), store, queue); err != nil {
		t.Fatalf("restoreQueuedJobs: %v", err)
	}
	job, payload, ok := queue.DequeueWithPayload()
	if !ok || job.ID != "queued" || len(payload) != 0 {
		t.Fatalf("dequeue = %+v payload=%s ok=%v", job, payload, ok)
	}
}

func TestRestoreQueuedJobsSkipsPrivatePayloads(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	queuedJob := domain.Job{ID: "queued-private", Model: "tiny", Status: domain.JobQueued, Handling: domain.HandlingPrivate}
	if err := store.SaveJob(context.Background(), queuedJob); err != nil {
		t.Fatalf("SaveJob queued: %v", err)
	}
	if err := store.Put(context.Background(), domain.JobRecord{
		JobID:       queuedJob.ID,
		Coordinator: "peer-a",
		Status:      domain.JobQueued,
		Request:     []byte(`{"encrypted":"aes-256-gcm"}`),
		Handling:    domain.HandlingPrivate,
		UpdatedAt:   time.Unix(2, 0).UTC(),
	}); err != nil {
		t.Fatalf("Put queued record: %v", err)
	}
	queue := scheduler.NewQueue(mocks.NewFakeClock(time.Unix(1, 0).UTC()))
	if err := restoreQueuedJobs(context.Background(), store, queue); err != nil {
		t.Fatalf("restoreQueuedJobs: %v", err)
	}
	job, payload, ok := queue.DequeueWithPayload()
	if !ok || job.ID != queuedJob.ID || len(payload) != 0 {
		t.Fatalf("dequeue = %+v payload=%s ok=%v", job, payload, ok)
	}
}

func TestRestoreQueuedJobsRejectsMalformedPublicPayload(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	queuedJob := domain.Job{ID: "queued-bad", Model: "tiny", Status: domain.JobQueued}
	if err := store.SaveJob(context.Background(), queuedJob); err != nil {
		t.Fatalf("SaveJob queued: %v", err)
	}
	if err := store.Put(context.Background(), domain.JobRecord{
		JobID:       queuedJob.ID,
		Coordinator: "peer-a",
		Status:      domain.JobQueued,
		Request:     []byte(`{`),
		UpdatedAt:   time.Unix(2, 0).UTC(),
	}); err != nil {
		t.Fatalf("Put queued record: %v", err)
	}
	queue := scheduler.NewQueue(mocks.NewFakeClock(time.Unix(1, 0).UTC()))
	if err := restoreQueuedJobs(context.Background(), store, queue); err == nil || !strings.Contains(err.Error(), "decode queued rescue payload") {
		t.Fatalf("restoreQueuedJobs err = %v", err)
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
	node := domain.Node{
		ID:           "node-a",
		Name:         "Node A",
		Status:       domain.NodeReady,
		MaxUtil:      1,
		Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 24576}},
	}
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
	for _, metric := range sustainedContextMetrics(project.ID, node.ID, now) {
		metric.TokensPerSec = 15
		if err := store.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	if _, err := runOptimizerEvaluation(context.Background(), store, mocks.NewFakeClock(now), telemetrySyncConfig{}); err != nil {
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
	if len(recs) != 1 || recs[0].Observed["avg_tokens"] != 3600 {
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
			remotePeer.ID: sustainedContextMetrics(project.ID, remotePeer.ID, time.Unix(30, 0).UTC()),
		},
		SamplesByPeer: map[string][]domain.SessionMetric{
			remotePeer.ID: []domain.SessionMetric{
				{SessionID: "session-remote", Sequence: 1, JobID: "remote-a", Phase: domain.TelemetryPhaseComplete, NodeID: remotePeer.ID, Project: project.ID, ContextUsed: 3500, At: time.Unix(30, 0).UTC()},
			},
		},
	}

	result, err := runOptimizerEvaluation(ctx, store, mocks.NewFakeClock(time.Unix(40, 0).UTC()), telemetrySyncConfig{
		SelfID: "peer-a",
		Peers:  &mocks.PeerDiscovery{PeersVal: []domain.Peer{remotePeer}},
		Client: client,
	})
	if err != nil {
		t.Fatalf("runOptimizerEvaluation: %v", err)
	}
	if result.ImportedMetrics != 25 || result.ImportedSamples != 1 || result.PushedRecommendations != 1 || len(result.SkippedPeers) != 0 {
		t.Fatalf("sync result = %+v", result)
	}
	samples, err := store.Samples(ctx, domain.SessionMetricQuery{SessionID: "session-remote"})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if len(samples) != 1 || samples[0].NodeID != remotePeer.ID {
		t.Fatalf("imported samples = %+v", samples)
	}
	if result.SlotID == "" {
		t.Fatalf("missing slot id: %+v", result)
	}
	recs, err := store.ListRecommendations(ctx, project.ID)
	if err != nil {
		t.Fatalf("ListRecommendations: %v", err)
	}
	if len(recs) != 1 || recs[0].Observed["avg_tokens"] != 3600 {
		t.Fatalf("recommendations = %+v", recs)
	}
	appliedProject, err := store.Project(ctx, project.ID)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if appliedProject.ContextCap != project.ContextCap {
		t.Fatalf("auto_apply=false project changed: %+v", appliedProject)
	}
	if pushed := client.PushedRecommendations[remotePeer.ID]; len(pushed) != 1 || pushed[0].ProjectID != project.ID || pushed[0].SlotID != result.SlotID {
		t.Fatalf("pushed recommendations = %+v", pushed)
	}
}

func TestRunOptimizerEvaluationSkipsLocalAnalysisWhenSlotAlreadyExists(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	project := domain.Project{ID: "project-a", ContextCap: 16000}
	if err := store.SaveProject(ctx, project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SavePreset(ctx, testPresetWithContext("small", 6000)); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := store.SavePreset(ctx, testPresetWithContext("large", 16000)); err != nil {
		t.Fatalf("SavePreset large: %v", err)
	}
	now := time.Unix(45, 0).UTC()
	interval := time.Minute
	slotID := telemetry.AnalysisSlotID(now, interval)
	remotePeer := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	remoteRec := domain.RecommendationRecord{
		ID:               "remote-slot-rec",
		SlotID:           slotID,
		Type:             optimizer.RecommendationContextCap,
		ProjectID:        project.ID,
		RecommendedValue: 6000,
		CreatedAt:        now,
	}
	client := &mocks.TelemetryPeerClient{
		RecommendationsByPeer: map[string][]domain.RecommendationRecord{remotePeer.ID: {remoteRec}},
	}

	result, err := runOptimizerEvaluation(ctx, store, mocks.NewFakeClock(now), telemetrySyncConfig{
		SelfID:   "peer-a",
		Peers:    &mocks.PeerDiscovery{PeersVal: []domain.Peer{remotePeer}},
		Client:   client,
		Interval: interval,
	})
	if err != nil {
		t.Fatalf("runOptimizerEvaluation: %v", err)
	}
	if result.ImportedRecommendations != 1 || result.PushedRecommendations != 1 {
		t.Fatalf("sync result = %+v", result)
	}
	recs, err := store.ListRecommendations(ctx, project.ID)
	if err != nil {
		t.Fatalf("ListRecommendations: %v", err)
	}
	if len(recs) != 1 || recs[0].ID != remoteRec.ID || recs[0].SlotID != slotID {
		t.Fatalf("recommendations = %+v", recs)
	}
	if pushed := client.PushedRecommendations[remotePeer.ID]; len(pushed) != 1 || pushed[0].ID != remoteRec.ID {
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
	result, err := runOptimizerEvaluation(ctx, store, mocks.NewFakeClock(time.Unix(50, 0).UTC()), telemetrySyncConfig{
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

func TestOptimizerSelectionAndTelemetrySyncErrors(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(0, 0).UTC())
	if _, err := shouldRunGroupOptimizer(ctx, nil, "peer-a", clk, time.Minute); err == nil {
		t.Fatal("nil fleet accepted")
	}
	if _, err := shouldRunGroupOptimizer(ctx, optimizerFleet{}, "", clk, time.Minute); err == nil {
		t.Fatal("empty self id accepted")
	}
	if _, err := shouldRunGroupOptimizer(ctx, optimizerFleet{}, "peer-a", nil, time.Minute); err == nil {
		t.Fatal("nil clock accepted")
	}
	boom := errors.New("fleet boom")
	if _, err := shouldRunGroupOptimizer(ctx, optimizerFleet{err: boom}, "peer-a", clk, time.Minute); !errors.Is(err, boom) {
		t.Fatalf("fleet err = %v", err)
	}

	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if _, err := runOptimizerEvaluation(ctx, nil, clk, telemetrySyncConfig{}); err == nil {
		t.Fatal("nil optimizer store accepted")
	}
	if _, err := runOptimizerEvaluation(ctx, store, nil, telemetrySyncConfig{}); err == nil {
		t.Fatal("nil optimizer clock accepted")
	}
	peerErr := errors.New("peers boom")
	if _, err := runOptimizerEvaluation(ctx, store, clk, telemetrySyncConfig{Peers: &mocks.PeerDiscovery{Err: peerErr}, Client: &mocks.TelemetryPeerClient{}}); !errors.Is(err, peerErr) {
		t.Fatalf("peer err = %v", err)
	}

	project := domain.Project{ID: "project-a", ContextCap: 16000}
	if err := store.SaveProject(ctx, project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SavePreset(ctx, testPresetWithContext("small", 6000)); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := store.SaveRecommendation(ctx, domain.RecommendationRecord{ID: "remote-rec", SlotID: "slot-a", Type: optimizer.RecommendationContextCap, ProjectID: project.ID, CreatedAt: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatalf("SaveRecommendation: %v", err)
	}
	client := &mocks.TelemetryPeerClient{
		RecommendationsByPeer:  map[string][]domain.RecommendationRecord{"peer-b": {{ID: "rec-b", ProjectID: project.ID, Type: optimizer.RecommendationContextCap, CreatedAt: time.Unix(2, 0).UTC()}}},
		PushRecommendationsErr: errors.New("push boom"),
	}
	result, reachable, err := pullFleetTelemetry(ctx, store, telemetrySyncConfig{
		SelfID: "peer-a",
		Peers: &mocks.PeerDiscovery{PeersVal: []domain.Peer{
			{},
			{ID: "peer-a"},
			{ID: "peer-b", Compute: true},
		}},
		Client: client,
	})
	if err != nil {
		t.Fatalf("pullFleetTelemetry: %v", err)
	}
	if len(reachable) != 1 || result.ImportedRecommendations != 1 {
		t.Fatalf("pull result=%+v reachable=%+v", result, reachable)
	}
	if err := pushFleetRecommendations(ctx, store, telemetrySyncConfig{Client: client}, reachable, "slot-a", &result); err != nil {
		t.Fatalf("pushFleetRecommendations: %v", err)
	}
	if len(result.SkippedPeers) != 1 || !strings.Contains(result.SkippedPeers[0], "push boom") {
		t.Fatalf("push skipped = %+v", result.SkippedPeers)
	}
	filtered := recommendationsForSlot([]domain.RecommendationRecord{
		{ID: "rec-a", SlotID: "slot-a"},
		{ID: "rec-a", SlotID: "slot-a"},
		{ID: "rec-b", SlotID: "slot-b"},
		{ID: "legacy"},
	}, "slot-a")
	if len(filtered) != 1 || filtered[0].ID != "rec-a" {
		t.Fatalf("filtered recommendations = %+v", filtered)
	}
}

func TestShouldRunGroupOptimizerSelectsOneReadyComputePeer(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(0, 0).UTC())
	fleet := optimizerFleet{snap: domain.FleetSnapshot{Nodes: []domain.Node{
		optimizerComputeNode("node-c"),
		optimizerComputeNode("node-a"),
		{ID: "node-b", Status: domain.NodeUnreachable},
		{ID: "thin-peer", Status: domain.NodeReady},
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
	defaultEstimator, ok := peerEstimator(PeerConfig{}, nil, nil).(*estimate.BackendAware)
	if !ok {
		t.Fatal("default estimator should be backend-aware")
	}
	if _, ok := defaultEstimator.LlamaCpp.(*estimate.GGUFEstimator); !ok {
		t.Fatalf("default estimator should use GGUF estimator boundary for llama.cpp: %+v", defaultEstimator)
	}
	model := filepath.Join(t.TempDir(), "model.gguf")
	if err := os.WriteFile(model, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	_, err := defaultEstimator.Estimate(context.Background(), domain.Preset{ID: "local", ModelRef: model, Backend: domain.BackendLlamaCpp}, 10, 1)
	if err == nil || !strings.Contains(err.Error(), "gguf parser") {
		t.Fatalf("default estimator err = %v", err)
	}
	configured, ok := peerEstimator(PeerConfig{GGUFParser: writeMetadataParser(t)}, nil, nil).(*estimate.BackendAware)
	if !ok {
		t.Fatal("configured estimator should be backend-aware")
	}
	if _, ok := configured.LlamaCpp.(*estimate.GGUFEstimator); !ok {
		t.Fatal("configured gguf parser should use GGUF estimator")
	}
	claim, err := configured.Estimate(context.Background(), domain.Preset{ID: "local", ModelRef: model, Backend: domain.BackendLlamaCpp}, 10, 1)
	if err != nil || claim.WeightsMB != 10 {
		t.Fatalf("configured estimate claim=%+v err=%v", claim, err)
	}
}

func writeMetadataParser(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gguf-parser")
	script := `#!/bin/sh
cat <<'JSON'
{"format":"gguf","weights_mb":10,"kv_per_token_mb":0.5,"context_length":2048,"layer_count":12}
JSON
`
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatalf("write parser: %v", err)
	}
	return path
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

func TestRunPeerReportsStartupErrors(t *testing.T) {
	if err := runPeer(context.Background(), []string{"--config", filepath.Join(t.TempDir(), "missing.json")}); err == nil || !strings.Contains(err.Error(), "open") {
		t.Fatalf("missing config err = %v", err)
	}
	configPath := writePeerConfig(t, PeerConfig{
		Listen:    "127.0.0.1:-1",
		StorePath: filepath.Join(t.TempDir(), "control.db"),
		Compute:   true,
		ComputeConfig: ComputeConfig{
			ID:            "peer-a",
			BackendListen: "127.0.0.1:51848",
			LlamaServer:   "/bin/echo",
		},
		Presets: []domain.Preset{testPreset("tiny")},
	})
	if err := runPeer(context.Background(), []string{"--config", configPath}); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("bad listen err = %v", err)
	}
}

func TestRunPeerServesUntilContextCanceled(t *testing.T) {
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
		},
		Presets: []domain.Preset{testPreset("tiny")},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runPeer(ctx, []string{"--config", configPath})
	}()
	waitForCondition(t, func() bool {
		return rawNodeExists(t, dbPath, "peer-a")
	})
	cancel()
	stopped := false
	for i := 0; i < 10000; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("runPeer: %v", err)
			}
			stopped = true
		default:
			runtime.Gosched()
		}
		if stopped {
			break
		}
	}
	if !stopped {
		t.Fatal("runPeer did not stop after context cancellation")
	}
}

func rawNodeExists(t *testing.T, dbPath, nodeID string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout%3d5000")
	if err != nil {
		return false
	}
	defer db.Close()
	var raw string
	return db.QueryRowContext(context.Background(), `SELECT data FROM nodes WHERE id = ?`, nodeID).Scan(&raw) == nil
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
	client := nodeagent.NewHTTPClient("http://local-node.test")
	client.Client = directHTTPClient(handler)
	job := domain.Job{ID: "job-a", Priority: domain.PriorityInteractive}
	preset := domain.Preset{ID: "preset-a", ArtifactSizeMB: 1, EstWeightsMB: 1}
	offer, err := client.Offer(context.Background(), domain.AdmissionRequest{Job: job, Preset: preset, Claim: domain.Claim{WeightsMB: 1}})
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

func TestBuildPeerGatewayAppliesFlagOverridesAndJoinURI(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	configPath := writePeerConfig(t, PeerConfig{
		Listen:    "127.0.0.1:1111",
		StorePath: dbPath,
		Presets:   []domain.Preset{testPreset("tiny")},
	})
	addr, handler, cleanup, err := buildPeerGateway(context.Background(), []string{
		"--config", configPath,
		"--join", "mycjoin://127.0.0.1:1?token=join-secret",
		"--rpc-token", "override-rpc",
		"--listen", "127.0.0.1:0",
		"--discovery-listen", "127.0.0.1:0",
		"--discovery-addr", "127.0.0.1:9",
		"--compute",
		"--backend-listen", "127.0.0.1:51848",
		"--id", "peer-override",
		"--name", "Override Peer",
		"--backend", string(domain.BackendLlamaCpp),
		"--backend-binary", "/bin/echo",
		"--llama-server", "/bin/false",
		"--gguf-parser", "/bin/echo",
		"--max-util", "0.5",
		"--disk-min-free-ratio", "0.30",
	})
	if err != nil {
		t.Fatalf("buildPeerGateway: %v", err)
	}
	if cleanup != nil {
		defer func() { _ = cleanup(context.Background()) }()
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/peer/health", nil)
	req.Header.Set("X-Myc-Join-Token", "join-secret")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("peer health = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/snapshot", nil)
	req.Header.Set("Authorization", "Bearer override-rpc")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %q", rec.Code, rec.Body.String())
	}
	var snap domain.NodeSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("snapshot json: %v", err)
	}
	if snap.Node.ID != "peer-override" || snap.Node.Name != "Override Peer" || snap.Node.MaxUtil != 0.5 || snap.Node.DiskMinFreeRatio != 0.30 || snap.Node.DiskTotalMB <= 0 {
		t.Fatalf("snapshot = %+v", snap.Node)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read persisted config: %v", err)
	}
	var persisted PeerConfig
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("decode persisted config: %v", err)
	}
	if persisted.JoinToken != "join-secret" || persisted.RPCToken != "override-rpc" || len(persisted.SeedPeers) != 1 || persisted.SeedPeers[0] != "127.0.0.1:1" {
		t.Fatalf("persisted join fields = %+v", persisted)
	}
	if persisted.Listen != "127.0.0.1:1111" || persisted.Compute {
		t.Fatalf("one-shot flags leaked into persisted config = %+v", persisted)
	}
}

func TestBuildPeerGatewayComputeFlagCanDisableConfiguredRuntime(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	configPath := writePeerConfig(t, PeerConfig{
		ID:        "peer-a",
		Listen:    "127.0.0.1:0",
		StorePath: dbPath,
		JoinToken: "join-secret",
		RPCToken:  "rpc-secret",
		Compute:   true,
		ComputeConfig: ComputeConfig{
			ID:            "peer-a",
			Name:          "Peer A",
			BackendListen: "127.0.0.1:51848",
			LlamaServer:   "/bin/echo",
		},
		Presets: []domain.Preset{testPreset("tiny")},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, cleanup, err := buildPeerGateway(ctx, []string{
		"--config", configPath,
		"--compute=off",
		"--discovery-listen", "127.0.0.1:0",
		"--discovery-addr", "127.0.0.1:9",
	})
	if err != nil {
		t.Fatalf("buildPeerGateway: %v", err)
	}
	if cleanup != nil {
		defer func() { _ = cleanup(context.Background()) }()
	}
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	nodes, err := store.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("compute-off registered local nodes = %+v", nodes)
	}
	if _, _, _, err := buildPeerGateway(context.Background(), []string{"--config", configPath, "--compute=maybe"}); err == nil || !strings.Contains(err.Error(), "compute must be on or off") {
		t.Fatalf("bad compute flag err = %v", err)
	}
}

func TestNewLocalPeerAgentRequiresAdmissionExtensions(t *testing.T) {
	agent := mocks.NewNodeAgent(domain.Node{ID: "peer-a"})
	reader := staticJobReader{jobs: map[string]domain.Job{"job-a": {ID: "job-a", Status: domain.JobDone}}}
	if got, err := newLocalPeerAgent(agent, &mocks.AdmissionController{}, reader); err != nil || got.LeaseBinder == nil || got.LeaseInspector == nil {
		t.Fatalf("newLocalPeerAgent = %+v err=%v", got, err)
	} else if status, found, err := got.JobStatus(context.Background(), "job-a"); err != nil || !found || status != domain.JobDone {
		t.Fatalf("JobStatus = %q found=%v err=%v", status, found, err)
	}
	if got, err := newLocalPeerAgent(agent, &mocks.AdmissionController{}); err != nil {
		t.Fatalf("newLocalPeerAgent no reader err=%v", err)
	} else if status, found, err := got.JobStatus(context.Background(), "job-a"); err != nil || found || status != "" {
		t.Fatalf("empty JobStatus = %q found=%v err=%v", status, found, err)
	}
	if _, err := newLocalPeerAgent(agent, localAdmissionOnly{}); err == nil || !strings.Contains(err.Error(), "lease inspection") {
		t.Fatalf("missing extension err = %v", err)
	}
}

type staticJobReader struct {
	jobs map[string]domain.Job
	err  error
}

func (r staticJobReader) Job(_ context.Context, id string) (domain.Job, error) {
	if r.err != nil {
		return domain.Job{}, r.err
	}
	job, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, fmt.Errorf("job %q not found", id)
	}
	return job, nil
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

func TestBuildComputeRuntimeAdvertisesMultipleBackends(t *testing.T) {
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
			BackendListen: "127.0.0.1:0",
			Backends: []BackendRuntimeConfig{
				{Backend: domain.BackendVLLM, BackendBinary: "/bin/echo", CustomArgs: []string{"--gpu-memory-utilization", "0.70"}},
				{Backend: domain.BackendLlamaCpp, LlamaServer: "/bin/echo"},
			},
		}),
	}, store)
	if err != nil {
		t.Fatalf("buildComputeRuntime: %v", err)
	}
	if runtime.node.Labels[domain.LabelPeerBackends] != "llamacpp,vllm" || runtime.node.Labels[LabelPeerBackend] != "" {
		t.Fatalf("node labels = %+v", runtime.node.Labels)
	}
}

func TestBuildComputeRuntimeWiresParserAndClosedStoreErrors(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	runtime, err := buildComputeRuntime(ctx, PeerConfig{
		Listen: "127.0.0.1:0",
		ComputeConfig: defaultedComputeConfig(ComputeConfig{
			ID:            "peer-a",
			Name:          "Peer A",
			Backend:       domain.BackendLlamaCpp,
			BackendListen: "127.0.0.1:51848",
			LlamaServer:   "/bin/echo",
			GGUFParser:    "/bin/echo",
		}),
	}, store)
	if err != nil {
		t.Fatalf("buildComputeRuntime: %v", err)
	}
	if runtime.node.Labels[LabelPeerBackend] != string(domain.BackendLlamaCpp) || runtime.shutdown == nil {
		t.Fatalf("runtime = %+v", runtime.node)
	}

	closed, err := storesqlite.Open(filepath.Join(t.TempDir(), "closed.sqlite"))
	if err != nil {
		t.Fatalf("Open closed: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("Close closed: %v", err)
	}
	if _, err := buildComputeRuntime(ctx, PeerConfig{ComputeConfig: defaultedComputeConfig(ComputeConfig{ID: "peer-a", LlamaServer: "/bin/echo"})}, closed); err == nil {
		t.Fatal("closed store build succeeded")
	}
}

func TestComputeBackendAdapterLaunchesCustomProcessWithRenderedArgs(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	registry := nodeagent.StoreProcessRegistry{Store: store, NodeID: "peer-a"}
	process := newFakeCustomProcess(1234)
	process.exitOnSignal = true
	runner := &fakeCustomProcessRunner{next: process}
	adapter, err := computeBackendAdapterWithProcessRunner(ComputeConfig{
		Backend:       domain.BackendCustom,
		BackendBinary: "custom-backend",
		CustomArgs: []string{
			"{model}|{preset}|{host}|{port}|{addr}",
			"base={preset}:{port}",
		},
		StopGraceMS: 25,
	}, registry, runner)
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

	wantArgs := []string{
		"model.gguf|custom-preset|127.0.0.1|54321|127.0.0.1:54321",
		"base=custom-preset:54321",
		"launch=custom-preset:127.0.0.1:54321",
	}
	if !reflect.DeepEqual(process.startedArgs, wantArgs) {
		t.Fatalf("custom args = %+v want %+v", process.startedArgs, wantArgs)
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

func TestComputeBackendAdapterAppendsLlamaCustomArgsAfterDefaults(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	adapter, err := computeBackendAdapter(ComputeConfig{
		Backend:     domain.BackendLlamaCpp,
		LlamaServer: "/bin/echo",
		CustomArgs:  []string{"--n-gpu-layers", "99"},
	}, nodeagent.StoreProcessRegistry{Store: store, NodeID: "peer-a"})
	if err != nil {
		t.Fatalf("computeBackendAdapter: %v", err)
	}
	preset := testPreset("preset-a")
	preset.ModelRef = "model.gguf"
	preset.LaunchArgs = []string{"--tensor-split", "1,1"}
	handle, err := adapter.Launch(ctx, preset, "127.0.0.1:54321")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	defer func() { _ = adapter.Stop(context.Background(), handle) }()
	want := []string{
		"--host", "127.0.0.1",
		"--port", "54321",
		"-m", "model.gguf",
		"-c", "2048",
		"--parallel", "1",
		"--n-gpu-layers", "99",
		"--tensor-split", "1,1",
	}
	if !reflect.DeepEqual(handle.Args, want) {
		t.Fatalf("llama args = %+v want %+v", handle.Args, want)
	}
}

func TestComputeBackendAdapterPassesConfiguredArgsToProcessBackends(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		backend domain.Backend
		base    []string
		custom  []string
	}{
		{backend: domain.BackendVLLM, base: []string{"serve", "model.gguf", "--host", "127.0.0.1", "--port", "54321"}, custom: []string{"--gpu-memory-utilization", "0.85"}},
		{backend: domain.BackendMLX, base: []string{"--model", "model.gguf", "--host", "127.0.0.1", "--port", "54321"}, custom: []string{"--trust-remote-code"}},
		{backend: domain.BackendOpenVINO, base: []string{"--model", "model.gguf", "--host", "127.0.0.1", "--port", "54321"}, custom: []string{"--device", "GPU"}},
	} {
		t.Run(string(tt.backend), func(t *testing.T) {
			store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer store.Close()
			process := newFakeCustomProcess(2234)
			process.exitOnSignal = true
			runner := &fakeCustomProcessRunner{next: process}
			adapter, err := computeBackendAdapterWithProcessRunner(ComputeConfig{
				Backend:       tt.backend,
				BackendBinary: "backend",
				CustomArgs:    append([]string(nil), tt.custom...),
			}, nodeagent.StoreProcessRegistry{Store: store, NodeID: "peer-a"}, runner)
			if err != nil {
				t.Fatalf("computeBackendAdapter: %v", err)
			}
			preset := testPreset("preset-a")
			preset.ModelRef = "model.gguf"
			preset.LaunchArgs = []string{"--served-model-name", "{preset}"}
			handle, err := adapter.Launch(ctx, preset, "127.0.0.1:54321")
			if err != nil {
				t.Fatalf("Launch: %v", err)
			}
			defer func() { _ = adapter.Stop(context.Background(), handle) }()
			want := append(append([]string(nil), tt.base...), tt.custom...)
			want = append(want, "--served-model-name", "preset-a")
			if tt.backend == domain.BackendVLLM {
				want = append(append([]string(nil), tt.base...), "--served-model-name", "preset-a")
				want = append(want, tt.custom...)
			}
			if !reflect.DeepEqual(process.startedArgs, want) {
				t.Fatalf("args = %+v want %+v", process.startedArgs, want)
			}
		})
	}
}

func TestComputeBackendAdapterRoutesByPresetBackend(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	process := newFakeCustomProcess(3234)
	process.exitOnSignal = true
	runner := &fakeCustomProcessRunner{next: process}
	adapter, err := computeBackendAdapterWithProcessRunner(ComputeConfig{
		Backends: []BackendRuntimeConfig{
			{Backend: domain.BackendVLLM, BackendBinary: "vllm-bin"},
			{Backend: domain.BackendMLX, BackendBinary: "mlx-bin"},
		},
	}, nodeagent.StoreProcessRegistry{Store: store, NodeID: "peer-a"}, runner)
	if err != nil {
		t.Fatalf("computeBackendAdapter: %v", err)
	}
	if adapter.Name() != "multi" {
		t.Fatalf("adapter name = %s", adapter.Name())
	}
	preset := testPreset("preset-vllm")
	preset.Backend = domain.BackendVLLM
	preset.ModelRef = "model.gguf"
	preset.LaunchArgs = []string{"--served-model-name", "{preset}"}
	handle, err := adapter.Launch(ctx, preset, "127.0.0.1:54321")
	if err != nil {
		t.Fatalf("Launch vllm: %v", err)
	}
	if handle.Kind != string(domain.BackendVLLM) {
		t.Fatalf("handle = %+v", handle)
	}
	want := []string{"serve", "model.gguf", "--host", "127.0.0.1", "--port", "54321", "--served-model-name", "preset-vllm"}
	if !reflect.DeepEqual(process.startedArgs, want) {
		t.Fatalf("vllm args = %+v want %+v", process.startedArgs, want)
	}
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatalf("Stop vllm: %v", err)
	}

	process = newFakeCustomProcess(3235)
	process.exitOnSignal = true
	runner.next = process
	preset = testPreset("preset-mlx")
	preset.Backend = domain.BackendMLX
	preset.ModelRef = "mlx-model"
	handle, err = adapter.Launch(ctx, preset, "127.0.0.1:54322")
	if err != nil {
		t.Fatalf("Launch mlx: %v", err)
	}
	if handle.Kind != string(domain.BackendMLX) {
		t.Fatalf("handle = %+v", handle)
	}
	want = []string{"--model", "mlx-model", "--host", "127.0.0.1", "--port", "54322"}
	if !reflect.DeepEqual(process.startedArgs, want) {
		t.Fatalf("mlx args = %+v want %+v", process.startedArgs, want)
	}
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatalf("Stop mlx: %v", err)
	}
}

func TestBackendRouterRoutesReadinessDynamicAndStopByRuntimeIdentity(t *testing.T) {
	ctx := context.Background()
	vllm := &routerDynamicAdapter{name: "vllm", backend: domain.BackendVLLM}
	mlx := &routerDynamicAdapter{name: "mlx", backend: domain.BackendMLX}
	adapter := newBackendRouter(map[domain.Backend]ports.BackendAdapter{
		domain.BackendVLLM: vllm,
		domain.BackendMLX:  mlx,
	})
	if adapter.Name() != "multi" {
		t.Fatalf("router name = %s", adapter.Name())
	}
	preset := testPreset("preset-vllm")
	preset.Backend = domain.BackendVLLM
	preset.ModelRef = "model-a"
	handle, err := adapter.Launch(ctx, preset, "127.0.0.1:5555")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if handle.Kind != string(domain.BackendVLLM) || len(vllm.launches) != 1 || len(mlx.launches) != 0 {
		t.Fatalf("handle=%+v vllm=%+v mlx=%+v", handle, vllm.launches, mlx.launches)
	}
	if err := adapter.WaitReady(ctx, handle.Addr); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	unknownKind := handle
	unknownKind.Kind = ""
	if err := adapter.Stop(ctx, unknownKind); err != nil {
		t.Fatalf("Stop by remembered PID: %v", err)
	}
	if len(vllm.stops) != 1 {
		t.Fatalf("stops = %+v", vllm.stops)
	}
	if err := adapter.WaitReady(ctx, handle.Addr); err == nil || !strings.Contains(err.Error(), "no backend is tracking") {
		t.Fatalf("forgotten ready addr err = %v", err)
	}

	preset = testPreset("preset-mlx")
	preset.Backend = domain.BackendMLX
	preset.ModelRef = "model-b"
	dynamic, err := adapter.(ports.DynamicBackendAdapter).LaunchDynamic(ctx, preset, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("LaunchDynamic: %v", err)
	}
	if dynamic.Kind != string(domain.BackendMLX) || !strings.Contains(dynamic.Addr, ":6") || len(mlx.dynamicLaunches) != 1 {
		t.Fatalf("dynamic=%+v dynamic launches=%+v", dynamic, mlx.dynamicLaunches)
	}
	if err := adapter.WaitReady(ctx, dynamic.Addr); err != nil {
		t.Fatalf("WaitReady dynamic: %v", err)
	}
}

func TestBackendRouterFailsLoudForAmbiguousOrUnsupportedRuntime(t *testing.T) {
	ctx := context.Background()
	single := newBackendRouter(map[domain.Backend]ports.BackendAdapter{
		domain.BackendVLLM: &routerDynamicAdapter{name: "vllm", backend: domain.BackendVLLM},
	})
	preset := testPreset("single-default")
	preset.Backend = ""
	preset.ModelRef = "model-a"
	if _, err := single.Launch(ctx, preset, "127.0.0.1:5555"); err != nil {
		t.Fatalf("single backend default launch: %v", err)
	}

	multi := newBackendRouter(map[domain.Backend]ports.BackendAdapter{
		domain.BackendVLLM: &routerDynamicAdapter{name: "vllm", backend: domain.BackendVLLM},
		domain.BackendMLX:  &routerStaticAdapter{name: "mlx", backend: domain.BackendMLX},
	})
	missingBackend := testPreset("missing-backend")
	missingBackend.Backend = ""
	if _, err := multi.Launch(ctx, missingBackend, "127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "requires a backend") {
		t.Fatalf("missing backend err = %v", err)
	}
	unknown := testPreset("unknown-backend")
	unknown.Backend = domain.BackendOpenVINO
	if _, err := multi.Launch(ctx, unknown, "127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unknown backend err = %v", err)
	}
	static := testPreset("static-mlx")
	static.Backend = domain.BackendMLX
	if _, err := multi.(ports.DynamicBackendAdapter).LaunchDynamic(ctx, static, "127.0.0.1:0"); err == nil || !strings.Contains(err.Error(), "does not support dynamic") {
		t.Fatalf("static dynamic err = %v", err)
	}
	if err := multi.WaitReady(ctx, "127.0.0.1:9"); err == nil || !strings.Contains(err.Error(), "no backend is tracking") {
		t.Fatalf("unknown ready err = %v", err)
	}
	if err := multi.Stop(ctx, ports.Handle{}); err == nil || !strings.Contains(err.Error(), "no runtime identity") {
		t.Fatalf("identity err = %v", err)
	}

	router := multi.(*backendRouter)
	router.remember(ports.Handle{PID: 42, Addr: "127.0.0.1:42"}, domain.BackendOpenVINO)
	if err := multi.WaitReady(ctx, "127.0.0.1:42"); err == nil || !strings.Contains(err.Error(), "no longer configured") {
		t.Fatalf("removed ready backend err = %v", err)
	}
	if err := multi.Stop(ctx, ports.Handle{PID: 42}); err == nil || !strings.Contains(err.Error(), "not configured for handle") {
		t.Fatalf("removed stop backend err = %v", err)
	}
}

func TestDecodePeerRPCResponseStrictRejectsTrailingJSON(t *testing.T) {
	var out map[string]string
	if err := decodePeerRPCResponseStrict(strings.NewReader(`{"ok":"yes"}`), &out); err != nil {
		t.Fatalf("strict response: %v", err)
	}
	if out["ok"] != "yes" {
		t.Fatalf("decoded response = %+v", out)
	}
	if err := decodePeerRPCResponseStrict(strings.NewReader(`{"ok":"yes"} {"extra":"no"}`), &out); err == nil || !strings.Contains(err.Error(), "multiple JSON values") {
		t.Fatalf("trailing JSON err = %v", err)
	}
	if err := decodePeerRPCResponseStrict(strings.NewReader(`{`), &out); err == nil {
		t.Fatal("invalid JSON response accepted")
	}
}

func TestComputeBackendAdapterRequiresCustomBinary(t *testing.T) {
	if _, err := computeBackendAdapter(ComputeConfig{Backend: domain.BackendCustom}, nodeagent.StoreProcessRegistry{}); err == nil || !strings.Contains(err.Error(), "custom compute backend binary") {
		t.Fatalf("missing custom binary err = %v", err)
	}
}

func TestBlockedModelCandidateReportsBackendCompatibilityRows(t *testing.T) {
	noRows := blockedModelCandidate(domain.BootstrapModelCandidate{Name: "gemma"})
	if len(noRows) != 1 || noRows[0].Kind != "model" || !strings.Contains(noRows[0].Reason, "no backend compatibility") {
		t.Fatalf("no-row blocked candidate = %+v", noRows)
	}
	rows := blockedModelCandidate(domain.BootstrapModelCandidate{
		Name: "gemma",
		Backends: []domain.BootstrapBackendCompatibility{
			{Backend: domain.BackendOpenVINO, Reason: "driver mismatch"},
			{Backend: domain.BackendVLLM},
		},
	})
	if len(rows) != 2 || rows[0].Backend != domain.BackendOpenVINO || rows[0].Reason != "driver mismatch" || rows[1].Reason != "backend compatibility is not ready" {
		t.Fatalf("blocked candidate rows = %+v", rows)
	}
}

type routerDynamicAdapter struct {
	name            string
	backend         domain.Backend
	launches        []domain.Preset
	dynamicLaunches []domain.Preset
	readyAddrs      []string
	stops           []ports.Handle
}

func (a *routerDynamicAdapter) Name() string {
	return a.name
}

func (a *routerDynamicAdapter) Launch(_ context.Context, preset domain.Preset, addr string) (ports.Handle, error) {
	a.launches = append(a.launches, preset)
	return ports.Handle{PID: 100 + len(a.launches), Addr: addr, Kind: string(a.backend), Ref: string(a.backend)}, nil
}

func (a *routerDynamicAdapter) LaunchDynamic(_ context.Context, preset domain.Preset, addr string) (ports.Handle, error) {
	a.dynamicLaunches = append(a.dynamicLaunches, preset)
	host, _, _ := strings.Cut(addr, ":")
	if host == "" {
		host = "127.0.0.1"
	}
	dynamicAddr := fmt.Sprintf("%s:%d", host, 6000+len(a.dynamicLaunches))
	return ports.Handle{PID: 200 + len(a.dynamicLaunches), Addr: dynamicAddr, Kind: string(a.backend), Ref: string(a.backend)}, nil
}

func (a *routerDynamicAdapter) WaitReady(_ context.Context, addr string) error {
	a.readyAddrs = append(a.readyAddrs, addr)
	return nil
}

func (a *routerDynamicAdapter) Stop(_ context.Context, handle ports.Handle) error {
	a.stops = append(a.stops, handle)
	return nil
}

type routerStaticAdapter struct {
	name    string
	backend domain.Backend
}

func (a *routerStaticAdapter) Name() string {
	return a.name
}

func (a *routerStaticAdapter) Launch(_ context.Context, _ domain.Preset, addr string) (ports.Handle, error) {
	return ports.Handle{PID: 300, Addr: addr, Kind: string(a.backend), Ref: string(a.backend)}, nil
}

func (a *routerStaticAdapter) WaitReady(context.Context, string) error {
	return nil
}

func (a *routerStaticAdapter) Stop(context.Context, ports.Handle) error {
	return nil
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

type serviceCommandRecorder struct {
	Commands []string
	Err      error
}

func (r *serviceCommandRecorder) Run(_ context.Context, name string, args ...string) error {
	r.Commands = append(r.Commands, strings.TrimSpace(name+" "+strings.Join(args, " ")))
	return r.Err
}

type fakeServiceManager struct {
	status string
	err    error
}

func (m fakeServiceManager) Install(context.Context, serviceSpec) error {
	return m.err
}

func (m fakeServiceManager) Status(context.Context, serviceSpec) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	if m.status == "" {
		return "active", nil
	}
	return m.status, nil
}

func (m fakeServiceManager) Uninstall(context.Context, serviceSpec) error {
	return m.err
}

type failingTokenStore struct{}

func (failingTokenStore) ListJoinTokens(context.Context) ([]domain.JoinTokenRecord, error) {
	return nil, errors.New("token store")
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
	if err := run(nil, []string{"bogus"}); err == nil {
		t.Fatal("nil-context unknown command expected error")
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

func waitForCondition(t *testing.T, ready func() bool) {
	t.Helper()
	for i := 0; i < 10000; i++ {
		if ready() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("condition did not become true")
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type optimizerFleet struct {
	snap domain.FleetSnapshot
	err  error
}

func (f optimizerFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	return f.snap, f.err
}

func optimizerComputeNode(id string) domain.Node {
	return domain.Node{ID: id, Status: domain.NodeReady, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1024}}}
}

type errorFleetSource struct {
	err error
}

func (f errorFleetSource) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	return domain.FleetSnapshot{}, f.err
}

type plainNodeResolver struct{}

func (plainNodeResolver) NodeAgent(string) (ports.NodeAgent, error) {
	return nil, errors.New("not found")
}

type signalingPeerDiscovery struct {
	peers      []domain.Peer
	err        error
	advertised chan domain.Peer
	peersSeen  chan struct{}
}

func (d *signalingPeerDiscovery) Advertise(_ context.Context, self domain.Peer) error {
	if d.err != nil {
		return d.err
	}
	if d.advertised != nil {
		d.advertised <- self
	}
	return nil
}

func (d *signalingPeerDiscovery) Peers(context.Context) ([]domain.Peer, error) {
	if d.err != nil {
		return nil, d.err
	}
	if d.peersSeen != nil {
		d.peersSeen <- struct{}{}
	}
	return append([]domain.Peer(nil), d.peers...), nil
}

func (d *signalingPeerDiscovery) WatchPeers(context.Context) (<-chan domain.Peer, error) {
	ch := make(chan domain.Peer)
	close(ch)
	return ch, nil
}

type recordingTelemetryRPCStore struct {
	err error
}

func (s *recordingTelemetryRPCStore) Record(context.Context, domain.RunMetric) error {
	return s.err
}

func (s *recordingTelemetryRPCStore) RecordSample(context.Context, domain.SessionMetric) error {
	return s.err
}

func (s *recordingTelemetryRPCStore) Metrics(context.Context, string) ([]domain.RunMetric, error) {
	if s.err != nil {
		return nil, s.err
	}
	return nil, nil
}

func (s *recordingTelemetryRPCStore) Samples(context.Context, domain.SessionMetricQuery) ([]domain.SessionMetric, error) {
	if s.err != nil {
		return nil, s.err
	}
	return nil, nil
}

func (s *recordingTelemetryRPCStore) SaveRecommendation(context.Context, domain.RecommendationRecord) error {
	return s.err
}

func (s *recordingTelemetryRPCStore) ListRecommendations(context.Context, string) ([]domain.RecommendationRecord, error) {
	if s.err != nil {
		return nil, s.err
	}
	return nil, nil
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

type runMetricRecorder interface {
	Record(context.Context, domain.RunMetric) error
}

func recordSustainedContextMetrics(t *testing.T, store runMetricRecorder, projectID, nodeID string, start time.Time) {
	t.Helper()
	for _, metric := range sustainedContextMetrics(projectID, nodeID, start) {
		if err := store.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record sustained metric: %v", err)
		}
	}
}

func sustainedContextMetrics(projectID, nodeID string, start time.Time) []domain.RunMetric {
	metrics := make([]domain.RunMetric, 0, 25)
	for i := 0; i < 25; i++ {
		contextUsed := 3500
		if i >= 20 {
			contextUsed = 4000
		}
		metrics = append(metrics, domain.RunMetric{
			JobID:       "ctx-" + strconv.Itoa(i),
			NodeID:      nodeID,
			Project:     projectID,
			ContextUsed: contextUsed,
			At:          start.Add(time.Duration(i) * time.Hour),
		})
	}
	return metrics
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return data
}
