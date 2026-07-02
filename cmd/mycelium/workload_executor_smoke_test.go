//go:build smoke

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	controlcli "mycelium/cmd/internal/controlcli"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/gateway"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/internal/workload"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestWorkloadExecutorLocalCanary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storesqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("Open control store: %v", err)
	}
	defer store.Close()

	var providerBody string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("provider path = %s", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read provider request: %v", err)
		}
		providerBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
	}))
	defer provider.Close()

	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	node.Accelerators = []domain.Accelerator{{Index: 0, Vendor: "test", Kind: "gpu", VRAMTotalMB: 1000}}
	node.MaxUtil = 1
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("model-a"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	preset.Backend = domain.BackendLlamaCpp
	agent := &addressedWorkloadAgent{node: node, addr: strings.TrimPrefix(provider.URL, "http://")}
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
		Clock:   clock.System{},
	}
	mux := http.NewServeMux()
	mountWorkloadHTTP(mux, service, store, "rpc-secret", "peer-a", router)
	peer := httptest.NewServer(mux)
	defer peer.Close()

	dir := t.TempDir()
	intentPath := filepath.Join(dir, "workload.json")
	requestPath := filepath.Join(dir, "request.json")
	resultPath := filepath.Join(dir, "result.json")
	writeJSONFile(t, intentPath, workload.AdmissionRequest{Job: domain.Job{
		ID:             "job-a",
		TaskType:       "chat",
		Model:          "model-a",
		Project:        "project-a",
		Priority:       domain.PriorityInteractive,
		SpeedPref:      domain.SpeedThroughput,
		Preemption:     domain.PreemptSoft,
		ContextRequest: 1024,
	}})
	if err := os.WriteFile(requestPath, []byte(`{"model":"model-a","messages":[{"role":"user","content":""}],"max_tokens":2}`), 0600); err != nil {
		t.Fatalf("write request body: %v", err)
	}

	admitOut := captureSmokeStdout(t, func() {
		err = controlcli.RunWithClient(ctx, []string{"jobs", "admit", "--url", peer.URL, "--rpc-token", "rpc-secret", "--file", intentPath, "--payload-kind", workload.ExecutablePayloadKindOpenAIChat, "--payload-file", requestPath}, peer.Client())
	})
	if err != nil {
		t.Fatalf("jobs admit: %v", err)
	}
	if !strings.Contains(admitOut, "execution_state=queued_not_executed") || strings.Contains(admitOut, "messages") {
		t.Fatalf("admit output = %q", admitOut)
	}

	placer.ready = true
	drainOut := captureSmokeStdout(t, func() {
		err = controlcli.RunWithClient(ctx, []string{"jobs", "drain", "--url", peer.URL, "--rpc-token", "rpc-secret", "--limit", "1", "--execute"}, peer.Client())
	})
	if err != nil {
		t.Fatalf("jobs drain --execute: %v", err)
	}
	if !strings.Contains(drainOut, "provider_output_capture=true") || !strings.Contains(drainOut, "result_replayable=true") || strings.Contains(drainOut, "choices") {
		t.Fatalf("drain output = %q", drainOut)
	}
	if providerBody == "" || !strings.Contains(providerBody, `"messages"`) {
		t.Fatalf("provider body = %q", providerBody)
	}

	statusOut := captureSmokeStdout(t, func() {
		err = controlcli.RunWithClient(ctx, []string{"jobs", "status", "--url", peer.URL, "--rpc-token", "rpc-secret", "--id", "job-a"}, peer.Client())
	})
	if err != nil {
		t.Fatalf("jobs status: %v", err)
	}
	if !strings.Contains(statusOut, "status=done") || !strings.Contains(statusOut, "execution_state=terminal_result_replayable") || !strings.Contains(statusOut, "provider_output_capture=true") || strings.Contains(statusOut, "choices") {
		t.Fatalf("status output = %q", statusOut)
	}

	resultOut := captureSmokeStdout(t, func() {
		err = controlcli.RunWithClient(ctx, []string{"jobs", "result", "--url", peer.URL, "--rpc-token", "rpc-secret", "--id", "job-a", "--out", resultPath}, peer.Client())
	})
	if err != nil {
		t.Fatalf("jobs result: %v", err)
	}
	resultBody, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read result file: %v", err)
	}
	digest := sha256.Sum256(resultBody)
	if !strings.Contains(string(resultBody), `"finish_reason":"stop"`) || !strings.Contains(resultOut, hex.EncodeToString(digest[:])) {
		t.Fatalf("result output=%q body=%s", resultOut, resultBody)
	}
	leases, err := store.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases after provider capture = %+v", leases)
	}
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, append(bytes.TrimSpace(data), '\n'), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func captureSmokeStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return string(out)
}
