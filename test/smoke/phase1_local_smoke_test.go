//go:build smoke

package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/node"
	"mycelium/internal/optimizer"
	"mycelium/internal/telemetry"
	"mycelium/test/fixtures"
)

func TestLocalPhase1LoadServeTelemetryRequeueReaper(t *testing.T) {
	binary := os.Getenv("MYCELIUM_LLAMA_CPP_BINARY")
	model := os.Getenv("MYCELIUM_LLAMA_CPP_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_LLAMA_CPP_BINARY and MYCELIUM_LLAMA_CPP_MODEL for Phase 1 local smoke")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	store, err := telemetry.NewSQLiteStore(t.TempDir() + "/telemetry.sqlite")
	if err != nil {
		t.Fatalf("telemetry store: %v", err)
	}
	defer store.Close()

	addr := freeAddr(t)
	adapter := newSmokeAdapter(binary)
	agent := node.NewAgent(fixtures.MakeNode(), adapter, clock.System{}, node.WithTelemetrySink(store), node.WithListenAddr(addr), node.WithAllocator(lease.NewAllocator()))
	preset := fixtures.MakePreset(
		fixtures.WithModelRef(model),
		fixtures.WithContextLength(2048),
		fixtures.WithWeights(1),
		fixtures.WithKVPerToken(0.01),
	)

	inst, err := agent.Load(ctx, preset)
	if err != nil {
		t.Fatalf("load ready-gated llama.cpp: %v", err)
	}
	if inst.Addr == "" {
		t.Fatalf("loaded instance has no addr: %+v", inst)
	}
	completion := requestCompletion(t, ctx, "http://"+addr+"/v1/completions", model)
	if completion.Usage.TotalTokens == 0 && completion.Timings.PromptN+completion.Timings.CacheN+completion.Timings.PredictedN == 0 {
		t.Fatalf("completion lacks usage/timings: %+v", completion)
	}

	contextUsed := completion.Usage.TotalTokens
	if contextUsed == 0 {
		contextUsed = completion.Timings.PromptN + completion.Timings.CacheN + completion.Timings.PredictedN
	}
	if err := agent.RecordRun(ctx, domain.RunMetric{
		JobID:           "smoke-local",
		InstanceID:      inst.ID,
		Project:         "smoke",
		TokensPerSec:    completion.Timings.PredictedPerSecond,
		TTFTms:          int(completion.Timings.PromptMS),
		LoadWallClockMS: 0,
		PeakVRAMMB:      0,
		ContextUsed:     contextUsed,
	}); err != nil {
		t.Fatalf("record run metric: %v", err)
	}
	metrics, err := store.Metrics(ctx, "smoke")
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	if len(metrics) != 1 || metrics[0].ContextUsed == 0 {
		t.Fatalf("metrics = %+v", metrics)
	}

	plan, err := optimizer.PlanReactiveRequeue(
		fixtures.MakeJob(fixtures.WithPreset(preset.ID)),
		preset,
		domain.ErrContextOverflow,
		preset.ContextLength+1,
		optimizer.ReactivePolicy{SharedContexts: []int{2048, 4096}},
	)
	if err != nil {
		t.Fatalf("reactive requeue: %v", err)
	}
	if plan.Preset.ContextLength != 4096 {
		t.Fatalf("requeue context = %d", plan.Preset.ContextLength)
	}

	if err := agent.Unload(ctx, inst.ID); err != nil {
		t.Fatalf("graceful stop: %v", err)
	}

	orphanAdapter := newSmokeAdapter(binary)
	orphanAddr := freeAddr(t)
	handle, err := orphanAdapter.Launch(ctx, preset, orphanAddr)
	if err != nil {
		t.Fatalf("launch orphan: %v", err)
	}
	processFile := t.TempDir() + "/processes.json"
	if err := node.WriteProcessRefs(processFile, []node.ProcessRef{{PID: handle.PID, Kind: handle.Kind, Ref: handle.Ref}}); err != nil {
		t.Fatalf("write orphan refs: %v", err)
	}
	if _, err := node.NewReaper(processFile, node.BackendProcessKiller{Backend: llamacpp.NewAdapter(llamacpp.Config{})}).Reap(ctx); err != nil {
		t.Fatalf("reap orphan: %v", err)
	}
	_ = orphanAdapter.Stop(ctx, handle)
}

func newSmokeAdapter(binary string) *llamacpp.Adapter {
	return llamacpp.NewAdapter(llamacpp.Config{
		BinaryPath: binary,
		Args: []string{
			"--host", "{host}",
			"--port", "{port}",
			"-m", "{model}",
			"-c", "{ctx}",
		},
		Clock:        clock.System{},
		PollInterval: 250 * time.Millisecond,
	})
}

type completionResponse struct {
	Usage struct {
		TotalTokens int `json:"total_tokens"`
	} `json:"usage"`
	Timings struct {
		PromptN            int     `json:"prompt_n"`
		CacheN             int     `json:"cache_n"`
		PredictedN         int     `json:"predicted_n"`
		PromptMS           float64 `json:"prompt_ms"`
		PredictedPerSecond float64 `json:"predicted_per_second"`
	} `json:"timings"`
}

func requestCompletion(t *testing.T, ctx context.Context, url, model string) completionResponse {
	t.Helper()
	body := []byte(`{"model":` + quote(model) + `,"prompt":"Say hi.","max_tokens":1,"stream":false}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("completion request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("completion do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("completion status = %s", resp.Status)
	}
	var out completionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode completion: %v", err)
	}
	return out
}

func quote(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(data)
}
