//go:build smoke

package smoke

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/gateway"
)

func TestSparkVLLMPeerRoutingSmoke(t *testing.T) {
	sshHost := os.Getenv("MYCELIUM_SPARK_SSH")
	sparkAddr := os.Getenv("MYCELIUM_SPARK_ADDR")
	if sshHost == "" || sparkAddr == "" {
		t.Skip("set MYCELIUM_SPARK_SSH and MYCELIUM_SPARK_ADDR for DGX Spark vLLM peer smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	before := captureSparkForensics(t, ctx, sshHost, "before")
	if before.AppUsedMB != 0 {
		t.Fatalf("Spark is not idle before smoke: nvidia-smi compute apps use %d MiB\n%s", before.AppUsedMB, before.Raw)
	}
	modelConfig := sparkSmokeModelFromEnv(t)
	t.Logf("Spark smoke model: alias=%s model_ref=%s est_weights_mb=%d max_util=%.2f vllm_gpu_util=%s context=%d max_tokens=%d", modelConfig.Alias, modelConfig.ModelRef, modelConfig.EstWeightsMB, modelConfig.MaxUtil, modelConfig.GPUUtil, modelConfig.ContextLength, modelConfig.MaxTokens)

	mycelium := buildSmokeBinaryFor(t, ctx, "linux", "arm64")
	workdir := "/tmp/mycelium-spark-vllm-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	remotePorts := sparkFreePorts(t, ctx, sshHost, 3)
	peerPort, discoveryPort, backendPort := remotePorts[0], remotePorts[1], remotePorts[2]
	sparkPeerAddr := sparkAddr + ":" + peerPort
	joinToken := "spark-vllm-smoke"
	rpcToken := "spark-vllm-smoke-rpc"
	gatewayToken := "spark-vllm-smoke-gateway"
	model := modelConfig.Alias

	remoteMycelium := workdir + "/mycelium"
	remoteWrapper := workdir + "/vllm-docker-wrapper.sh"
	remoteConfig := workdir + "/spark-peer.json"
	runSSH(t, ctx, sshHost, "mkdir -p "+shellQuote(workdir))
	scpToRemote(t, ctx, sshHost, mycelium, remoteMycelium)
	scpToRemote(t, ctx, sshHost, filepath.Join(repoRoot(t), "tools", "smoke", "vllm-docker-wrapper.sh"), remoteWrapper)
	runSSH(t, ctx, sshHost, "chmod +x "+shellQuote(remoteMycelium)+" "+shellQuote(remoteWrapper))

	configPath := writeSparkPeerConfig(t, sparkPeerAddr, "0.0.0.0:"+discoveryPort, "127.0.0.1:"+backendPort, remoteWrapper, joinToken, rpcToken, gatewayToken, modelConfig)
	scpToRemote(t, ctx, sshHost, configPath, remoteConfig)
	cleanupSpark := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "ssh", "-o", "BatchMode=yes", sshHost, "if [ -f "+shellQuote(workdir+"/peer.pid")+" ]; then kill -INT $(cat "+shellQuote(workdir+"/peer.pid")+") 2>/dev/null || true; fi; docker rm -f myc-vllm-"+backendPort+" >/dev/null 2>&1 || true; rm -rf "+shellQuote(workdir)).Run()
	}
	defer cleanupSpark()

	sparkPeer := startRemoteSparkPeer(t, ctx, sshHost, workdir, remoteMycelium, remoteConfig)
	defer func() {
		cleanupSpark()
		sparkPeer.stopRemoteSSH(t)
	}()
	waitForRemoteSparkNodeReady(t, ctx, sparkPeer, "http://"+sparkPeerAddr, rpcToken)
	readySnap := fetchSparkSnapshot(t, ctx, "http://"+sparkPeerAddr, rpcToken)

	gatewayAddr := freeAddr(t)
	gatewayDiscoveryAddr := freeAddr(t)
	gatewayConfig := writeSparkGatewayConfig(t, gatewayAddr, gatewayDiscoveryAddr, sparkPeerAddr, joinToken, rpcToken, modelConfig)
	localMycelium := buildSmokeBinary(t, ctx)
	gatewayPeer := startSmokeProcess(t, ctx, localMycelium, "run", "--config", gatewayConfig)
	defer gatewayPeer.stop(t)

	body, instanceID, nodeID, perf := assertSparkGatewayChatEventually(t, ctx, "http://"+gatewayAddr, model, modelConfig.MaxTokens)
	if nodeID != "spark-vllm-peer" {
		t.Fatalf("gateway routed to node %q, want spark-vllm-peer body=%s", nodeID, body)
	}
	t.Logf("Spark gateway performance: ttft_ms=%d tokens_per_sec=%.2f completion_tokens=%d token_source=%s total_ms=%d body=%q", perf.TTFTMS, perf.TokensPerSec, perf.CompletionTokens, perf.TokenSource, perf.TotalMS, body)
	during := captureSparkForensics(t, ctx, sshHost, "during")
	loadedSnap := fetchSparkSnapshot(t, ctx, "http://"+sparkPeerAddr, rpcToken)
	if instanceID == "" {
		instanceID = sparkLoadedInstanceID(t, loadedSnap, model)
		t.Logf("Spark stream committed loading-state headers before instance binding; resolved instance from snapshot: %s", instanceID)
	}
	assertSparkVLLMCalibration(t, readySnap.Node, loadedSnap, instanceID, during)
	unloadJoinedInstance(t, ctx, "http://"+sparkPeerAddr, instanceID, rpcToken)
	after := captureSparkForensics(t, ctx, sshHost, "after")
	t.Logf("Spark forensics captured: before_app_mb=%d during_app_mb=%d after_app_mb=%d", before.AppUsedMB, during.AppUsedMB, after.AppUsedMB)
}

func buildSmokeBinaryFor(t *testing.T, ctx context.Context, goos, goarch string) string {
	t.Helper()
	root := repoRoot(t)
	out := filepath.Join(t.TempDir(), "mycelium-"+goos+"-"+goarch)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, "./cmd/mycelium")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch)
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s/%s smoke binary: %v\n%s", goos, goarch, err, data)
	}
	return out
}

func sparkFreePorts(t *testing.T, ctx context.Context, sshHost string, count int) []string {
	t.Helper()
	script := `python3 - <<'PY'
import socket
sockets = []
for _ in range(` + strconv.Itoa(count) + `):
    s = socket.socket()
    s.bind(("", 0))
    sockets.append(s)
print(" ".join(str(s.getsockname()[1]) for s in sockets))
PY`
	out := runSSHOutput(t, ctx, sshHost, script)
	fields := strings.Fields(out)
	if len(fields) != count {
		t.Fatalf("remote free ports = %q", out)
	}
	return fields
}

type sparkSmokeModel struct {
	Alias         string
	ModelRef      string
	GPUUtil       string
	MaxUtil       float64
	EstWeightsMB  int
	ContextLength int
	MaxTokens     int
}

func sparkSmokeModelFromEnv(t *testing.T) sparkSmokeModel {
	t.Helper()
	cfg := sparkSmokeModel{
		Alias:         getenvDefault("MYCELIUM_SPARK_VLLM_ALIAS", "spark-vllm-smoke"),
		ModelRef:      getenvDefault("MYCELIUM_SPARK_VLLM_MODEL_REF", "Qwen/Qwen3.5-0.8B"),
		GPUUtil:       getenvDefault("MYCELIUM_SPARK_VLLM_GPU_UTIL", "0.12"),
		MaxUtil:       getenvFloatDefault(t, "MYCELIUM_SPARK_VLLM_MAX_UTIL", 0.50),
		EstWeightsMB:  getenvIntDefault(t, "MYCELIUM_SPARK_VLLM_EST_WEIGHTS_MB", 2048),
		ContextLength: getenvIntDefault(t, "MYCELIUM_SPARK_VLLM_CONTEXT", 512),
		MaxTokens:     getenvIntDefault(t, "MYCELIUM_SPARK_VLLM_MAX_TOKENS", 24),
	}
	if cfg.Alias == "" || cfg.ModelRef == "" {
		t.Fatal("Spark smoke model alias and ref are required")
	}
	if cfg.MaxUtil <= 0 || cfg.MaxUtil > 1 {
		t.Fatalf("MYCELIUM_SPARK_VLLM_MAX_UTIL must be in (0,1], got %.3f", cfg.MaxUtil)
	}
	gpuUtil, err := strconv.ParseFloat(cfg.GPUUtil, 64)
	if err != nil || gpuUtil <= 0 || gpuUtil > 0.85 {
		t.Fatalf("MYCELIUM_SPARK_VLLM_GPU_UTIL must parse in (0,0.85], got %q", cfg.GPUUtil)
	}
	if cfg.EstWeightsMB <= 0 || cfg.ContextLength <= 0 || cfg.MaxTokens <= 0 {
		t.Fatalf("Spark smoke model limits must be positive: %+v", cfg)
	}
	return cfg
}

func getenvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvIntDefault(t *testing.T, key string, fallback int) int {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return value
}

func getenvFloatDefault(t *testing.T, key string, fallback float64) float64 {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return value
}

func writeSparkPeerConfig(t *testing.T, listen, discoveryListen, backendListen, wrapper, joinToken, rpcToken, gatewayToken string, model sparkSmokeModel) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spark-peer.json")
	cfg := struct {
		ID                   string          `json:"id"`
		Listen               string          `json:"listen"`
		StorePath            string          `json:"store_path"`
		Compute              bool            `json:"compute"`
		JoinToken            string          `json:"join_token"`
		RPCToken             string          `json:"rpc_token"`
		GatewayToken         string          `json:"gateway_token"`
		DiscoveryListen      string          `json:"discovery_listen"`
		DiscoveryAdvertiseMS int             `json:"discovery_advertise_ms"`
		DiscoveryScanMS      int             `json:"discovery_scan_ms"`
		ComputeConfig        ComputePeerJSON `json:"compute_config"`
		Presets              []domain.Preset `json:"presets"`
	}{
		ID:                   "spark-vllm-peer",
		Listen:               listen,
		StorePath:            filepath.Join(filepath.Dir(wrapper), "spark.sqlite"),
		Compute:              true,
		JoinToken:            joinToken,
		RPCToken:             rpcToken,
		GatewayToken:         gatewayToken,
		DiscoveryListen:      discoveryListen,
		DiscoveryAdvertiseMS: 100,
		DiscoveryScanMS:      250,
		ComputeConfig: ComputePeerJSON{
			ID:            "spark-vllm-peer",
			Name:          "DGX Spark vLLM",
			Backend:       domain.BackendVLLM,
			BackendBinary: wrapper,
			CustomArgs:    []string{"--gpu-memory-utilization", model.GPUUtil},
			BackendListen: backendListen,
			MaxUtil:       model.MaxUtil,
		},
		Presets: []domain.Preset{sparkVLLMPreset(model)},
	}
	writeJSONFile(t, path, cfg)
	return path
}

type ComputePeerJSON struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Backend       domain.Backend `json:"backend"`
	BackendBinary string         `json:"backend_binary"`
	CustomArgs    []string       `json:"custom_args,omitempty"`
	BackendListen string         `json:"backend_listen"`
	MaxUtil       float64        `json:"max_util"`
}

func writeSparkGatewayConfig(t *testing.T, listen, discoveryListen, seedPeer, joinToken, rpcToken string, model sparkSmokeModel) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spark-gateway.json")
	cfg := struct {
		ID                   string           `json:"id"`
		Listen               string           `json:"listen"`
		StorePath            string           `json:"store_path"`
		JoinToken            string           `json:"join_token"`
		RPCToken             string           `json:"rpc_token"`
		SeedPeers            []string         `json:"seed_peers"`
		DiscoveryListen      string           `json:"discovery_listen"`
		DiscoveryAdvertiseMS int              `json:"discovery_advertise_ms"`
		DiscoveryScanMS      int              `json:"discovery_scan_ms"`
		DefaultProject       string           `json:"default_project"`
		Projects             []domain.Project `json:"projects"`
		Presets              []domain.Preset  `json:"presets"`
	}{
		ID:                   "spark-gateway-peer",
		Listen:               listen,
		StorePath:            filepath.Join(t.TempDir(), "gateway.sqlite"),
		JoinToken:            joinToken,
		RPCToken:             rpcToken,
		SeedPeers:            []string{seedPeer},
		DiscoveryListen:      discoveryListen,
		DiscoveryAdvertiseMS: 100,
		DiscoveryScanMS:      250,
		DefaultProject:       "spark-smoke",
		Projects: []domain.Project{{
			ID:         "spark-smoke",
			Priority:   domain.PriorityInteractive,
			SpeedPref:  domain.SpeedThroughput,
			Preemption: domain.PreemptSoft,
		}},
		Presets: []domain.Preset{sparkVLLMPreset(model)},
	}
	writeJSONFile(t, path, cfg)
	return path
}

func sparkVLLMPreset(model sparkSmokeModel) domain.Preset {
	return domain.Preset{
		ID:            model.Alias,
		ModelRef:      model.ModelRef,
		Aliases:       []string{model.Alias},
		Backend:       domain.BackendVLLM,
		ContextLength: model.ContextLength,
		Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityCompletion},
		LaunchArgs: []string{
			"--served-model-name", model.Alias,
			"--gpu-memory-utilization", model.GPUUtil,
			"--max-model-len", strconv.Itoa(model.ContextLength),
			"--max-num-seqs", "1",
			"--max-num-batched-tokens", strconv.Itoa(model.ContextLength),
			"--disable-log-stats",
		},
		EstWeightsMB: model.EstWeightsMB,
		KVPerTokenMB: 0.01,
	}
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

type sparkForensics struct {
	Stage     string
	Raw       string
	AppUsedMB int
}

func captureSparkForensics(t *testing.T, ctx context.Context, sshHost, stage string) sparkForensics {
	t.Helper()
	script := `
set +e
echo __GPU__
nvidia-smi --query-gpu=index,name,memory.total,memory.used,compute_cap --format=csv,noheader,nounits 2>&1
echo __APPS__
nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader,nounits 2>&1
echo __FREE__
free -m 2>&1
echo __DMESG__
dmesg | tail -80 2>&1
`
	raw := runSSHOutput(t, ctx, sshHost, script)
	apps := sectionBetween(raw, "__APPS__", "__FREE__")
	snap := sparkForensics{Stage: stage, Raw: raw, AppUsedMB: parseNVIDIASMIAppUsedMB(apps)}
	t.Logf("Spark %s snapshot app_used_mb=%d\n%s", stage, snap.AppUsedMB, raw)
	return snap
}

func sectionBetween(raw, start, end string) string {
	from := strings.Index(raw, start)
	if from < 0 {
		return ""
	}
	from += len(start)
	to := strings.Index(raw[from:], end)
	if to < 0 {
		return strings.TrimSpace(raw[from:])
	}
	return strings.TrimSpace(raw[from : from+to])
}

func parseNVIDIASMIAppUsedMB(raw string) int {
	total := 0
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "No running processes") {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 3 {
			continue
		}
		value := strings.TrimSpace(parts[len(parts)-1])
		value = strings.TrimSuffix(value, " MiB")
		used, err := strconv.Atoi(value)
		if err == nil && used > 0 {
			total += used
		}
	}
	return total
}

func fetchSparkSnapshot(t *testing.T, ctx context.Context, nodeURL, rpcToken string) domain.NodeSnapshot {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(nodeURL, "/")+"/snapshot", nil)
	if err != nil {
		t.Fatalf("snapshot request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+rpcToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status=%s body=%s", resp.Status, data)
	}
	var snap domain.NodeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatalf("decode snapshot: %v body=%s", err, data)
	}
	return snap
}

func assertSparkVLLMCalibration(t *testing.T, before domain.Node, after domain.NodeSnapshot, instanceID string, during sparkForensics) {
	t.Helper()
	var inst domain.ModelInstance
	for _, candidate := range after.Instances {
		if candidate.ID == instanceID {
			inst = candidate
			break
		}
	}
	if inst.ID == "" {
		t.Fatalf("loaded instance %q missing from Spark snapshot: %+v", instanceID, after.Instances)
	}
	totalMB := selectedAcceleratorTotalMB(after.Node, inst.AcceleratorSet)
	if totalMB == 0 {
		totalMB = selectedAcceleratorTotalMB(before, inst.AcceleratorSet)
	}
	if totalMB == 0 {
		t.Fatalf("Spark snapshot has no selected accelerator capacity: node=%+v instance=%+v", after.Node, inst)
	}
	maxUtil := after.Node.MaxUtil
	if maxUtil == 0 {
		maxUtil = before.MaxUtil
	}
	ceilingMB := int(float64(totalMB) * maxUtil)
	claimMB := inst.Claim.WeightsMB + inst.Claim.KVReservedMB
	if claimMB <= 0 {
		t.Fatalf("Spark instance has empty scheduler claim: %+v", inst)
	}
	actualMB := during.AppUsedMB
	if actualMB <= 0 {
		t.Fatalf("Spark nvidia-smi did not report vLLM memory during load\n%s", during.Raw)
	}
	if actualMB > ceilingMB {
		t.Fatalf("Spark vLLM used %d MiB above scheduler ceiling %d MiB: total=%d max_util=%.2f claim=%d\n%s", actualMB, ceilingMB, totalMB, maxUtil, claimMB, during.Raw)
	}
	lowerMB := claimMB / 2
	upperMB := claimMB + 4096
	if actualMB < lowerMB || actualMB > upperMB {
		t.Fatalf("Spark vLLM memory %d MiB does not track scheduler claim %d MiB (allowed %d-%d)\n%s", actualMB, claimMB, lowerMB, upperMB, during.Raw)
	}
	t.Logf("Spark VRAM calibration held: nvidia_smi_app_used_mb=%d scheduler_claim_mb=%d ceiling_mb=%d total_mb=%d max_util=%.2f", actualMB, claimMB, ceilingMB, totalMB, maxUtil)
}

func sparkLoadedInstanceID(t *testing.T, snap domain.NodeSnapshot, presetID string) string {
	t.Helper()
	var matches []domain.ModelInstance
	for _, inst := range snap.Instances {
		if inst.PresetID == presetID && inst.State == domain.InstReady {
			matches = append(matches, inst)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("expected one ready Spark instance for preset %q, got %+v", presetID, snap.Instances)
	}
	return matches[0].ID
}

func selectedAcceleratorTotalMB(node domain.Node, selected []int) int {
	total := 0
	for _, idx := range selected {
		for _, acc := range node.Accelerators {
			if acc.Index == idx {
				total += acc.VRAMTotalMB
				break
			}
		}
	}
	return total
}

func startRemoteSparkPeer(t *testing.T, ctx context.Context, sshHost, workdir, binary, config string) *smokeProcess {
	t.Helper()
	remote := "cd " + shellQuote(workdir) + "; echo $$ > peer.pid; exec " + shellQuote(binary) + " run --config " + shellQuote(config)
	proc := &smokeProcess{cmd: exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", sshHost, remote), done: make(chan error, 1)}
	proc.cmd.Stdout = &proc.stdout
	proc.cmd.Stderr = &proc.stderr
	if err := proc.cmd.Start(); err != nil {
		t.Fatalf("start remote spark peer: %v", err)
	}
	go func() { proc.done <- proc.cmd.Wait() }()
	return proc
}

func (p *smokeProcess) stopRemoteSSH(t *testing.T) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	if err, ok := p.pollExit(); ok {
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") && !strings.Contains(err.Error(), "exit status 255") {
			t.Logf("remote ssh process exited before stop: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
		}
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-p.done:
		p.err, p.exited = err, true
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") && !strings.Contains(err.Error(), "exit status 255") {
			t.Logf("remote ssh process exited: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		err := <-p.done
		p.err, p.exited = err, true
		t.Logf("remote ssh process killed: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
	}
}

func waitForRemoteSparkNodeReady(t *testing.T, ctx context.Context, proc *smokeProcess, nodeURL, rpcToken string) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: 2 * time.Second}
	t.Logf("waiting for Spark peer readiness at %s", nodeURL)
	for {
		if err, ok := proc.pollExit(); ok {
			t.Fatalf("remote Spark peer exited before readiness: %v\nstdout:\n%s\nstderr:\n%s", err, proc.stdout.String(), proc.stderr.String())
		}
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, strings.TrimRight(nodeURL, "/")+"/snapshot", nil)
		if err != nil {
			reqCancel()
			t.Fatalf("snapshot request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+rpcToken)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				reqCancel()
				return
			}
		}
		reqCancel()
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for Spark peer: %v\nstdout:\n%s\nstderr:\n%s", ctx.Err(), proc.stdout.String(), proc.stderr.String())
		case <-ticker.C:
		}
	}
}

type sparkGatewayPerf struct {
	TTFTMS           int
	TotalMS          int
	CompletionTokens int
	TokensPerSec     float64
	TokenSource      string
}

func assertSparkGatewayChatEventually(t *testing.T, ctx context.Context, gatewayURL, model string, maxTokens int) (string, string, string, sparkGatewayPerf) {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		body, instanceID, nodeID, perf, ok := trySparkGatewayChat(t, ctx, gatewayURL, model, maxTokens)
		if ok {
			return body, instanceID, nodeID, perf
		}
		last = body
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for Spark gateway chat: %v last=%s", ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func trySparkGatewayChat(t *testing.T, ctx context.Context, gatewayURL, model string, maxTokens int) (string, string, string, sparkGatewayPerf, bool) {
	t.Helper()
	body := []byte(`{"model":` + quote(model) + `,"messages":[{"role":"user","content":"Write exactly eight short words about a quiet test run."}],"max_tokens":` + strconv.Itoa(maxTokens) + `,"temperature":0,"stream":true,"stream_options":{"include_usage":true}}`)
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(gatewayURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err.Error(), "", "", sparkGatewayPerf{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read gateway response: %v", err)
		}
		return string(data), "", "", sparkGatewayPerf{}, false
	}
	content, perf, err := readSparkGatewayStream(resp.Body, start)
	if err != nil {
		t.Fatalf("read Spark gateway stream: %v", err)
	}
	if strings.TrimSpace(content) == "" {
		t.Fatalf("gateway stream produced empty content headers=%+v", resp.Header)
	}
	return content, resp.Header.Get(gateway.HeaderInstance), resp.Header.Get(gateway.HeaderNode), perf, true
}

func readSparkGatewayStream(body io.Reader, start time.Time) (string, sparkGatewayPerf, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	var content strings.Builder
	firstTokenAt := time.Time{}
	end := start
	chunkTokens := 0
	usageTokens := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if raw == "" {
			continue
		}
		end = time.Now()
		if raw == "[DONE]" {
			break
		}
		part, tokens, err := parseSparkStreamChunk(raw)
		if err != nil {
			return "", sparkGatewayPerf{}, err
		}
		if tokens > 0 {
			usageTokens = tokens
		}
		if part != "" {
			if firstTokenAt.IsZero() {
				firstTokenAt = end
			}
			chunkTokens++
			content.WriteString(part)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", sparkGatewayPerf{}, err
	}
	if firstTokenAt.IsZero() {
		firstTokenAt = end
	}
	completionTokens := usageTokens
	source := "usage"
	if completionTokens == 0 {
		completionTokens = chunkTokens
		source = "stream_chunks"
	}
	seconds := end.Sub(firstTokenAt).Seconds()
	if seconds <= 0 {
		seconds = end.Sub(start).Seconds()
	}
	perf := sparkGatewayPerf{
		TTFTMS:           int(firstTokenAt.Sub(start) / time.Millisecond),
		TotalMS:          int(end.Sub(start) / time.Millisecond),
		CompletionTokens: completionTokens,
		TokenSource:      source,
	}
	if completionTokens > 0 && seconds > 0 {
		perf.TokensPerSec = float64(completionTokens) / seconds
	}
	return content.String(), perf, nil
}

func parseSparkStreamChunk(raw string) (string, int, error) {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
		Usage *struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
		return "", 0, err
	}
	var content strings.Builder
	for _, choice := range chunk.Choices {
		content.WriteString(choice.Delta.Content)
	}
	tokens := 0
	if chunk.Usage != nil {
		tokens = chunk.Usage.CompletionTokens
	}
	return content.String(), tokens, nil
}

func scpToRemote(t *testing.T, ctx context.Context, sshHost, local, remote string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "scp", "-o", "BatchMode=yes", local, sshHost+":"+remote)
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scp %s to %s:%s: %v\n%s", local, sshHost, remote, err, data)
	}
}

func runSSH(t *testing.T, ctx context.Context, sshHost, command string) {
	t.Helper()
	_ = runSSHOutput(t, ctx, sshHost, command)
}

func runSSHOutput(t *testing.T, ctx context.Context, sshHost, command string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", sshHost, command)
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh %s %q: %v\n%s", sshHost, command, err, data)
	}
	return strings.TrimSpace(string(data))
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
