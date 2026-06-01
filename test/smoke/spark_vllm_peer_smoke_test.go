//go:build smoke

package smoke

import (
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

	mycelium := buildSmokeBinaryFor(t, ctx, "linux", "arm64")
	workdir := "/tmp/mycelium-spark-vllm-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	remotePorts := sparkFreePorts(t, ctx, sshHost, 3)
	peerPort, discoveryPort, backendPort := remotePorts[0], remotePorts[1], remotePorts[2]
	sparkPeerAddr := sparkAddr + ":" + peerPort
	joinToken := "spark-vllm-smoke"
	rpcToken := "spark-vllm-smoke-rpc"
	model := "spark-vllm-smoke"

	remoteMycelium := workdir + "/mycelium"
	remoteWrapper := workdir + "/vllm-docker-wrapper.sh"
	remoteConfig := workdir + "/spark-peer.json"
	runSSH(t, ctx, sshHost, "mkdir -p "+shellQuote(workdir))
	scpToRemote(t, ctx, sshHost, mycelium, remoteMycelium)
	scpToRemote(t, ctx, sshHost, filepath.Join(repoRoot(t), "tools", "smoke", "vllm-docker-wrapper.sh"), remoteWrapper)
	runSSH(t, ctx, sshHost, "chmod +x "+shellQuote(remoteMycelium)+" "+shellQuote(remoteWrapper))

	configPath := writeSparkPeerConfig(t, sparkPeerAddr, "0.0.0.0:"+discoveryPort, "127.0.0.1:"+backendPort, remoteWrapper, joinToken, rpcToken)
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
	waitForNodeReady(t, ctx, "http://"+sparkPeerAddr, rpcToken)

	gatewayAddr := freeAddr(t)
	gatewayDiscoveryAddr := freeAddr(t)
	gatewayConfig := writeSparkGatewayConfig(t, gatewayAddr, gatewayDiscoveryAddr, sparkPeerAddr, joinToken, rpcToken)
	localMycelium := buildSmokeBinary(t, ctx)
	gatewayPeer := startSmokeProcess(t, ctx, localMycelium, "run", "--config", gatewayConfig)
	defer gatewayPeer.stop(t)

	body, instanceID, nodeID := assertSparkGatewayChatEventually(t, ctx, "http://"+gatewayAddr, model)
	if nodeID != "spark-vllm-peer" {
		t.Fatalf("gateway routed to node %q, want spark-vllm-peer body=%s", nodeID, body)
	}
	if instanceID == "" {
		t.Fatalf("gateway response missing %s body=%s", gateway.HeaderInstance, body)
	}
	unloadJoinedInstance(t, ctx, "http://"+sparkPeerAddr, instanceID, rpcToken)
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

func writeSparkPeerConfig(t *testing.T, listen, discoveryListen, backendListen, wrapper, joinToken, rpcToken string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spark-peer.json")
	cfg := struct {
		ID                   string          `json:"id"`
		Listen               string          `json:"listen"`
		StorePath            string          `json:"store_path"`
		Compute              bool            `json:"compute"`
		JoinToken            string          `json:"join_token"`
		RPCToken             string          `json:"rpc_token"`
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
		DiscoveryListen:      discoveryListen,
		DiscoveryAdvertiseMS: 100,
		DiscoveryScanMS:      250,
		ComputeConfig: ComputePeerJSON{
			ID:            "spark-vllm-peer",
			Name:          "DGX Spark vLLM",
			Backend:       domain.BackendVLLM,
			BackendBinary: wrapper,
			BackendListen: backendListen,
			MaxUtil:       0.50,
		},
		Presets: []domain.Preset{sparkVLLMPreset()},
	}
	writeJSONFile(t, path, cfg)
	return path
}

type ComputePeerJSON struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Backend       domain.Backend `json:"backend"`
	BackendBinary string         `json:"backend_binary"`
	BackendListen string         `json:"backend_listen"`
	MaxUtil       float64        `json:"max_util"`
}

func writeSparkGatewayConfig(t *testing.T, listen, discoveryListen, seedPeer, joinToken, rpcToken string) string {
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
		Presets: []domain.Preset{sparkVLLMPreset()},
	}
	writeJSONFile(t, path, cfg)
	return path
}

func sparkVLLMPreset() domain.Preset {
	return domain.Preset{
		ID:            "spark-vllm-smoke",
		ModelRef:      "Qwen/Qwen3.5-0.8B",
		Aliases:       []string{"spark-vllm-smoke"},
		Backend:       domain.BackendVLLM,
		ContextLength: 512,
		Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityCompletion},
		LaunchArgs: []string{
			"--served-model-name", "spark-vllm-smoke",
			"--gpu-memory-utilization", "0.12",
			"--max-model-len", "512",
			"--max-num-seqs", "1",
			"--max-num-batched-tokens", "512",
			"--disable-log-stats",
		},
		EstWeightsMB: 2048,
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

func startRemoteSparkPeer(t *testing.T, ctx context.Context, sshHost, workdir, binary, config string) *smokeProcess {
	t.Helper()
	remote := "cd " + shellQuote(workdir) + "; echo $$ > peer.pid; exec " + shellQuote(binary) + " run --config " + shellQuote(config)
	proc := &smokeProcess{cmd: exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", sshHost, remote)}
	proc.cmd.Stdout = &proc.stdout
	proc.cmd.Stderr = &proc.stderr
	if err := proc.cmd.Start(); err != nil {
		t.Fatalf("start remote spark peer: %v", err)
	}
	return proc
}

func (p *smokeProcess) stopRemoteSSH(t *testing.T) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") && !strings.Contains(err.Error(), "exit status 255") {
			t.Logf("remote ssh process exited: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		err := <-done
		t.Logf("remote ssh process killed: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
	}
}

func assertSparkGatewayChatEventually(t *testing.T, ctx context.Context, gatewayURL, model string) (string, string, string) {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		body, instanceID, nodeID, ok := trySparkGatewayChat(t, ctx, gatewayURL, model)
		if ok {
			return body, instanceID, nodeID
		}
		last = body
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for Spark gateway chat: %v last=%s", ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func trySparkGatewayChat(t *testing.T, ctx context.Context, gatewayURL, model string) (string, string, string, bool) {
	t.Helper()
	body := []byte(`{"model":` + quote(model) + `,"messages":[{"role":"user","content":"Reply with exactly one word: hi"}],"max_tokens":4,"temperature":0}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(gatewayURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err.Error(), "", "", false
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read gateway response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return string(data), "", "", false
	}
	if !strings.Contains(string(data), `"choices"`) {
		t.Fatalf("gateway body missing choices headers=%+v body=%s", resp.Header, data)
	}
	return string(data), resp.Header.Get(gateway.HeaderInstance), resp.Header.Get(gateway.HeaderNode), true
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
