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
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/gateway"
)

func TestPhase4JoinedNodeGatewaySmoke(t *testing.T) {
	gatewayURL := os.Getenv("MYCELIUM_JOIN_GATEWAY")
	model := os.Getenv("MYCELIUM_JOIN_MODEL")
	if gatewayURL != "" && model != "" {
		runPhase4ManualJoinSmoke(t, gatewayURL, model)
		return
	}

	binary := os.Getenv("MYCELIUM_LLAMA_CPP_BINARY")
	model = os.Getenv("MYCELIUM_LLAMA_CPP_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_LLAMA_CPP_BINARY and MYCELIUM_LLAMA_CPP_MODEL for automated Phase 4 join smoke, or set MYCELIUM_JOIN_GATEWAY and MYCELIUM_JOIN_MODEL for manual smoke")
	}
	runPhase4AutomatedJoinSmoke(t, binary, model)
}

func runPhase4ManualJoinSmoke(t *testing.T, gatewayURL, model string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	nodes := waitForJoinedNodes(t, ctx, gatewayURL, "")
	if !hasReadyNode(nodes) {
		t.Fatalf("no ready joined node: %+v", nodes)
	}
	assertGatewayChat(t, ctx, gatewayURL, model)
}

func runPhase4AutomatedJoinSmoke(t *testing.T, binary, model string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	mycelium := buildSmokeBinary(t, ctx)
	nodeAddr := freeAddr(t)
	gatewayAddr := freeAddr(t)
	backendAddr := freeAddr(t)
	nodeConfig := writePhase4ComputePeerConfig(t, nodeAddr, backendAddr, binary, model)

	node := startSmokeProcess(t, ctx, mycelium,
		"run",
		"--config", nodeConfig,
	)
	defer node.stop(t)
	waitForNodeReady(t, ctx, "http://"+nodeAddr)

	gatewayConfig := writePhase4GatewayPeerConfig(t, gatewayAddr, nodeAddr, model)
	gatewayPeer := startSmokeProcess(t, ctx, mycelium, "run", "--config", gatewayConfig)
	defer gatewayPeer.stop(t)

	gatewayURL := "http://" + gatewayAddr
	respBody, instanceID := assertGatewayChat(t, ctx, gatewayURL, model)
	if instanceID == "" {
		t.Fatalf("gateway response missing %s body=%s", gateway.HeaderInstance, respBody)
	}
	unloadJoinedInstance(t, ctx, "http://"+nodeAddr, instanceID)
}

type phase4Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Status  string `json:"status"`
}

func waitForJoinedNodes(t *testing.T, ctx context.Context, gatewayURL, directNodeAddr string) []phase4Node {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var last []phase4Node
	for {
		nodes, ok := fetchJoinedNodes(t, ctx, gatewayURL)
		if ok {
			last = nodes
			if hasReadyNode(nodes) && (directNodeAddr == "" || joinedThroughLoopback(nodes, directNodeAddr)) {
				return nodes
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for joined node: %v last=%+v", ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func waitForGatewayReady(t *testing.T, ctx context.Context, gatewayURL string) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, ok := fetchJoinedNodes(t, ctx, gatewayURL); ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for gateway: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForNodeReady(t *testing.T, ctx context.Context, nodeURL string) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(nodeURL, "/")+"/snapshot", nil)
		if err != nil {
			t.Fatalf("snapshot request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for node: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func fetchJoinedNodes(t *testing.T, ctx context.Context, gatewayURL string) ([]phase4Node, bool) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(gatewayURL, "/")+"/nodes", nil)
	if err != nil {
		t.Fatalf("nodes request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var nodes []phase4Node
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		t.Fatalf("decode nodes: %v", err)
	}
	return nodes, true
}

func joinedThroughLoopback(nodes []phase4Node, directNodeAddr string) bool {
	for _, node := range nodes {
		if node.Status == "ready" && strings.HasPrefix(node.Address, "127.0.0.1:") && node.Address != directNodeAddr {
			return true
		}
	}
	return false
}

func hasReadyNode(nodes []phase4Node) bool {
	for _, node := range nodes {
		if node.Status == "ready" {
			return true
		}
	}
	return false
}

func assertGatewayChat(t *testing.T, ctx context.Context, gatewayURL, model string) (string, string) {
	t.Helper()
	body := []byte(`{"model":` + quote(model) + `,"messages":[{"role":"user","content":"Say hi."}],"max_tokens":1}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(gatewayURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway post: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read gateway response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gateway status = %s body=%s", resp.Status, data)
	}
	if resp.Header.Get(gateway.HeaderNode) == "" || !strings.Contains(string(data), `"choices"`) {
		t.Fatalf("gateway response headers=%+v body=%s", resp.Header, data)
	}
	return string(data), resp.Header.Get(gateway.HeaderInstance)
}

func unloadJoinedInstance(t *testing.T, ctx context.Context, nodeURL, instanceID string) {
	t.Helper()
	body := []byte(`{"instance_id":` + quote(instanceID) + `}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(nodeURL, "/")+"/unload", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("unload request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unload joined instance: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unload status = %s body=%s", resp.Status, data)
	}
}

func buildSmokeBinary(t *testing.T, ctx context.Context) string {
	t.Helper()
	root := repoRoot(t)
	out := filepath.Join(t.TempDir(), "mycelium")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, "./cmd/mycelium")
	cmd.Dir = root
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build smoke binary: %v\n%s", err, data)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func writePhase4GatewayPeerConfig(t *testing.T, addr, nodeAddr, model string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway-peer.json")
	cfg := struct {
		Listen         string           `json:"listen"`
		StorePath      string           `json:"store_path"`
		NodeURLs       []string         `json:"node_urls"`
		DefaultProject string           `json:"default_project"`
		Projects       []domain.Project `json:"projects"`
		Presets        []domain.Preset  `json:"presets"`
	}{
		Listen:         addr,
		StorePath:      filepath.Join(t.TempDir(), "control.sqlite"),
		NodeURLs:       []string{"http://" + nodeAddr},
		DefaultProject: "phase4",
		Projects: []domain.Project{{
			ID:         "phase4",
			Priority:   domain.PriorityInteractive,
			SpeedPref:  domain.SpeedThroughput,
			Preemption: domain.PreemptSoft,
		}},
		Presets: []domain.Preset{{
			ID:            "phase4-model",
			ModelRef:      model,
			Backend:       domain.BackendLlamaCpp,
			ContextLength: 2048,
			Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityCompletion},
			EstWeightsMB:  1,
			KVPerTokenMB:  0.01,
		}},
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal gateway peer config: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write gateway peer config: %v", err)
	}
	return path
}

func writePhase4ComputePeerConfig(t *testing.T, addr, backendAddr, binary, model string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "compute-peer.json")
	cfg := struct {
		Listen        string `json:"listen"`
		StorePath     string `json:"store_path"`
		Compute       bool   `json:"compute"`
		ComputeConfig struct {
			ID            string `json:"id"`
			Name          string `json:"name"`
			BackendListen string `json:"backend_listen"`
			LlamaServer   string `json:"llama_server"`
			VRAMMB        int    `json:"vram_mb"`
		} `json:"compute_config"`
		Presets []domain.Preset `json:"presets"`
	}{
		Listen:    addr,
		StorePath: filepath.Join(t.TempDir(), "compute.sqlite"),
		Compute:   true,
		Presets: []domain.Preset{{
			ID:            "phase4-model",
			ModelRef:      model,
			Backend:       domain.BackendLlamaCpp,
			ContextLength: 2048,
			Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityCompletion},
			EstWeightsMB:  1,
			KVPerTokenMB:  0.01,
		}},
	}
	cfg.ComputeConfig.ID = "phase4-node"
	cfg.ComputeConfig.Name = "Phase 4 Node"
	cfg.ComputeConfig.BackendListen = backendAddr
	cfg.ComputeConfig.LlamaServer = binary
	cfg.ComputeConfig.VRAMMB = 8192
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal compute peer config: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write compute peer config: %v", err)
	}
	return path
}

type smokeProcess struct {
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func startSmokeProcess(t *testing.T, ctx context.Context, bin string, args ...string) *smokeProcess {
	t.Helper()
	proc := &smokeProcess{cmd: exec.CommandContext(ctx, bin, args...)}
	proc.cmd.Stdout = &proc.stdout
	proc.cmd.Stderr = &proc.stderr
	if err := proc.cmd.Start(); err != nil {
		t.Fatalf("start %s %v: %v", bin, args, err)
	}
	return proc
}

func (p *smokeProcess) stop(t *testing.T) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			t.Logf("process exited: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		err := <-done
		t.Logf("process killed: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
	}
}
