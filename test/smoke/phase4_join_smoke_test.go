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
		runPhase4ManualJoinSmoke(t, gatewayURL, model, smokeGatewayToken("MYCELIUM_JOIN_GATEWAY_TOKEN"))
		return
	}

	binary := os.Getenv("MYCELIUM_LLAMA_CPP_BINARY")
	model = os.Getenv("MYCELIUM_LLAMA_CPP_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_LLAMA_CPP_BINARY and MYCELIUM_LLAMA_CPP_MODEL for automated Phase 4 join smoke, or set MYCELIUM_JOIN_GATEWAY and MYCELIUM_JOIN_MODEL for manual smoke")
	}
	runPhase4AutomatedJoinSmoke(t, binary, model)
}

func runPhase4ManualJoinSmoke(t *testing.T, gatewayURL, model, token string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	_, _, nodeID := assertGatewayChatEventually(t, ctx, gatewayURL, model, token)
	if want := os.Getenv("MYCELIUM_JOIN_EXPECT_NODE"); want != "" && nodeID != want {
		t.Fatalf("gateway placed on node %q, want %q", nodeID, want)
	}
}

func runPhase4AutomatedJoinSmoke(t *testing.T, binary, model string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	mycelium := buildSmokeBinary(t, ctx)
	nodeAddr := freeAddr(t)
	gatewayAddr := freeAddr(t)
	backendAddr := freeAddr(t)
	nodeDiscoveryAddr := freeAddr(t)
	gatewayDiscoveryAddr := freeAddr(t)
	joinToken := "phase4-smoke"
	rpcToken := "phase4-smoke-rpc"
	parser := writePhase4GGUFParser(t)
	nodeConfig := writePhase4ComputePeerConfig(t, nodeAddr, backendAddr, nodeDiscoveryAddr, gatewayDiscoveryAddr, joinToken, rpcToken, binary, parser, model)

	node := startSmokeProcess(t, ctx, mycelium,
		"run",
		"--config", nodeConfig,
	)
	defer node.stop(t)
	waitForNodeReady(t, ctx, "http://"+nodeAddr, rpcToken)

	gatewayConfig := writePhase4GatewayPeerConfig(t, gatewayAddr, gatewayDiscoveryAddr, nodeDiscoveryAddr, joinToken, rpcToken, parser, model)
	gatewayPeer := startSmokeProcess(t, ctx, mycelium, "run", "--config", gatewayConfig)
	defer gatewayPeer.stop(t)

	gatewayURL := "http://" + gatewayAddr
	respBody, instanceID, _ := assertGatewayChatEventually(t, ctx, gatewayURL, model, "")
	if instanceID == "" {
		t.Fatalf("gateway response missing %s body=%s", gateway.HeaderInstance, respBody)
	}
	unloadJoinedInstance(t, ctx, "http://"+nodeAddr, instanceID, rpcToken)
}

func waitForNodeReady(t *testing.T, ctx context.Context, nodeURL, rpcToken string) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: 2 * time.Second}
	for {
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
			t.Fatalf("waiting for node: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertGatewayChatEventually(t *testing.T, ctx context.Context, gatewayURL, model, token string) (string, string, string) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		body, instanceID, nodeID, ok := tryGatewayChat(t, ctx, gatewayURL, model, token)
		if ok {
			return body, instanceID, nodeID
		}
		last = body
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for gateway chat: %v last=%s", ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func tryGatewayChat(t *testing.T, ctx context.Context, gatewayURL, model, token string) (string, string, string, bool) {
	t.Helper()
	body := []byte(`{"model":` + quote(model) + `,"messages":[{"role":"user","content":"Say hi."}],"max_tokens":1}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(gatewayURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
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
	if resp.Header.Get(gateway.HeaderNode) == "" || !strings.Contains(string(data), `"choices"`) {
		t.Fatalf("gateway response headers=%+v body=%s", resp.Header, data)
	}
	return string(data), resp.Header.Get(gateway.HeaderInstance), resp.Header.Get(gateway.HeaderNode), true
}

func smokeGatewayToken(specificEnv string) string {
	if specificEnv != "" {
		if token := strings.TrimSpace(os.Getenv(specificEnv)); token != "" {
			return token
		}
	}
	return strings.TrimSpace(os.Getenv("MYCELIUM_GATEWAY_TOKEN"))
}

func unloadJoinedInstance(t *testing.T, ctx context.Context, nodeURL, instanceID, rpcToken string) {
	t.Helper()
	body := []byte(`{"instance_id":` + quote(instanceID) + `}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(nodeURL, "/")+"/unload", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("unload request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+rpcToken)
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

func writePhase4GatewayPeerConfig(t *testing.T, addr, discoveryListen, discoveryAddr, joinToken, rpcToken, parser, model string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway-peer.json")
	cfg := struct {
		Listen               string           `json:"listen"`
		StorePath            string           `json:"store_path"`
		JoinToken            string           `json:"join_token"`
		RPCToken             string           `json:"rpc_token"`
		GGUFParser           string           `json:"gguf_parser"`
		DiscoveryListen      string           `json:"discovery_listen"`
		DiscoveryAddr        string           `json:"discovery_addr"`
		DiscoveryAdvertiseMS int              `json:"discovery_advertise_ms"`
		DefaultProject       string           `json:"default_project"`
		Projects             []domain.Project `json:"projects"`
		Presets              []domain.Preset  `json:"presets"`
	}{
		Listen:               addr,
		StorePath:            filepath.Join(t.TempDir(), "control.sqlite"),
		JoinToken:            joinToken,
		RPCToken:             rpcToken,
		GGUFParser:           parser,
		DiscoveryListen:      discoveryListen,
		DiscoveryAddr:        discoveryAddr,
		DiscoveryAdvertiseMS: 50,
		DefaultProject:       "phase4",
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

func writePhase4ComputePeerConfig(t *testing.T, addr, backendAddr, discoveryListen, discoveryAddr, joinToken, rpcToken, binary, parser, model string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "compute-peer.json")
	cfg := struct {
		Listen               string `json:"listen"`
		StorePath            string `json:"store_path"`
		Compute              bool   `json:"compute"`
		JoinToken            string `json:"join_token"`
		RPCToken             string `json:"rpc_token"`
		DiscoveryListen      string `json:"discovery_listen"`
		DiscoveryAddr        string `json:"discovery_addr"`
		DiscoveryAdvertiseMS int    `json:"discovery_advertise_ms"`
		ComputeConfig        struct {
			ID            string `json:"id"`
			Name          string `json:"name"`
			BackendListen string `json:"backend_listen"`
			LlamaServer   string `json:"llama_server"`
			GGUFParser    string `json:"gguf_parser"`
			VRAMMB        int    `json:"vram_mb"`
		} `json:"compute_config"`
		Presets []domain.Preset `json:"presets"`
	}{
		Listen:               addr,
		StorePath:            filepath.Join(t.TempDir(), "compute.sqlite"),
		Compute:              true,
		JoinToken:            joinToken,
		RPCToken:             rpcToken,
		DiscoveryListen:      discoveryListen,
		DiscoveryAddr:        discoveryAddr,
		DiscoveryAdvertiseMS: 50,
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
	cfg.ComputeConfig.GGUFParser = parser
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

func writePhase4GGUFParser(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gguf-parser")
	script := `#!/bin/sh
cat <<'JSON'
{"format":"gguf","weights_mb":1,"kv_per_token_mb":0.01,"context_length":2048}
JSON
`
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatalf("write phase4 gguf parser: %v", err)
	}
	return path
}

type smokeProcess struct {
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
	done   chan error
	err    error
	exited bool
}

func startSmokeProcess(t *testing.T, ctx context.Context, bin string, args ...string) *smokeProcess {
	t.Helper()
	proc := &smokeProcess{cmd: exec.CommandContext(ctx, bin, args...), done: make(chan error, 1)}
	proc.cmd.Stdout = &proc.stdout
	proc.cmd.Stderr = &proc.stderr
	if err := proc.cmd.Start(); err != nil {
		t.Fatalf("start %s %v: %v", bin, args, err)
	}
	go func() { proc.done <- proc.cmd.Wait() }()
	return proc
}

func (p *smokeProcess) stop(t *testing.T) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	if err, ok := p.pollExit(); ok {
		if err != nil {
			t.Logf("process exited before stop: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
		}
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-p.done:
		p.err, p.exited = err, true
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			t.Logf("process exited: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		err := <-p.done
		p.err, p.exited = err, true
		t.Logf("process killed: %v\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
	}
}

func (p *smokeProcess) pollExit() (error, bool) {
	if p.exited {
		return p.err, true
	}
	if p.done == nil {
		return nil, false
	}
	select {
	case err := <-p.done:
		p.err, p.exited = err, true
		return err, true
	default:
		return nil, false
	}
}
