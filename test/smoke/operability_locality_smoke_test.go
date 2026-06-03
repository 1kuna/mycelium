//go:build smoke

package smoke

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestOperabilityLocalServiceSmoke(t *testing.T) {
	if os.Getenv("MYCELIUM_SMOKE_OPERABILITY_APPLY") != "1" {
		t.Skip("set MYCELIUM_SMOKE_OPERABILITY_APPLY=1 to install/start/uninstall a real local Mycelium user service")
	}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("local service smoke supports macOS launchd and Linux systemd")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	bin := buildSmokeBinary(t, ctx)
	home := t.TempDir()
	configPath := filepath.Join(home, ".mycelium", "peer.json")
	listen := freeAddr(t)
	name := "smoke-" + strings.ReplaceAll(filepath.Base(home), ".", "-")
	runSmokeCommand(t, ctx, bin, "config", "init", "--config", configPath, "--compute", "off", "--listen", listen, "--backend", "auto", "--max-util", "0.8", "--disk-min-free-ratio", "0.25")
	patchSmokeConfig(t, configPath, map[string]any{
		"join_token":          "operability-smoke",
		"rpc_token":           "operability-smoke-rpc",
		"store_path":          filepath.Join(home, ".mycelium", "control.db"),
		"catalog_dir":         filepath.Join(home, ".mycelium", "catalog"),
		"discovery_listen":    freeAddr(t),
		"discovery_addr":      "127.0.0.1:" + strings.Split(freeAddr(t), ":")[1],
		"engine_profiles":     []any{},
		"projects":            []any{},
		"presets":             []any{},
		"reservations":        []any{},
		"submitter_policy":    map[string]any{},
		"private_storage_key": "",
	})
	runSmokeCommand(t, ctx, bin, "bootstrap", "--doctor", "--config", configPath, "--json")
	defer runSmokeCommand(t, context.Background(), bin, "service", "uninstall", "--config", configPath, "--user", "--name", name)
	runSmokeCommand(t, ctx, bin, "service", "install", "--config", configPath, "--user", "--name", name)
	waitForSmokeHTTP(t, ctx, "http://"+listen+"/peer/health", "operability-smoke")
	runSmokeCommand(t, ctx, bin, "service", "status", "--config", configPath, "--user", "--name", name)
}

func TestRemotePeerCleanHomeJoinSmoke(t *testing.T) {
	url := os.Getenv("MYCELIUM_REMOTE_PEER_URL")
	token := os.Getenv("MYCELIUM_REMOTE_PEER_RPC_TOKEN")
	if url == "" || token == "" {
		t.Skip("set MYCELIUM_REMOTE_PEER_URL and MYCELIUM_REMOTE_PEER_RPC_TOKEN for second peer join smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	join := runSmokeCommandOutput(t, ctx, "go", "run", "./cmd/myce", "nodes", "invite", "--url", url, "--rpc-token", token)
	if !strings.Contains(join, "mycjoin://") {
		t.Fatalf("invite output = %q", join)
	}
	waitForSmokeHTTP(t, ctx, strings.TrimRight(url, "/")+"/peer/health", "")
}

func TestFleetLocalitySmoke(t *testing.T) {
	dbPath := os.Getenv("MYCELIUM_LOCALITY_DB")
	rpcToken := os.Getenv("MYCELIUM_LOCALITY_RPC_TOKEN")
	if dbPath == "" || rpcToken == "" {
		t.Skip("set MYCELIUM_LOCALITY_DB and MYCELIUM_LOCALITY_RPC_TOKEN for fleet locality smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	planID := "smoke-locality-" + time.Now().UTC().Format("20060102150405")
	out := runSmokeCommandOutput(t, ctx, "go", "run", "./cmd/myce", "models", "locality", "plan", "--db", dbPath, "--id", planID)
	if !strings.Contains(out, "locality-plan\t"+planID) {
		t.Fatalf("locality plan output = %q", out)
	}
	runSmokeCommand(t, ctx, "go", "run", "./cmd/myce", "models", "locality", "apply", "--db", dbPath, "--id", planID, "--rpc-token", rpcToken)
	report := runSmokeCommandOutput(t, ctx, "go", "run", "./cmd/myce", "models", "locality", "report", "--db", dbPath)
	if strings.TrimSpace(report) == "" {
		t.Fatalf("locality report is empty after apply")
	}
}

func runSmokeCommand(t *testing.T, ctx context.Context, command string, args ...string) {
	t.Helper()
	_ = runSmokeCommandOutput(t, ctx, command, args...)
}

func runSmokeCommandOutput(t *testing.T, ctx context.Context, command string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", command, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out)
}

func patchSmokeConfig(t *testing.T, path string, values map[string]any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	for key, value := range values {
		raw[key] = value
	}
	data, err = json.MarshalIndent(raw, "", "  ")
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func waitForSmokeHTTP(t *testing.T, ctx context.Context, url, joinToken string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("health request: %v", err)
		}
		if joinToken != "" {
			req.Header.Set("X-Myc-Join-Token", joinToken)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
			lastErr = os.ErrInvalid
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %v", url, lastErr)
}
