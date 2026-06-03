//go:build smoke

package smoke

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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
	seedURL := os.Getenv("MYCELIUM_REMOTE_PEER_URL")
	rpcToken := os.Getenv("MYCELIUM_REMOTE_PEER_RPC_TOKEN")
	sshHost := os.Getenv("MYCELIUM_REMOTE_PEER_SSH")
	if seedURL == "" || rpcToken == "" || sshHost == "" {
		t.Skip("set MYCELIUM_REMOTE_PEER_URL, MYCELIUM_REMOTE_PEER_RPC_TOKEN, and MYCELIUM_REMOTE_PEER_SSH for second peer clean-home join smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	join := runSmokeCommandOutput(t, ctx, "go", "run", "./cmd/myce", "nodes", "invite", "--url", seedURL, "--rpc-token", rpcToken)
	if !strings.Contains(join, "mycjoin://") {
		t.Fatalf("invite output = %q", join)
	}
	joinToken := inviteJoinToken(t, join)
	joinRPC := inviteRPCToken(t, join)
	waitForSmokeHTTP(t, ctx, strings.TrimRight(seedURL, "/")+"/peer/health", joinToken)

	mycelium := buildSmokeBinaryFor(t, ctx, "darwin", "arm64")
	workdir := "/tmp/mycelium-clean-home-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	remoteHome := workdir + "/home"
	remoteBin := workdir + "/mycelium"
	remoteConfig := remoteHome + "/.mycelium/peer.json"
	runOperabilitySSH(t, ctx, sshHost, "rm -rf "+shellQuote(workdir)+"; mkdir -p "+shellQuote(remoteHome+"/.mycelium"))
	defer runOperabilitySSH(t, context.Background(), sshHost, "if [ -f "+shellQuote(workdir+"/peer.pid")+" ]; then kill -INT $(cat "+shellQuote(workdir+"/peer.pid")+") 2>/dev/null || true; fi; rm -rf "+shellQuote(workdir))
	scpOperabilityRemote(t, ctx, sshHost, mycelium, remoteBin)
	runOperabilitySSH(t, ctx, sshHost, "chmod +x "+shellQuote(remoteBin))
	runOperabilitySSH(t, ctx, sshHost, "HOME="+shellQuote(remoteHome)+" "+shellQuote(remoteBin)+" bootstrap --join "+shellQuote(strings.TrimSpace(join))+" --compute auto --config "+shellQuote(remoteConfig)+" --apply")
	inspect := runOperabilitySSHOutput(t, ctx, sshHost, "python3 - <<'PY'\nimport json, os, stat\np="+pythonQuote(remoteConfig)+"\nwith open(p) as f: c=json.load(f)\nmode=oct(stat.S_IMODE(os.stat(p).st_mode))\nprint(c.get('listen',''))\nprint(c.get('join_token',''))\nprint(c.get('rpc_token',''))\nprint(' '.join(c.get('seed_peers',[])))\nprint(str(c.get('compute')).lower())\nprint(mode)\nPY")
	lines := strings.Split(strings.TrimSpace(inspect), "\n")
	if len(lines) != 6 {
		t.Fatalf("bootstrap inspection = %q", inspect)
	}
	listen, gotJoin, gotRPC, seeds, computeRaw, mode := lines[0], lines[1], lines[2], lines[3], lines[4], lines[5]
	if listen == "" || gotJoin != joinToken || gotRPC != joinRPC || !strings.Contains(seeds, seedHost(t, seedURL)) || mode != "0o600" {
		t.Fatalf("bootstrap config listen=%q join=%q rpc=%q seeds=%q mode=%q", listen, gotJoin, gotRPC, seeds, mode)
	}
	if computeRaw != "true" && computeRaw != "false" {
		t.Fatalf("bootstrap compute status = %q", computeRaw)
	}
	discoveryPort := operabilityRemoteFreePorts(t, ctx, sshHost, 1)[0]
	runOperabilitySSH(t, ctx, sshHost, "python3 - <<'PY'\nimport json\np="+pythonQuote(remoteConfig)+"\nwith open(p) as f: c=json.load(f)\nc['discovery_listen']=':"+discoveryPort+"'\nc['discovery_addr']='255.255.255.255:"+discoveryPort+"'\nwith open(p, 'w') as f:\n    json.dump(c, f, indent=2)\n    f.write('\\n')\nPY")
	remote := "cd " + shellQuote(workdir) + "; echo $$ > peer.pid; HOME=" + shellQuote(remoteHome) + " exec " + shellQuote(remoteBin) + " run --config " + shellQuote(remoteConfig)
	proc := &smokeProcess{cmd: exec.CommandContext(ctx, "ssh", operabilitySSHArgs(sshHost, remote)...)}
	if pass := operabilitySSHPass(); pass != "" {
		proc.cmd = exec.CommandContext(ctx, "sshpass", append([]string{"-e", "ssh"}, operabilitySSHArgs(sshHost, remote)...)...)
		proc.cmd.Env = append(os.Environ(), "SSHPASS="+pass)
	}
	proc.cmd.Stdout = &proc.stdout
	proc.cmd.Stderr = &proc.stderr
	if err := proc.cmd.Start(); err != nil {
		t.Fatalf("start clean-home second peer peer: %v", err)
	}
	defer proc.stopRemoteSSH(t)
	waitForSmokeHTTP(t, ctx, "http://"+listen+"/peer/health", joinToken)
	waitForSmokeRPCDiagnostics(t, ctx, "http://"+listen+"/peer/diagnostics", joinRPC)
}

func TestFleetLocalitySmoke(t *testing.T) {
	dbPath := os.Getenv("MYCELIUM_LOCALITY_DB")
	rpcToken := os.Getenv("MYCELIUM_LOCALITY_RPC_TOKEN")
	peers := strings.Fields(os.Getenv("MYCELIUM_LOCALITY_PEER_URLS"))
	if dbPath == "" || rpcToken == "" || len(peers) == 0 {
		t.Skip("set MYCELIUM_LOCALITY_DB, MYCELIUM_LOCALITY_RPC_TOKEN, and MYCELIUM_LOCALITY_PEER_URLS for fleet locality smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	planID := "smoke-locality-" + time.Now().UTC().Format("20060102150405")
	args := []string{"run", "./cmd/myce", "models", "locality", "plan", "--db", dbPath, "--id", planID, "--rpc-token", rpcToken}
	for _, peer := range peers {
		args = append(args, "--peer-url", peer)
	}
	out := runSmokeCommandOutput(t, ctx, "go", args...)
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

func inviteJoinToken(t *testing.T, output string) string {
	t.Helper()
	raw := strings.TrimSpace(output)
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse invite %q: %v", raw, err)
	}
	token := parsed.Query().Get("token")
	if token == "" {
		t.Fatalf("invite output missing token: %q", raw)
	}
	return token
}

func inviteRPCToken(t *testing.T, output string) string {
	t.Helper()
	parsed, err := url.Parse(strings.TrimSpace(output))
	if err != nil {
		t.Fatalf("parse invite %q: %v", output, err)
	}
	token := parsed.Query().Get("rpc_token")
	if token == "" {
		t.Fatalf("invite output missing rpc_token: %q", output)
	}
	return token
}

func seedHost(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse seed url %q: %v", raw, err)
	}
	return parsed.Host
}

func waitForSmokeRPCDiagnostics(t *testing.T, ctx context.Context, rawURL, rpcToken string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(2 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			t.Fatalf("diagnostics request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+rpcToken)
		resp, err := client.Do(req)
		if err == nil {
			data, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			last = string(data)
			if resp.StatusCode >= 200 && resp.StatusCode < 300 && strings.Contains(last, `"ready":true`) {
				return
			}
		} else {
			last = err.Error()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for diagnostics %s: %s", rawURL, last)
}

func runOperabilitySSH(t *testing.T, ctx context.Context, sshHost, command string) {
	t.Helper()
	_ = runOperabilitySSHOutput(t, ctx, sshHost, command)
}

func runOperabilitySSHOutput(t *testing.T, ctx context.Context, sshHost, command string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "ssh", operabilitySSHArgs(sshHost, command)...)
	if pass := operabilitySSHPass(); pass != "" {
		cmd = exec.CommandContext(ctx, "sshpass", append([]string{"-e", "ssh"}, operabilitySSHArgs(sshHost, command)...)...)
		cmd.Env = append(os.Environ(), "SSHPASS="+pass)
	}
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh %s %q: %v\n%s", sshHost, command, err, data)
	}
	return strings.TrimSpace(string(data))
}

func scpOperabilityRemote(t *testing.T, ctx context.Context, sshHost, local, remote string) {
	t.Helper()
	args := []string{"-o", "StrictHostKeyChecking=no"}
	if operabilitySSHPass() == "" {
		args = append(args, "-o", "BatchMode=yes")
	} else {
		args = append(args, "-o", "PreferredAuthentications=password", "-o", "PubkeyAuthentication=no", "-o", "NumberOfPasswordPrompts=1")
	}
	args = append(args, local, sshHost+":"+remote)
	cmd := exec.CommandContext(ctx, "scp", args...)
	if pass := operabilitySSHPass(); pass != "" {
		cmd = exec.CommandContext(ctx, "sshpass", append([]string{"-e", "scp"}, args...)...)
		cmd.Env = append(os.Environ(), "SSHPASS="+pass)
	}
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scp %s to %s:%s: %v\n%s", local, sshHost, remote, err, data)
	}
}

func operabilityRemoteFreePorts(t *testing.T, ctx context.Context, sshHost string, count int) []string {
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
	out := runOperabilitySSHOutput(t, ctx, sshHost, script)
	fields := strings.Fields(out)
	if len(fields) != count {
		t.Fatalf("remote free ports = %q", out)
	}
	return fields
}

func operabilitySSHArgs(sshHost, command string) []string {
	args := []string{"-o", "StrictHostKeyChecking=no"}
	if operabilitySSHPass() == "" {
		args = append(args, "-o", "BatchMode=yes")
	} else {
		args = append(args, "-o", "PreferredAuthentications=password", "-o", "PubkeyAuthentication=no", "-o", "NumberOfPasswordPrompts=1")
	}
	return append(args, sshHost, command)
}

func operabilitySSHPass() string {
	if pass := os.Getenv("MYCELIUM_REMOTE_PEER_SSHPASS"); pass != "" {
		return pass
	}
	return os.Getenv("SSHPASS")
}

func pythonQuote(value string) string {
	return "r'" + strings.ReplaceAll(value, "'", "\\'") + "'"
}
