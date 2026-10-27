package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// The ordinary generic-server adapter supervises this small loopback backend.
// A file releases the stream naturally; no GPU/model or fake queue call exists.
func TestFleetUpgradeModelStreamCommand(t *testing.T) {
	if os.Getenv("FLEET_STREAM_CHILD") != "1" {
		return
	}
	if os.Getenv("MYCE_TOKEN") != "" {
		t.Fatal("model server inherited fleet authority")
	}
	port, dir := os.Args[len(os.Args)-2], os.Args[len(os.Args)-1]
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"status":"ok"}`) })
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"stream started\"}}]}\n\n")
		w.(http.Flusher).Flush()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			if _, err := os.Stat(filepath.Join(dir, "finish-stream")); err == nil {
				break
			}
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
			}
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"stream finished naturally\"}}]}\n\ndata: [DONE]\n\n")
	})
	if err := http.ListenAndServe("127.0.0.1:"+port, mux); err != nil {
		t.Fatal(err)
	}
}

// Extend onboarding's fake managers only with the finite-helper launch. The
// HTTP manager owns and reaps both services and helpers, so a service restart
// cannot terminate its updater. No native service manager is invoked.
const fleetFakeManagerPrefix = `#!/bin/sh
set -eu
if test "$(basename "$0")" = systemd-run || { test "$(basename "$0")" = launchctl && test "$1" = submit; }; then
  /usr/bin/curl --noproxy '*' --fail --silent --show-error --request POST "$(cat "$HOME/fake-manager.url")/helper"
  exit
fi
`

type fleetE2EManager struct {
	*enrollmentTestManager
	config  myce.ServiceConfig
	helpers []*enrollmentTestChild
	starts  int
}

func fleetE2EEnv(home string) []string {
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != "HOME" && key != "PATH" && !strings.HasPrefix(key, "MYCE_") {
			env = append(env, entry)
		}
	}
	return append(env, "HOME="+home, "PATH="+filepath.Join(home, "bin")+":/usr/bin:/bin:/usr/sbin:/sbin", "GOMAXPROCS=2")
}

func newFleetE2EManager(t *testing.T, c myce.ServiceConfig, home string) *fleetE2EManager {
	t.Helper()
	m := &fleetE2EManager{enrollmentTestManager: &enrollmentTestManager{home: home}, config: c}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		var err error
		switch r.URL.Path {
		case "/start":
			if m.current != nil {
				select {
				case <-m.current.done:
					m.current = nil
				default:
					fmt.Fprintln(w, m.current.cmd.Process.Pid)
					return
				}
			}
			var child *enrollmentTestChild
			child, err = m.spawn("service", "--config", c.Config)
			if err == nil {
				m.current = child
				m.starts++
				fmt.Fprintln(w, child.cmd.Process.Pid)
			}
		case "/stop":
			err = m.stop()
		case "/helper":
			var child *enrollmentTestChild
			child, err = m.spawn("fleet-apply", "--config", c.Config, "--receipt", filepath.Join(c.State, "fleet-update.json"))
			if err == nil {
				m.helpers = append(m.helpers, child)
			}
		default:
			w.WriteHeader(404)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
		}
	}))
	t.Cleanup(func() {
		// Helpers must settle before removing their service or private fixture.
		m.mu.Lock()
		helpers := append([]*enrollmentTestChild(nil), m.helpers...)
		m.mu.Unlock()
		for _, child := range helpers {
			select {
			case <-child.done:
			case <-time.After(70 * time.Second):
				_ = child.cmd.Process.Kill()
				<-child.done
				t.Error("finite update helper did not finish")
			}
		}
		m.mu.Lock()
		if err := m.stop(); err != nil {
			t.Error(err)
		}
		for _, child := range m.children {
			select {
			case <-child.done:
			default:
				t.Error("fake manager left an unreaped child")
			}
		}
		m.mu.Unlock()
		server.Close()
		if t.Failed() {
			data, _ := os.ReadFile(filepath.Join(home, "fake-service.log"))
			t.Logf("%s service/helper log: %s", c.HostID, data)
		}
	})
	fleetE2EWrite(t, filepath.Join(home, "fake-manager.url"), []byte(server.URL), 0600)
	manager := fleetFakeManagerPrefix + strings.TrimPrefix(enrollmentFakeManager, "#!/bin/sh\n")
	manager = strings.ReplaceAll(manager, "$HOME/.local/share/mycelium/worker", "$HOME/.local/share/mycelium/"+c.Role)
	for _, name := range []string{"systemctl", "systemd-run", "launchctl", "codesign"} {
		fleetE2EWrite(t, filepath.Join(home, "bin", name), []byte(manager), 0700)
	}
	return m
}

func (m *fleetE2EManager) spawn(args ...string) (*enrollmentTestChild, error) {
	cmd := exec.Command(m.config.Binary, args...)
	cmd.Env = fleetE2EEnv(m.home)
	output, err := os.OpenFile(filepath.Join(m.home, "fake-service.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	cmd.Stdout, cmd.Stderr = output, output
	if err = cmd.Start(); err != nil {
		_ = output.Close()
		return nil, err
	}
	child := &enrollmentTestChild{cmd: cmd, done: make(chan struct{})}
	m.children = append(m.children, child)
	go func() { _ = cmd.Wait(); _ = output.Close(); close(child.done) }()
	return child, nil
}

func fleetE2EWrite(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
}

func (m *fleetE2EManager) command(ctx context.Context, name string, args ...string) ([]byte, error) {
	if !filepath.IsAbs(name) {
		name = filepath.Join(m.home, "bin", name)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = fleetE2EEnv(m.home)
	return cmd.CombinedOutput()
}

func (m *fleetE2EManager) process() (pid, starts int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current != nil {
		select {
		case <-m.current.done:
		default:
			pid = m.current.cmd.Process.Pid
		}
	}
	return pid, m.starts
}

// Dev-style acceptance uses the real queue API and command runner with fake
// inventory, as myce dev does, but separate installed executable services make
// the running-image/PID evidence meaningful across binary replacement.
func TestFleetUpgradeDevSignedReleasesEndToEnd(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("fake manager supports Darwin and Linux")
	}
	// This bounds a broken acceptance harness, never production work/drain time.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	dir := t.TempDir()
	const token = "fleet-e2e-test-authority"
	const oldVersion, newVersion = "v99.244.1", "v99.244.2"
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	type release struct {
		binary, hash string
		signed       []byte
	}
	buildRelease := func(version string) release {
		t.Helper()
		path := filepath.Join(dir, version, "myce")
		cmd := exec.CommandContext(ctx, "go", "build", "-p", "1", "-tags=myce_test", "-ldflags", "-X mycelium/internal/myce.BuildVersion="+version, "-o", path, ".")
		cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build signed fixture: %v\n%s", err, out)
		}
		hash, err := myce.FileSHA256(path)
		if err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		manifest := myce.ReleaseManifest{Version: version, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol}
		for _, target := range [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "arm64"}} {
			a := myce.ReleaseArtifact{OS: target[0], Arch: target[1], File: "myce-" + target[0] + "-" + target[1], SHA256: strings.Repeat("a", 64), Size: 1, NativeSensing: target[0] == "darwin"}
			if a.OS == runtime.GOOS && a.Arch == runtime.GOARCH {
				a.SHA256, a.Size = hash, st.Size()
			}
			manifest.Artifacts = append(manifest.Artifacts, a)
		}
		raw, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		signed, err := json.Marshal(myce.SignedRelease{Manifest: raw, Signature: ed25519.Sign(private, raw)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = myce.VerifyRelease(signed, pub); err != nil {
			t.Fatal(err)
		}
		return release{path, hash, signed}
	}
	old, target := buildRelease(oldVersion), buildRelease(newVersion)
	if old.hash == target.hash {
		t.Fatal("release binaries must differ")
	}
	var publisherMu sync.RWMutex
	published := old.binary
	publisher := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		publisherMu.RLock()
		path := published
		publisherMu.RUnlock()
		if r.Header.Get("Authorization") != "" {
			t.Error("fleet credential disclosed to publisher")
		}
		http.ServeFile(w, r, path)
	}))
	t.Cleanup(publisher.Close)
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: publisher.Certificate().Raw}))
	nas := filepath.Join(dir, "nas")
	if err = os.Mkdir(nas, 0700); err != nil {
		t.Fatal(err)
	}
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	modelPath := filepath.Join(nas, "mycelium", "models", "fleet-model")
	fleetE2EWrite(t, modelPath, []byte("x"), 0600)
	modelHash, err := myce.FileSHA256(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	model := myce.Model{Name: "fleet-stream", Engine: "server", Path: "fleet-model", DirectNAS: true, SizeBytes: 1, SHA256: modelHash, MemoryGB: 1, Platforms: []string{"fake"}, AllowedMachines: []string{"worker-3"}, StartCommand: []string{"sh", "-c", `export MYCE_TEST_DOCKER_BIN="$1" MYCE_TEST_ISOLATED_HOME="$2"; exec "$3" -test.run=^TestFleetUpgradeModelStreamCommand$ -- "$4" "$5"`, "fleet-stream", os.Getenv("MYCE_TEST_DOCKER_BIN"), os.Getenv("MYCE_TEST_ISOLATED_HOME"), helper, "{port}", dir}, HealthURL: "http://127.0.0.1:{port}/health", Env: map[string]string{"FLEET_STREAM_CHILD": "1"}}
	models, err := json.Marshal([]myce.Model{model})
	if err != nil {
		t.Fatal(err)
	}
	catalog := filepath.Join(dir, "models.yaml")
	fleetE2EWrite(t, catalog, append([]byte("models: "), append(models, '\n')...), 0600)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	client := myce.Client{URL: "http://" + address, Token: token, HTTP: &http.Client{Timeout: 2 * time.Second}}
	var managers []*fleetE2EManager
	install := func(role, host string) *fleetE2EManager {
		t.Helper()
		home := filepath.Join(dir, host)
		if err := os.Mkdir(home, 0700); err != nil {
			t.Fatal(err)
		}
		c, err := myce.PlanInstall(role, home, "", nas, client.URL, host, myce.Detection{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "fake", MemoryMode: "fixed", MemoryGB: 8})
		if err != nil {
			t.Fatal(err)
		}
		c.HostID, c.Listen, c.Heartbeat = host, address, 100*time.Millisecond
		c.Path = filepath.Join(home, "bin") + ":/usr/bin:/bin:/usr/sbin:/sbin"
		c.AllowDeviceless = true
		c.ModelCatalog = catalog
		m := newFleetE2EManager(t, c, home)
		managers = append(managers, m)
		if role == "queue" {
			// Publisher trust is provisioned locally, as on first installation;
			// the authenticated approval API cannot introduce its own trust key.
			if err := os.MkdirAll(c.Checkpoints, 0700); err != nil {
				t.Fatal(err)
			}
			q, err := myce.Open(c.DB, c.Checkpoints, myce.RealClock{})
			if err != nil {
				t.Fatal(err)
			}
			err = q.ConfigureStartupEnrollmentRelease(old.signed, pub, publisher.URL, ca, false)
			closeErr := q.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("provision local publisher trust: %v %v", err, closeErr)
			}
		}
		if err = myce.Install(ctx, c, old.binary, token, m.command); err != nil {
			t.Fatalf("install %s: %v", host, err)
		}
		return m
	}
	queue := install("queue", "queue")
	var snapshot myce.Snapshot
	var lastErr error
	poll := func(description string, predicate func() bool) {
		t.Helper()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			if predicate() {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("waiting for %s: %v; last API error=%v; status=%+v", description, ctx.Err(), lastErr, snapshot.FleetUpgrade)
			case <-ticker.C:
			}
		}
	}
	status := func() bool {
		snapshot = myce.Snapshot{}
		lastErr = client.Call(ctx, "GET", "/api/status", nil, &snapshot)
		return lastErr == nil
	}
	poll("queue startup", func() bool {
		if pid, _ := queue.process(); pid == 0 {
			t.Fatal("queue service exited during startup")
		}
		return status()
	})
	approve := func(r release) {
		t.Helper()
		publisherMu.Lock()
		published = r.binary
		publisherMu.Unlock()
		err := client.Call(ctx, "POST", "/api/invites/release", myce.EnrollmentReleaseApproval{SignedRelease: r.signed, PublicKey: pub, Origin: publisher.URL, CACert: ca}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	approve(old)
	for i := 1; i <= 3; i++ {
		install("worker", fmt.Sprintf("worker-%d", i))
	}
	poll("three actual worker runtime proofs", func() bool {
		if !status() || len(snapshot.Machines) != 3 {
			return false
		}
		for _, m := range snapshot.Machines {
			if m.Fleet == nil || !m.Fleet.ServiceHealthy || !m.Fleet.RunnerSmoke || m.Fleet.SHA256 != old.hash {
				return false
			}
		}
		return true
	})
	before := map[string]myce.Machine{}
	t.Cleanup(func() {
		// Release owned command fixtures before service cleanup, even on failure.
		for _, m := range managers[1:] {
			fleetE2EWrite(t, filepath.Join(dir, "finish-"+m.config.Name), []byte("cleanup\n"), 0600)
		}
		fleetE2EWrite(t, filepath.Join(dir, "finish-stream"), []byte("cleanup\n"), 0600)
	})
	for _, m := range snapshot.Machines {
		before[m.ID] = *m
	}
	queuePID, _ := queue.process()
	jobs := map[string]myce.Job{}
	for _, m := range snapshot.Machines {
		spec := myce.Spec{Name: "rollout-command-" + m.ID, Project: "fleet-test", Priority: "production", AllowedMachines: []string{m.ID}}
		spec.Needs.MemoryGB, spec.Needs.Platform.Any = 1, true
		// The command controls its own completion; the rollout never signals it.
		spec.Run.Command = []string{"sh", "-c", `test -z "${MYCE_TOKEN:-}" || exit 91; printf '%s\n' "$MYCE_ATTEMPT" >> "$1/starts-$2"; while ! test -f "$1/finish-$2"; do sleep 0.05; done; printf 'completed naturally\n'`, "fleet-command", dir, m.ID}
		var job myce.Job
		if err = client.Call(ctx, "POST", "/api/jobs", spec, &job); err != nil {
			t.Fatal(err)
		}
		jobs[job.ID] = job
	}
	poll("all three real commands started", func() bool {
		if !status() {
			return false
		}
		for id := range jobs {
			found := false
			for _, j := range snapshot.Jobs {
				if j.ID == id && j.State == "running" && j.StartedAt != nil {
					jobs[id] = *j
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
	req, err := http.NewRequestWithContext(ctx, "POST", client.URL+"/v1/chat/completions", strings.NewReader(`{"model":"fleet-stream","stream":true,"messages":[{"role":"user","content":"hold stream during rollout"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Myce-Project", "fleet-test")
	req.Header.Set("X-Myce-Priority", "production")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	stream := bufio.NewReader(response.Body)
	first, err := stream.ReadString('\n')
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(first, "stream started") {
		t.Fatalf("real model stream: HTTP %d first=%q err=%v", response.StatusCode, first, err)
	}
	approve(target)
	// The operator is an independent, short-lived CLI process. Its exit removes
	// the operator connection while the queue continues durable coordination.
	upgrade := func() myce.FleetUpgradeStatus {
		t.Helper()
		cmd := exec.CommandContext(ctx, old.binary, "fleet", "upgrade", "--release", newVersion, "--queue", client.URL, "--token", token)
		cmd.Env = fleetE2EEnv(queue.home)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("operator CLI: %v %s", err, out)
		}
		var result myce.FleetUpgradeStatus
		if err = json.Unmarshal(out, &result); err != nil {
			t.Fatalf("operator status: %v %s", err, out)
		}
		return result
	}
	operation := upgrade()
	poll("visible in-flight model-call cutover wait", func() bool {
		if !status() || snapshot.FleetUpgrade == nil {
			return false
		}
		if snapshot.FleetUpgrade.State == "stopped" {
			t.Fatalf("upgrade stopped: %+v", snapshot.FleetUpgrade)
		}
		for _, host := range snapshot.FleetUpgrade.Hosts {
			if host.Host == "queue" && strings.Contains(host.Wait, "model calls") {
				return true
			}
		}
		return false
	})
	if pid, starts := queue.process(); pid != queuePID || starts != 1 {
		t.Error("queue restarted before admitted model stream finished")
	}
	streamRerun := upgrade()
	if streamRerun.Operation != operation.Operation {
		t.Error("rerun replaced operation during cutover wait")
	}
	fleetE2EWrite(t, filepath.Join(dir, "finish-stream"), []byte("finish naturally\n"), 0600)
	rest, err := io.ReadAll(stream)
	if err != nil || !bytes.Contains(rest, []byte("stream finished naturally")) || !bytes.Contains(rest, []byte("[DONE]")) {
		t.Fatalf("admitted model stream interrupted: %q %v", rest, err)
	}
	poll("queue restarted while command attempts remain running", func() bool {
		if !status() {
			return false
		}
		pid, starts := queue.process()
		return pid != 0 && pid != queuePID && starts == 2 && snapshot.FleetUpgrade != nil && snapshot.FleetUpgrade.Hosts[0].Phase == "complete"
	})
	assertAttempts := func() {
		t.Helper()
		for id, initial := range jobs {
			var observed *myce.Job
			for _, j := range snapshot.Jobs {
				if j.ID == id {
					observed = j
				}
			}
			if observed == nil {
				t.Errorf("job %s lost", id)
				continue
			}
			if observed.Attempt != initial.Attempt || observed.Starts != 1 || observed.Losses != 0 || observed.InfrastructureRetries != 0 || observed.Session != initial.Session || observed.Machine != initial.Machine || observed.StartedAt == nil || !observed.StartedAt.Equal(*initial.StartedAt) {
				t.Errorf("attempt identity or continuity changed: before=%+v after=%+v", initial, observed)
			}
			if observed.State == "failed" || observed.State == "cancelled" {
				t.Errorf("rollout interrupted command: %+v", observed)
			}
		}
	}
	assertAttempts()
	rerun := upgrade()
	if rerun.Operation != operation.Operation {
		t.Errorf("operator rerun replaced durable operation: %s -> %s", operation.Operation, rerun.Operation)
	}
	if pid, starts := queue.process(); pid == queuePID || starts != 2 {
		t.Error("operator rerun repeated queue restart")
	}
	// Each worker must retain its old PID/session until its command finishes.
	for _, m := range managers[1:] {
		if pid, starts := m.process(); pid != before[m.config.Name].Fleet.PID || starts != 1 {
			t.Errorf("busy worker restarted: %s", m.config.Name)
		}
	}
	for _, m := range managers[1:] {
		fleetE2EWrite(t, filepath.Join(dir, "finish-"+m.config.Name), []byte("finish naturally\n"), 0600)
	}
	poll("all commands succeeded and signed rollout completed", func() bool {
		if !status() {
			return false
		}
		if snapshot.FleetUpgrade != nil && snapshot.FleetUpgrade.State == "stopped" {
			t.Fatalf("upgrade stopped: %+v", snapshot.FleetUpgrade)
		}
		if snapshot.FleetUpgrade == nil || snapshot.FleetUpgrade.State != "complete" {
			return false
		}
		for id := range jobs {
			found := false
			for _, j := range snapshot.Jobs {
				if j.ID == id && j.State == "succeeded" {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
	assertAttempts()
	for _, m := range snapshot.Machines {
		old := before[m.ID]
		if m.Fleet == nil || m.Fleet.SHA256 != target.hash || m.Fleet.Version != newVersion || m.Fleet.PID == old.Fleet.PID || m.Session == old.Session || m.Fleet.Session != m.Session || !m.Fleet.ServiceHealthy || !m.Fleet.RunnerSmoke {
			t.Errorf("worker did not activate actual target runtime: %+v", m)
		}
		data, err := os.ReadFile(filepath.Join(dir, "starts-"+m.ID))
		if err != nil || string(data) != "1\n" {
			t.Errorf("real command executed more than once: %q %v", data, err)
		}
	}
	for _, m := range managers {
		if _, starts := m.process(); starts != 2 {
			t.Errorf("%s service starts=%d, want 2", m.config.HostID, starts)
		}
		hash, err := myce.FileSHA256(m.config.Binary)
		if err != nil || hash != target.hash {
			t.Errorf("%s installed target differs: %s %v", m.config.HostID, hash, err)
		}
		for _, file := range []string{"fake-service.log", "fake-manager.log"} {
			data, _ := os.ReadFile(filepath.Join(m.home, file))
			if bytes.Contains(data, []byte(token)) {
				t.Errorf("test authority leaked in %s", file)
			}
		}
	}
	final := upgrade()
	if final.Operation != operation.Operation || final.State != "complete" {
		t.Errorf("completed rerun lost operation: %+v", final)
	}
	poll("finite helpers settled after stable manager health", func() bool {
		for _, m := range managers {
			m.mu.Lock()
			helpers := append([]*enrollmentTestChild(nil), m.helpers...)
			m.mu.Unlock()
			if len(helpers) != 1 {
				return false
			}
			select {
			case <-helpers[0].done:
			default:
				return false
			}
		}
		return true
	})
	for _, m := range managers {
		m.mu.Lock()
		for _, child := range m.helpers {
			if !child.cmd.ProcessState.Success() {
				t.Errorf("%s updater failed: %s", m.config.HostID, child.cmd.ProcessState)
			}
		}
		m.mu.Unlock()
	}
	t.Logf("two locally signed releases; queue and three real workers restarted once; three commands completed with unchanged attempt/session and no losses or retries")
}
