package myce

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Swap the actual executable of a live subprocess before its first hash. This
// catches both accidentally reopening os.Executable and caching the disk file
// only when the first fleet observation happens.
func TestFleetRunningImageSurvivesDiskSwap(t *testing.T) {
	if os.Getenv("MYCE_TEST_FLEET_IMAGE") == "1" {
		fmt.Println("ready")
		if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
			t.Fatal(err)
		}
		before, err := fleetStartupImage.sha256()
		if err != nil {
			t.Fatal(err)
		}
		after, err := fleetStartupImage.sha256()
		if err != nil || before != after {
			t.Fatal("running image evidence changed", err)
		}
		fmt.Println(before)
		return
	}
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	dir := t.TempDir()
	installed := filepath.Join(dir, "worker")
	f, err := os.OpenFile(installed, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(f, source)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
	want, err := FileSHA256(installed)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, installed, "-test.run=^TestFleetRunningImageSurvivesDiskSwap$")
	cmd.Env = append(withoutMyceToken(os.Environ()), "MYCE_TEST_FLEET_IMAGE=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	reader := bufio.NewScanner(stdout)
	if !reader.Scan() || reader.Text() != "ready" {
		t.Fatal("child did not reach startup", reader.Text(), reader.Err(), stderr.String())
	}
	replacement := filepath.Join(dir, "replacement")
	if err := os.WriteFile(replacement, []byte("different on-disk executable"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, installed); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(stdin, "observe\n"); err != nil {
		t.Fatal(err)
	}
	_ = stdin.Close()
	if !reader.Scan() || reader.Text() != want {
		t.Fatal("proof did not hash running image after disk swap", reader.Text(), reader.Err())
	}
	_, _ = io.Copy(io.Discard, stdout)
	if err := cmd.Wait(); err != nil {
		t.Fatal("child failed", err, stderr.String())
	}
	if disk, err := FileSHA256(installed); err != nil || disk == want {
		t.Fatal("fixture did not replace disk image", err)
	}
}

func TestFleetPublisherBootstrapNeverReplacesTrust(t *testing.T) {
	key := bytes.Repeat([]byte{1}, ed25519.PublicKeySize)
	rotated := bytes.Repeat([]byte{2}, ed25519.PublicKeySize)
	state := t.TempDir()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/api/fleet/publisher" || r.Header.Get("Authorization") != "Bearer test" {
			t.Error("bootstrap did not use authenticated control path")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"public_key": key})
	}))
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	got, err := provisionFleetPublisher(context.Background(), state, client)
	if err != nil || !bytes.Equal(got, key) {
		t.Fatal("first bootstrap", got, err)
	}
	// Local rotation is authoritative, including when the queue still serves
	// the old bootstrap key. No second request or overwrite is permitted.
	path := filepath.Join(state, "fleet-publisher.pub")
	if err := os.WriteFile(path, rotated, 0600); err != nil {
		t.Fatal(err)
	}
	got, err = provisionFleetPublisher(context.Background(), state, client)
	if err != nil || !bytes.Equal(got, rotated) || requests.Load() != 1 {
		t.Fatal("bootstrap replaced locally provisioned trust", got, requests.Load(), err)
	}
	if err := os.WriteFile(path, []byte("incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := provisionFleetPublisher(context.Background(), state, client); err == nil || requests.Load() != 1 {
		t.Fatal("invalid existing trust was repaired remotely", err)
	}
}

func TestFleetOrdinaryWorkerNeverActivates(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		t.Error("ordinary worker contacted fleet endpoint")
	}))
	defer server.Close()
	w := Worker{Client: Client{URL: server.URL, Token: "test"}, Machine: Machine{ID: "worker", HostID: "host", Session: "session"}}
	runtime := w.newFleetRuntime()
	if runtime != nil || runtime.observe(context.Background(), w.Machine, true) != nil || requests.Load() != 0 {
		t.Fatal("worker without installed config gained activation authority")
	}
	if p, err := CollectFleetRuntime(context.Background(), ServiceConfig{}, "session"); p != nil || err == nil {
		t.Fatal("missing installed service gained runtime proof")
	}
}

func fleetRuntimeTestConfig(t *testing.T) ServiceConfig {
	t.Helper()
	state := t.TempDir()
	return ServiceConfig{Role: "worker", State: state, Config: filepath.Join(state, "service.json"), Service: "myce-worker.service", Detection: Detection{OS: "linux"}}
}

func TestFleetRuntimeManagerMustOwnExactPID(t *testing.T) {
	c := fleetRuntimeTestConfig(t)
	command := func(pid int) func(context.Context, string, ...string) ([]byte, error) {
		return func(context.Context, string, ...string) ([]byte, error) {
			return []byte("ActiveState=active\nSubState=running\nMainPID=" + strconv.Itoa(pid) + "\nNRestarts=0\nResult=success\n"), nil
		}
	}
	p, err := collectFleetRuntime(context.Background(), c, "boot-session", command(os.Getpid()+1))
	if err == nil || p == nil || p.ServiceHealthy {
		t.Fatal("accepted another manager process", p, err)
	}
	p, err = collectFleetRuntime(context.Background(), c, "boot-session", command(os.Getpid()))
	if err != nil || p == nil || !p.ServiceHealthy || !p.RunnerSmoke || p.PID != os.Getpid() || p.Session != "boot-session" || len(p.SHA256) != 64 || p.StartGate != StartGateProtocol || p.AttemptProtocol != AttemptProtocol || p.ObservedAt.IsZero() {
		t.Fatal("missing exact process evidence", p, err)
	}
}

func TestFleetRuntimePollFencesLaunchAndSanitizesFailure(t *testing.T) {
	c := fleetRuntimeTestConfig(t)
	key := bytes.Repeat([]byte{1}, ed25519.PublicKeySize)
	if err := os.WriteFile(filepath.Join(c.State, "fleet-publisher.pub"), key, 0600); err != nil {
		t.Fatal(err)
	}
	i := FleetUpdateInstruction{Operation: "upgrade", Revision: 1, Host: "host", Machine: "worker", Session: "session", PublisherKey: key}
	var current atomic.Value
	current.Store(i)
	acks := make(chan fleetLaunchFailure, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Error("missing control authentication")
		}
		switch r.URL.Path {
		case "/api/fleet/instruction":
			if r.URL.Query().Get("host") != "host" || r.URL.Query().Get("machine") != "worker" || r.URL.Query().Get("session") != "session" {
				t.Error("missing exact identity query")
			}
			_ = json.NewEncoder(w).Encode(current.Load().(FleetUpdateInstruction))
		case "/api/fleet/ack":
			var ack fleetLaunchFailure
			_ = json.NewDecoder(r.Body).Decode(&ack)
			acks <- ack
		default:
			t.Error("unexpected trust replacement request", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	collect := func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
		return &FleetRuntimeProof{Session: "session", ServiceHealthy: true}, nil
	}
	launches := 0
	launch := func(context.Context, ServiceConfig, FleetUpdateInstruction) error {
		launches++
		return errors.New("secret and arbitrary helper stderr must not escape")
	}
	// The queue may return a stale instruction; it cannot activate this process.
	i.Session = "stale"
	current.Store(i)
	result := pollFleetRuntime(context.Background(), c, client, collect, launch, "host", "worker", "session", true, "", nil)
	if launches != 0 || result.attempted != "" {
		t.Fatal("stale instruction launched")
	}
	i.Session = "session"
	current.Store(i)
	result = pollFleetRuntime(context.Background(), c, client, collect, launch, "host", "worker", "session", true, "", nil)
	ack := <-acks
	if launches != 1 || result.attempted != fleetInstructionKey(i) || ack != (fleetLaunchFailure{i.Operation, i.Revision, i.Host, i.Machine, i.Session, "failed", "helper_launch_failed"}) {
		t.Fatal("failed launch acknowledgement", launches, ack)
	}
	_ = pollFleetRuntime(context.Background(), c, client, collect, launch, "host", "worker", "session", true, result.attempted, nil)
	if launches != 1 {
		t.Fatal("failed operation automatically relaunched")
	}
	i.Revision = 2 // only an explicit operator rerun advances this generation.
	current.Store(i)
	result = pollFleetRuntime(context.Background(), c, client, collect, launch, "host", "worker", "session", true, result.attempted, nil)
	if launches != 2 || result.attempted != fleetInstructionKey(i) {
		t.Fatal("explicit operator rerun did not recover helper activation")
	}
	// A fresh operation cannot smuggle publisher-key rotation into an instruction.
	i.Operation = "next"
	i.PublisherKey = bytes.Repeat([]byte{2}, ed25519.PublicKeySize)
	current.Store(i)
	_ = pollFleetRuntime(context.Background(), c, client, collect, launch, "host", "worker", "session", true, result.attempted, nil)
	disk, err := os.ReadFile(filepath.Join(c.State, "fleet-publisher.pub"))
	if err != nil || launches != 2 || !bytes.Equal(disk, key) {
		t.Fatal("instruction replaced publisher trust", launches, err)
	}
}

func TestFleetRuntimeSlowObservationDoesNotBlockWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	r := &workerFleetRuntime{collect: func(ctx context.Context, c ServiceConfig, session string) (*FleetRuntimeProof, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return &FleetRuntimeProof{Session: session, PID: 42, ObservedAt: (RealClock{}).Now()}, nil
	}}
	m := Machine{ID: "worker", HostID: "host", Session: "session"}
	if p := r.observe(ctx, m, false); p != nil {
		t.Fatal("unobserved proof published")
	}
	<-entered
	for n := 0; n < 10; n++ {
		if p := r.observe(ctx, m, false); p != nil {
			t.Fatal("pending observation blocked or published proof")
		}
	}
	// Retain a heartbeat's proof as the next observation completes. Publishing a
	// new value must not mutate a proof already shared with an attempt goroutine.
	close(release)
	result := <-r.pending
	r.pending, r.proof = nil, result.proof
	cancel() // suppress another probe; this test controls completion explicitly.
	first := r.observe(ctx, m, false)
	first.PID = 7
	if second := r.observe(ctx, m, false); second == nil || second.PID != 42 {
		t.Fatal("published proof aliases mutable runtime state")
	}
	m.Session = "new-session"
	if r.observe(ctx, m, false) != nil {
		t.Fatal("old proof was attributed to a new session")
	}
}

func TestFleetRuntimeStaleFailureAckDoesNotConcealRecovery(t *testing.T) {
	c := fleetRuntimeTestConfig(t)
	key := bytes.Repeat([]byte{1}, ed25519.PublicKeySize)
	if err := os.WriteFile(filepath.Join(c.State, "fleet-publisher.pub"), key, 0600); err != nil {
		t.Fatal(err)
	}
	i := FleetUpdateInstruction{Operation: "upgrade", Revision: 2, Host: "host", Machine: "worker", Session: "session", PublisherKey: key}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/fleet/ack" {
			w.WriteHeader(409) // revision 1 acknowledgement arrived after rerun.
			return
		}
		_ = json.NewEncoder(w).Encode(i)
	}))
	defer server.Close()
	collect := func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
		return &FleetRuntimeProof{Session: "session", ServiceHealthy: true}, nil
	}
	launches := 0
	launch := func(context.Context, ServiceConfig, FleetUpdateInstruction) error { launches++; return nil }
	client := Client{URL: server.URL, Token: "test"}
	failed := &fleetLaunchFailure{i.Operation, 1, i.Host, i.Machine, i.Session, "failed", "helper_launch_failed"}
	result := pollFleetRuntime(context.Background(), c, client, collect, launch, i.Host, i.Machine, i.Session, true, "upgrade/1", failed)
	if result.failed != nil || launches != 0 {
		t.Fatal("obsolete acknowledgement remained pending")
	}
	result = pollFleetRuntime(context.Background(), c, client, collect, launch, i.Host, i.Machine, i.Session, true, result.attempted, result.failed)
	if launches != 1 || result.attempted != "upgrade/2" {
		t.Fatal("explicit recovery concealed by stale failure acknowledgement")
	}
}

func TestFleetRuntimeInstructionWireResponses(t *testing.T) {
	for _, response := range []struct {
		name     string
		status   int
		body     string
		launches int
	}{
		{"empty-object", 200, `{}`, 0},
		{"null", 200, `null`, 0},
		{"no-content", 204, "", 0},
		{"null-wrapper", 200, `{"instruction":null}`, 0},
		{"wrapped-instruction", 200, `{"instruction":{"operation":"upgrade","revision":1,"host":"host","machine":"worker","session":"session","publisher_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="}}`, 1},
	} {
		t.Run(response.name, func(t *testing.T) {
			c := fleetRuntimeTestConfig(t)
			if err := os.WriteFile(filepath.Join(c.State, "fleet-publisher.pub"), bytes.Repeat([]byte{1}, ed25519.PublicKeySize), 0600); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(response.status)
				_, _ = io.WriteString(w, response.body)
			}))
			defer server.Close()
			collect := func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
				return &FleetRuntimeProof{Session: "session", ServiceHealthy: true}, nil
			}
			launches := 0
			launch := func(context.Context, ServiceConfig, FleetUpdateInstruction) error { launches++; return nil }
			_ = pollFleetRuntime(context.Background(), c, Client{URL: server.URL}, collect, launch, "host", "worker", "session", true, "", nil)
			if launches != response.launches {
				t.Fatal("unexpected activation for instruction response", launches)
			}
		})
	}
}

func TestFleetRuntimeCachedProofExpiresWithoutRenewingObservation(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)}
	m := Machine{ID: "worker", HostID: "host", Session: "session"}
	completed := make(chan fleetPollResult, 1)
	completed <- fleetPollResult{proof: &FleetRuntimeProof{Session: m.Session, ServiceHealthy: true, ObservedAt: clock.Now()}}
	r := &workerFleetRuntime{clock: clock, pending: completed}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // no network is needed to exercise completed/cached observations.
	first := r.observe(ctx, m, false)
	if first == nil || r.generation != 1 || first.ObservedAt != clock.Now() {
		t.Fatal("completed probe did not advance generation")
	}
	clock.Advance(fleetRuntimeFreshFor - time.Nanosecond)
	if p := r.observe(ctx, m, false); p == nil || p.ObservedAt != first.ObservedAt || r.generation != 1 {
		t.Fatal("cached heartbeat renewed manager observation")
	}
	clock.Advance(time.Nanosecond)
	if r.observe(ctx, m, false) != nil || r.generation != 1 {
		t.Fatal("stale manager proof remained eligible")
	}
}
