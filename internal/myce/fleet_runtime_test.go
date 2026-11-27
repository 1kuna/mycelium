package myce

import (
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
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestFleetRuntimeRestartProofDiagnosticsThroughCheckin(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	c := fleetRuntimeTestConfig(t)
	c.Detection.OS = "darwin"
	m := machine("worker", "fake")
	previousSession := m.Session
	m.Session = "replacement-session"
	managerPID := os.Getpid() + 1
	collect := func(ctx context.Context, c ServiceConfig, session string) (*FleetRuntimeProof, error) {
		return collectFleetRuntimeAt(ctx, c, session, func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name != "launchctl" || len(args) != 2 || args[0] != "print" {
				t.Fatalf("unexpected Darwin service probe: %s %v", name, args)
			}
			return []byte(fmt.Sprintf("service = {\n\tstate = running\n\truns = 2\n\tpid = %d\n\tother = {\n\t\tstate = inactive\n\t}\n}\n", managerPID)), nil
		}, clock)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Apply completed observations deterministically, without launching another poll.
	r := &workerFleetRuntime{clock: clock}
	checkStatus := func(want FleetProofIssue) {
		t.Helper()
		completed := make(chan fleetPollResult, 1)
		completed <- pollFleetRuntimeAt(context.Background(), c, client, collect, nil, hostID(&m), m.ID, m.Session, false, fleetPollResult{}, clock)
		r.pending = completed
		m.Fleet = r.observe(ctx, m, false)
		if r.proofIssue() != want || (m.Fleet == nil) != (want != "") {
			t.Fatalf("proof presence=%t issue=%q, want issue %q", m.Fleet != nil, r.proofIssue(), want)
		}
		var reply Reply
		if err := client.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, fleetProofIssue: r.proofIssue()}, &reply); err != nil {
			t.Fatal(err)
		}
		var status CompactStatus
		if err := client.Call(context.Background(), "GET", "/api/status", nil, &status); err != nil {
			t.Fatal(err)
		}
		if len(status.Hyphas) != 1 || status.Hyphas[0].FleetProofIssue != want {
			t.Fatalf("status hyphas=%+v, want issue %q", status.Hyphas, want)
		}
		if rendered := strings.Join(status.Lines(false), "\n"); want != "" && !strings.Contains(rendered, "fleet runtime proof unavailable: "+string(want)) {
			t.Fatalf("missing proof issue in status: %s", rendered)
		}
	}
	if err := os.WriteFile(filepath.Join(c.State, fleetUpdateReceiptFile), []byte("invalid receipt"), 0600); err != nil {
		t.Fatal(err)
	}
	checkStatus(fleetProofReceiptInvalid)
	oldProof := fleetTestProof(clock, previousSession, managerPID, false)
	if err := writeFleetReceipt(c, fleetInstallReceipt{
		Phase: "restarting", ConfigSHA256: oldProof.SHA256, BeforeSHA256: oldProof.SHA256,
		BeforePID: oldProof.PID, BeforeRuntime: *oldProof, BeforeVersion: oldProof.Version,
		Instruction: FleetUpdateInstruction{Host: hostID(&m), Machine: m.ID, Session: previousSession, Operation: "update", Revision: 1},
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	checkStatus(fleetProofManagerPID)
	managerPID = os.Getpid()
	clock.Advance(time.Second)
	checkStatus("")
	if m.Fleet.Session != m.Session || m.Fleet.PID != managerPID || !m.Fleet.ObservedAt.Equal(clock.Now()) || !fleetProofReady(m.Fleet) {
		t.Fatalf("invalid replacement proof: %+v", m.Fleet)
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
	if launches != 1 || result.attempted != fleetInstructionKey(i) || ack != (fleetLaunchFailure{i.Operation, i.Revision, i.Host, i.Machine, i.Session, "failed", "fleet_helper_launch_unclassified", ""}) {
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
	failed := &fleetLaunchFailure{i.Operation, 1, i.Host, i.Machine, i.Session, "failed", "helper_launch_failed", ""}
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

// Current workers fetch their instruction only when the check-in reply says one
// may be waiting; with no protocol (before the first reply, or a previous-release
// queue) they keep fetching every poll (MY-42).
func TestFleetRuntimeFetchesInstructionOnlyWhenWaiting(t *testing.T) {
	c := fleetRuntimeTestConfig(t)
	key := bytes.Repeat([]byte{1}, ed25519.PublicKeySize)
	if err := os.WriteFile(filepath.Join(c.State, "fleet-publisher.pub"), key, 0600); err != nil {
		t.Fatal(err)
	}
	i := FleetUpdateInstruction{Operation: "upgrade", Revision: 1, Host: "host", Machine: "worker", Session: "session", PublisherKey: key}
	var fetches, launches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/fleet/instruction" {
			t.Error("unexpected request", r.URL.Path)
		}
		fetches.Add(1)
		_ = json.NewEncoder(w).Encode(i)
	}))
	defer server.Close()
	r := &workerFleetRuntime{config: c, client: Client{URL: server.URL}, clock: RealClock{}, collect: func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
		return &FleetRuntimeProof{Session: "session", ServiceHealthy: true, ObservedAt: (RealClock{}).Now()}, nil
	}, launch: func(context.Context, ServiceConfig, FleetUpdateInstruction) error { launches.Add(1); return nil }}
	m := Machine{ID: "worker", HostID: "host", Session: "session"}
	idle := Reply{FleetInstructionProtocol: fleetInstructionProtocol}
	steps := []struct {
		reply             Reply
		fetches, launches int32
	}{{Reply{FleetInstructionProtocol: "unset"}, 1, 1}} // start: no reply yet, asks once
	for range 20 { // idle heartbeats never ask (#1393)
		steps = append(steps, struct {
			reply             Reply
			fetches, launches int32
		}{idle, 1, 1})
	}
	steps = append(steps, []struct {
		reply             Reply
		fetches, launches int32
	}{
		{Reply{FleetInstructionProtocol: fleetInstructionProtocol, FleetInstructionWaiting: true}, 2, 1},
		{Reply{}, 3, 1}, // previous-release queue
	}...)
	for n, step := range steps {
		if n > 0 {
			r.noteReply(step.reply)
		}
		r.observe(context.Background(), m, true)
		result := <-r.pending
		r.pending, r.attempted, r.proof = nil, result.attempted, result.proof
		if fetches.Load() != step.fetches || launches.Load() != step.launches {
			t.Fatalf("step %d: fetches=%d launches=%d", n, fetches.Load(), launches.Load())
		}
		if r.proof == nil {
			t.Fatalf("step %d: proof withheld", n)
		}
	}
}

// The queue never checks in, so its own runtime gates on queue state: no
// instruction requests while idle or while its host's worker updates, and one
// fetch per pass once its own queue_update phase starts (#1393).
func TestFleetQueueRuntimeFetchesInstructionOnlyInItsUpdate(t *testing.T) {
	q, c, _ := fleetFixture(t, "q", "q")
	key := bytes.Repeat([]byte{1}, ed25519.PublicKeySize)
	config := fleetRuntimeTestConfig(t)
	if err := os.WriteFile(filepath.Join(config.State, "fleet-publisher.pub"), key, 0600); err != nil {
		t.Fatal(err)
	}
	var fetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/fleet/instruction" || r.URL.Query().Get("machine") != "@queue" {
			t.Error("unexpected request", r.URL.String())
		}
		fetches.Add(1)
		_, _ = w.Write([]byte("null"))
	}))
	defer server.Close()
	session := q.fleetQueue.Proof.Session
	observer := &workerFleetRuntime{config: config, client: Client{URL: server.URL}, clock: c, collect: func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
		return fleetTestProof(c, session, 100, false), nil
	}, launch: func(context.Context, ServiceConfig, FleetUpdateInstruction) error { t.Error("launched"); return nil }}
	machine := Machine{ID: "@queue", HostID: "q", Session: session}
	var generation uint64
	pass := func() {
		t.Helper()
		q.observeFleetQueue(context.Background(), observer, machine, &generation)
		done := make(chan fleetPollResult, 1)
		done <- <-observer.pending // finish this pass's poll; the next pass collects it
		observer.pending = done
	}
	for range 20 {
		pass()
	}
	if fetches.Load() != 0 {
		t.Fatalf("idle queue fetched %d instructions", fetches.Load())
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	for step := 0; ; step++ {
		fleetTick(t, q)
		h := q.fleetActiveHost()
		if step > 8 || h == nil {
			t.Fatalf("queue update not reached: %+v", q.fleetUpgradeStatus())
		}
		if h.Phase == "queue_update" {
			break
		}
		pass()
		if fetches.Load() != 0 {
			t.Fatalf("queue fetched during phase %s", h.Phase)
		}
		if h.Phase == "worker_update" {
			fleetActivate(t, q, c)
		} else {
			fleetHeartbeatAdvance(t, q, c)
		}
	}
	pass()
	if fetches.Load() != 1 {
		t.Fatalf("queue_update fetches=%d, want 1", fetches.Load())
	}
}
