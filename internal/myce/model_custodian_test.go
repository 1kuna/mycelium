package myce

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Only native service membership is synthetic. This subprocess owns the real
// generic engine, its health/Wait loop, and the bridge's live response socket.
func TestDetachedModelCustodianHelper(t *testing.T) {
	spool, id := os.Getenv("MYCE_MODEL_HELPER_SPOOL"), os.Getenv("MYCE_MODEL_HELPER_ID")
	if spool == "" || id == "" {
		return
	}
	var r modelCustodianRequest
	if err := readCustodianJSON(custodianDir(spool), custodianName(id, "model-request"), &r); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	w := Worker{StateDir: spool, Custodian: id, Machine: r.Machine, Client: Client{Token: "test"}, Clock: RealClock{}, Interval: 10 * time.Millisecond, ServiceConfig: &ServiceConfig{State: spool}, custodianIsolation: func(string) (string, error) { return "test-independent", nil }}
	if err := w.runExecutionCustodian(ctx); err != nil {
		t.Fatal(err)
	}
}

func testModelCustodian(t *testing.T, execution ...HostEngineExecution) (*modelCustodian, string) {
	t.Helper()
	spool := t.TempDir()
	model := serverTestModel(t, "ready")
	model.Name = "test"
	model.HostMemoryGB = 0.25
	machine := machine("model-worker", "fake")
	machine.HostMemoryBudgetGB = 32
	g := &modelCustodian{dir: custodianDir(spool), clock: RealClock{}, interval: 10 * time.Millisecond, token: "test", request: modelCustodianRequest{Protocol: custodianProtocol, ID: ID(), Machine: machine, Original: model, Prepared: model, Execution: HostEngineExecution{Machine: machine.ID, Session: machine.Session, Model: model.Name, Attempt: ID()}}}
	if len(execution) != 0 {
		g.request.Execution = execution[0]
		g.request.Machine.ID, g.request.Machine.Session = execution[0].Machine, execution[0].Session
	}

	g.verify = func(_ string, _ int, scope string) error {
		if scope != "test-independent" {
			return errors.New("wrong test scope")
		}
		return nil
	}
	g.launch = func(ctx context.Context, _ ServiceConfig, dir, id string) error {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDetachedModelCustodianHelper$")
		cmd.Env = append(withoutMyceToken(os.Environ()), "MYCE_MODEL_HELPER_SPOOL="+spool, "MYCE_MODEL_HELPER_ID="+id)
		log, err := os.Create(filepath.Join(spool, "helper.log"))
		if err != nil {
			return err
		}
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			log.Close()
			return err
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait(); log.Close() }()
		t.Cleanup(func() {
			_ = g.Unload()
			select {
			case err := <-done:
				if err != nil {
					data, _ := os.ReadFile(filepath.Join(spool, "helper.log"))
					t.Errorf("model custodian exited: %v %s", err, data)
				}
			case <-time.After(testWaitTimeout):
				_ = cmd.Process.Kill()
				<-done
				t.Error("model helper did not release")
			}
		})
		return waitCustodian(ctx, dir, id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := g.Load(ctx, g.request.Prepared); err != nil {
		t.Fatal(err)
	}
	return g, spool
}

func detachedModelRequest(t *testing.T, target, session, attempt, endpoint string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), "POST", target+"/models/test"+endpoint, strings.NewReader("input"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Session", session)
	req.Header.Set("X-Myce-Engine-Attempt", attempt)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestDetachedModelBridgeSurvivesWorkerReplacement(t *testing.T) {
	g, spool := testModelCustodian(t)
	e := g.request.Execution
	res := detachedModelRequest(t, g.bridgeURL(), e.Session, e.Attempt, "/stream")
	defer res.Body.Close()
	stream := bufio.NewReader(res.Body)
	if first, err := stream.ReadString('\n'); err != nil || first != "first\n" {
		t.Fatal("stream did not begin", first, err)
	}
	await(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.read() == nil && g.last.Report.InFlight == 1 })
	before := g.last
	serviceStop, cancel := context.WithCancel(context.Background())
	cancel()
	old, err := startModels(Worker{Machine: g.request.Machine, Clock: RealClock{}, Client: Client{Token: "test"}, serviceStop: serviceStop}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old.models[engineKey(e.Model, e.Device)] = &workerModel{model: g.request.Original, engine: g, state: "ready", attempt: e.Attempt, inflight: 1}
	// An adjacent-release queue still routes through the worker's original
	// listener. New worker + old queue must preserve that call path too.
	legacy := detachedModelRequest(t, old.url, e.Session, e.Attempt, "/echo")
	data, readErr := io.ReadAll(legacy.Body)
	legacy.Body.Close()
	if legacy.StatusCode != 200 || readErr != nil || !strings.Contains(string(data), "input") {
		t.Fatalf("adjacent queue route failed: %d %s %v", legacy.StatusCode, data, readErr)
	}
	if err := old.close(); err != nil {
		t.Fatal(err)
	}
	// Reconstruct only the replacement coordinator. No Load call, new process,
	// route or attempt is allowed; the old HTTP response remains open.
	next := &modelRuntime{models: map[string]*workerModel{}, dir: spool, clock: RealClock{}, machine: e.Machine, session: e.Session}
	if err := next.restoreModelCustodiansWith(g.verify); err != nil {
		t.Fatal(err)
	}
	reports, memory := next.report()
	if len(reports) != 1 || reports[0].InFlight != 1 || reports[0].Attempt != e.Attempt || reports[0].URL != before.URL || reports[0].Custody == nil || memory != g.request.Original.MemoryGB {
		t.Fatalf("replacement lost/duplicated engine accounting: %+v memory=%v", reports, memory)
	}
	adopted := next.models[engineKey(e.Model, e.Device)].engine.(*modelCustodian)
	if adopted.last.PID != before.PID || adopted.last.Birth != before.Birth {
		t.Fatal("replacement restarted the model owner")
	}
	for _, pair := range [][2]string{{"stale-session", e.Attempt}, {e.Session, "stale-attempt"}} {
		stale := detachedModelRequest(t, before.URL, pair[0], pair[1], "/echo")
		stale.Body.Close()
		if stale.StatusCode != 401 && stale.StatusCode != 503 {
			t.Fatal("stale identity reached the engine", stale.StatusCode)
		}
	}
	release := detachedModelRequest(t, before.URL, e.Session, e.Attempt, "/release-stream")
	release.Body.Close()
	last, err := io.ReadAll(stream)
	if err != nil || string(last) != "last\n" {
		t.Fatalf("worker replacement interrupted/duplicated response: %q %v", last, err)
	}
	await(t, func() bool { r, _ := next.report(); return r[0].InFlight == 0 })
}

func TestDetachedModelProofRefusesUnconfirmedIdentity(t *testing.T) {
	g, _ := testModelCustodian(t)
	g.mu.Lock()
	defer g.mu.Unlock()
	good := g.last
	for _, kind := range []string{"session", "attempt", "birth", "stale", "release"} {
		t.Run(kind, func(t *testing.T) {
			v := good
			switch kind {
			case "session":
				v.Execution.Session = "old"
			case "attempt":
				v.Execution.Attempt = "old"
			case "birth":
				v.Birth = "old"
			case "stale":
				v.At = v.At.Add(-time.Hour)
			case "release":
				v.Finished = true
			}
			// Freeze the child publication by checking an isolated copy.
			copyDir := t.TempDir()
			if err := saveWorkerJSON(copyDir, custodianName(g.request.ID, "snapshot"), v); err != nil {
				t.Fatal(err)
			}
			probe := &modelCustodian{dir: copyDir, request: g.request, clock: g.clock, interval: g.interval, verify: g.verify}
			if err := probe.read(); err == nil || probe.verified {
				t.Fatal("unproved model custody accepted", kind)
			}
		})
	}
}

func TestFleetUpgradeDetachedModelsAndLateCall(t *testing.T) {
	for _, kind := range []string{"idle", "inflight", "late-call", "legacy", "unready", "wrong-session"} {
		t.Run(kind, func(t *testing.T) {
			q, clock, workers := fleetFixture(t, "q", "a")
			m := workers[0]
			config := ModelConfig{Models: []Model{{Name: "test", Engine: "fake", Path: "fake", MemoryGB: 1, Platforms: []string{"fake"}}}, Idle: time.Hour}
			if err := q.ConfigureModels(config); err != nil {
				t.Fatal(err)
			}
			m.ModelConfigHash, m.ModelURL = modelConfigHash(config), "http://127.0.0.1:1234"
			m.Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol}
			check(t, q, m, nil)
			if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
				t.Fatal(err)
			}
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			if s == nil {
				t.Fatal("missing pinned slot")
			}
			s.State, s.Custody, s.URL = "ready", &ModelCustody{Protocol: custodianProtocol, ID: ID(), WorkerPID: m.Fleet.PID}, "http://127.0.0.1:1235"
			before := *s
			if kind == "legacy" {
				s.Custody = nil
			}
			if kind == "unready" {
				s.State = "loading"
			}
			if kind == "wrong-session" {
				s.Session = "old-session"
			}
			if kind == "inflight" {
				if err := q.addCall(&ModelCall{Model: "test", Project: "test", Priority: "experiment"}); err != nil {
					t.Fatal(err)
				}
				if s.InFlight != 1 {
					t.Fatal("call was not dispatched")
				}
			}
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			h := q.s.FleetUpgrade.Hosts[0]
			if kind == "legacy" || kind == "unready" || kind == "wrong-session" {
				if h.Takeover || q.fleetAdoptionReason(h) == "" {
					t.Fatal("unconfirmed model authorized takeover", h)
				}
				return
			}
			if !h.Takeover || h.Phase != "worker_update" {
				t.Fatal("resident model blocked upgrade", h)
			}
			i := fleetInstructionFor(t, q)
			if kind == "late-call" {
				// Routing to an existing engine can start after the last proof;
				// its detached bridge, rather than a per-call snapshot, is custody.
				q.calls["late"] = &ModelCall{ID: "late", Model: s.Model, Machine: s.Machine, Session: s.Session, Device: s.Device, Attempt: s.Attempt, Project: "test", Priority: "experiment", Queued: clock.Now()}
				s.InFlight++
			}
			if err := q.authorizeFleetUpdate(i.Operation, i.Host, i.Machine, i.Session); err != nil {
				t.Fatal("late call invalidated detached bridge custody", err)
			}
			if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
				t.Fatal(err)
			}
			if !q.retainWorkerHandover(q.s.Machines[m.ID]) {
				t.Fatal("handover did not retain live model custody")
			}
			updated := cloneMachine(q.s.Machines[m.ID])
			updated.Fleet = fleetTestProof(clock, m.Session, 777, true)
			updated.Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol}
			// A new PID carrying only the previous coordinator's stored receipt
			// cannot claim adoption; its own exact report is required.
			q.s.Machines[m.ID] = &updated
			if !q.retainWorkerHandover(q.s.Machines[m.ID]) {
				t.Fatal("replacement accepted old coordinator's model receipt")
			}
			reports := []ModelReport{{Name: s.Model, Attempt: s.Attempt, State: "ready", Custody: s.Custody, URL: s.URL, InFlight: s.InFlight, CallsMeasured: true}}
			if _, err := q.Checkin(Checkin{Machine: updated, ModelReports: reports, period: time.Second}); err != nil {
				t.Fatal(err)
			}
			if q.retainWorkerHandover(q.s.Machines[m.ID]) {
				t.Fatal("replacement did not settle exact model custody")
			}
			if s.Attempt != before.Attempt || s.Session != before.Session || s.Custody.ID != before.Custody.ID || s.State != "ready" || len(q.s.Pins) != 1 {
				t.Fatalf("upgrade replaced engine or pin: %+v", s)
			}
			for id := range q.calls {
				if !q.callAttemptLive(q.calls[id]) {
					t.Fatal("in-flight call lost exact engine")
				}
				if err := q.finishCall(id); err != nil {
					t.Fatal(err)
				}
			}
			if s.InFlight != 0 {
				t.Fatal("call accounting did not settle exactly once", s.InFlight)
			}
		})
	}
}

// The real queue HTTP path must route directly to the detached bridge. A
// worker listener can disappear without owning any part of the response.
func TestDetachedModelQueueRoute(t *testing.T) {
	q, _, m := modelFixture(t)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s == nil {
		t.Fatal("pin did not assign an engine")
	}
	g, _ := testModelCustodian(t, HostEngineExecution{Machine: m.ID, Session: m.Session, Model: s.Model, Device: s.Device, Attempt: s.Attempt})
	e := g.request.Execution
	// Reconcile through the worker protocol, preserving the queue's assigned
	// attempt and all placement/accounting state. The worker URL is deliberately
	// unreachable: the exact slot's independent bridge must own this response.
	g.mu.Lock()
	report := g.last.Report
	g.mu.Unlock()
	report.URL = g.bridgeURL()
	report.Custody = &ModelCustody{Protocol: custodianProtocol, ID: g.request.ID}
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{report}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), testWaitTimeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/models/test/stream", nil)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "experiment")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("route: %d %s", res.StatusCode, b)
	}
	stream := bufio.NewReader(res.Body)
	if first, err := stream.ReadString('\n'); err != nil || first != "first\n" {
		t.Fatal(first, err)
	}
	release := detachedModelRequest(t, g.bridgeURL(), e.Session, e.Attempt, "/release-stream")
	release.Body.Close()
	if last, err := io.ReadAll(stream); err != nil || string(last) != "last\n" {
		t.Fatal(string(last), err)
	}
}
