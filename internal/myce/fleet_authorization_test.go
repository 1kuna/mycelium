package myce

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #1710: replacing a normal operation's drain with an operator checkpoint
// stop, then starting a successor while the disable is held. This sequence
// alone must not refuse authorization. This is a queue/HTTP unit test; exact
// historical process compatibility needs a separate cross-version dev run.
func TestFleetAuthorizationOperatorDisabledSuccessor(t *testing.T) {
	q, clock, workers := fleetFixture(t, "queue", "already-current", "worker")
	current := workers[0]
	current.Fleet = fleetTestProof(clock, current.Session, current.Fleet.PID, true)
	check(t, q, current, nil)
	m := workers[1]
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
	check(t, q, m, nil)
	s := spec("experiment")
	s.Resumable = true
	run := fleetRun(t, q, m, s)
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	predecessor := q.s.FleetUpgrade.Operation
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, run)
	r := result(run, false)
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	fleetRefresh(t, q, clock)
	if u := q.fleetUpgradeStatus(); u.State != "stopped" || fleetHostStatus(t, q, m.HostID).Failure != "maintenance_changed" {
		t.Fatalf("predecessor: %+v", u)
	}
	before := q.s.HostMaintenance[m.HostID]
	if before.Mode != "disabled" || !q.hostMaintenanceStatus(m.HostID).Quiescent {
		t.Fatal("operator disable is not quiescent")
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	if h := fleetHostStatus(t, q, current.HostID); h.Phase != "complete" || h.Wait != "already on the release" {
		t.Fatalf("current worker not skipped: %+v", h)
	}
	if i.Operation == predecessor || i.Session != m.Session {
		t.Fatal("successor lost operation/session identity")
	}
	mux := newRequestMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	p, err := fleetAuthorize(context.Background(), Client{URL: server.URL}, i, nil)
	if err != nil || !fleetProofReady(&p) || p.Session != m.Session || p.PID != m.Fleet.PID {
		t.Fatalf("successor authorization: proof=%+v err=%v", p, err)
	}
	if !reflect.DeepEqual(q.s.HostMaintenance[m.HostID], before) {
		t.Fatal("authorization changed operator maintenance")
	}
	if got := job(t, q, run.ID); got.State != "queued" || got.Attempt != run.Attempt || got.Losses != 0 {
		t.Fatalf("maintenance changed job identity or losses: %+v", got)
	}
}

// Exercise the actual client decoding boundary: all these failures previously
// collapsed to fleet_authorization_refused with no evidence of which branch
// failed. Keep that stable code, but retain a sanitized cause in the log.
func TestFleetAuthorizationDiagnostics(t *testing.T) {
	ready := fleetTestProof(&fakeClock{}, "session", 123, false)
	for _, tc := range []struct {
		name, cause string
		mutate      func(*FleetRuntimeProof)
		status      int
		body        string
	}{
		{name: "pid", cause: "proof_pid_missing", mutate: func(p *FleetRuntimeProof) { p.PID = 0 }},
		{name: "session", cause: "proof_session_missing", mutate: func(p *FleetRuntimeProof) { p.Session = "" }},
		{name: "hash", cause: "proof_sha256_invalid", mutate: func(p *FleetRuntimeProof) { p.SHA256 = "private-value" }},
		{name: "service", cause: "proof_service_unhealthy", mutate: func(p *FleetRuntimeProof) { p.ServiceHealthy = false }},
		{name: "smoke", cause: "proof_runner_smoke_failed", mutate: func(p *FleetRuntimeProof) { p.RunnerSmoke = false }},
		{name: "start_gate", cause: "proof_start_gate_mismatch", mutate: func(p *FleetRuntimeProof) { p.StartGate = "private-value" }},
		{name: "attempt_protocol", cause: "proof_attempt_protocol_mismatch", mutate: func(p *FleetRuntimeProof) { p.AttemptProtocol = "private-value" }},
		{name: "session_mismatch", cause: "proof_session_mismatch", mutate: func(p *FleetRuntimeProof) { p.Session = "private-value" }},
		{name: "typed_http", cause: "HTTP 401 code=authentication_required", status: 401, body: `{"code":"authentication_required","error":"private-value"}`},
		{name: "untrusted_http", cause: "HTTP 400 code=request_failed", status: 400, body: `{"code":"private-value","error":"private-value"}`},
		{name: "untrusted_well_formed_code", cause: "HTTP 400 code=request_failed", status: 400, body: `{"code":"private_value","error":"private-value"}`},
		{name: "decode", cause: "response_decode_failed", body: `{"pid":"private-value"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs testLogBuffer
			old := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(old)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
					return
				}
				p := *ready
				tc.mutate(&p)
				_ = json.NewEncoder(w).Encode(p)
			}))
			defer server.Close()
			_, err := fleetAuthorize(context.Background(), Client{URL: server.URL, Token: "private-value"}, FleetUpdateInstruction{Session: ready.Session}, nil)
			if err == nil || err.Error() != "fleet_authorization_refused" {
				t.Fatalf("lost stable failure code: %v", err)
			}
			if out := logs.String(); !strings.Contains(out, `event=fleet_authorization side=worker`) || !strings.Contains(out, `cause="`+tc.cause+`"`) || strings.Contains(out, "private-value") || strings.Contains(out, "private_value") {
				t.Fatalf("missing or unsafe diagnostic: %s", out)
			}
		})
	}
}

// The queue log must remain useful to an installed older helper that cannot
// report a detailed cause. Test the real authorization handler, including its
// readiness and operation/session fences.
func TestFleetAuthorizationQueueDiagnostics(t *testing.T) {
	q, _, _ := fleetFixture(t, "queue", "worker")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	mux := newRequestMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	for _, tc := range []struct {
		name, cause        string
		operation, session string
		revision           string
		unready, cancelled bool
		stopping           bool
	}{
		{name: "authorized", cause: "ready", operation: i.Operation, session: i.Session},
		{name: "operation", cause: "HTTP 409 code=fleet_instruction_stale", operation: "private-value", session: i.Session},
		{name: "revision", cause: "HTTP 409 code=fleet_instruction_stale", operation: i.Operation, session: i.Session, revision: "2"},
		{name: "session", cause: "HTTP 409 code=fleet_instruction_stale", operation: i.Operation, session: "private-value"},
		{name: "unready", cause: "HTTP 409 code=fleet_instruction_stale", operation: i.Operation, session: i.Session, unready: true},
		{name: "cancelled", cause: "ready", operation: i.Operation, session: i.Session, cancelled: true},
		{name: "stopping", cause: "HTTP 503 code=queue_stopping", operation: i.Operation, session: i.Session, stopping: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs testLogBuffer
			old := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(old)
			q.s.Machines[i.Machine].Fleet.RunnerSmoke = !tc.unready
			q.stopping.Store(tc.stopping)
			defer q.stopping.Store(false)
			revision := tc.revision
			if revision == "" {
				revision = strconv.Itoa(i.Revision)
			}
			v := url.Values{"operation": {tc.operation}, "revision": {revision}, "host": {i.Host}, "machine": {i.Machine}, "session": {tc.session}}
			r := httptest.NewRequest("GET", "/api/fleet/authorize?"+v.Encode(), nil)
			caller := "active"
			if tc.cancelled {
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
				caller = "cancelled"
			}
			mux.ServeHTTP(httptest.NewRecorder(), r)
			if out := logs.String(); !strings.Contains(out, `event=fleet_authorization side=queue cause="`+tc.cause+`" caller=`+caller) || strings.Contains(out, "private-value") {
				t.Fatalf("missing or unsafe queue diagnostic: %s", out)
			}
		})
	}
}

func TestFleetAuthorizationDeadlineDiagnostic(t *testing.T) {
	var logs testLogBuffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	ctx, cancel := context.WithDeadline(context.Background(), time.Time{})
	defer cancel()
	_, err := fleetAuthorize(ctx, Client{URL: "http://private-value.invalid", Token: "private-value"}, FleetUpdateInstruction{}, nil)
	if err == nil || err.Error() != "fleet_authorization_refused" || !strings.Contains(logs.String(), `cause="timeout"`) || strings.Contains(logs.String(), "private-value") {
		t.Fatalf("lost or unsafe deadline cause: %v %s", err, logs.String())
	}
}

func TestFleetAuthorizationDoesNotRenewServiceStep(t *testing.T) {
	q, clock, _ := fleetFixture(t, "queue", "worker")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	stepAt := q.fleetActiveHost().StepAt
	mux := newRequestMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	v := url.Values{"operation": {i.Operation}, "revision": {strconv.Itoa(i.Revision)}, "host": {i.Host}, "machine": {i.Machine}, "session": {i.Session}}
	for n := 0; n < 3; n++ {
		clock.Advance(fleetStepTimeout / 4)
		fleetRefresh(t, q, clock)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest("GET", "/api/fleet/authorize?"+v.Encode(), nil))
		if response.Code != http.StatusOK || q.fleetActiveHost().StepAt != stepAt {
			t.Fatal("authorization refused healthy proof or renewed the step", response.Code)
		}
	}
	clock.Advance(fleetStepTimeout / 4)
	fleetRefresh(t, q, clock)
	if q.s.FleetUpgrade.State != "stopped" || fleetHostStatus(t, q, i.Host).Failure != "worker_update_not_verified" {
		t.Fatal("repeated authorization escaped the service-step bound", q.fleetUpgradeStatus())
	}
}

// A transient transport result confers no authority. Replaying the exact GET
// must observe any operation, session, revision or proof change since the stall.
func TestFleetAuthorizationStallRechecksFences(t *testing.T) {
	for _, fence := range []string{"operation", "session", "revision", "proof"} {
		t.Run(fence, func(t *testing.T) {
			q, clock, _ := fleetFixture(t, "queue", "worker")
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			i := fleetInstructionFor(t, q)
			mux := newRequestMux()
			API{Queue: q}.registerFleetUpgrade(mux)
			calls := 0
			client := Client{URL: "http://fixture.invalid", Clock: clock, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return nil, &QueueStalledError{Method: r.Method, Path: r.URL.Path, Window: 4 * clientRecoveryPeriod}
				}
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, r)
				return response.Result(), nil
			})}}
			wait := func(_ context.Context, d time.Duration) error { clock.Advance(d); return nil }
			_, err := fleetAuthorize(context.Background(), client, i, wait)
			var stall *QueueStalledError
			if !errors.As(err, &stall) {
				t.Fatal("typed stall lost", err)
			}
			switch fence {
			case "operation":
				q.s.FleetUpgrade.Operation = "successor-operation"
			case "session":
				q.s.Machines[i.Machine].Session = "replacement-session"
			case "revision":
				q.s.FleetUpgrade.Revision++
			case "proof":
				q.s.Machines[i.Machine].Fleet.RunnerSmoke = false
			}
			if proof, err := fleetAuthorize(context.Background(), client, i, wait); err == nil || fleetProofReady(&proof) {
				t.Fatal("stalled request retained obsolete authority", proof, err)
			}
		})
	}
}
