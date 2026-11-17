package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveNASMissingWorkerPathBeforeSubmission(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "resolve-nas-fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "resolve-nas-fixture"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// This checks the real HTTP/worker metadata round trip, not elapsed-time
	// freshness. A pending check and its report span two heartbeats; the dev
	// fixture's 10ms cadence gave them a load-dependent expiry while await polled.
	// Drive both clocks in explicit 1ms test steps, independent of host I/O.
	// Even 60s of 5ms await polls advances at most 12s of the fixture's 60s
	// freshness window. Heartbeats still become due, without expiring evidence.
	// TestResolveNASCacheFreshnessAndSession separately exercises expiration.
	w := Worker{Clock: clock, EnvironmentLimit: 1 << 30, StateDir: t.TempDir(), CheckpointRoot: q.root, Client: client, Machine: machine("worker", "cpu", "fixture-worker-host"), Interval: 10 * time.Millisecond}
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
				return true
			default:
				return false
			}
		})
	}()
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		snap, err := q.Snapshot()
		return err == nil && len(snap.Machines) > 0 && snap.Machines[0].SharedInputCache != nil
	})
	s := cpuResolveSpec()
	s.SharedInputs = map[string]SharedInput{"movie": {NAS: "media/missing", SHA256: strings.Repeat("a", 64), SizeBytes: 10}}
	var report ResolveReport
	request := ResolveRequest{Spec: s, InspectNAS: true}
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		report = ResolveReport{}
		if err := client.Call(context.Background(), "POST", "/api/resolve", request, &report); err != nil {
			t.Fatal(err)
		}
		return resolveFinding(report, "shared_inputs.movie.nas") != nil
	})
	if len(report.Targets) != 1 || report.Targets[0].Eligible || report.Targets[0].NASInputs[0].State != "missing" || report.Targets[0].NASInputs[0].ObservedAt.IsZero() {
		t.Fatalf("missing source appeared ready: %+v", report)
	}
	q.mu.Lock()
	jobs, reservations := len(q.s.Jobs), len(q.s.Reservations)
	q.mu.Unlock()
	if jobs+reservations != 0 {
		t.Fatal("inspection submitted or reserved work")
	}
	client.Token = "wrong"
	if err := client.Call(context.Background(), "POST", "/api/resolve", request, nil); err == nil {
		t.Fatal("inspection unauthenticated")
	}
}

func TestResolveNASCacheFreshnessAndSession(t *testing.T) {
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(3*time.Minute, 3*time.Minute, 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	m := cacheCapacityMachine("worker", 1<<20)
	m.GPU = "cpu"
	m.Session = "one"
	m.LastSeen = clock.Now()
	m.Capabilities = append(m.Capabilities, "nas_inspection_v1")
	check(t, q, m, nil)
	req := ResolveRequest{Spec: cpuResolveSpec(), InspectNAS: true}
	req.Spec.SharedInputs = map[string]SharedInput{"data": {NAS: "data/input", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	cache := newResolveNASCache(clock)
	resolve := func() ResolveReport {
		r, err := q.Resolve(req)
		if err != nil {
			t.Fatal(err)
		}
		cache.resolve(req, &r)
		return r
	}
	before := string(wireJSON(t, q.s))
	r := resolve()
	if r.Targets[0].NASInputs[0].State != "pending" {
		t.Fatal(r)
	}
	checks := cache.checkin(Checkin{Machine: m})
	if len(checks) != 1 {
		t.Fatal(checks)
	}
	foreign := m
	foreign.Session = "other"
	report := NASPathReport{ID: checks[0].ID, State: "present"}
	cache.checkin(Checkin{Machine: foreign, NASPathReports: []NASPathReport{report}})
	if resolve().Targets[0].NASInputs[0].State != "pending" {
		t.Fatal("foreign session supplied evidence")
	}
	cache.checkin(Checkin{Machine: m, NASPathReports: []NASPathReport{report}})
	r = resolve()
	if r.Targets[0].NASInputs[0].State != "present" {
		t.Fatal(r)
	}
	if after := string(wireJSON(t, q.s)); before != after {
		t.Fatal("diagnostics changed durable queue state")
	}
	observed := r.Targets[0].NASInputs[0].ObservedAt
	clock.Advance(30 * time.Second)
	cache.checkin(Checkin{Machine: m, NASPathReports: []NASPathReport{report}})
	if !resolve().Targets[0].NASInputs[0].ObservedAt.Equal(observed) {
		t.Fatal("duplicate report renewed evidence")
	}
	clock.Advance(31 * time.Second)
	check(t, q, m, nil)
	if got := resolve().Targets[0].NASInputs[0]; got.State != "present" || !got.ObservedAt.Equal(observed) {
		t.Fatal("session-fresh evidence disappeared at a fixed minute", got)
	}
	clock.Advance(2 * time.Minute)
	check(t, q, m, nil)
	if resolve().Targets[0].NASInputs[0].State != "pending" {
		t.Fatal("expired signal reused")
	}
	// A pending check also expires. Even fresh intervening heartbeats cannot
	// make a report for the retired ID observable; a new check must complete.
	// This is why the metadata round-trip fixture above controls elapsed time.
	checks = cache.checkin(Checkin{Machine: m})
	if len(checks) != 1 {
		t.Fatal(checks)
	}
	for range 2 {
		clock.Advance(2 * time.Minute)
		check(t, q, m, nil)
	}
	cache.checkin(Checkin{Machine: m, NASPathReports: []NASPathReport{{ID: checks[0].ID, State: "missing"}}})
	if got := resolve().Targets[0]; got.Freshness != "fresh" || got.NASInputs[0].State != "pending" || !got.NASInputs[0].ObservedAt.IsZero() {
		t.Fatal("expired pending report reused or worker unexpectedly stale", got)
	}
	next := cache.checkin(Checkin{Machine: m})
	if len(next) != 1 || next[0].ID == checks[0].ID {
		t.Fatal("expired check identity reused", next)
	}
	cache.checkin(Checkin{Machine: m, NASPathReports: []NASPathReport{{ID: next[0].ID, State: "missing"}}})
	if got := resolve().Targets[0]; got.Eligible || got.NASInputs[0].State != "missing" || got.NASInputs[0].ObservedAt.IsZero() {
		t.Fatal("fresh replacement report lost", got)
	}
}

func TestResolveNASWorkerChecksAreConfinedAndCached(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "input"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, state string }{{"input", "present"}, {"missing", "missing"}, {"escape/missing", "unverified"}, {"../outside", "unverified"}} {
		if got := inspectNASPath(root, tc.path); got != tc.state {
			t.Errorf("%s: %s", tc.path, got)
		}
	}
	_, clock := fixture(t)
	w := Worker{Clock: clock}
	checks := []NASPathCheck{{ID: "input", Path: "input"}}
	w.inspectNASPaths(checks, root)
	reports := w.nasPathReports()
	if len(reports) != 1 || reports[0].State != "present" {
		t.Fatal(reports)
	}
	clock.Advance(10 * time.Second)
	if reports = w.nasPathReports(); reports[0].AgeSeconds != 10 {
		t.Fatal("old sample was renewed", reports)
	}
}

func TestResolveNASInspectionSharedAcrossListeners(t *testing.T) {
	q, clock := fixture(t)
	m := cacheCapacityMachine("worker", 16<<30)
	m.GPU, m.Session = "cpu", "session"
	m.Capabilities = append(m.Capabilities, "nas_inspection_v1")
	machineCheck(t, q, m)
	api := API{Queue: q, Token: "test"}
	joining := httptest.NewServer(api.HandlerWithJoiningIdentity("test-pin"))
	defer joining.Close()
	control := httptest.NewServer(api.Handler())
	defer control.Close()
	observer := Client{URL: joining.URL, Token: "test"}
	worker := Client{URL: control.URL, Token: "test"}
	request := ResolveRequest{Spec: cpuResolveSpec(), InspectNAS: true}
	request.Spec.SharedInputs = map[string]SharedInput{"data": {NAS: "shared/input", SHA256: strings.Repeat("a", 64), SizeBytes: 8 << 30}}
	resolve := func() ResolveNASInput {
		t.Helper()
		var report ResolveReport
		if err := observer.Call(context.Background(), "POST", "/api/resolve", request, &report); err != nil {
			t.Fatal(err)
		}
		if len(report.Targets) != 1 || len(report.Targets[0].NASInputs) != 1 {
			t.Fatal(report)
		}
		return report.Targets[0].NASInputs[0]
	}
	if got := resolve(); got.State != "pending" || !got.ObservedAt.IsZero() {
		t.Fatal("inspection did not start pending", got)
	}
	clock.Advance(time.Second)
	var reply Reply
	if err := worker.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}, &reply); err != nil {
		t.Fatal(err)
	}
	if len(reply.NASPathChecks) != 1 || reply.NASPathChecks[0].Path != "shared/input" {
		t.Fatal("joining listener inspection never reached control listener heartbeat", reply.NASPathChecks)
	}
	clock.Advance(time.Second)
	report := NASPathReport{ID: reply.NASPathChecks[0].ID, State: "present"}
	if err := worker.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, NASPathReports: []NASPathReport{report}}, &reply); err != nil {
		t.Fatal(err)
	}
	if got := resolve(); got.State != "present" || !got.ObservedAt.Equal(clock.Now()) {
		t.Fatal("control listener result did not reach joining listener resolve", got)
	}
	if len(q.s.Jobs) != 0 || len(q.s.Reservations) != 0 {
		t.Fatal("NAS inspection submitted or reserved work")
	}
}
