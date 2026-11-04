package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveNASMissingWorkerPathBeforeSubmission(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), NASCheckpoints: true, Token: "resolve-nas-fixture", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	client := Client{URL: d.URL, Token: "resolve-nas-fixture"}
	await(t, func() bool {
		snap, err := d.Queue.Snapshot()
		return err == nil && len(snap.Machines) > 0 && snap.Machines[0].SharedInputCache != nil
	})
	s := cpuResolveSpec()
	s.SharedInputs = map[string]SharedInput{"movie": {NAS: "media/missing", SHA256: strings.Repeat("a", 64), SizeBytes: 10}}
	var report ResolveReport
	request := ResolveRequest{Spec: s, InspectNAS: true}
	await(t, func() bool {
		report = ResolveReport{}
		if err := client.Call(context.Background(), "POST", "/api/resolve", request, &report); err != nil {
			t.Fatal(err)
		}
		return resolveFinding(report, "shared_inputs.movie.nas") != nil
	})
	if len(report.Targets) != 1 || report.Targets[0].Eligible || report.Targets[0].NASInputs[0].State != "missing" || report.Targets[0].NASInputs[0].ObservedAt.IsZero() {
		t.Fatalf("missing source appeared ready: %+v", report)
	}
	d.Queue.mu.Lock()
	jobs, reservations := len(d.Queue.s.Jobs), len(d.Queue.s.Reservations)
	d.Queue.mu.Unlock()
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
	m := cacheCapacityMachine("worker", 1<<20)
	m.GPU = "cpu"
	m.Session = "one"
	m.LastSeen = clock.Now()
	m.Capabilities = append(m.Capabilities, "nas_inspection_v1")
	q.s.Machines[m.ID] = &m
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
	m.LastSeen = clock.Now()
	if resolve().Targets[0].NASInputs[0].State != "pending" {
		t.Fatal("expired signal reused")
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
