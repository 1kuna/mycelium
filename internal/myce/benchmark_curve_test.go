package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBenchmarkCurveRealReferenceCommand(t *testing.T) {
	q, clock, m, e, _ := curveFixture(t)
	root := t.TempDir()
	fingerprint, err := ExtensionFingerprint(context.Background(), "cpu", "")
	if err != nil {
		t.Fatal(err)
	}
	m.ExtensionFingerprint = fingerprint
	r := newExtensionReport(e, &m, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "local-reference"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	if err := writeExtensionReports(root, e, "cpu", m.ExtensionReports); err != nil {
		t.Fatal(err)
	}
	target := cloneMachine(q.s.Machines[m.ID])
	c := BenchmarkConfig{Clock: clock, StateDir: newTestStateRoot(t), ExtensionRoot: root, CurveMaxConcurrency: 2}
	points := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", curveMonitor(clock, false))
	if len(points) != 2 || points[0].Rate != 5 || points[1].Rate != 10 || points[1].HostMemoryGB != 2 {
		t.Fatal("real reference replay failed", points)
	}
	// Every concurrent item must pass the frozen baseline, not just item zero.
	e.Version = "2"
	e.Files["curve.sh"] = append(e.Files["curve.sh"], []byte("printf bad > \"$MYCE_CHECK_OUTPUT_DIR/1\"\n")...)
	e = freezeComposition(t, e)
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	r = newExtensionReport(e, &target, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "changed-reference"
	target.ExtensionReports = []ExtensionReport{r}
	if err := writeExtensionReports(root, e, "cpu", target.ExtensionReports); err != nil {
		t.Fatal(err)
	}
	got := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", curveMonitor(clock, false))
	if len(got) != 1 {
		t.Fatal("incorrect item learned", got)
	}
	// Process supervision completed before return; only disposable cache remains.
	active, err := filepath.Glob(filepath.Join(c.StateDir, "attempt-command-*.json"))
	if err != nil || len(active) != 0 {
		t.Fatal("command custody leaked", active, err)
	}
	files, err := os.ReadDir(extensionCache(root, e, "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.IsDir() && len(f.Name()) >= 6 && f.Name()[:6] == "curve-" {
			t.Fatal("scratch leaked", f.Name())
		}
	}
}

func TestBenchmarkCurveWorkerPublishesExactCleanup(t *testing.T) {
	q, clock, m, e, _ := curveFixture(t)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	w := Worker{Machine: m, Clock: clock, StateDir: newTestStateRoot(t), ExtensionRoot: root, Interval: time.Millisecond, CurveMaxConcurrency: 2,
		Client: Client{URL: server.URL, Token: "test"}, HostMemorySource: func(context.Context) (float64, error) { return 32, nil },
		OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} },
		Benchmark: func(ctx context.Context, c BenchmarkConfig, p *CapabilityProfile) error {
			c.Measure = func(context.Context, string) ([]Throughput, error) {
				return []Throughput{{Rate: 1, Unit: "records/s"}}, nil
			}
			c.MeasureCurve = func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
				return sampleCurve(count, 2), nil
			}
			return RunOnboardingBenchmark(ctx, c, p)
		}}
	w.availability = newAvailabilityMonitor(clock, false, false, false)
	w.availability.sample(w.OwnerSignals(context.Background()))
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err = w.runBenchmark(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.WorkCurves) != 2 || len(q.s.PreparationPlans) != 0 || snap.Machines[0].BenchmarkPending {
		t.Fatalf("worker did not publish after exact cleanup: %+v", snap.WorkCurves)
	}
	// A stale completion cannot inject or modify evidence after custody is gone.
	points := snap.WorkCurves
	points[0].SampleID = ID()
	points[0].Rate = 999
	if _, err = q.Checkin(Checkin{Machine: m, BenchmarkDone: "stale", CurveReports: points}); err != nil {
		t.Fatal(err)
	}
	for _, point := range q.s.WorkObservations {
		if point.Rate == 999 {
			t.Fatal("stale benchmark publication accepted")
		}
	}
}
