package myce

import (
	"bytes"
	"context"
	"errors"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func curveFixture(t *testing.T) (*Queue, *workerFakeClock, Machine, Extension, Spec) {
	t.Helper()
	q, initial := fixture(t)
	clock := &workerFakeClock{now: initial.Now()}
	q.clock = clock
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target.Platform = "cpu"
	v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	v.Sweeps = map[string]ReferenceSweep{"extension:sample": {Command: []string{"sh", "curve.sh"}, Counts: []int{1, 2, 4, 8}, MaxConcurrency: 8}}
	e.Files["curve.sh"] = []byte(`test -z "$MYCE_TOKEN" || exit 99
c=$MYCE_BENCHMARK_CONCURRENCY
i=0
while [ "$i" -lt "$c" ]; do cat "$MYCE_CHECK_INPUT" > "$MYCE_CHECK_OUTPUT_DIR/$i"; i=$((i+1)); done
printf '{"units":%s,"seconds":2,"latency_seconds":2,"baseline_host_memory_gb":1,"host_memory_gb":2,"baseline_device_memory_gb":0,"device_memory_gb":0,"out_of_memory":false}' "$((10*c))" > "$MYCE_BENCHMARK_OUTPUT"
`)
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("curves", "curve-host", "cpu", 8, 32)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	m.ExtensionFingerprint = "cpu-v1"
	r := newExtensionReport(e, &m, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "curve-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	return q, clock, m, e, Spec{Name: "count", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
}
func curveMonitor(clock Clock, daily bool) *availabilityMonitor {
	monitor := newAvailabilityMonitor(clock, daily, false, false)
	monitor.sample(idleSignals())
	return monitor
}
func sampleCurve(count int, seconds float64) CurveMeasurement {
	return CurveMeasurement{Units: 10 * float64(count), Seconds: seconds, LatencySeconds: seconds, BaselineHostMemoryGB: 1, HostMemoryGB: 2}
}
func publishCurves(t *testing.T, q *Queue, m Machine, points []WorkObservation) {
	t.Helper()
	m.BenchmarkPending = true
	reply := check(t, q, m, nil)
	if reply.BenchmarkPlan == nil {
		t.Fatal("no benchmark custody")
	}
	if err := q.AuthorizeLaunch(*reply.BenchmarkPlan); err != nil {
		t.Fatal(err)
	}
	m.BenchmarkPending = false
	if _, err := q.Checkin(Checkin{Machine: m, BenchmarkDone: reply.BenchmarkPlan.Attempt, CurveReports: points}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkCurveRisingFallingAcceptedEWMAAndStatus(t *testing.T) {
	q, clock, m, e, s := curveFixture(t)
	target := cloneMachine(q.s.Machines[m.ID])
	monitor := curveMonitor(clock, false)
	c := BenchmarkConfig{Clock: clock, CurveMaxConcurrency: 8, MeasureCurve: func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
		seconds := map[int]float64{1: 2, 2: 2, 4: 5}[count]
		if seconds == 0 {
			t.Fatal("continued past falling throughput")
		}
		clock.Advance(time.Second)
		return sampleCurve(count, seconds), nil
	}}
	points := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", monitor)
	if len(points) != 3 || points[0].Rate != 5 || points[1].Rate != 10 || points[2].Rate != 8 || points[1].IncrementalHostMemoryGB != 1 {
		t.Fatalf("wrong sparse curve: %+v", points)
	}
	publishCurves(t, q, m, points)
	if len(q.s.WorkObservations) != 3 {
		t.Fatal("points not published", q.s.WorkObservations)
	}
	// Each accepted attempt is a clean, stable-occupancy concurrency-one window.
	j := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	finish(t, q, &j, true)
	id := identityHash(points[0].Key)
	learned := q.s.WorkObservations[id]
	if learned.Rate != 6 || math.Abs(learned.LatencySeconds-1.8) > 1e-9 || learned.Samples != 2 || learned.HostMemoryGB != 2 {
		t.Fatalf("not EWMA/high-water: %+v", learned)
	}
	finish(t, q, &j, true)
	if q.s.WorkObservations[id].Samples != 2 {
		t.Fatal("duplicate refined curve")
	}
	var before, after int
	q.db.QueryRow("SELECT total_changes()").Scan(&before)
	snap, err := q.snapshot(true)
	if err != nil {
		t.Fatal(err)
	}
	q.db.QueryRow("SELECT total_changes()").Scan(&after)
	var html bytes.Buffer
	if err = statusPage.Execute(&html, snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.WorkCurves) != 3 || before != after || !strings.Contains(html.String(), "6.00 records/s") || !strings.Contains(learned.Summary(), "incremental host/device 1.00/0.00") {
		t.Fatal("status did not expose read-only curves", html.String())
	}
	var rows int
	if err = q.db.QueryRow("SELECT count(*) FROM queue_state WHERE key LIKE 'work_observations/%'").Scan(&rows); err != nil || rows != 3 {
		t.Fatal("not per-record SQLite", rows, err)
	}
	// Cache replay on another exact cleanup must not create another sample.
	publishCurves(t, q, m, points)
	if q.s.WorkObservations[id].Samples != 2 {
		t.Fatal("cached benchmark was counted again")
	}
}

func TestWorkCurveFakeClockOwnerReturnAndBudget(t *testing.T) {
	q, clock, m, e, _ := curveFixture(t)
	target := cloneMachine(q.s.Machines[m.ID])
	monitor := curveMonitor(clock, true)
	called := 0
	c := BenchmarkConfig{Clock: clock, CurveMaxConcurrency: 8, MeasureCurve: func(ctx context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
		called++
		clock.Advance(signalRefresh)
		active := idleSignals()
		active.IdleSeconds = 0
		monitor.sample(active)
		select {
		case <-ctx.Done():
			return CurveMeasurement{}, ctx.Err()
		case <-time.After(time.Second):
			t.Fatal("owner return failed to cancel")
			return CurveMeasurement{}, nil
		}
	}}
	if got := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", monitor); len(got) != 0 || called != 1 {
		t.Fatal("cancelled work learned", got, called)
	}
	called = 0
	if got := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", monitor); len(got) != 0 || called != 0 {
		t.Fatal("active owner started sweep")
	}
	monitor = curveMonitor(clock, false)
	c.MeasureCurve = func(ctx context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, _ int) (CurveMeasurement, error) {
		called++
		clock.Advance(11 * time.Second)
		select {
		case <-ctx.Done():
			return CurveMeasurement{}, ctx.Err()
		case <-time.After(time.Second):
			t.Fatal("Clock budget did not cancel")
			return CurveMeasurement{}, nil
		}
	}
	if got := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", monitor); len(got) != 0 || called != 1 {
		t.Fatal("over-budget point learned", got, called)
	}
	_ = m
}

func TestWorkCurveBusyHostNeverStartsAndSerialCustody(t *testing.T) {
	q, _, m, _, s := curveFixture(t)
	running := startObservation(t, q, m, s)
	m.BenchmarkPending = true
	if reply := check(t, q, m, nil); reply.BenchmarkPlan != nil {
		t.Fatal("busy host offered sweep")
	}
	finish(t, q, &running, true)
	reply := check(t, q, m, nil)
	if reply.BenchmarkPlan == nil {
		t.Fatal("idle host never offered sweep")
	}
	if err := q.AuthorizeLaunch(*reply.BenchmarkPlan); err != nil {
		t.Fatal(err)
	}
	// The same host's ordinary work cannot overlap even before the first point.
	submit(t, q, cpuSpec(1, 1))
	if got := check(t, q, m, nil); got.Job != nil {
		t.Fatal("job overlapped benchmark")
	}
	// A final launch recheck rejects work that appeared after an offer.
	other := submit(t, q, cpuSpec(1, 1))
	if err := q.update(func() error { q.assign(q.s.Jobs[other.ID], q.s.Machines[m.ID]); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(*reply.BenchmarkPlan); !errors.Is(err, ErrStale) {
		t.Fatal("launch bypassed busy check", err)
	}
}

func TestWorkCurveFingerprintStalenessAndOOM(t *testing.T) {
	q, clock, m, e, s := curveFixture(t)
	target := cloneMachine(q.s.Machines[m.ID])
	c := BenchmarkConfig{Clock: clock, CurveMaxConcurrency: 8, MeasureCurve: func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
		if count == 2 {
			return CurveMeasurement{OutOfMemory: true}, errors.New("synthetic OOM")
		}
		return sampleCurve(count, 2), nil
	}}
	points := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", curveMonitor(clock, false))
	if len(points) != 2 || !points[1].Forbidden {
		t.Fatal("OOM did not forbid point", points)
	}
	publishCurves(t, q, m, points)
	c.PreviousCurves = points
	c.MeasureCurve = func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
		if count != 1 {
			t.Fatal("retried forbidden point")
		}
		return sampleCurve(count, 1), nil
	}
	if got := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", curveMonitor(clock, false)); len(got) != 1 {
		t.Fatal(got)
	}
	// A successful result can never clear an OOM on the same identity.
	success := points[1]
	success.SampleID = ID()
	success.Forbidden = false
	success.Rate = 10
	success.Units = 20
	success.Seconds = 2
	success.LatencySeconds = 2
	publishCurves(t, q, m, []WorkObservation{success})
	if !q.s.WorkObservations[identityHash(&success.Key)].Forbidden {
		t.Fatal("success cleared OOM")
	}
	// Real job OOM has exact execution custody, forbids c=1 and learns no speed.
	j := startObservation(t, q, m, s)
	if err := q.Complete(Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, OutOfMemory: true}); err != nil {
		t.Fatal(err)
	}
	if !q.s.WorkObservations[identityHash(points[0].Key)].Forbidden {
		t.Fatal("ordinary OOM was not retained")
	}
	m.ExtensionFingerprint = "changed-driver"
	check(t, q, m, nil)
	for _, p := range q.s.WorkObservations {
		if !strings.Contains(p.StaleReason, "fingerprint changed") {
			t.Fatalf("no stale fingerprint reason: %+v", p)
		}
		if strings.Contains(p.Summary(), "records/s") {
			t.Fatal("stale rate presented as reusable")
		}
	}
	r := newExtensionReport(e, &m, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "new-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	job := startObservation(t, q, m, s)
	if got := q.s.Jobs[job.ID].Timing; got.PredictedSeconds != nil {
		t.Fatal("new fingerprint reused rate", got)
	}
}

func TestWorkCurveBoundsAndSharedFakeDevices(t *testing.T) {
	q, _, m, e, _ := curveFixture(t)
	target := cloneMachine(q.s.Machines[m.ID])
	needs := ResourceNeeds{CPUCores: 2, MemoryGB: 4, HostMemoryGB: 2}
	target.GPU, target.UnifiedMemory = "fake", true
	target.FreeMemoryGB, target.TotalMemoryGB = 16, 16
	target.FreeHostMemoryGB, target.EffectiveHostMemoryBudgetGB = 10, 10
	target.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "card-0", BudgetGB: 16}, {ID: "1", Platform: "fake", Identity: "card-1", BudgetGB: 16}}
	if !curveFits(&target, needs, 1) || curveFits(&target, needs, 2) {
		t.Fatal("unified RAM charged incorrectly")
	}
	target.UnifiedMemory = false
	if !curveFits(&target, needs, 2) || curveFits(&target, needs, 8) {
		t.Fatal("shared CPU/device bounds not enforced")
	}
	target.HostMemoryMeasured = false
	if curveFits(&target, needs, 1) {
		t.Fatal("unknown RAM authorized sweep")
	}
	// Operator bounds and explicit opt-in bound even a recipe with spare capacity.
	clock := &workerFakeClock{now: q.clock.Now()}
	monitor := curveMonitor(clock, false)
	cpu := cloneMachine(q.s.Machines[m.ID])
	called := []int{}
	c := BenchmarkConfig{Clock: clock, CurveMaxConcurrency: 2, MeasureCurve: func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
		called = append(called, count)
		return sampleCurve(count, 2), nil
	}}
	runReferenceSweep(context.Background(), c, e, &cpu, "extension:sample", monitor)
	if !slices.Equal(called, []int{1, 2}) {
		t.Fatal("operator bound exceeded", called)
	}
	called = nil
	c.CurveMaxConcurrency = 0
	runReferenceSweep(context.Background(), c, e, &cpu, "extension:sample", monitor)
	if len(called) != 0 {
		t.Fatal("sweep ran without opt-in")
	}
	invalid := e
	invalid.Variants[0].Sweeps["extension:sample"] = ReferenceSweep{Command: []string{"echo"}, Counts: []int{1, 3}, MaxConcurrency: 3}
	if invalid.validateVariants() == nil {
		t.Fatal("unsupported points accepted")
	}
}

func TestWorkCurveOwnerReturnRetainsMeasuredAndOOMEvidence(t *testing.T) {
	q, clock, m, e, _ := curveFixture(t)
	monitor := curveMonitor(clock, true)
	available := monitor.snapshot()
	m.CapabilityProfile.Availability = &available
	m.BenchmarkPending = true
	reply := check(t, q, m, nil)
	if reply.BenchmarkPlan == nil || reply.BenchmarkMachine == nil {
		t.Fatal("idle daily host did not get custody")
	}
	if err := q.AuthorizeLaunch(*reply.BenchmarkPlan); err != nil {
		t.Fatal(err)
	}
	c := BenchmarkConfig{Clock: clock, CurveMaxConcurrency: 8, MeasureCurve: func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
		clock.Advance(time.Second)
		if count == 1 {
			return sampleCurve(count, 2), nil
		}
		owner := idleSignals()
		owner.IdleSeconds = 0
		monitor.sample(owner)
		return CurveMeasurement{OutOfMemory: true}, errors.New("OOM raced owner return")
	}}
	points := runReferenceSweep(context.Background(), c, e, reply.BenchmarkMachine, "extension:sample", monitor)
	if len(points) != 2 || !points[1].Forbidden {
		t.Fatal("cancel raced OOM", points)
	}
	returned := monitor.snapshot()
	m.CapabilityProfile.Availability = &returned
	m.BenchmarkPending = false
	if _, err := q.Checkin(Checkin{Machine: m, BenchmarkDone: reply.BenchmarkPlan.Attempt, CurveReports: points}); err != nil {
		t.Fatal(err)
	}
	if len(q.s.WorkObservations) != 2 {
		t.Fatal("owner return discarded measured/OOM evidence", q.s.WorkObservations)
	}
	for _, point := range q.s.WorkObservations {
		if point.StaleReason != "" {
			t.Fatal("presence changed curve identity", point.StaleReason)
		}
		if point.Key.Concurrency == 2 && !point.Forbidden {
			t.Fatal("OOM was cleared")
		}
	}
}

func TestWorkCurveReportCannotPlantMemoryEvidence(t *testing.T) {
	q, clock, m, e, _ := curveFixture(t)
	target := cloneMachine(q.s.Machines[m.ID])
	c := BenchmarkConfig{Clock: clock, CurveMaxConcurrency: 1, MeasureCurve: func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
		return sampleCurve(count, 2), nil
	}}
	points := runReferenceSweep(context.Background(), c, e, &target, "extension:sample", curveMonitor(clock, false))
	host := 1.0
	points[0].MemoryPeak, points[0].MemoryRecordedAt, points[0].MemoryConfiguration = &AttemptMemoryPeak{HostGB: &host}, clock.Now(), "forged"
	publishCurves(t, q, m, points)
	p, ok := q.s.WorkObservations[identityHash(points[0].Key)]
	if !ok || p.MemoryPeak != nil || !p.MemoryRecordedAt.IsZero() || p.MemoryConfiguration != "" {
		t.Fatalf("benchmark report planted memory evidence: %+v", p)
	}
}

// #210: memory-only observations (auto extension checks) carry memory_peak with
// zero legacy usage maxima; status curve lines must show the accepted peak.
func TestWorkCurveSummaryShowsMemoryPeak(t *testing.T) {
	host, device := 3.0, 1.5
	point := WorkObservation{Key: ObservationKey{Work: "check", Host: "host", Component: "cpu", Concurrency: 1}, MemoryPeak: &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}, Samples: 1}
	if line := point.Summary(); !strings.Contains(line, "peak host/device 3.00/1.50 GiB") {
		t.Fatalf("memory-only peak not shown: %s", line)
	}
	point.MemoryPeak, point.HostMemoryGB = nil, 2
	if line := point.Summary(); !strings.Contains(line, "peak host/device 2.00/0.00 GiB") {
		t.Fatalf("legacy peak fallback lost: %s", line)
	}
}
