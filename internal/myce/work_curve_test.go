package myce

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
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

// A worker upgrade reports physical inventory before its asynchronous backend
// fingerprint probe completes. That heartbeat must not poison retained curves.
