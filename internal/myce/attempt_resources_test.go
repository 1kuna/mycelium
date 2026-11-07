package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func resourceCgroupFixture(t testing.TB) *memoryAttempt {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"cpu.stat":    "usage_usec 9000000\nuser_usec 7000000\nsystem_usec 2000000\nnr_periods 30\nnr_throttled 2\nthrottled_usec 8000\n",
		"io.stat":     "8:0 rbytes=100 wbytes=200 rios=3 wios=4 dbytes=0 dios=0\n8:1 rbytes=10 wbytes=20 rios=1 wios=2\n",
		"io.pressure": "some avg10=1.25 avg60=2.5 avg300=3.75 total=90000\nfull avg10=0.25 avg60=0.5 avg300=0.75 total=8000\n",
	}
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return &memoryAttempt{fs: osMemoryFS{}, path: root}
}
func TestAttemptResourcesCountersAndClock(t *testing.T) {
	_, clock := fixture(t)
	a := resourceCgroupFixture(t)
	o := &resourceObserver{clock: clock, start: clock.Now(), read: func() *AttemptResources { return cgroupResources(a, clock.Now()) }}
	o.sample()
	clock.Advance(3 * time.Second)
	o.sample()
	r := o.snapshot()
	if *r.CPU.UsageSeconds != 9 || *r.CPU.UserSeconds != 7 || *r.CPU.SystemSeconds != 2 || *r.CPU.Periods != 30 || *r.CPU.ThrottledPeriods != 2 || *r.CPU.ThrottledSeconds != .008 || r.ElapsedSeconds != 3 {
		t.Fatalf("CPU/time: %+v", r)
	}
	if *r.IO.ReadBytes != 110 || *r.IO.WriteBytes != 220 || *r.IO.ReadOps != 4 || *r.IO.WriteOps != 6 || r.IO.Pressure.Some.TotalUS != 90000 || r.IO.Pressure.Full.Avg10 != .25 {
		t.Fatalf("IO: %+v", r.IO)
	}
	// Host pressure uses the same parser, including a kernel without full PSI.
	p := readIOPressure(func(string) ([]byte, error) { return []byte("some avg10=0 avg60=0 avg300=0 total=0\n"), nil }, "unused", clock.Now())
	if p.Some == nil || p.Full != nil || p.Unavailable != "" {
		t.Fatal(p)
	}
	clock.Advance(time.Second)
	if o.snapshot().AgeSeconds != 1 {
		t.Fatal("clock age not retained")
	}
	if err := os.Remove(filepath.Join(a.path, "cpu.stat")); err != nil {
		t.Fatal(err)
	}
	o.sample()
	r = o.snapshot()
	if r.CPU.UsageSeconds == nil || *r.CPU.UsageSeconds != 9 || !r.CPU.LowerBound || r.CPU.Unavailable != "final_read_failed" {
		t.Fatal(r.CPU)
	}
	if err := os.WriteFile(filepath.Join(a.path, "io.stat"), []byte("8:0 rbytes=oops wbytes=1 rios=2 wios=3"), 0600); err != nil {
		t.Fatal(err)
	}
	r = cgroupResources(a, clock.Now())
	if r.IO.ReadBytes != nil || r.IO.Unavailable != "invalid_counters" {
		t.Fatal(r.IO)
	}
}
func TestAttemptResourceProcessLifetimes(t *testing.T) {
	seen := map[string]float64{}
	roots := map[int]string{10: "launch-birth"}
	sample := func(rows string, want float64) {
		t.Helper()
		got, err := resourceTreeCPU([]byte(rows), roots, seen)
		if err != nil || got != want {
			t.Fatalf("cpu=%v want=%v err=%v", got, want, err)
		}
	}
	sample("10 1 10 0:01 Mon Nov 2 10:00:00 2026\n11 10 10 0:02 Mon Nov 2 10:00:01 2026\n99 1 99 9:00 Mon Nov 2 10:00:00 2026", 3)
	// Exited child, a descendant in another group, and a reused PID.
	sample("10 1 10 0:02 Mon Nov 2 10:00:00 2026\n12 10 12 0:03 Mon Nov 2 10:00:02 2026\n11 10 10 0:01 Mon Nov 2 10:00:03 2026", 8)
	sample("10 1 10 0:03 Mon Nov 2 10:00:00 2026", 9)
	if len(seen) != 2 {
		t.Fatalf("retired PID history grew: %v", seen)
	}
}
func TestAttemptResourceQueueFencesAndRetention(t *testing.T) {
	q, clock := fixture(t)
	m := machine("resource-worker", "fake")
	j := submit(t, q, spec("experiment"))
	reply := check(t, q, m, nil)
	if reply.Job == nil {
		t.Fatal("no assignment")
	}
	run := *reply.Job
	r := cgroupResources(resourceCgroupFixture(t), clock.Now())
	_, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: j.ID, Attempt: run.Attempt}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: run.Attempt}, Session: m.Session, Started: true, Resources: r}
	if _, err = q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	facts := job(t, q, j.ID).ResourceAttempts
	if len(facts) != 1 || *facts[0].Resources.CPU.UsageSeconds != 9 {
		t.Fatal(facts)
	}
	report.Attempt++
	report.Resources = &AttemptResources{CPU: AttemptCPUTime{Source: "cgroup_v2"}}
	if _, err = q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID).ResourceAttempts; !reflect.DeepEqual(got, facts) {
		t.Fatal("stale report changed telemetry")
	}
	result := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: run.Attempt}, Success: true, Resources: r}
	if err = q.Complete(result); err != nil {
		t.Fatal(err)
	}
	// Persisted facts survive a restart and the public job projection.
	q = memoryFactsRestart(t, q, clock)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	got, getErr := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), j.ID)
	if getErr != nil || len(got.ResourceAttempts) != 1 || *got.ResourceAttempts[0].Resources.CPU.UsageSeconds != 9 {
		t.Fatalf("public/restart facts: %+v %v", got, getErr)
	}
	copy := Job{Attempt: 1, Machine: "worker", Session: "session"}
	for i := 1; i <= 32+2; i++ {
		copy.Attempt = i
		q.retainAttemptResources(&copy, r)
	}
	if len(copy.ResourceAttempts) != 34 || copy.ResourceAttempts[0].Attempt != 1 || copy.ResourceAttemptsDropped != 0 {
		t.Fatal("retention mismatch")
	}
}

func BenchmarkAttemptResourceCgroupSample(b *testing.B) {
	a := resourceCgroupFixture(b)
	at := time.Unix(1, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cgroupResources(a, at)
	}
}

func TestHostIOPressureProjection(t *testing.T) {
	q, clock := fixture(t)
	m := machine("io-worker", "fake")
	m.IOPressure = readIOPressure(func(string) ([]byte, error) {
		return []byte("some avg10=12 avg60=3 avg300=1 total=90000\nfull avg10=4 avg60=1 avg300=0 total=10000\n"), nil
	}, "fake-proc", clock.Now())
	m.IOPressure.AgeSeconds = 2
	m.IOPressure.ReceivedAt = clock.Now().Add(48 * time.Hour)
	check(t, q, m, nil)
	clock.Advance(3 * time.Second)
	snap, err := q.Snapshot()
	if err != nil || len(snap.Machines) != 1 || snap.Machines[0].IOPressure == nil {
		t.Fatal(snap, err)
	}
	p := snap.Machines[0].IOPressure
	if p.AgeSeconds != 5 || p.Some.Avg10 != 12 || p.Full.TotalUS != 10000 || !p.ReceivedAt.Equal(clock.Now().Add(-3*time.Second)) {
		t.Fatal(p)
	}
}
