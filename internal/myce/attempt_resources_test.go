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

func TestAttemptResourceQueueFencesAndRetention(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("AttemptResourcesCountersAndClock", func(t *testing.T) {
		_, clock := fixture(t)
		a := resourceCgroupFixture(t)
		o := &resourceObserver{clock: clock, start: clock.Now(), read: func() *AttemptResources { return cgroupResources(a, clock.Now()) }}
		o.sample()
		clock.Advance(3 * time.Second)
		o.sample()
		r := o.snapshot()
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
	})

	t.Run("HostIOPressureProjection", func(t *testing.T) {
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

	})
}
