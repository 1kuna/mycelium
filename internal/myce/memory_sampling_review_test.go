package myce

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type memorySamplingFS struct {
	fakeMemoryFS
	reads       atomic.Int32
	queries     atomic.Int32
	blockedAt   atomic.Int64
	duringQuery bool
	spike       bool
	marker      string
	observed    chan struct{}
}

func (f *memorySamplingFS) ReadFile(p string) ([]byte, error) {
	if filepath.Base(p) == "cgroup.procs" && f.duringQuery && f.queries.Add(1) == 2 {
		f.blockedAt.Store(time.Now().UnixNano())
	}
	if filepath.Base(p) != "memory.stat" {
		return f.fakeMemoryFS.ReadFile(p)
	}
	n := f.reads.Add(1)
	if f.spike {
		if n == 3 {
			close(f.observed)
		}
		switch n {
		case 1:
			return []byte("anon 0\nkernel 0\nshmem 0\n"), nil
		case 2:
			return []byte("anon 8589934592\nkernel 0\nshmem 0\n"), nil
		}
	} else if _, err := os.Stat(f.marker); err == nil {
		select {
		case <-f.observed:
		default:
			close(f.observed)
		}
	}
	if start := f.blockedAt.Load(); start != 0 {
		elapsed := time.Since(time.Unix(0, start))
		if elapsed >= 200*time.Millisecond && elapsed < 1200*time.Millisecond {
			return []byte("anon 8589934592\nkernel 0\nshmem 0\n"), nil
		}
	}
	return []byte("anon 2147483648\nkernel 0\nshmem 0\n"), nil
}

func TestMemorySamplingShortHostPeak(t *testing.T) {
	a, err := fakeCeilings(t).create("attempt-short", 16<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := &memorySamplingFS{spike: true, observed: make(chan struct{})}
	a.fs = f
	finish := (Worker{}).observeAttemptMemory(a, Job{Spec: cpuSpec(1, 0)}, nil)
	defer finish()
	select {
	case <-f.observed:
	case <-time.After(time.Second):
		t.Fatal("short-lived host spike was not sampled before exit")
	}
	p := finish()
	if p == nil || p.HostGB == nil || *p.HostGB != 8 {
		t.Fatalf("post-exit reading replaced transient host peak: %+v", p)
	}
}

func TestMemorySamplingGPUStartsLate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MYCE_TEST_DEVICE_PROBE", dir)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	// Empty, timeout, then memory. The fourth probe establishes that the
	// preceding measurement was consumed before cancellation, and stops as
	// unsupported. A host spike during the timeout must remain observable.
	script := `#!/bin/sh
if [ ! -e "$MYCE_TEST_DEVICE_PROBE/empty" ]; then
  : > "$MYCE_TEST_DEVICE_PROBE/empty"
  exit 0
fi
if [ ! -e "$MYCE_TEST_DEVICE_PROBE/timeout" ]; then
  : > "$MYCE_TEST_DEVICE_PROBE/timeout"
  exec sleep 30
fi
if [ ! -e "$MYCE_TEST_DEVICE_PROBE/value" ]; then
  : > "$MYCE_TEST_DEVICE_PROBE/value"
  printf 'GPU-one, 42, 4096\n'
  exit 0
fi
printf 'GPU-one, 42, N/A\n'
: > "$MYCE_TEST_DEVICE_PROBE/measured"
`
	if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	a, err := fakeCeilings(t).create("attempt-late-gpu", 16<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.path, "cgroup.procs"), []byte("42"), 0600); err != nil {
		t.Fatal(err)
	}
	f := &memorySamplingFS{duringQuery: true, marker: filepath.Join(dir, "measured"), observed: make(chan struct{})}
	a.fs = f
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one"}}
	j := Job{Spec: Spec{Needs: ResourceNeeds{Memory: "auto", Platform: Platform{Required: []string{"cuda"}}}}}
	finish := w.observeAttemptMemory(a, j, nil)
	defer finish()
	select {
	case <-f.observed:
	case <-time.After(25 * time.Second):
		t.Fatal("empty or timed-out GPU query permanently disabled sampling")
	}
	p := finish()
	if p == nil || p.DeviceGB == nil || *p.DeviceGB != 4 {
		t.Fatalf("late CUDA allocation was not retained: %+v", p)
	}
	if p.HostGB == nil || *p.HostGB != 8 {
		t.Fatalf("slow GPU query opened a gap in host sampling: %+v", p)
	}
}

func TestMemorySamplingOOMInvalidatesAndRelearns(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	host := 2.0
	r := result(&first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// Invalidate exact, same-family and other-family memory estimates together.
	for _, point := range q.s.WorkObservations {
		point.Key.Host = "another-host"
		q.s.WorkObservations[identityHash(point.Key)] = point
		point.Key.Target.Arch = "arm64"
		q.s.WorkObservations[identityHash(point.Key)] = point
		break
	}
	second := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r = result(&second, false)
	r.Error, r.MemoryPeak = "exceeded its 2.5 GiB memory ceiling", &AttemptMemoryPeak{Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	for _, point := range q.s.WorkObservations {
		if point.MemoryPeak != nil || point.Forbidden {
			t.Fatalf("auto OOM retained a memory estimate or poisoned predictions: %+v", point)
		}
	}
	probe := first
	q.beginWorkTiming(&probe)
	if probe.Timing.PredictedSeconds == nil {
		t.Fatal("memory invalidation suppressed completion predictions")
	}
	if err := q.Cancel(second.ID); err != nil {
		t.Fatal(err)
	}
	explore := startObservation(t, q, m, s)
	if explore.MemoryEnvelope.Source != "exploratory first run" {
		t.Fatalf("invalidated class did not explore: %+v", explore.MemoryEnvelope)
	}
	clock.Advance(time.Second)
	r = result(&explore, true)
	host = 8
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, s)
	if next.Spec.Needs.HostMemoryGB != 10 || !strings.Contains(next.MemoryEnvelope.Source, "accepted peak") {
		t.Fatalf("successful exploration did not re-teach memory: %+v", next.MemoryEnvelope)
	}
}

func TestMemorySamplingHostOOMKeepsGPUReservation(t *testing.T) {
	q, clock := fixture(t)
	m := machine("gpu", "fake")
	m.TotalMemoryGB, m.FreeMemoryGB = 8, 8
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 32, 32
	check(t, q, m, nil)
	s := spec("experiment")
	s.Resumable = false
	s.Needs.Memory, s.Needs.MemoryGB, s.Needs.HostMemoryGB = "auto", 2, 4
	queued := submit(t, q, s)
	first := check(t, q, m, nil).Job
	if first == nil {
		t.Fatal(job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(first, false)
	r.OutOfMemory, r.Error = true, "exceeded its host memory ceiling"
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := check(t, q, m, nil).Job
	if next == nil || next.Spec.Needs.MemoryGB != first.Spec.Needs.MemoryGB {
		t.Fatalf("host-only OOM increased GPU reservation: first=%+v next=%+v", first.Spec.Needs, next)
	}
	probe := *next
	probe.State, probe.MemoryOOMFloor = "queued", nil
	probe.MemoryFallback = &SizeHint{MemoryGB: 64, HostMemoryGB: 4}
	if n := q.memoryCandidate(&probe, q.s.Machines[m.ID]).Spec.Needs.MemoryGB; n > m.TotalMemoryGB {
		t.Fatalf("auto reservation exceeds card total: %g", n)
	}
}

func TestMemorySamplingOOMRetryExceedsCapacity(t *testing.T) {
	for _, backend := range []string{"cpu", "fake"} {
		t.Run(backend, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("small", "small-host", backend, 8, 8)
			m.HostMemoryBudgetGB = 8
			m.TotalMemoryGB, m.FreeMemoryGB = 4, 4
			check(t, q, m, nil)
			s := cpuSpec(1, 0)
			s.Needs.Memory = "auto"
			if backend != "cpu" {
				s = spec("experiment")
				s.Resumable = false
				s.Needs.Memory, s.Needs.MemoryGB, s.Needs.HostMemoryGB = "auto", 2, 4
			}
			queued := submit(t, q, s)
			first := check(t, q, m, nil).Job
			if first == nil {
				t.Fatal(job(t, q, queued.ID).Reason)
			}
			if err := q.Started(result(first, false)); err != nil {
				t.Fatal(err)
			}
			if backend != "cpu" {
				q.s.Machines[m.ID].TotalMemoryGB = 1
			}
			clock.Advance(time.Second)
			r := result(first, false)
			r.OutOfMemory = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, queued.ID)
			if got.State != "failed" || !strings.Contains(got.Reason, "needs more memory than any eligible host allows (") {
				t.Fatalf("impossible retry waited forever: %s %s", got.State, got.Reason)
			}
		})
	}
}

func TestMemorySamplingSubTickDoesNotLearn(t *testing.T) {
	a, err := fakeCeilings(t).create("attempt-subtick", 4<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	a.fs = &memorySamplingFS{}
	peak := (Worker{}).observeAttemptMemory(a, Job{Spec: cpuSpec(1, 0)}, nil)()
	if peak == nil || !peak.Constrained {
		t.Fatalf("start/post-exit readings were learned as a peak: %+v", peak)
	}
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	r.MemoryPeak = peak
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if p, _, _ := q.memoryEstimate(&first, q.s.Machines[m.ID]); p != nil {
		t.Fatal("sub-tick run trained memory")
	}
}

func TestMemorySamplingXPUKernelSizes(t *testing.T) {
	for _, tc := range []struct {
		value string
		gb    float64
	}{{"0", 0}, {"4096", 4096.0 / (1 << 30)}, {"2 MiB", 2.0 / 1024}, {"3 KiB", 3.0 / (1 << 20)}} {
		t.Run(tc.value, func(t *testing.T) {
			_, gb, err := drmAttemptMemory([]byte("drm-client-id: 7\ndrm-pdev: 0000:03:00.0\ndrm-resident-vram0: "+tc.value+"\n"), "0000:03:00.0")
			if err != nil || gb != tc.gb {
				t.Fatalf("kernel size %q: %g %v", tc.value, gb, err)
			}
		})
	}
}

func TestMemorySamplingDockerRestartDoesNotLearnPostExit(t *testing.T) {
	_, clock := fixture(t)
	a, err := fakeCeilings(t).create("attempt-recovered", 4<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	a.fs = &memorySamplingFS{}
	d := &dockerAttempt{memory: a, clock: clock, dir: t.TempDir(), state: dockerExecution{Identity: "recovered", CgroupParent: "/synthetic", MainFinished: true, MemoryLimitBytes: 4 << 30}}
	if err := d.releaseEnvelope(); err != nil {
		t.Fatal(err)
	}
	r := dockerOutcome(d.state, Result{}, nil)
	if !r.Success {
		t.Fatalf("known main outcome changed: %+v", r)
	}
	if r.MemoryPeak != nil && r.MemoryPeak.HostGB != nil && !r.MemoryPeak.Constrained {
		t.Fatalf("post-exit Docker reading became a learned peak: %+v", r.MemoryPeak)
	}
}
