package myce

import (
	"os"
	"path/filepath"
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
	finish := (Worker{}).observeAttemptMemory(a, Job{Spec: cpuSpec(1, 0)})
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
	finish := w.observeAttemptMemory(a, j)
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

func TestMemorySamplingOOMCorrectsStoredEstimate(t *testing.T) {
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
	second := startObservation(t, q, m, s)
	for attempt := 0; attempt < 2; attempt++ {
		clock.Advance(time.Second)
		r = result(&second, false)
		r.OutOfMemory, r.Error = true, "exceeded its memory ceiling"
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		floor := second.Spec.Needs.HostMemoryGB * 1.25
		probe := second
		probe.State, probe.MemoryOOMFloor = "queued", nil
		peak, _, _ := q.memoryEstimate(&probe, q.s.Machines[m.ID])
		if peak == nil || peak.HostGB == nil || *peak.HostGB < floor {
			t.Fatalf("new jobs retain underestimated history after OOM: peak=%+v floor=%g", peak, floor)
		}
		if attempt == 0 {
			next := check(t, q, m, nil).Job
			if next == nil {
				t.Fatal("OOM retry missing")
			}
			second = *next
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
		}
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
