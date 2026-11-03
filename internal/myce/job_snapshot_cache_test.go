package myce

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestJobSnapshotCacheCommitAndRollback(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	read := func() *Job {
		t.Helper()
		got, err := q.jobSnapshot(j.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := read()
	aborted := errors.New("abort")
	if err := q.update(func() error { q.touchJob(j.ID).State = "paused"; return aborted }); !errors.Is(err, aborted) {
		t.Fatal(err)
	}
	if got := read(); got.State != before.State {
		t.Fatal("served rolled-back state", got.State)
	}
	if err := q.update(func() error { q.touchJob(j.ID).State = "paused"; return nil }); err != nil {
		t.Fatal(err)
	}
	if got := read(); got.State != "paused" {
		t.Fatal("commit did not invalidate", got.State)
	}
}

func TestJobSnapshotCacheTTL(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	read := func() float64 {
		t.Helper()
		got, err := q.jobSnapshot(j.ID)
		if err != nil || got.Runtime == nil || got.Runtime.ElapsedSeconds == nil {
			t.Fatal(got, err)
		}
		return *got.Runtime.ElapsedSeconds
	}
	initial := read()
	clock.Advance(jobSnapshotLifetime - time.Nanosecond)
	if got := read(); got != initial {
		t.Fatal("did not reuse within TTL", got)
	}
	clock.Advance(time.Nanosecond)
	if got := read(); got != initial+jobSnapshotLifetime.Seconds() {
		t.Fatal("TTL did not refresh elapsed time", got)
	}
}

func TestJobSnapshotCacheSingleflight(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	var captures atomic.Int32
	q.observeRead = func(kind string, _, _ time.Duration) {
		if kind == "job" {
			captures.Add(1)
		}
	}
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := j.ID
			if i%2 == 0 {
				id = id[:8]
			}
			got, err := q.jobSnapshot(id)
			if err != nil {
				t.Error(err)
				return
			}
			got.Spec.Project = "caller-mutation"
			if len(got.Resources) > 0 {
				got.Resources[0].Dimension = "caller-mutation"
			}
		}(i)
	}
	wg.Wait()
	got, err := q.jobSnapshot(j.ID)
	if err != nil || got.Spec.Project == "caller-mutation" {
		t.Fatal("cache aliased response", err)
	}
	if captures.Load() != 1 {
		t.Fatal("duplicate diagnostic captures", captures.Load())
	}
	// An unchanged SQLite heartbeat must retain the window.
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := q.jobSnapshot(j.ID); err != nil {
		t.Fatal(err)
	}
	if captures.Load() != 1 {
		t.Fatal("unchanged commit invalidated the cache")
	}
}

func TestJobSnapshotCacheCommitDuringRefresh(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	captured, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	q.observeRead = func(kind string, _, _ time.Duration) {
		if kind == "job" {
			once.Do(func() { close(captured); <-resume })
		}
	}
	done := make(chan *Job, 1)
	go func() {
		got, err := q.jobSnapshot(j.ID)
		if err != nil {
			t.Error(err)
		}
		done <- got
	}()
	<-captured
	if err := q.update(func() error { q.touchJob(j.ID).State = "paused"; return nil }); err != nil {
		t.Fatal(err)
	}
	close(resume)
	if got := <-done; got == nil || got.State != "paused" {
		t.Fatal("published invalidated refresh", got)
	}
}
