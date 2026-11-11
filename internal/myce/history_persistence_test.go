package myce

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// crashCopy opens the committed SQLite state as a crash leaves it: the live
// queue never closes, so nothing held only in memory reaches the copy.
func crashCopy(t *testing.T, q *Queue, clock *fakeClock) *Queue {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "crashed.db")
	q.mu.Lock()
	_, err := q.db.Exec("VACUUM INTO ?", path)
	q.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, FormatMarker), []byte(stateFormat), 0o600); err != nil {
		t.Fatal(err)
	}
	crashed, err := Open(path, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { crashed.Close() })
	fakeCheckpointAvailability(t, crashed, true)
	return crashed
}

func storedHistories(t *testing.T, q *Queue) (EfficiencyHost, *hostLaunchMemory) {
	t.Helper()
	q.mu.RLock()
	data := q.persisted["state"]
	q.mu.RUnlock()
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return s.EfficiencyHosts["host"], s.HostLaunchMemory["host"]
}

func liveHistories(q *Queue) (EfficiencyHost, hostLaunchMemory, float64) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.s.EfficiencyHosts["host"], *q.s.HostLaunchMemory["host"], q.coldProbeDriftMarginGB(q.s.Machines["worker"])
}

// Steady check-ins leave both histories as stored; each is written when its
// stored copy would stop supporting the live decision. After a crash the
// reconstructed integrals and the cold-probe reserve match the live queue,
// and the next cold probe gets the same ceiling.
func TestMeasurementHistoriesResumeAfterCrash(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 32, UsedGB: 10, MyceliumGB: 2}
	step := func(available, mycelium float64) {
		t.Helper()
		clock.Advance(time.Second)
		m.HostAvailableMemoryGB, m.MemoryObservation.MyceliumGB = available, mycelium
		machineCheck(t, q, m)
	}
	step(19.48, 2)
	step(19.46, 2.5)
	written, writtenLaunch := storedHistories(t, q)
	if writtenLaunch == nil || len(writtenLaunch.Samples) < 2 || written.Rate.MeasuredMemoryGB == nil {
		t.Fatalf("first reserve and measured rate were not stored: %+v %+v", written, writtenLaunch)
	}

	// The same 0.02 GiB drop again: the reserve is unchanged, so no write.
	for i := range 6 {
		step([]float64{19.48, 19.46}[i%2], 2+float64(i)/10)
		if !clock.Now().Before(written.Rate.ValidUntil) {
			break
		}
		stored, launch := storedHistories(t, q)
		if !reflect.DeepEqual(stored, written) || !reflect.DeepEqual(launch, writtenLaunch) {
			t.Fatalf("steady check-in %d rewrote a history", i)
		}
	}
	live, liveLaunch, _ := liveHistories(q)
	if len(liveLaunch.Samples) <= len(writtenLaunch.Samples) || !live.At.After(written.At) {
		t.Fatal("histories did not advance in memory")
	}

	// The stored measurement's own validity lapses: write the integral.
	clock.Advance(written.Rate.ValidUntil.Sub(clock.Now()))
	step(19.48, 3)
	stored, _ := storedHistories(t, q)
	live, _, _ = liveHistories(q)
	if !reflect.DeepEqual(stored, live) {
		t.Fatalf("lapsed measurement not rewritten:\nstored %+v\nlive   %+v", stored, live)
	}
	// A larger drop changes the reserve: write the samples.
	step(19.30, 3)
	_, launch := storedHistories(t, q)
	_, liveLaunch, liveMargin := liveHistories(q)
	if !reflect.DeepEqual(*launch, liveLaunch) {
		t.Fatal("reserve change not written")
	}
	step(19.32, 3.5) // Smaller drop: batched.

	// Crash before any further write.
	crashed := crashCopy(t, q, clock)
	_, crashedLaunch, crashedMargin := liveHistories(crashed)
	if crashedMargin != liveMargin || len(crashedLaunch.Samples) >= len(liveHistoriesSamples(q)) {
		t.Fatalf("restart reserve %v, live %v", crashedMargin, liveMargin)
	}
	// At the crash instant: seconds, reservations and measured coverage are
	// exact; readings since the last write use the stored one, never more than
	// the readings' spread over that window. Coverage then pauses until the
	// first check-in, as for any restart.
	now := clock.Now()
	stored, _ = storedHistories(t, q)
	q.mu.RLock()
	want := q.s.EfficiencyHosts["host"]
	q.mu.RUnlock()
	crashed.mu.RLock()
	got := crashed.s.EfficiencyHosts["host"]
	crashed.mu.RUnlock()
	want.advance(now)
	got.advance(now)
	if got.Seconds != want.Seconds || got.ReservedMemoryGBSeconds != want.ReservedMemoryGBSeconds || got.ReservedCPUSeconds != want.ReservedCPUSeconds || got.MemoryCoveredSeconds != want.MemoryCoveredSeconds || got.Identity != want.Identity || !got.Since.Equal(want.Since) {
		t.Fatalf("restart integral diverged:\ngot  %+v\nwant %+v", got.ResourceIntegral, want.ResourceIntegral)
	}
	if spread := 0.5 * now.Sub(stored.At).Seconds(); math.Abs(got.MeasuredMemoryGBSeconds-want.MeasuredMemoryGBSeconds) > spread+1e-9 {
		t.Fatalf("measured integral off by more than the unwritten readings: got %v want %v", got.MeasuredMemoryGBSeconds, want.MeasuredMemoryGBSeconds)
	}

	// Both queues receive the same check-in and cold probe; ceilings match.
	m.HostAvailableMemoryGB = 19.40
	machineCheck(t, q, m)
	machineCheck(t, crashed, m)
	ceilings := []float64{}
	for _, queue := range []*Queue{q, crashed} {
		queued := submit(t, queue, s)
		probe := assignedJob(machineCheck(t, queue, m))
		if probe == nil || probe.ID != queued.ID || probe.MemoryEnvelope == nil {
			t.Fatal("cold probe not assigned", job(t, queue, queued.ID).Reason)
		}
		ceilings = append(ceilings, probe.MemoryEnvelope.HostGB)
	}
	if ceilings[0] != ceilings[1] {
		t.Fatalf("cold-probe ceiling after crash %v, live %v", ceilings[1], ceilings[0])
	}

	// A clean close writes the full live histories.
	step(19.41, 4)
	live, liveLaunch, _ = liveHistories(q)
	q = reopenQueue(t, q, clock)
	reopened, reopenedLaunch, _ := liveHistories(q)
	// Startup recomputes the rate without telemetry; the integral is intact.
	if !reflect.DeepEqual(reopened.ResourceIntegral, live.ResourceIntegral) || !reopened.At.Equal(live.At) || reopened.Identity != live.Identity || !reflect.DeepEqual(reopenedLaunch, liveLaunch) {
		t.Fatalf("clean close lost history:\n%+v\n%+v\n%+v\n%+v", reopened, live, reopenedLaunch, liveLaunch)
	}
}

func liveHistoriesSamples(q *Queue) []hostHeadroomSample {
	_, h, _ := liveHistories(q)
	return h.Samples
}
