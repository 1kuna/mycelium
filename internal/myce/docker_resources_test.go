package myce

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDockerResourcesRetainFailedFinalCounters(t *testing.T) {
	_, clock := fixture(t)
	a := resourceCgroupFixture(t)
	observer := &resourceObserver{clock: clock, start: clock.Now(), read: func() *AttemptResources { return cgroupResources(a, clock.Now()) }}
	observer.sample()
	// Docker's releaseEnvelope takes its raw snapshot after these counter reads
	// fail, while a still-running observer owns the previous usable sample.
	for _, file := range []string{"cpu.stat", "io.stat"} {
		if err := os.Remove(filepath.Join(a.path, file)); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(2 * time.Second)
	d := dockerAttempt{state: dockerExecution{Resources: cgroupResources(a, clock.Now())}}
	observer.sample()
	d.finishResourceObservation(observer.snapshot())
	result := dockerOutcome(d.state, Result{}, nil)
	r := result.Resources
	if r == nil || r.CPU.UsageSeconds == nil || *r.CPU.UsageSeconds != 9 || r.IO.ReadBytes == nil || *r.IO.ReadBytes != 110 || r.ElapsedSeconds != 2 {
		t.Fatalf("lost sampled counters: %+v", r)
	}
	if !r.CPU.LowerBound || r.CPU.Unavailable != "final_read_failed" || r.IO.Unavailable != "final_read_failed" {
		t.Fatalf("missing failed-final-read provenance: %+v", r)
	}
	if r.IO.Pressure.Some.TotalUS != 90000 {
		t.Fatal("usable final PSI was discarded")
	}
}
