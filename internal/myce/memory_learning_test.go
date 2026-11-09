package myce

import (
	"encoding/json"
	"testing"
	"time"
)

// #486 review reproduction: rejected Usage used to turn accepted 2.5/5 GiB
// program reservations into 50/25 GiB, surviving restart and the next submit.
func TestMemoryLearningRejectedProgramUsage(t *testing.T) {
	for _, reason := range []string{"constrained", "pressure_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			for _, seed := range []bool{reason == "pressure_unavailable"} {
				name := "cannot seed"
				if seed {
					name = "cannot overwrite"
				}
				t.Run(name, func(t *testing.T) {
					q, clock := fixture(t)
					e := fakeExtension(t)
					if err := q.AddExtension(e); err != nil {
						t.Fatal(err)
					}
					m := placementMachine("synthetic", "separate", 128, 128, e)
					check(t, q, m, nil)
					spec := placementSpec(true)
					complete := func(usage SizeHint, reject bool) (*Job, *AttemptMemoryPeak) {
						t.Helper()
						queued := submit(t, q, spec)
						run := check(t, q, m, nil).Job
						if run == nil || run.ID != queued.ID {
							t.Fatal("not assigned")
						}
						if err := q.Started(result(run, false)); err != nil {
							t.Fatal(err)
						}
						clock.Advance(time.Second)
						r := result(run, true)
						host := usage.HostMemoryGB
						r.Usage = &usage
						r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, Constrained: reject && reason == "constrained", PressureUnavailable: reject && reason == "pressure_unavailable"}
						body, err := json.Marshal(r)
						if err != nil {
							t.Fatal(err)
						}
						var wire Result
						if err := json.Unmarshal(body, &wire); err != nil {
							t.Fatal(err)
						}
						if err := q.Complete(wire); err != nil {
							t.Fatal(err)
						}
						return run, wire.MemoryPeak
					}
					if seed {
						accepted, _ := complete(SizeHint{MemoryGB: 2, HostMemoryGB: 4}, false)
						if facts := job(t, q, accepted.ID).MemoryAttempts; len(facts) != 1 || facts[0].LearningReason != "eligible" {
							t.Fatal("program learning gate disagrees with exported reason", facts)
						}
					}
					rejected, peak := complete(SizeHint{MemoryGB: 40, HostMemoryGB: 20}, true)
					if got := job(t, q, rejected.ID); got.MeasuredNeeds != nil || got.State != "succeeded" {
						t.Fatal("rejected sample published measured needs or changed outcome", got.MeasuredNeeds, got.State)
					}
					q = memoryFactsRestart(t, q, clock)
					memoryFactsGET(t, q, *rejected, peak, false, reason)
					next := submit(t, q, spec)
					wantDevice, wantHost := rejected.Spec.Needs.MemoryGB, rejected.Spec.Needs.HostMemoryGB
					if seed {
						wantDevice, wantHost = 2, 4
					}
					if next.Spec.Needs.MemoryGB != wantDevice || next.Spec.Needs.HostMemoryGB != wantHost {
						t.Errorf("rejected sample changed future reservations: want=%v/%v got=%+v", wantDevice, wantHost, next.Spec.Needs)
					}

				})
			}
		})
	}
}
