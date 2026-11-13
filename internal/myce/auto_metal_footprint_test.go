package myce

import (
	"context"
	"testing"
	"time"
)

func autoMetalFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	m := metalFootprintMachine()
	m.HostMemoryBudgetGB, m.FreeMemoryGB, m.TotalMemoryGB = 40, 40, 40
	m.CapabilityProfile.SystemMemoryBytes = 40 << 30
	m.ExtensionFingerprint = "metal-v1"
	m.Devices = []MachineDevice{{ID: "0", Platform: "metal", Identity: "Apple Silicon", Fingerprint: "Apple Silicon", Selector: "0", MemoryMode: "unified", UnifiedMemory: true, TotalMemoryGB: 40, BudgetGB: 40, FreeMemoryGB: 40, MemoryMeasured: true, ExtensionFingerprint: "metal-v1"}}
	machineCheck(t, q, m)
	s := spec("experiment")
	s.Needs = ResourceNeeds{Memory: "auto", Platform: Platform{Required: []string{"metal"}}}
	// Teach both vectors, with measured growth distinct from the accepted peak.
	runMemory(t, q, clock, m, s, 1, func(r *Result) {
		device, growth := 3.0, .5
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: r.MemoryPeak.HostGB, DeviceGB: &device, GrowthGB: &growth, DeviceSource: deviceFootprintSource, Source: processGroupMemorySource}
	})
	return q, clock, m, s
}

func startAutoMetal(t *testing.T, q *Queue, m Machine, s Spec) *Job {
	t.Helper()
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != queued.ID {
		t.Fatal("auto Metal not assigned", job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	return job(t, q, j.ID)
}

// The actual worker sampler uses footprint even when sizing is automatic.
// Only real host contention stops growth past a learned prediction. A killed
// footprint is a lower bound, while a clean grown footprint teaches demand.
func TestAutoMetalFootprintGuardRelearns(t *testing.T) {
	for _, pressure := range []bool{false, true} {
		t.Run(map[bool]string{false: "room", true: "pressure"}[pressure], func(t *testing.T) {
			q, clock, m, s := autoMetalFixture(t)
			run := startAutoMetal(t, q, m, s)
			if run.MemoryEnvelope.Basis != "learned" {
				t.Fatalf("missing learned envelope: %+v", run.MemoryEnvelope)
			}
			w := recoveryWorker(t)
			w.Machine = m
			w.Machine.Capabilities = nil // Runtime probes establish worker support before advertised copies.
			wc := &workerFakeClock{now: clock.Now()}
			w.Clock = wc
			w.HostMemorySource = func(context.Context) (float64, error) { return 40, nil }
			reads := 0
			w.processMemory = &processMemoryProbe{
				Pressure: func(context.Context) (*gpuHostState, error) {
					h := &gpuHostState{available: 32, level: 1}
					if pressure {
						h.available, h.level = .25, 2
					}
					return h, nil
				},
				OwnedResident: func(map[int]string) (uint64, int, error) { return 64 << 20, 1, nil },
				OwnedFootprint: func(map[int]string) (uint64, uint64, int, error) {
					wc.Advance(processMemorySamplePeriod)
					reads++
					if reads == 1 {
						return 3 << 30, 64 << 20, 1, nil
					}
					return 8 << 30, 64 << 20, 1, nil
				},
			}
			if !w.footprintGuarded(*run) || !w.reportsProcessSample(*run) {
				t.Fatal("auto Metal is not footprint guarded and reported")
			}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			child := recoveryJob(t, []string{"sleep", ".8"})
			if pressure {
				child.Spec.Run.Command = []string{"sleep", "60"}
			}
			child.Spec.Needs, child.MemoryEnvelope = run.Spec.Needs, run.MemoryEnvelope
			p, err := startAfterFixtureInputs(t, w, store, child)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.stopDirective(wc.Now(), 0, true, true) })
			r := d5Finish(t, p)
			peak := r.MemoryPeak
			if peak == nil || peak.HostGB == nil || peak.DeviceGB == nil || *peak.HostGB != .0625 || *peak.DeviceGB != 7.9375 || peak.DeviceSource != deviceFootprintSource {
				t.Fatalf("lost footprint split: %+v", peak)
			}
			if pressure {
				e := r.ExecutionCause
				if r.Success || !r.OutOfMemory || e == nil || e.RSSLimit == nil || e.RSSLimit.Quality != footprintGuardQuality || e.RSSLimit.ObservedBytes != 8<<30 || float64(e.RSSLimit.LimitBytes)/(1<<30) != run.MemoryEnvelope.HostGB {
					t.Fatalf("missing footprint stop: %+v", r)
				}
			} else if !r.Success || r.ExecutionCause != nil {
				t.Fatalf("stopped despite measured room: %+v", r)
			}
			clock.Advance(time.Second)
			completed := result(run, r.Success)
			completed.MemoryPeak, completed.OutOfMemory, completed.ExecutionCause = peak, r.OutOfMemory, r.ExecutionCause
			if err := q.Complete(completed); err != nil {
				t.Fatal(err)
			}
			ended := job(t, q, run.ID)
			facts := ended.MemoryAttempts[len(ended.MemoryAttempts)-1]
			if pressure && (facts.LearningReason != "oom" || !facts.MemoryPeak.Constrained) || !pressure && facts.LearningReason != "eligible" {
				t.Fatalf("wrong learning disposition: %+v", facts)
			}
			if !pressure {
				submit(t, q, s)
			}
			candidate := assignedJob(machineCheck(t, q, m))
			if candidate == nil {
				t.Fatal("grown footprint cannot be assigned", ended.State, ended.Reason)
			}
			if pressure && (candidate.ID != run.ID || candidate.Attempt != run.Attempt+1) {
				t.Fatal("did not retry the exact stopped job", candidate.ID, candidate.Attempt)
			}
			if candidate.MemoryEnvelope.HostGB < 8 || candidate.MemoryEnvelope.HostGB <= run.MemoryEnvelope.HostGB {
				t.Fatalf("footprint did not raise envelope: before=%+v after=%+v", run.MemoryEnvelope, candidate.MemoryEnvelope)
			}
		})
	}
}

// Exercise the same sample object produced by the worker and consumed by a
// keyed check-in. Footprint, not the tiny RSS, determines the charge/credit;
// stale evidence restores the envelope.
func TestAutoMetalFootprintSamplesCharged(t *testing.T) {
	q, clock, m, s := autoMetalFixture(t)
	j := startAutoMetal(t, q, m, s)
	w := Worker{Machine: m, processMemory: &processMemoryProbe{OwnedFootprint: func(map[int]string) (uint64, uint64, int, error) { return 0, 0, 0, nil }}}
	if !w.reportsProcessSample(*j) {
		t.Fatal("worker suppresses auto Metal samples")
	}
	p := &process{}
	p.rssMemory.Store(&processResidentSample{gb: 2, at: clock.Now(), observed: true})
	sample := p.currentProcessHostMemory()
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: sample}}}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	owners := q.hostMemoryLedgers[m.ID].Owners
	q.mu.Unlock()
	found := false
	for _, o := range owners {
		if o.Execution == nil || o.Execution.ID != j.ID {
			continue
		}
		found = true
		if o.ChargeGB < 4 || o.ChargeGB >= j.MemoryEnvelope.HostGB || !near(o.ChargeGB-o.GrowthGB, 2) {
			t.Fatalf("fresh footprint not charged/credited once: %+v envelope=%+v", o, j.MemoryEnvelope)
		}
	}
	if !found {
		t.Fatal("missing Metal ledger owner")
	}
	stale := currentHostMemorySamples(c.Attempts, clock.Now())[jobKey(*j)]
	clock.Advance(2 * hostMemorySampleFreshFor)
	if _, lend := lendingFloor(j, &m, stale, clock.Now()); lend != lendNone {
		t.Fatal("stale sample lends memory")
	}
}
