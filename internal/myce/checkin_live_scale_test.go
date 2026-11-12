package myce

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Retained history must not turn the five workers' one-second heartbeats into
// whole-history transactions. All data is synthetic; SQLite commits are real.
func BenchmarkQueueCheckinLiveScale(b *testing.B) {
	for _, retained := range []int{160, 6500} {
		b.Run(fmt.Sprint(retained), func(b *testing.B) {
			clock := &fakeClock{now: time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC)}
			q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { q.Close() })
			q.checkpointProbe = func() bool { return true }
			workers := make([]Machine, 5)
			for i := range workers {
				id := fmt.Sprintf("worker-%d", i)
				workers[i] = cpuMachine(id, id, "cpu", 8, 32)
				workers[i].ExtensionFingerprint = "synthetic-cpu"
				workers[i].CPUExtensionFingerprint = "synthetic-cpu"
				if _, err := q.Checkin(Checkin{Machine: workers[i]}); err != nil {
					b.Fatal(err)
				}
			}
			q.s.Submissions = map[string]*submissionRecord{}
			for i := range retained + 180 {
				id := fmt.Sprintf("job-%05d", i)
				s := cpuSpec(2, 2)
				s.Project = fmt.Sprintf("project-%d", i%6)
				s.SubmissionKey = id
				s.Needs.Memory = "auto"
				s.Run.Command = []string{"echo", fmt.Sprintf("program-%d", i%60)}
				if err := s.Validate(); err != nil {
					b.Fatal(err)
				}
				j := &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
				if i >= retained+60 {
					j.State = "paused"
				}
				if i < retained {
					j.State, j.FinishedAt, j.ReleaseConfirmed = "succeeded", &clock.now, true
				}
				entries := 8
				if i%1000 == 0 {
					entries = 300
				}
				if i == 0 {
					entries = 17400
				}
				for n := range entries {
					q.appendJobLedger(j, JobLedgerEntry{Attempt: 1, Kind: "custody", Machine: workers[i%5].ID, Session: workers[i%5].Session, Reason: "synthetic retained execution history", Release: "confirmed", ProcessID: fmt.Sprint(n)})
				}
				q.s.Jobs[id] = j
				q.s.Submissions[submissionDigest(s.Project, id)] = &submissionRecord{Kind: submissionBinding, Project: s.Project, Key: id, JobID: id, Job: j, Created: &clock.now}
			}
			q.s.WorkObservations = map[string]WorkObservation{}
			hostPeak := 2.0
			for i := range 1300 {
				// Sixty queued recipes have same-work evidence on all five
				// hosts. The other 200 recipes are retained unrelated history.
				recipe := i / 5
				source := q.s.Jobs[fmt.Sprintf("job-%05d", retained+recipe%60)]
				provenance := *source
				if recipe >= 60 {
					provenance.Spec.Run.Command = []string{"echo", fmt.Sprintf("retained-program-%d", recipe)}
				}
				provenance.Machine, provenance.Session = workers[i%5].ID, workers[i%5].Session
				key, why := q.workObservationKey(&provenance)
				if key == nil {
					b.Fatal(why)
				}
				q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: *key, Samples: 1 + i%139, MemorySamples: 1 + i%42, MemoryConfiguration: memoryConfiguration(provenance.Spec.Needs, nil, nil), MemoryLayout: "separate", MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &hostPeak}, LastSeconds: 1, RecordedAt: clock.Now()}
			}

			// Full CPU occupancy keeps the backlog stable across check-ins.
			for i := range workers {
				id := fmt.Sprintf("running-%d", i)
				s := cpuSpec(8, 8)
				if err := s.Validate(); err != nil {
					b.Fatal(err)
				}
				q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "running", Machine: workers[i].ID, Session: workers[i].Session, Attempt: 1, Queued: clock.Now(), StartedAt: &clock.now}
			}
			if err := q.update(func() error { return nil }); err != nil {
				b.Fatal(err)
			}
			var total, maximum, all time.Duration
			q.observeUpdate = func(_, hold time.Duration) { total += hold; maximum = max(maximum, hold) }
			b.ResetTimer()
			for i := range b.N {
				if i%5 == 0 {
					clock.Advance(time.Second)
				}
				m := workers[i%5]
				c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i%5), Attempt: 1}, Session: m.Session, Started: true}}}
				span := &requestSpan{}
				ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
				if _, err := q.CheckinContext(ctx, c); err != nil {
					b.Fatal(err)
				}
				all += span.hold
			}
			b.StopTimer()
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "hold-ns/op")
			b.ReportMetric(float64(maximum.Nanoseconds()), "max-hold-ns")
			b.ReportMetric(float64(all.Nanoseconds())/float64(b.N), "all-holds-ns/op")
		})
	}
}
