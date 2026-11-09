package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Block decoding a retained record, then require a real commit to finish.
// This proves bulk decoding/traversal cannot retain the live queue mutex,
// independent of machine speed, Go instrumentation or a timing percentile.
func TestReadSnapshotBulkWorkDoesNotHoldQueue(t *testing.T) {
	for _, id := range []string{"", "retained-000"} {
		t.Run(fmt.Sprintf("job=%s", id), func(t *testing.T) {
			q, clock := fixture(t)
			for i := range 1000 {
				key := fmt.Sprintf("retained-%03d", i)
				q.s.Jobs[key] = &Job{ID: key, State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment")}
			}
			if err := q.update(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			record := readGet(q.readRoot, "jobs/retained-000")
			decoding, resume := make(chan struct{}), make(chan struct{})
			// Own sync.Once until the writer commits; the reader must wait outside q.mu.
			go record.once.Do(func() { close(decoding); <-resume; record.value, _ = decodeReadJob(record.data) })
			<-decoding
			captured := make(chan struct{})
			var once sync.Once
			q.observeRead = func(_ string, _, _ time.Duration) { once.Do(func() { close(captured) }) }
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			readDone := make(chan error, 1)
			go func() { _, _, err := q.captureReadObservation(id, nil, ctx); readDone <- err }()
			<-captured
			writeDone := make(chan error, 1)
			go func() {
				writeDone <- q.update(func() error { q.touchJob("retained-000").Reason = "committed"; return nil })
			}()
			select {
			case err := <-writeDone:
				if err != nil {
					t.Error(err)
				}
			case <-ctx.Done():
				t.Error("record decoding blocked a control commit")
			}
			close(resume)
			if err := <-readDone; err != nil {
				t.Fatal(err)
			}
			q.observeRead = nil
			got, err := q.jobSnapshot("retained-000")
			if err != nil || got.Reason != "committed" {
				t.Fatal("new root did not expose the commit", got, err)
			}
		})
	}
}

// Retain the constrained execution evidence from the #1030 incident fixture.
// Stage times are diagnostic measurements, not machine-dependent pass criteria.
func BenchmarkReadStages(b *testing.B) {
	for _, mode := range []string{"legacy", "published"} {
		for _, retained := range []int{160, 6500} {
			for _, endpoint := range []string{"status", "history", "queued", "running", "terminal"} {
				b.Run(fmt.Sprintf("%s/retained=%d/%s", mode, retained, endpoint), func(b *testing.B) {
					q, _ := candidateCostFixture(b)
					for i := 160; i < retained; i++ {
						source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
						j := cloneJob(source)
						j.ID = fmt.Sprintf("retained-%04d", i)
						j.Timing.Execution.ID = j.ID
						q.s.Jobs[j.ID] = &j
					}
					if err := q.update(func() error { return nil }); err != nil {
						b.Fatal(err)
					}
					var capture, derive, encode, hold time.Duration
					var size int
					q.observeRead = func(_ string, _, d time.Duration) { hold += d }
					// Isolate accounting CPU from OS probes, with ample synthetic
					// capacity. Exercise the same preflight as HTTP handlers.
					budgetHost(q, 1<<40, 1)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						ctx, release, err := q.requestBudgets.projectionContext(context.Background())
						if err != nil {
							b.Fatal(err)
						}
						id := map[string]string{"queued": "queued-001", "running": "running-1", "terminal": "retained-001"}[endpoint]
						options := &statusOptions{jobs: "recent", limit: 100}
						if endpoint == "history" {
							options = nil
						}
						started := time.Now()
						captureRead := q.captureReadObservation
						if mode == "legacy" {
							captureRead = q.captureStatusObservation
						}
						v, _, err := captureRead(id, options, ctx)
						capture += time.Since(started)
						if err != nil {
							b.Fatal(err)
						}
						started = time.Now()
						var out any
						if id == "" {
							v.memoryPlanning = newMemoryPlanningCache()
							v.observationScan = v.placementScan()
							v.observationScan.observationOwner = v
							v.observationScan.memory = v.memoryPlanning
							out = v.projectSnapshot(true)
						} else {
							j, err := v.findObservationJob(id)
							if err != nil {
								b.Fatal(err)
							}
							var reasons map[string]string
							if j.State == "queued" {
								if mode == "published" {
									v.memoryPlanning = newMemoryPlanningCache()
								}
								v.memoryObservationIndex = v.memoryObservationCatalog()
								if mode == "published" {
									v.observationScan = v.placementScan()
									v.observationScan.observationOwner = v
									v.observationScan.memory = v.memoryPlanning
								}
								reasons = map[string]string{j.ID: v.waitReason(j)}
							}
							j = v.projectJob(j, nil, reasons, true)
							if j.Resources == nil {
								j.Resources = jobResources(j)
							}
							v.projectJobDiagnostics(j, true, true)
							j.DeadlineGuidance = v.deadlineGuidance(j)
							out = j
						}
						derive += time.Since(started)
						started = time.Now()
						data, err := json.Marshal(out)
						encode += time.Since(started)
						if err != nil {
							b.Fatal(err)
						}
						size = len(data)
						release()
					}
					b.StopTimer()
					for name, d := range map[string]time.Duration{"capture": capture, "derive": derive, "encode": encode, "hold": hold} {
						b.ReportMetric(float64(d.Nanoseconds())/float64(b.N), name+"-ns/op")
					}
					b.ReportMetric(float64(size), "reply-bytes")
				})
			}
		}
	}
}
