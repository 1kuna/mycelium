package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReadSnapshotColdReceiptDiscardsLedgerBeforeBudgeting(t *testing.T) {
	q, clock := fixture(t)
	j := &Job{ID: "retained-ledger", State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment")}
	for i := range 10000 {
		j.LedgerEntries = append(j.LedgerEntries, JobLedgerEntry{Sequence: i + 1, Reason: strings.Repeat(`history, \"ledger_entries\": []`, 4)})
	}
	if err := q.update(func() error { q.putJob(j.ID, j); return nil }); err != nil {
		t.Fatal(err)
	}
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/jobs/"+j.ID, nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		API{q, "test"}.Handler().ServeHTTP(w, r)
		return w
	}
	budgetHost(q, 2, 1)
	if w := request(); w.Code != 503 {
		t.Fatal("cold decode escaped capacity", w.Code)
	}
	budgetHost(q, 2<<20, 1)
	w := request()
	var got Job
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil || got.ID != j.ID || len(got.LedgerEntries) != 0 {
		t.Fatal("discarded ledger prevented receipt recovery", w.Code, w.Body.String())
	}
	if q.requestBudgets.held != 0 {
		t.Fatal("decode capacity leaked")
	}
}

func TestReadSnapshotColdKeyedStatusDiscardsLedgerBeforeBudgeting(t *testing.T) {
	for _, endpoint := range []string{"/api/status", "/api/status?jobs=active"} {
		t.Run(endpoint, func(t *testing.T) {
			q, _ := fixture(t)
			s := spec("experiment")
			s.SubmissionKey = "keyed-ledger"
			j := submit(t, q, s)
			if err := q.update(func() error {
				live := q.touchJob(j.ID)
				live.State, live.ReleaseConfirmed = "succeeded", true
				for i := range 10000 {
					live.LedgerEntries = append(live.LedgerEntries, JobLedgerEntry{Sequence: i + 1, Reason: strings.Repeat("retained history", 8)})
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			budgetHost(q, 2<<20, 1)
			r := httptest.NewRequest("GET", endpoint, nil)
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			API{q, "test"}.Handler().ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatal("embedded ledger prevented cold status", w.Code, w.Body.String())
			}
			if len(q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)].Job.LedgerEntries) != 10000 {
				t.Fatal("read discarded durable ledger")
			}
			if q.requestBudgets.held != 0 {
				t.Fatal("decode capacity leaked")
			}
		})
	}
}

func TestReadSnapshotLogRefreshesMachineWithoutControlCommit(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("worker", "fake")
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("no assignment")
	}
	// An accepted log is contact, not a report: only a check-in rebuilds the
	// reported half after restart, so the machine stays unreported.
	if err := q.update(func() error { q.unreported[m.ID] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	before := q.readGeneration
	clock.Advance(time.Minute)
	if err := q.AppendLog(LogChunk{Result: result(j, true), Text: "still running"}); err != nil {
		t.Fatal(err)
	}
	if q.readGeneration != before {
		t.Fatal("test requires no control commit after log")
	}
	v, _, err := q.captureReadObservation("", nil)
	if err != nil || !v.unreported[m.ID] || !v.s.Machines[m.ID].LastSeen.Equal(clock.Now()) {
		t.Fatal("fresh capture missed accepted log contact", err)
	}
}

// Block decoding a retained record, then require a real commit to finish.
// This proves bulk decoding/traversal cannot retain the live queue mutex,
// independent of machine speed, Go instrumentation or a timing percentile.
func TestReadSnapshotBulkWorkDoesNotHoldQueue(t *testing.T) {
	for _, id := range []string{"", "retained-000"} {
		t.Run(fmt.Sprintf("job=%s", id), func(t *testing.T) {
			q, clock := fixture(t)
			// One blocked record proves exclusion; extra records add no interleaving.
			for i := range 4 {
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
			ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
			defer cancel()
			readDone := make(chan error, 1)
			go func() { _, _, err := q.captureReadObservation(id, nil, ctx); readDone <- err }()
			<-captured
			writeDone := make(chan error, 1)
			go func() {
				if _, err := q.Checkin(Checkin{Machine: machine("control", "fake")}); err != nil {
					writeDone <- err
					return
				}
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

func TestReadSnapshotCommitRollbackAndDeletion(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	before := q.readGeneration
	read := func(g *readGeneration) *Job {
		t.Helper()
		s, _, err := g.inputs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return s.Jobs[j.ID]
	}
	aborted := errors.New("rollback")
	if err := q.update(func() error { q.touchJob(j.ID).Spec.Run.Command[0] = "aborted"; return aborted }); !errors.Is(err, aborted) {
		t.Fatal(err)
	}
	if q.readGeneration != before {
		t.Fatal("rollback published a generation")
	}
	if err := q.update(func() error { q.touchJob(j.ID).Spec.Run.Command[0] = "committed"; return nil }); err != nil {
		t.Fatal(err)
	}
	if read(before).Spec.Run.Command[0] == "committed" || read(q.readGeneration).Spec.Run.Command[0] != "committed" {
		t.Fatal("immutable input alias or stale commit")
	}
	if err := q.update(func() error { q.deleteJob(j.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if read(q.readGeneration) != nil || read(before) == nil {
		t.Fatal("deletion changed the wrong generation")
	}
}

func TestReadSnapshotProjectionEquivalence(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	now := clock.Now()
	peak := 3.0
	if err := q.update(func() error {
		live := q.touchJob(j.ID)
		live.State = "failed"
		live.Attempt = 1
		live.ReleaseConfirmed = true
		live.FinishedAt = &now
		live.MemoryAttempts = []AttemptMemoryFacts{{Attempt: 1, MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}}
		live.LedgerEntries = []JobLedgerEntry{{}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", j.ID} {
		old, _, err := q.captureStatusObservation(id, nil)
		if err != nil {
			t.Fatal(err)
		}
		current, _, err := q.captureReadObservation(id, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := old.projectSnapshot(true)
		got := current.projectSnapshot(true)
		// Compare the public wire contract: JSON intentionally omits internal fields.
		a, _ := json.Marshal(want)
		b, _ := json.Marshal(got)
		var av, bv any
		_ = json.Unmarshal(a, &av)
		_ = json.Unmarshal(b, &bv)
		if !reflect.DeepEqual(av, bv) {
			t.Fatalf("read projection changed:\n%s\n%s", a, b)
		}
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

func TestReadSnapshotKeyedReferencesStayWithinGeneration(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "generation-reference"
	j := submit(t, q, s)
	key := submissionDigest(s.Project, s.SubmissionKey)
	originalReason := q.s.Jobs[j.ID].Reason
	before := q.readGeneration
	if err := q.update(func() error {
		q.touchJob(j.ID).Reason = "new generation"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	current := q.readGeneration
	for _, sample := range []struct {
		generation *readGeneration
		reason     string
	}{{before, originalReason}, {current, "new generation"}} {
		for _, selected := range []string{"", j.ID} {
			var state state
			var err error
			if selected == "" {
				state, _, err = sample.generation.inputs(context.Background())
			} else {
				state, _, err = sample.generation.jobInputs(context.Background(), selected)
			}
			if err != nil {
				t.Fatal(err)
			}
			answer := state.Submissions[key]
			if answer == nil || answer.Job == nil || answer.Job != state.Jobs[j.ID] || answer.Job.Reason != sample.reason {
				t.Fatal("keyed answer escaped its committed generation", selected, answer)
			}
		}
	}
	if err := q.update(func() error { q.deleteJob(j.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	removed, _, err := q.readGeneration.inputs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	answer := removed.Submissions[key]
	if removed.Jobs[j.ID] != nil || answer == nil || answer.Job == nil || answer.Job.Reason != "new generation" {
		t.Fatal("removed keyed answer lost its retained snapshot", answer)
	}
	old, _, err := before.inputs(context.Background())
	if err != nil || old.Submissions[key].Job.Reason != originalReason {
		t.Fatal("later commit mutated an earlier keyed answer", err)
	}
}

func TestReadSnapshotKeyedCompletionClearsLiveReference(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "released-reference"
	j := submit(t, q, s)
	queued := submit(t, q, spec("experiment"))
	key := "submissions/" + submissionDigest(s.Project, s.SubmissionKey)
	if err := q.update(func() error {
		job := q.touchJob(j.ID)
		job.State, job.Attempt, job.ReleaseConfirmed = "succeeded", 1, false
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := q.readGeneration
	pending := readGet(before.root, key)
	if pending == nil || !pending.mutable {
		t.Fatal("completion dropped pending attempt custody")
	}
	if err := q.update(func() error {
		q.touchJob(j.ID).ReleaseConfirmed = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after := q.readGeneration
	released := readGet(after.root, key)
	if released == nil || released.mutable || !bytes.Equal(pending.data, released.data) {
		t.Fatal("unchanged reference retained live traversal after release")
	}
	for _, sample := range []struct {
		generation *readGeneration
		want       bool
	}{{before, true}, {after, false}} {
		visited := false
		readWalk(sample.generation.root, true, func(name string, _ *readRecord) {
			if name == key {
				visited = true
			}
		})
		if visited != sample.want {
			t.Fatal("reference liveness changed in the wrong generation", visited, sample.want)
		}
		live, _, err := sample.generation.jobInputs(context.Background(), queued.ID)
		if err != nil {
			t.Fatal(err)
		}
		if (live.Submissions[submissionDigest(s.Project, s.SubmissionKey)] != nil) != sample.want {
			t.Fatal("queued read retained released keyed history")
		}
	}
}
