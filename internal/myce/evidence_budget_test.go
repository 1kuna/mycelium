package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQueueRetentionCapacityIgnoresWorkerMemoryOverride(t *testing.T) {
	workerSource, queueSource := workerHostMemorySource, queueHostMemorySource
	t.Cleanup(func() {
		workerHostMemorySource, queueHostMemorySource = workerSource, queueSource
	})
	workerCalled := false
	workerHostMemorySource = func(float64) (MemorySource, error) {
		workerCalled = true
		return nil, errors.New("worker inventory is not ready")
	}
	SetQueueHostMemorySource(func(reserve float64) (MemorySource, error) {
		if reserve != 0 {
			t.Errorf("queue memory reserve = %v, want 0", reserve)
		}
		return func(context.Context) (float64, error) { return 32, nil }, nil
	})
	capacity, err := queueRetentionCapacity(context.Background(), t.TempDir())
	if workerCalled {
		t.Fatal("queue retention capacity invoked the worker inventory override")
	}
	if err != nil {
		t.Fatal(err)
	}
	if capacity.MemoryAvailableBytes != 32<<30 || capacity.DiskAvailableBytes == 0 {
		t.Fatalf("queue host capacity = %+v", capacity)
	}
}

func TestEvidenceRetentionCapacityValueAndPins(t *testing.T) {
	for _, hosts := range []int{1, 200} {
		t.Run(fmt.Sprint(hosts), func(t *testing.T) {
			q, clock := fixture(t)
			q.fileRetention, q.logRetention = 0, 0
			capacity := RetentionCapacity{MemoryAvailableBytes: 1 << 40, DiskAvailableBytes: 1 << 40}
			if hosts == 1 {
				capacity = RetentionCapacity{MemoryAvailableBytes: 1024, DiskAvailableBytes: 1024}
			}
			q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) {
				if !q.mu.TryLock() {
					t.Error("capacity measurement held control lock")
				} else {
					q.mu.Unlock()
				}
				return capacity, nil
			}
			key := ObservationKey{Work: "open-work", Host: "synthetic", Component: "cpu"}
			makeJob := func(id, state string) *Job {
				ended := clock.Now()
				j := &Job{ID: id, State: state, Attempt: 1, Queued: clock.Now(), Spec: Spec{Name: id, Project: "fixture"}, FinishedAt: &ended, ReleaseConfirmed: true}
				for range 100 {
					j.AttemptHistory = append(j.AttemptHistory, AttemptOutcome{Attempt: 1, Machine: "synthetic", Session: "session", EndedAt: clock.Now(), Reason: "same diagnosis"})
				}
				j.LedgerEntries = []JobLedgerEntry{{Kind: "custody_released", CustodyID: id, Release: "confirmed"}}
				return j
			}
			if err := q.update(func() error {
				for i := range hosts {
					q.touchMachine(fmt.Sprint(i))
					q.s.Machines[fmt.Sprint(i)] = &Machine{ID: fmt.Sprint(i)}
				}
				for i := range 25 {
					j := makeJob(fmt.Sprint(i), "succeeded")
					q.putJob(j.ID, j)
				}
				j := makeJob("open", "queued")
				j.Timing = &WorkTiming{Key: &key}
				q.putJob(j.ID, j)
				q.putWorkObservation(identityHash(key), WorkObservation{Key: key, StaleReason: "hardware changed", RecordedAt: clock.Now()})
				q.putWorkObservation("drifted", WorkObservation{StaleReason: "recipe or reference changed", RecordedAt: clock.Now()})
				q.putWorkObservation("valid", WorkObservation{RecordedAt: clock.Now().Add(-1000 * 24 * time.Hour)})
				q.putWorkObservation("negative", WorkObservation{StaleReason: "hardware changed", Forbidden: true})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			ledger := append([]JobLedgerEntry(nil), q.s.Jobs["0"].LedgerEntries...)
			clock.Advance(1000 * 24 * time.Hour)
			drainEvidence(t, q)
			if _, ok := q.s.WorkObservations["drifted"]; !ok {
				t.Fatal("open pooled dependency dropped")
			}
			for _, id := range []string{identityHash(key), "valid", "negative"} {
				if _, ok := q.s.WorkObservations[id]; !ok {
					t.Fatalf("dependent/useful evidence dropped: %s", id)
				}
			}
			if len(q.s.Jobs["open"].AttemptHistory) != 100 {
				t.Fatal("open attempts lost evidence")
			}
			if !reflect.DeepEqual(q.s.Jobs["0"].LedgerEntries, ledger) {
				t.Fatal("custody ledger changed")
			}
			want := 100
			if hosts == 1 {
				want = 2
			}
			if got := len(q.s.Jobs["0"].AttemptHistory); got != want {
				t.Fatalf("capacity did not control redundant history: got %d want %d", got, want)
			}
			report := q.evidenceRetention
			if report == nil || report.Capacity != capacity || report.OverBudget != (hosts == 1) {
				t.Fatalf("missing measured/protected budget: %+v", report)
			}
			if err := q.update(func() error { q.touchJob("open").State = "succeeded"; return nil }); err != nil {
				t.Fatal(err)
			}
			drainEvidence(t, q)
			if _, ok := q.s.WorkObservations[identityHash(key)]; ok {
				t.Fatal("closed work pinned drift forever")
			}
		})
	}
}

func TestEvidenceRetentionConcurrentDependencyFencesPlan(t *testing.T) {
	q, clock := fixture(t)
	key := ObservationKey{Work: "drift"}
	if err := q.update(func() error {
		q.putWorkObservation(identityHash(key), WorkObservation{Key: key, StaleReason: "hardware changed"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) {
		if err := q.update(func() error {
			q.putJob("new", &Job{ID: "new", State: "queued", Queued: clock.Now(), Timing: &WorkTiming{Key: &key}})
			return nil
		}); err != nil {
			t.Error(err)
		}
		return RetentionCapacity{MemoryAvailableBytes: 1, DiskAvailableBytes: 1}, nil
	}
	if err := q.maintainEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.s.WorkObservations[identityHash(key)]; !ok {
		t.Fatal("new dependency lost its source")
	}
}

func TestEvidenceRetentionAutomaticFilesAndReplaySurviveAge(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileRetention(0); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureLogRetention(0); err != nil {
		t.Fatal(err)
	}
	s := keyedSpec()
	s.Priority = "experiment"
	j := submit(t, q, s)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10000 * 24 * time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, clock)
	if replay := submit(t, q, s); replay.ID != j.ID || replay.State != "cancelled" {
		t.Fatal("time changed unique replay evidence")
	}
}

func TestEvidenceBudgetUsesLocalHeadroom(t *testing.T) {
	if n := evidenceBudget(RetentionCapacity{1024, 8192}, 512, 768); n != 256 {
		t.Fatal(n)
	}
	if n := evidenceBudget(RetentionCapacity{1 << 40, 1 << 42}, 512, 768); n != (1<<40)-768 {
		t.Fatal(n)
	}
	if n := evidenceBudget(RetentionCapacity{}, 512, 768); n != 0 {
		t.Fatal("unknown capacity invented room", n)
	}
}

func TestEvidenceRetentionRevisitsHistoryWhenCapacityFalls(t *testing.T) {
	q, clock := fixture(t)
	capacity := RetentionCapacity{MemoryAvailableBytes: 1 << 40, DiskAvailableBytes: 1 << 40}
	q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) { return capacity, nil }
	row := AttemptOutcome{Attempt: 1, EndedAt: clock.Now(), Reason: "same evidence"}
	if err := q.update(func() error {
		q.putJob("closed", &Job{ID: "closed", State: "cancelled", ReleaseConfirmed: true, AttemptHistory: []AttemptOutcome{row, row, row}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	drainEvidence(t, q)
	if len(q.s.Jobs["closed"].AttemptHistory) != 3 {
		t.Fatal("history compacted without pressure")
	}
	capacity = RetentionCapacity{MemoryAvailableBytes: 1, DiskAvailableBytes: 1}
	if err := q.maintainEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(q.s.Jobs["closed"].AttemptHistory) != 2 {
		t.Fatal("unchanged history ignored reduced host capacity")
	}
}

func TestExternalEvidenceNextSamplePreservesCompactedCounts(t *testing.T) {
	q, clock := fixture(t)
	q.s.ExternalMemory = map[string]*externalMemoryRing{"synthetic": {Slots: 10000, Buckets: map[int]*externalMemoryBucket{}}}
	m := Machine{ID: "synthetic", MemoryObservation: &HostMemoryObservation{TotalGB: 16, UsedGB: 4}}
	q.recordExternalMemory(&m)
	clock.Advance(time.Second)
	q.recordExternalMemory(&m)
	ring := q.s.ExternalMemory["synthetic"]
	compactExternalEvidence(ring, clock.Now(), 4096)
	clock.Advance(time.Second)
	q.recordExternalMemory(&m)
	if got := q.externalMemoryStatus("synthetic").Week.Samples; got != 3 {
		t.Fatalf("next check-in replaced compacted samples: got %d", got)
	}
}

func TestExternalEvidenceExpiresWithoutNewCheckins(t *testing.T) {
	q, clock := fixture(t)
	q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) {
		return RetentionCapacity{MemoryAvailableBytes: 1 << 40, DiskAvailableBytes: 1 << 40}, nil
	}
	m := Machine{ID: "synthetic", MemoryObservation: &HostMemoryObservation{TotalGB: 16, UsedGB: 4}}
	if err := q.update(func() error { q.recordExternalMemory(&m); return nil }); err != nil {
		t.Fatal(err)
	}
	drainEvidence(t, q)
	clock.Advance(8 * 24 * time.Hour)
	if err := q.maintainEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(q.s.ExternalMemory["synthetic"].Buckets) != 0 {
		t.Fatal("offline host retained evidence outside every reader window")
	}
	if q.evidenceMaintenance.expiry.Len() != 0 {
		t.Fatal("empty history kept an expiry deadline")
	}
}

func TestExternalEvidenceResizesWhenCapacityFallsWithoutCheckins(t *testing.T) {
	q, _ := fixture(t)
	capacity := RetentionCapacity{MemoryAvailableBytes: 1 << 40, DiskAvailableBytes: 1 << 40}
	q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) { return capacity, nil }
	m := Machine{ID: "synthetic", MemoryObservation: &HostMemoryObservation{TotalGB: 16, UsedGB: 4}}
	if err := q.update(func() error { q.recordExternalMemory(&m); return nil }); err != nil {
		t.Fatal(err)
	}
	drainEvidence(t, q)
	capacity = RetentionCapacity{MemoryAvailableBytes: 1, DiskAvailableBytes: 1}
	if err := q.maintainEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if q.s.ExternalMemory["synthetic"].Slots != 1 {
		t.Fatal("unchanged external summary ignored reduced capacity")
	}
}

func drainEvidence(t *testing.T, q *Queue) {
	t.Helper()
	for len(q.evidenceRows) > 0 || q.evidenceMaintenance.revisiting {
		if err := q.maintainEvidence(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
func TestDiagnosticReservationsConcurrentHeadroom(t *testing.T) {
	var pool diagnosticReservations
	capacity := RetentionCapacity{MemoryAvailableBytes: 4096, DiskAvailableBytes: 8192}
	a, releaseA := pool.reserve(capacity, 0, 2048, 2)
	b, releaseB := pool.reserve(capacity, 0, 2048, 2)
	if a != 2048 || b != 0 {
		t.Fatal("concurrent captures overcommitted", a, b)
	}
	releaseA()
	releaseA()
	releaseB()
	c, releaseC := pool.reserve(capacity, 0, 1024, 4)
	defer releaseC()
	if c != 1024 {
		t.Fatal("released budget unavailable", c)
	}
}

// An idle subscription and retained events must leave memory for exit evidence;
// reservations follow consumed bytes, including a record spanning read pages.
func TestDiagnosticStreamsShareActualBytes(t *testing.T) {
	var pool diagnosticReservations
	page := uint64(os.Getpagesize())
	capacity := RetentionCapacity{MemoryAvailableBytes: 16 * page * diagnosticExpansion, DiskAvailableBytes: 32 * page * diagnosticExpansion}
	reserve := func(n uint64) (int64, func()) {
		grant, release := pool.reserve(capacity, 0, n, diagnosticExpansion)
		return int64(grant), release
	}
	_, releaseReader := reserve(page)
	defer releaseReader()
	writer := &dockerEventWriter{identity: "ATTEMPT", reserve: reserve}
	defer func() {
		for _, release := range writer.releases {
			release()
		}
	}()
	// Repeated long private metadata is parsed transiently; only the much
	// smaller sanitized events consume persistent capacity, through kill/die.
	input := strings.ReplaceAll(dockerEventsInput(time.Unix(1, 0)), "DO-NOT-RETAIN", strings.Repeat("x", int(2*page)))
	if err := diagnosticLines(strings.NewReader(input), reserve, writer.event); err != nil {
		t.Fatal(err)
	}
	tails := []*diagnosticTail{{reserve: reserve}, {reserve: reserve}}
	for _, tail := range tails {
		defer tail.release()
		_, _ = tail.Write([]byte("payload stderr\n"))
		if string(tail.data) != "payload stderr\n" {
			t.Fatal("event stream starved stderr capture")
		}
	}
	if len(writer.archive.Events) != 260 || pool.reserved >= capacity.MemoryAvailableBytes {
		t.Fatal("discarded metadata exhausted event capacity", len(writer.archive.Events))
	}
	kill, exit := writer.archive.Events[258], writer.archive.Events[259]
	if kill.Signal != "9" || exit.ExitCode == nil || *exit.ExitCode != 137 {
		t.Fatal("terminal evidence lost", kill, exit)
	}
	encoded, err := json.Marshal(writer.archive)
	if err != nil || strings.Contains(string(encoded), "private") || strings.Contains(string(encoded), strings.Repeat("x", int(2*page))) {
		t.Fatal("discarded attributes entered archive", err)
	}
}
func TestEvidenceRetentionLearntKeyAndUniqueWaitRemain(t *testing.T) {
	q, clock := fixture(t)
	q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) {
		return RetentionCapacity{MemoryAvailableBytes: 1, DiskAvailableBytes: 1}, nil
	}
	key := ObservationKey{Work: "learned"}
	peak, predicted, actual := 2.0, 20.0, 10.0
	err := q.update(func() error {
		q.putWorkObservation(identityHash(key), WorkObservation{Key: key, StaleReason: "hardware changed"})
		job := &Job{ID: "decision", Attempt: 1, State: "succeeded", ReleaseConfirmed: true,
			LedgerEntries: []JobLedgerEntry{{WorkLearning: &WorkLearningChange{Key: key}}},
			Efficiency:    &JobEfficiency{WaitSeconds: map[string]float64{"memory_reservation": 42}},
			MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: "synthetic", Session: "session", LearningReason: "eligible",
				MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", HostGB: 12}, MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}},
			Timing: &WorkTiming{Execution: Execution{ID: "decision", Attempt: 1}, LastAcceptedSeconds: &predicted, ActualSeconds: &actual}}
		for range 100 {
			job.AttemptHistory = append(job.AttemptHistory, AttemptOutcome{Attempt: 1, Machine: "synthetic", Session: "session", Reason: "same diagnosis"})
		}
		q.putJob(job.ID, job)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	before := efficiencyPredictions(q.s.Jobs["decision"])
	if len(before) != 1 {
		t.Fatal("missing prediction fixture", before)
	}
	clock.Advance(10000 * 24 * time.Hour)
	drainEvidence(t, q)
	if _, ok := q.s.WorkObservations[identityHash(key)]; !ok {
		t.Fatal("learned decision lost its input")
	}
	job := q.s.Jobs["decision"]
	if job.Efficiency.WaitSeconds["memory_reservation"] != 42 || !reflect.DeepEqual(before, efficiencyPredictions(job)) {
		t.Fatal("capacity maintenance lost wait or prediction-error readers")
	}
	// First/latest provenance survives even for otherwise identical rows.
	if len(job.AttemptHistory) != 2 {
		t.Fatal("fixture did not exercise redundant-history compaction")
	}
}

func TestInputClaimPinsHeadThroughCollectionAndSubmission(t *testing.T) {
	q, clock := fixture(t)
	client := retentionClient(t, q)
	_, ref := retentionUpload(t, client, "claimed existing bytes")
	s := spec("experiment")
	s.Files = FileManifest{"input": ref}
	old := submit(t, q, s)
	if err := q.Cancel(old.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention)
	if err := q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	q.s.PublicationHorizon = 0
	claim, err := q.claimFileInput(context.Background(), Spec{Project: "next"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), fileInputClaimKey{}, claim.ID)
	if err := client.HasFile(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("claim lost bytes between HEAD and submit", err)
	}
	s.Project = "next"
	if _, err := q.Submit(s); err != nil {
		t.Fatal("claimed submission failed", err)
	}
}

func TestExternalEvidenceCapacityAndWindowDrift(t *testing.T) {
	for _, budget := range []uint64{1024, 1 << 20} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			ring := &externalMemoryRing{Buckets: map[int]*externalMemoryBucket{}}
			for i := range 4000 {
				at := now.Add(time.Duration(i) * time.Second)
				ring.Buckets[i] = &externalMemoryBucket{Start: at, End: at, ByteCounts: map[uint8]uint64{33: 1}, ByteMax: map[uint8]float64{33: 4}, MaxGB: 4, PeakAt: at}
			}
			compactExternalEvidence(ring, now.Add(4000*time.Second), budget)
			if len(ring.Buckets) > ring.Slots+1 || ring.window(now.Add(4000*time.Second), 7*24*time.Hour).Samples != 4000 {
				t.Fatal("capacity lost counts or failed to coalesce", len(ring.Buckets), ring.Slots)
			}
			compactExternalEvidence(ring, now.Add(8*24*time.Hour), budget)
			if len(ring.Buckets) != 0 {
				t.Fatal("evidence outside every reader window retained")
			}
		})
	}
}
