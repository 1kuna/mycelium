package myce

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCheckinMemoryIdentitiesFenceAndRollback(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "worker", "cpu", 8, 32)
	m.ExtensionFingerprint = "cpu-v1"
	check(t, q, m, nil)
	s := cpuSpec(1, 1)
	s.Needs.Memory = "auto"
	j := submit(t, q, s)
	live := q.s.Jobs[j.ID]
	want := profileMemoryIdentity(live)
	before := q.snapshotCheckinMemoryIdentities(context.Background())
	if before.current(q)[live] != profileMemoryIdentity(live) {
		t.Fatal("snapshot differs from ordinary identity")
	}
	q.checkinMemoryIdentities = before.current(q)
	target := q.s.Machines[m.ID]
	cached := q.candidates(target).pick(nil)
	q.checkinMemoryIdentities = nil
	ordinary := q.batchCandidateWithResources(live, target, nil)
	if cached == nil || ordinary == nil {
		t.Fatal("missing memory candidate")
	}
	cachedKey, cachedReason := q.workObservationKey(cached)
	ordinaryKey, ordinaryReason := q.workObservationKey(ordinary)
	if !reflect.DeepEqual(cachedKey, ordinaryKey) || cachedReason != ordinaryReason || !reflect.DeepEqual(cached.MemoryEnvelope, ordinary.MemoryEnvelope) || !reflect.DeepEqual(cached.Spec.Needs, ordinary.Spec.Needs) {
		t.Fatal("prepared identity changed memory admission or observation fences")
	}
	if live.planningMemoryIdentity != "" {
		t.Fatal("planner identity escaped into the durable job")
	}

	// A failed concurrent transaction may replace pointers without advancing
	// the commit revision. Its replacement must miss the prepared identity.
	abort := errors.New("rollback")
	if err := q.update(func() error {
		q.touchJob(j.ID).Spec.Run.Command = []string{"echo", "rolled back"}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	live = q.s.Jobs[j.ID]
	if _, ok := before.current(q)[live]; ok {
		t.Fatal("rollback replacement reused an old object's identity")
	}
	if profileMemoryIdentity(live) != want {
		t.Fatal("rollback changed the recipe identity")
	}
	before = q.snapshotCheckinMemoryIdentities(context.Background())
	if err := q.update(func() error {
		q.touchJob(j.ID).Spec.Run.Command = []string{"echo", "changed"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if before.current(q) != nil {
		t.Fatal("committed mutation did not invalidate the snapshot")
	}
	// Stale snapshots use the ordinary planner, including the changed recipe.
	set := q.candidates(q.s.Machines[m.ID])
	if len(set.jobs) != 1 || profileMemoryIdentity(set.jobs[0]) != profileMemoryIdentity(q.s.Jobs[j.ID]) {
		t.Fatal("fallback reused stale recipe identity")
	}
}

func TestCloneJobInputManifestIsolation(t *testing.T) {
	live := &Job{}
	original := FileRef{SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 1}
	live.Spec.Files = FileManifest{"input": original}
	copy := cloneJob(live)
	copy.Spec.Files["input"] = FileRef{SizeBytes: 2}
	copy.Spec.Files["new"] = FileRef{SizeBytes: 3}
	if live.Spec.Files["input"] != original || len(live.Spec.Files) != 1 {
		t.Fatal("clone changed the original input manifest")
	}
}

// Synthetic fleet, real SQLite transactions. Wall time is observational only;
// every scheduling decision uses the frozen queue clock. No fleet data is used.
func BenchmarkQueueCheckinFleet(b *testing.B) {
	clock := &fakeClock{now: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
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
		workers[i].Devices = []MachineDevice{}
		if i < 3 {
			workers[i].Devices = m3Machine().Devices
		}
		if _, err := q.Checkin(Checkin{Machine: workers[i]}); err != nil {
			b.Fatal(err)
		}
	}
	for i := range 220 {
		id := fmt.Sprintf("job-%03d", i)
		s := cpuSpec(2, 2)
		s.Project = fmt.Sprintf("project-%d", i%6)
		s.Needs.Memory = "auto"
		if i%3 != 0 {
			s.Needs.Platform = Platform{Required: []string{"fake"}}
			s.Needs.HostMemoryGB = 2
			s.Needs.MemoryGB = 16
		}
		if err := s.Validate(); err != nil {
			b.Fatal(err)
		}
		s.Files = FileManifest{}
		for f := range 768 {
			s.Files[fmt.Sprintf("inputs/piece-%03d.json", f)] = FileRef{SHA256: strings.Repeat("a", 64), SizeBytes: 1024}
		}
		j := &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
		if i >= 60 {
			j.State, j.FinishedAt, j.ReleaseConfirmed = "succeeded", &clock.now, true
		}
		q.s.Jobs[id] = j
	}
	q.s.Reservations = map[string]Reservation{}
	for i := range 2 {
		id := fmt.Sprintf("reservation-%d", i)
		host := workers[i].HostID
		q.s.Reservations[id] = Reservation{ReservationOptions: ReservationOptions{ID: id, Selector: "host=" + host, Owner: "benchmark", Reason: "synthetic validation"}, Job: fmt.Sprintf("job-%03d", i), Target: ComponentExclusion{Host: host}, CreatedAt: clock.Now(), ExpiresAt: clock.Now().Add(time.Hour)}
	}
	q.s.WorkObservations = map[string]WorkObservation{}
	for i := range 100 {
		key := ObservationKey{Work: fmt.Sprintf("plain:synthetic-%d", i/5), Host: workers[i%5].HostID, Component: "cpu", Hardware: "synthetic-cpu", Fingerprint: "synthetic-cpu", Concurrency: 1, BatchSize: 1}
		q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: key, Samples: 3, LastSeconds: 1, RecordedAt: clock.Now()}
	}
	// Keep the backlog stable while exercising maintenance and planning.
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
	var total, maximum, requestTotal, requestMaximum time.Duration
	q.observeUpdate = func(_, hold time.Duration) {
		total += hold
		maximum = max(maximum, hold)
	}
	b.ResetTimer()
	for i := range b.N {
		m := workers[i%len(workers)]
		c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i%len(workers)), Attempt: 1}, Session: m.Session, Started: true}}}
		span := &requestSpan{}
		ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
		if _, err := q.CheckinContext(ctx, c); err != nil {
			b.Fatal(err)
		}
		requestTotal += span.hold
		requestMaximum = max(requestMaximum, span.hold)
	}
	b.StopTimer()
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "hold-ns/op")
	b.ReportMetric(float64(maximum.Nanoseconds()), "max-hold-ns")
	b.ReportMetric(float64(requestTotal.Nanoseconds())/float64(b.N), "all-holds-ns/op")
	b.ReportMetric(float64(requestMaximum.Nanoseconds()), "max-all-holds-ns")
}
