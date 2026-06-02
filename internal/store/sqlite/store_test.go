package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
)

func TestStorePersistsControlPlaneState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if store.db.Stats().MaxOpenConnections != 1 {
		t.Fatalf("MaxOpenConnections = %d", store.db.Stats().MaxOpenConnections)
	}
	var busyTimeout int
	if err := store.db.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil || busyTimeout != 5000 {
		t.Fatalf("busy_timeout = %d, %v", busyTimeout, err)
	}

	project := domain.Project{ID: "proj", Priority: domain.PriorityInteractive, SpeedPref: domain.SpeedLatency, ContextCap: 8192, ExpectedConcurrency: 4, AutoApply: true}
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("/models/a.gguf"))
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	lease := domain.Lease{ID: "lease-a", JobID: "job-a", InstanceID: inst.ID, NodeID: node.ID, Claim: fixtures.MakeClaim(1, 2), GrantedAt: time.Unix(1, 0).UTC()}
	reservation := domain.Reservation{ID: "res-a", Kind: domain.ReservationHeadroom, NodeID: node.ID, Headroom: fixtures.MakeClaim(3, 4)}
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID))
	admissionState := domain.AdmissionState{
		NodeID: node.ID,
		Fence:  4,
		Leases: []domain.AdmissionLeaseRecord{{
			Lease: lease,
		}},
	}
	jobRecord := fixtures.MakeJobRecord(fixtures.WithRecordJobID(job.ID))
	rec := domain.RecommendationRecord{ID: "rec-a", Type: "context_cap_recommendation", ProjectID: project.ID, RecommendedValue: 4096, CreatedAt: time.Unix(2, 0).UTC()}
	refs := []domain.ProcessRef{{PID: 12, Kind: "process", Ref: "12"}}
	token := domain.JoinTokenRecord{Hash: "hash-a", Active: true, Current: true}

	must(t, store.SaveProject(ctx, project))
	must(t, store.SavePreset(ctx, preset))
	must(t, store.SaveNode(ctx, node))
	must(t, store.SaveInstance(ctx, inst))
	must(t, store.SaveLease(ctx, lease))
	must(t, store.SaveReservation(ctx, reservation))
	must(t, store.SaveJob(ctx, job))
	must(t, store.SaveAdmissionState(ctx, admissionState))
	must(t, store.Put(ctx, jobRecord))
	must(t, store.SaveRecommendation(ctx, rec))
	must(t, store.SaveProcessRefs(ctx, node.ID, refs))
	must(t, store.SaveJoinToken(ctx, token))
	must(t, store.Close())

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	gotProject, err := reopened.Project(ctx, project.ID)
	if err != nil || gotProject.ID != project.ID || gotProject.ExpectedConcurrency != 4 || !gotProject.AutoApply {
		t.Fatalf("Project = %+v, %v", gotProject, err)
	}
	if gotPreset, err := reopened.Preset(ctx, preset.ID); err != nil || gotPreset.ModelRef != preset.ModelRef {
		t.Fatalf("Preset = %+v, %v", gotPreset, err)
	}
	if gotNode, err := reopened.Node(ctx, node.ID); err != nil || gotNode.ID != node.ID {
		t.Fatalf("Node = %+v, %v", gotNode, err)
	}
	if gotInst, err := reopened.Instance(ctx, inst.ID); err != nil || gotInst.PresetID != preset.ID {
		t.Fatalf("Instance = %+v, %v", gotInst, err)
	}
	if gotRefs, err := reopened.ProcessRefs(ctx, node.ID); err != nil || len(gotRefs) != 1 || gotRefs[0].PID != 12 {
		t.Fatalf("ProcessRefs = %+v, %v", gotRefs, err)
	}
	if gotTokens, err := reopened.ListJoinTokens(ctx); err != nil || len(gotTokens) != 1 || gotTokens[0].Hash != token.Hash || !gotTokens[0].Current {
		t.Fatalf("JoinTokens = %+v, %v", gotTokens, err)
	}

	if projects, err := reopened.ListProjects(ctx); err != nil || len(projects) != 1 {
		t.Fatalf("ListProjects len = %d, %v", len(projects), err)
	}
	if presets, err := reopened.ListPresets(ctx); err != nil || len(presets) != 1 {
		t.Fatalf("ListPresets len = %d, %v", len(presets), err)
	}
	if nodes, err := reopened.ListNodes(ctx); err != nil || len(nodes) != 1 {
		t.Fatalf("ListNodes len = %d, %v", len(nodes), err)
	}
	if instances, err := reopened.ListInstances(ctx); err != nil || len(instances) != 1 {
		t.Fatalf("ListInstances len = %d, %v", len(instances), err)
	}
	if leases, err := reopened.ListLeases(ctx); err != nil || len(leases) != 1 {
		t.Fatalf("ListLeases len = %d, %v", len(leases), err)
	}
	if gotJob, err := reopened.Job(ctx, job.ID); err != nil || gotJob.ID != job.ID {
		t.Fatalf("Job = %+v, %v", gotJob, err)
	}
	if gotAdmission, found, err := reopened.AdmissionState(ctx, node.ID); err != nil || !found || gotAdmission.Fence != 4 {
		t.Fatalf("AdmissionState = %+v found=%v err=%v", gotAdmission, found, err)
	}
	if _, found, err := reopened.AdmissionState(ctx, "missing"); err != nil || found {
		t.Fatalf("missing AdmissionState found=%v err=%v", found, err)
	}
	if reservations, err := reopened.ListReservations(ctx); err != nil || len(reservations) != 1 {
		t.Fatalf("ListReservations len = %d, %v", len(reservations), err)
	}
	if jobs, err := reopened.ListJobs(ctx); err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobs len = %d, %v", len(jobs), err)
	}
	if records, err := reopened.Snapshot(ctx); err != nil || len(records) != 1 || records[0].JobID != job.ID {
		t.Fatalf("Snapshot job records = %+v, %v", records, err)
	}
	if recs, err := reopened.ListRecommendations(ctx, project.ID); err != nil || len(recs) != 1 {
		t.Fatalf("ListRecommendations len = %d, %v", len(recs), err)
	}

	must(t, reopened.MarkRecommendationApplied(ctx, rec.ID, time.Unix(3, 0).UTC()))
	recs, err := reopened.ListRecommendations(ctx, "")
	if err != nil || !recs[0].Applied || recs[0].AppliedAt.IsZero() {
		t.Fatalf("applied recs = %+v, %v", recs, err)
	}

	must(t, reopened.DeleteInstance(ctx, inst.ID))
	must(t, reopened.DeleteLease(ctx, lease.ID))
	must(t, reopened.DeleteReservation(ctx, reservation.ID))
	must(t, reopened.DeleteProcessRefs(ctx, node.ID))
}

func TestStoreTelemetryAndErrors(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.SaveProject(ctx, domain.Project{}); err == nil {
		t.Fatal("SaveProject should require an id")
	}
	for name, err := range map[string]error{
		"preset":         store.SavePreset(ctx, domain.Preset{}),
		"node":           store.SaveNode(ctx, domain.Node{}),
		"instance":       store.SaveInstance(ctx, domain.ModelInstance{}),
		"lease":          store.SaveLease(ctx, domain.Lease{}),
		"admission":      store.SaveAdmissionState(ctx, domain.AdmissionState{}),
		"reservation":    store.SaveReservation(ctx, domain.Reservation{}),
		"job":            store.SaveJob(ctx, domain.Job{}),
		"recommendation": store.SaveRecommendation(ctx, domain.RecommendationRecord{}),
		"process refs":   store.SaveProcessRefs(ctx, "", nil),
		"join token":     store.SaveJoinToken(ctx, domain.JoinTokenRecord{}),
	} {
		if err == nil {
			t.Fatalf("%s save expected id error", name)
		}
	}
	if _, err := store.Project(ctx, "missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing project err = %v", err)
	}
	if _, err := store.Job(ctx, "missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing job err = %v", err)
	}
	if _, err := store.ProcessRefs(ctx, ""); err == nil {
		t.Fatal("ProcessRefs should require node id")
	}
	if err := store.Record(ctx, domain.RunMetric{At: time.Unix(1, 0).UTC()}); err == nil {
		t.Fatal("Record should require job id")
	}
	if err := store.SaveRecommendation(ctx, domain.RecommendationRecord{ID: "rec-no-time"}); err == nil {
		t.Fatal("SaveRecommendation should require created_at")
	}
	if err := store.Record(ctx, domain.RunMetric{JobID: "metric"}); err == nil {
		t.Fatal("Record should require timestamp")
	}
	if err := store.Put(ctx, domain.JobRecord{}); err == nil {
		t.Fatal("Put job record should require an id")
	}
	for name, rec := range map[string]domain.JobRecord{
		"coordinator": recordWith(fixtures.MakeJobRecord(), func(r *domain.JobRecord) { r.Coordinator = "" }),
		"status":      recordWith(fixtures.MakeJobRecord(), func(r *domain.JobRecord) { r.Status = "" }),
		"request":     recordWith(fixtures.MakeJobRecord(), func(r *domain.JobRecord) { r.Request = nil }),
		"updated":     recordWith(fixtures.MakeJobRecord(), func(r *domain.JobRecord) { r.UpdatedAt = time.Time{} }),
	} {
		if err := store.Put(ctx, rec); err == nil {
			t.Fatalf("Put job record should require %s", name)
		}
	}
	if err := store.Put(ctx, recordWith(fixtures.MakeJobRecord(), func(r *domain.JobRecord) {
		r.Request = nil
		r.PayloadRedacted = true
	})); err == nil {
		t.Fatal("Put redacted standard job record should require private handling")
	}
	if err := store.Put(ctx, recordWith(fixtures.MakeJobRecord(), func(r *domain.JobRecord) {
		r.Request = nil
		r.Handling = domain.HandlingPrivate
		r.PayloadRedacted = true
	})); err != nil {
		t.Fatalf("Put redacted private job record should be accepted: %v", err)
	}
	if _, err := store.Watch(ctx, "not-time"); err == nil {
		t.Fatal("Watch should reject bad cursor")
	}

	at := time.Unix(10, 0).UTC()
	must(t, store.Record(ctx, domain.RunMetric{JobID: "m1", InstanceID: "i", NodeID: "n", PresetID: "preset-a", Backend: domain.BackendLlamaCpp, Project: "p1", ContextUsed: 10, At: at}))
	must(t, store.Record(ctx, domain.RunMetric{JobID: "m2", InstanceID: "i", NodeID: "n", PresetID: "preset-b", Backend: domain.BackendMLX, Project: "p2", ContextUsed: 20, At: at.Add(time.Second)}))
	all, err := store.Metrics(ctx, "")
	if err != nil || len(all) != 2 {
		t.Fatalf("Metrics all len = %d, %v", len(all), err)
	}
	filtered, err := store.Metrics(ctx, "p2")
	if err != nil || len(filtered) != 1 || filtered[0].JobID != "m2" || filtered[0].PresetID != "preset-b" || filtered[0].Backend != domain.BackendMLX {
		t.Fatalf("Metrics filtered = %+v, %v", filtered, err)
	}
	rec := domain.RecommendationRecord{ID: "rec-a", ProjectID: "p2", Type: "context", RecommendedValue: 30, CreatedAt: time.Unix(11, 0).UTC()}
	must(t, store.SaveRecommendation(ctx, rec))
	gotRec, err := store.Recommendation(ctx, rec.ID)
	if err != nil || gotRec.ID != rec.ID || gotRec.CreatedAt.IsZero() {
		t.Fatalf("Recommendation = %+v, %v", gotRec, err)
	}
}

func TestStoreJobRegistryLWWAndWatch(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	first := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-a"))
	first.UpdatedAt = time.Unix(20, 0).UTC()
	first.Request = []byte("first")
	must(t, store.Put(ctx, first))
	older := first
	older.UpdatedAt = first.UpdatedAt.Add(-time.Second)
	older.Status = domain.JobRunning
	must(t, store.Put(ctx, older))
	newer := first
	newer.UpdatedAt = first.UpdatedAt.Add(time.Second)
	newer.Fence = 2
	newer.Status = domain.JobRunning
	newer.Request = []byte("newer")
	must(t, store.Put(ctx, newer))
	newer.Request[0] = 'X'
	other := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-b"))
	other.UpdatedAt = newer.UpdatedAt
	must(t, store.Put(ctx, other))
	higherFence := newer
	higherFence.Fence = 3
	higherFence.Request = []byte("higher-fence")
	must(t, store.Put(ctx, higherFence))
	tieWinner := higherFence
	tieWinner.Coordinator = "zz-peer"
	tieWinner.Request = []byte("tie")
	must(t, store.Put(ctx, tieWinner))

	snap, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 2 || snap[0].JobID != "job-a" || snap[1].JobID != "job-b" || string(snap[0].Request) != "tie" {
		t.Fatalf("snapshot = %+v", snap)
	}
	snap[0].Request[0] = '!'
	again, err := store.Snapshot(ctx)
	if err != nil || string(again[0].Request) != "tie" {
		t.Fatalf("snapshot clone = %+v %v", again, err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	watch, err := store.Watch(watchCtx, first.UpdatedAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if got := receiveStoreRecord(t, watch); got.JobID != "job-a" {
		t.Fatalf("initial watch record = %+v", got)
	}
	if got := receiveStoreRecord(t, watch); got.JobID != "job-b" {
		t.Fatalf("second watch record = %+v", got)
	}
	future := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-c"))
	future.UpdatedAt = newer.UpdatedAt.Add(time.Second)
	must(t, store.Put(ctx, future))
	if got := receiveStoreRecord(t, watch); got.JobID != "job-c" {
		t.Fatalf("future watch record = %+v", got)
	}
	cancel()
	if got, ok := receiveStoreClosed(t, watch); ok {
		t.Fatalf("watch stayed open with record %+v", got)
	}
}

func TestStoreJobRegistryRejectsStalledWatchers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	watch, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	for i := 0; i < 128; i++ {
		rec := fixtures.MakeJobRecord(fixtures.WithRecordJobID(fmt.Sprintf("job-%03d", i)))
		rec.UpdatedAt = time.Unix(100+int64(i), 0).UTC()
		must(t, store.Put(ctx, rec))
	}
	overflow := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-overflow"))
	overflow.UpdatedAt = time.Unix(300, 0).UTC()
	if err := store.Put(ctx, overflow); err == nil || !strings.Contains(err.Error(), "not draining") {
		t.Fatalf("overflow err = %v", err)
	}
	for range watch {
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func recordWith(rec domain.JobRecord, mutate func(*domain.JobRecord)) domain.JobRecord {
	mutate(&rec)
	return rec
}

func receiveStoreRecord(t *testing.T, ch <-chan domain.JobRecord) domain.JobRecord {
	t.Helper()
	select {
	case rec, ok := <-ch:
		if !ok {
			t.Fatal("watch closed")
		}
		return rec
	default:
		t.Fatal("record was not ready")
	}
	return domain.JobRecord{}
}

func receiveStoreClosed(t *testing.T, ch <-chan domain.JobRecord) (domain.JobRecord, bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		select {
		case rec, ok := <-ch:
			return rec, ok
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("watch close was not ready")
	return domain.JobRecord{}, false
}
