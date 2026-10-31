package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func migrationAction(t *testing.T, q *Queue, action string) *StorageMigrationStatus {
	t.Helper()
	var out StorageMigrationStatus
	method, path := "POST", "/api/files/admission/"+action
	if action == "status" {
		method, path = "GET", "/api/files/admission"
	}
	if err := deliveryClient(t, q).Call(context.Background(), method, path, nil, &out); err != nil {
		t.Fatal(action, err)
	}
	return &out
}

// The proof is a real HTTP custody/delivery/collection cycle with a fake clock,
// SQLite and small files. Its duration includes the unchanged publication grace;
// it is measured, never selected as an activation timeout.
func TestStorageMigrationRefusesWithoutEvidenceAndPreservesLegacy(t *testing.T) {
	q, c, _ := protectionQueue(t)
	q.fileStoreProductionReserve, q.files.productionReserve = 16<<30, 16<<30
	before := q.fileStoreStatus(context.Background())
	initial := migrationAction(t, q, "observe")
	if initial.Ready || initial.Saved.LimitBytes != 185<<30 || initial.Saved.ProductionReserveBytes != 16<<30 {
		t.Fatal(initial)
	}
	c.Advance(365 * 24 * time.Hour)
	still := migrationAction(t, q, "observe")
	if still.RequiredSeconds != 0 || still.Ready {
		t.Fatal("idle time fabricated evidence", still)
	}
	var out StorageMigrationStatus
	err := deliveryClient(t, q).Call(context.Background(), "POST", "/api/files/admission/activate", nil, &out)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "storage_migration_incomplete" || q.s.StorageAdmissionActive {
		t.Fatal("unqualified activation", err)
	}
	after := q.fileStoreStatus(context.Background())
	if before.LimitBytes != after.LimitBytes || before.ProductionReserveBytes != after.ProductionReserveBytes {
		t.Fatal("shadow changed legacy settings", before, after)
	}
}

func TestStorageMigrationCycleCutoverRollbackAndRestart(t *testing.T) {
	f := migrationQualifiedCycle(t)
	q := f.q
	report := migrationAction(t, q, "status")
	if !report.Ready || report.CompletedCycles != 1 || report.RequiredSeconds != report.ConsistentSeconds || report.RequiredSeconds < filePublicationGrace.Seconds() || report.PeriodJob != f.j.ID {
		t.Fatalf("missing measured lifecycle: %+v", report)
	}
	saved := *report.Saved
	migrationAction(t, q, "activate")
	live := q.fileStoreStatus(context.Background())
	if live.LimitBytes != 0 || live.ProductionReserveBytes != 0 || live.FloorBytes != 0 || live.FreeBytes == 0 {
		t.Fatal("guards did not retire together", live)
	}
	if !migrationAction(t, q, "status").ManualWriterLimitsRetirable {
		t.Fatal("verified delivery/collection did not retire external writer agreements")
	}
	// Reinstallation may supply defaults; persisted saved settings and cutover win.
	root, files := q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.db, root, f.c, WithFileStore(files, 100<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fakeCheckpointAvailability(t, reopened, true)
	if !reopened.s.StorageAdmissionActive {
		t.Fatal("restart lost cutover")
	}
	if status := reopened.fileStoreStatus(context.Background()); status.Error == "" {
		t.Fatal("restart renewed native qualification")
	}
	if err = reopened.ConfigureStorageProtection(context.Background(), protectionBackend(f.w.Machine), protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	if status := reopened.fileStoreStatus(context.Background()); status.LimitBytes != 0 || status.Error != "" {
		t.Fatal("reverified active restart", status)
	}
	rolled := migrationAction(t, reopened, "rollback")
	if !rolled.SavedSettingsActive || *rolled.Saved != saved || reopened.s.StorageAdmissionActive {
		t.Fatal("rollback lost exact saved settings", rolled)
	}
	status := reopened.fileStoreStatus(context.Background())
	if status.LimitBytes != saved.LimitBytes || status.ProductionReserveBytes != saved.ProductionReserveBytes || status.FloorBytes == 0 {
		t.Fatal("rollback used reinstallation defaults", status)
	}
	// Rollback neither rewrites frozen policies nor deletes preserved destination.
	if err := verifyQualifiedTaskOutputDelivery(*job(t, reopened, f.j.ID), f.w.OutputDestinations["archive"]); err != nil {
		t.Fatal(err)
	}
}

func migrationQualifiedCycle(t *testing.T, settings ...StorageLegacySettings) *taskOutputFixture {
	t.Helper()
	f := newTaskOutputFixture(t, "nas", "", func(q *Queue, m Machine) {
		q.fileStoreProductionReserve, q.files.productionReserve = 16<<30, 16<<30
		if len(settings) > 0 {
			q.files.limit = int64(settings[0].LimitBytes)
			q.fileStoreProductionReserve = int64(settings[0].ProductionReserveBytes)
			q.files.productionReserve = settings[0].ProductionReserveBytes
		}
		migrationAction(t, q, "observe")
	})
	f.pending(t)
	f.c.Advance(time.Minute)
	f.pending(t)
	if j := job(t, f.q, f.j.ID); j.Delivery == nil || j.Delivery.State != "verified" {
		t.Fatal("no real destination verification", j.Delivery)
	}
	if err := f.q.Complete(Result{Execution: Execution{ID: f.j.ID, Attempt: f.j.Attempt}, Machine: f.j.Machine, Session: f.j.Session, ReleaseOnly: true}); err != nil {
		t.Fatal(err)
	}
	f.c.Advance(DefaultFileRetention)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal("legal local collection", err)
	}
	migrationAction(t, f.q, "observe")
	return f
}

func TestStorageMigrationLegacyBacklogAndMismatch(t *testing.T) {
	f := migrationQualifiedCycle(t)
	q := f.q
	client := deliveryClient(t, q)
	_, ref := retentionUpload(t, client, "protected legacy experiment")
	legacy := submit(t, q, spec("experiment"))
	if err := q.update(func() error { j := q.touchJob(legacy.ID); j.OutputRefs = FileManifest{"old": ref}; return nil }); err != nil {
		t.Fatal(err)
	}
	report := migrationAction(t, q, "status")
	if report.Ready || !strings.Contains(strings.Join(report.Blockers, ";"), "legacy shared experiment backlog") {
		t.Fatal("protected legacy backlog advertised isolation", report)
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("migration deleted protected bytes", err)
	}
	if err := q.update(func() error {
		j := q.touchJob(legacy.ID)
		j.OutputRefs = nil
		j.AttemptHistory = []AttemptOutcome{{Diagnostics: &AttemptDiagnostics{Files: FileManifest{"failure-progress": ref}}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if status := migrationAction(t, q, "status"); status.Ready || !strings.Contains(strings.Join(status.Blockers, ";"), "legacy shared experiment backlog") {
		t.Fatal("failed-attempt diagnostic custody escaped the drain gate", status)
	}

	// Existing preservation/release authority remains the only way to drain it.
	if err := q.Cancel(legacy.ID); err != nil {
		t.Fatal(err)
	}
	q.deliveryRoot = t.TempDir()
	q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 100 << 30, FreeBytes: 2 << 30}, nil }
	// No active legacy estimate is changed by shadowing a proposed larger learned
	// commitment. A decision mismatch resets the streak instead of waiting it out.
	s := storageAdmissionSpec("production", 1)
	submit(t, q, s)
	before := q.s.StorageMigration.Disagreements
	q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 100 << 30, FreeBytes: 0}, nil }
	var activation StorageMigrationStatus
	if err := deliveryClient(t, q).Call(context.Background(), "POST", "/api/files/admission/activate", nil, &activation); err == nil {
		t.Fatal("mismatching shadow activated")
	}
	migrationAction(t, q, "status") // Read-only status must expose the committed failed comparison.
	if q.s.StorageMigration.Disagreements <= before || q.s.StorageMigration.CompletedCycles != 0 {
		t.Fatal("mismatch did not reset evidence", q.s.StorageMigration)
	}
}

func TestStorageMigrationReadinessIncludesHiddenLegacyOutputs(t *testing.T) {
	for _, retainedAnswer := range []bool{false, true} {
		t.Run(fmt.Sprintf("keyed_answer=%t", retainedAnswer), func(t *testing.T) {
			f := migrationQualifiedCycle(t)
			q := f.q
			client := deliveryClient(t, q)
			if !migrationAction(t, q, "status").Ready {
				t.Fatal("fixture has not qualified for activation")
			}
			var before Snapshot
			if err := client.Call(context.Background(), "GET", "/api/status?jobs=active", nil, &before); err != nil {
				t.Fatal(err)
			}
			if !before.FileStore.Admission.Migration.Ready {
				t.Fatal("paged status hides qualified readiness before the legacy output arrives")
			}
			_, ref := retentionUpload(t, client, "protected experiment result")
			s := spec("experiment")
			s.SubmissionKey = "legacy-result"
			legacy := submit(t, q, s)
			if err := q.Cancel(legacy.ID); err != nil {
				t.Fatal(err)
			}
			f.c.Advance(time.Second)
			if err := q.update(func() error {
				j := q.touchJob(legacy.ID)
				j.OutputRefs = FileManifest{"old": ref, "alias": ref}
				if retainedAnswer {
					q.deleteJob(legacy.ID)
				}
				// Push the retained holder beyond even the default bounded page.
				for i := range defaultStatusJobs + 1 {
					id := fmt.Sprintf("recent-%03d", i)
					q.putJob(id, &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: f.c.Now(), Spec: Spec{Project: "recent"}})
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if retainedAnswer && (q.s.Jobs[legacy.ID] != nil || q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)].Job.OutputRefs["old"] != ref) {
				t.Fatal("fixture did not retain an answer outside job history")
			}
			var activation StorageMigrationStatus
			err := client.Call(context.Background(), "POST", "/api/files/admission/activate", nil, &activation)
			var api *HTTPError
			if !errors.As(err, &api) || api.Status != 409 || api.Code != "storage_migration_incomplete" || q.s.StorageAdmissionActive {
				t.Fatal("protected legacy output did not block activation", err)
			}
			exact := migrationAction(t, q, "status")
			assertStorageMigrationIndex(t, q)
			if exact.Ready || !strings.Contains(strings.Join(exact.Blockers, ";"), "legacy shared experiment backlog") {
				t.Fatalf("missing activation blocker: %+v", exact)
			}
			if !strings.Contains(strings.Join(exact.Blockers, ";"), fmt.Sprintf("1 stored payloads (%d bytes)", ref.SizeBytes)) {
				t.Fatalf("missing deduplicated protected output count/bytes: %+v", exact)
			}
			for _, path := range []string{
				"/api/status", "/api/status?jobs=active", "/api/status?jobs=recent&limit=1",
				"/api/status?jobs=all&limit=1", "/api/status?project=recent", "/api/status?detail=full", "/api/inspection",
			} {
				t.Run(path, func(t *testing.T) {
					var snapshot Snapshot
					if err := client.Call(context.Background(), "GET", path, nil, &snapshot); err != nil {
						t.Fatal(err)
					}
					if snapshot.FileStore == nil || snapshot.FileStore.Admission == nil || snapshot.FileStore.Admission.Migration == nil {
						t.Fatal("missing migration readiness")
					}
					got := snapshot.FileStore.Admission.Migration
					if got.Ready != exact.Ready || got.ManualWriterLimitsRetirable != exact.ManualWriterLimitsRetirable || !slices.Equal(got.Blockers, exact.Blockers) {
						t.Fatalf("status disagrees with activation: got %+v; dedicated %+v", got, exact)
					}
					if !strings.Contains(path, "full") && path != "/api/inspection" {
						for _, j := range snapshot.Jobs {
							if j.ID == legacy.ID {
								t.Fatal("fixture did not hide retained holder from paged status")
							}
						}
					}
				})
			}
			if _, err := os.Stat(q.files.blob(ref)); err != nil {
				t.Fatal("readiness checks lost protected output", err)
			}
		})
	}
}

func TestStorageMigrationInvalidatesReplacedBackingEvidence(t *testing.T) {
	f := migrationQualifiedCycle(t)
	before := migrationAction(t, f.q, "status")
	if !before.Ready {
		t.Fatal(before)
	}
	backend := protectionBackend(f.w.Machine)
	backend.snapshot.Backings[0].NativeIdentity = identityHash("replacement-control-volume")
	if err := f.q.ConfigureStorageProtection(context.Background(), backend, protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	changed := migrationAction(t, f.q, "status")
	if changed.Ready {
		t.Fatal("new boundary inherited old shadow proof")
	}
	migrationAction(t, f.q, "observe")
	if f.q.s.StorageMigration.CompletedCycles != 0 || f.q.s.StorageMigration.Since != f.c.Now() {
		t.Fatal("boundary replacement did not restart measured evidence")
	}
}

func TestStorageMigrationShadowRestartCannotCountDowntime(t *testing.T) {
	f := migrationQualifiedCycle(t)
	root, files := f.q.root, f.q.files.root
	if err := f.q.Close(); err != nil {
		t.Fatal(err)
	}
	f.c.Advance(24 * time.Hour)
	q, err := Open(f.db, root, f.c, WithFileStore(files, 100<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	if err = q.ConfigureStorageProtection(context.Background(), protectionBackend(f.w.Machine), protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	report := migrationAction(t, q, "status")
	if report.Ready {
		t.Fatal("restart counted downtime as consistent observations")
	}
	report = migrationAction(t, q, "observe")
	if report.Ready || report.CompletedCycles != 0 || report.ConsistentSeconds != 0 || report.Saved.LimitBytes != 185<<30 {
		t.Fatal("restart lost settings or fabricated continuity", report)
	}
}
