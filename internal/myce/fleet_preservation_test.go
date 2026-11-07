package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFleetQueuePreservationFencesOfflineConversionUntilConsumed(t *testing.T) {
	c, clock, mapping := m4OfflineQueue(t)
	q, err := Open(c.DB, c.Checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, true)
	configureManagedPreservation(t, q, clock)
	requestManagedQueueHandoff(t, q)
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	seal, err := os.ReadFile(q.preservationPath)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := os.ReadFile(c.ProjectConfig)
	if err != nil {
		t.Fatal(err)
	}
	run := func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
	// This is otherwise valid conversion intent, including read-only preview.
	if err = ConvertQueueInstall(context.Background(), c, mapping, true, false, clock, run); err != nil {
		t.Fatal("valid preview refused", err)
	}
	err = ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, run)
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Code != "fleet_preservation_pending" {
		t.Fatal("writable conversion accepted unconsumed handoff", err)
	}
	afterSeal, sealErr := os.ReadFile(q.preservationPath)
	afterPolicy, policyErr := os.ReadFile(c.ProjectConfig)
	if sealErr != nil || policyErr != nil || !reflect.DeepEqual(seal, afterSeal) || !reflect.DeepEqual(policy, afterPolicy) {
		t.Fatal("refusal changed certificate or policy", sealErr, policyErr)
	}
	// Startup can still consume the exact sealed records after refusal.
	q, err = Open(c.DB, c.Checkpoints, clock)
	if err != nil {
		t.Fatal("offline refusal invalidated queue startup", err)
	}
	if q.s.MachineConversion != nil || q.fleetActiveHost().Preservation.State != "verified" {
		t.Fatal("offline conversion mutated state or consumed proof")
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	if err = ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, run); err != nil {
		t.Fatal("conversion remained blocked after proof was consumed", err)
	}
	q, err = Open(c.DB, c.Checkpoints, clock)
	if err != nil {
		t.Fatal("converted queue failed startup", err)
	}
	defer q.Close()
	if q.s.MachineConversion == nil || q.s.Machines[mapping[0].From].Replacement == nil {
		t.Fatal("authorized conversion was not durable")
	}
}

func TestFleetQueuePreservationAcceptedFileReferences(t *testing.T) {
	// Reuse the existing supported file/program fixture and its bound root.
	q, clock, m, spec, output := storageFixture(t)
	configureManagedPreservation(t, q, clock)
	spec.Resumable, spec.SubmissionKey = true, "accepted-files"
	run := startObservation(t, q, m, spec)
	requestManagedQueueHandoff(t, q)
	client := deliveryClient(t, q)
	_, checkpoint := retentionUpload(t, client, `{"myce_checkpoint":{"generation":7},"value":5}`)
	if err := q.publishCheckpoint(context.Background(), checkpointPublication{Result: result(&run, true), Checkpoint: FileCheckpoint{Generation: 7, Files: FileManifest{"checkpoint.json": checkpoint}}}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.Complete(storageResult(&run, output, 4)); err != nil {
		t.Fatal(err)
	}
	before := cloneJob(q.s.Jobs[run.ID])
	path, root, files := strings.TrimSuffix(q.preservationPath, ".fleet-preservation"), q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, clock, WithFileStore(files, 1<<20, true))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after := reopened.s.Jobs[run.ID]
	best := bestFileCheckpoint(after)
	if after.State != "succeeded" || !reflect.DeepEqual(after.FileCheckpoints, before.FileCheckpoints) || !reflect.DeepEqual(after.OutputRefs, before.OutputRefs) || best == nil || best.Generation != 7 || after.OutputRefs["a"] != output {
		t.Fatal("accepted checkpoint/result references changed")
	}
}

func managedPreservationFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec, FleetUpdateInstruction) {
	t.Helper()
	q, clock, m, spec := observationFixture(t)
	configureManagedPreservation(t, q, clock)
	spec.SubmissionKey = "accepted-before-handoff"
	completed := startObservation(t, q, m, spec)
	clock.Advance(time.Second)
	finish(t, q, &completed, true)
	if _, err := q.CancelSubmission(spec.Project, "reserved-before-handoff"); err != nil {
		t.Fatal(err)
	}
	i := requestManagedQueueHandoff(t, q)
	return q, clock, m, spec, i
}

func configureManagedPreservation(t *testing.T, q *Queue, clock *fakeClock) {
	t.Helper()
	signed, key, _ := signedTestRelease(t)
	if err := q.ConfigureEnrollmentRelease(signed, key, "https://publisher.invalid/release"); err != nil {
		t.Fatal(err)
	}
	config := ServiceConfig{Role: "queue", HostID: "queue", Config: filepath.Join(t.TempDir(), "config"), Binary: "synthetic-binary", Service: "synthetic-unit", Detection: Detection{OS: "linux", Arch: "amd64"}}
	if err := q.ConfigureFleetService(config); err != nil {
		t.Fatal(err)
	}
	q.fleetQueue.Proof = fleetTestProof(clock, q.fleetQueue.Session, 100, false)
	q.fleetQueue.ObservedAt = clock.Now()
}

func requestManagedQueueHandoff(t *testing.T, q *Queue) FleetUpdateInstruction {
	t.Helper()
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{QueueOnly: true}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	return i
}

func TestFleetQueuePreservationCommittedHandoff(t *testing.T) {
	q, clock, m, spec, instruction := managedPreservationFixture(t)
	// Commits after restart intent are included, while remote work remains live.
	spec.SubmissionKey = "running-during-handoff"
	running := startObservation(t, q, m, spec)
	before := cloneJob(q.s.Jobs[running.ID])
	curves := map[string]string{}
	for key, point := range q.s.WorkObservations {
		curves[key] = curveMeasurementDigest(point)
	}
	releases := maps.Clone(q.s.HostReleases)
	root, path, config := q.root, strings.TrimSuffix(q.preservationPath, ".fleet-preservation"), *q.fleetService
	// Close must fence/cancel requests before waiting for a transaction, then
	// seal those committed records while still holding the process file lock.
	q.initShutdown()
	q.mu.Lock()
	closed := make(chan error, 1)
	go func() { closed <- q.Close() }()
	select {
	case <-q.shutdownContext.Done():
	case <-time.After(time.Second):
		q.mu.Unlock()
		<-closed
		t.Fatal("managed Close waited for the queue mutex before its shutdown fence")
	}
	q.mu.Unlock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	h := q.fleetActiveHost()
	if h.Preservation == nil || h.Preservation.State != "verified" || h.Preservation.Records < 5 || h.Preservation.Bytes == 0 || h.Preservation.Session != instruction.Session || !h.Preservation.VerifiedAt.Equal(clock.Now()) {
		t.Fatal("missing exact handoff receipt", h.Preservation)
	}
	after := q.s.Jobs[running.ID]
	if after.State != before.State || after.Attempt != before.Attempt || after.Machine != before.Machine || after.Session != before.Session || !reflect.DeepEqual(after.StartedAt, before.StartedAt) || !reflect.DeepEqual(q.s.HostReleases, releases) {
		t.Fatal("restart changed running attempt or custody")
	}
	// Startup labels stale capacity while worker inventory is unconfirmed.
	// Accepted measurements, provenance and counters must remain exact.
	afterCurves := map[string]string{}
	for key, point := range q.s.WorkObservations {
		afterCurves[key] = curveMeasurementDigest(point)
	}
	if !reflect.DeepEqual(curves, afterCurves) {
		t.Fatal("restart lost accepted learning")
	}
	spec.SubmissionKey = "accepted-before-handoff"
	if j := submit(t, q, spec); j.State != "succeeded" || j.ID == running.ID {
		t.Fatal("terminal keyed replay changed", j.ID, j.State)
	}
	ack, err := q.CancelSubmission(spec.Project, "reserved-before-handoff")
	if err != nil || !ack.WillNotRun {
		t.Fatal("reserved key lost", err)
	}
	if err = q.ConfigureFleetService(config); err != nil {
		t.Fatal(err)
	}
	q.fleetQueue.Proof = fleetTestProof(clock, q.fleetQueue.Session, 101, true)
	// A delayed duplicate ACK cannot erase the consumed proof.
	if err = q.AckFleetUpgrade(FleetUpgradeAck{Operation: instruction.Operation, Revision: instruction.Revision, Host: instruction.Host, Machine: instruction.Machine, Session: instruction.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	if q.fleetActiveHost().Preservation.State != "verified" {
		t.Fatal("replayed restart intent erased verification")
	}
	// Existing exact runtime health/custody checks still own completion.
	for n := 0; n < 40; n++ {
		fleetHeartbeatAdvance(t, q, clock)
	}
	if q.s.FleetUpgrade.State != "complete" || q.s.Jobs[running.ID].Attempt != before.Attempt {
		t.Fatal("verified queue handoff did not finish independently of remote batch work", q.fleetUpgradeStatus())
	}
	// Verified receipt survives an ordinary later restart; the consumed seal is
	// not compared to newer legitimate records or treated as a DB restore point.
	q = reopenCancellationQueue(t, q, clock)
	defer q.Close()
	if q.s.FleetUpgrade.Hosts[0].Preservation.State != "verified" {
		t.Fatal("verification was not durable")
	}
}

func TestFleetQueuePreservationRefusesMissingChangedOrUndecodableRecords(t *testing.T) {
	for _, scenario := range []string{"missing_seal", "changed_record", "unknown_job_field"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock, _, _, _ := managedPreservationFixture(t)
			path, root := strings.TrimSuffix(q.preservationPath, ".fleet-preservation"), q.root
			want := "fleet_preservation_seal_missing"
			if scenario == "unknown_job_field" {
				// Simulate a supported predecessor retaining a field this binary
				// cannot decode. Transport equality alone must not bless its loss.
				for key, raw := range q.persisted {
					if !strings.HasPrefix(key, "jobs/") {
						continue
					}
					var value map[string]json.RawMessage
					if err := json.Unmarshal(raw, &value); err != nil {
						t.Fatal(err)
					}
					value["future_accepted_progress"] = json.RawMessage(`9007199254740993`)
					data, _ := json.Marshal(value)
					if _, err := q.db.Exec("UPDATE queue_state SET data=? WHERE key=?", data, key); err != nil {
						t.Fatal(err)
					}
					q.persisted[key] = data
					break
				}
				want = "fleet_preservation_decode_loss"
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "missing_seal":
				if err := os.Remove(q.preservationPath); err != nil {
					t.Fatal(err)
				}
			case "changed_record":
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.Exec("DELETE FROM queue_state WHERE key LIKE 'jobs/%'")
				if closeErr := db.Close(); err != nil || closeErr != nil {
					t.Fatal(err, closeErr)
				}
				want = "fleet_preservation_records_changed"
			}
			reopened, err := Open(path, root, clock)
			if reopened != nil {
				reopened.Close()
			}
			if err == nil || err.Error() != want {
				t.Fatal("unsafe handoff accepted", err, "want", want)
			}
		})
	}
}
