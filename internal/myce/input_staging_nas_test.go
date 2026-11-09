package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #660: stored source exists, but a shared NAS input was authored relative to
// the mount instead of the worker's checkpoint root. Preserve that boundary
// through the same runtime redaction and durable result path used by an actor.
func TestInputStagingMissingNASNamesSourceAndSurvivesReopen(t *testing.T) {
	q, clock := fixture(t)
	s := submitVerificationInput(t, q)
	if err := q.ConfigureFileStore(q.files.root, 1<<20, false); err != nil {
		t.Fatal(err)
	}
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "movie", "verified media outside checkpoint root")
	checkpointRoot := filepath.Join(nas, "checkpoints")
	if err := os.Mkdir(checkpointRoot, 0700); err != nil {
		t.Fatal(err)
	}
	s.SharedInputs = map[string]SharedInput{"movie": {NAS: "movie", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
	s.Resumable = false
	m := cacheCapacityMachine("nas-reader", 1<<20)
	check(t, q, m, nil)
	submitted := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("not assigned", job(t, q, submitted.ID))
	}
	w := Worker{Clock: clock, CheckpointRoot: checkpointRoot, sharedInputs: &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}}
	_, release, err := w.acquireSharedInputs(context.Background(), *assigned)
	if release != nil {
		release()
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected the missing NAS input: %v", err)
	}
	staging := runtimeErrorFor("input_staging", err)
	cause := runtimeDiagnostic(staging)
	data, err := json.Marshal(cause)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		PathDiagnostic *PathDiagnostic `json:"path_diagnostic"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.PathDiagnostic == nil || wire.PathDiagnostic.Field != "shared_inputs.movie.nas" || wire.PathDiagnostic.ResolutionBase != "worker-checkpoint-root" || wire.PathDiagnostic.Cause != "missing" || cause.Class != "not_found" {
		t.Fatalf("missing input lost its source boundary: %+v; %v", cause, staging)
	}
	r := startFailureOutcome(result(assigned, false), staging)
	r.Error = staging.Error()
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	database, root, store := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(database, root, clock, WithFileStore(store, 1<<20, false))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	got, err := (Client{URL: api.URL, Token: "test"}).GetJob(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "failed" || got.Losses != 0 || got.ExecutionCause == nil || got.ExecutionCause.Class != "not_found" || !strings.Contains(got.Reason, "shared_inputs.movie.nas") || strings.Contains(got.Reason, nas) {
		t.Fatalf("reopen lost missing-input diagnosis or exposed private root: %+v", got)
	}
}

func TestInputStagingPathDiagnosticRefusesPrivatePaths(t *testing.T) {
	c := &RuntimeCause{Operation: "input_staging", Class: "not_found", PathDiagnostic: &PathDiagnostic{Field: "shared_inputs.movie.nas", Side: "worker", Path: "/synthetic/private/movie", ResolutionBase: "worker-checkpoint-root", Cause: "missing"}}
	if safeRuntimeCause(c).PathDiagnostic != nil || strings.Contains(c.String(), "/synthetic/private") {
		t.Fatal("worker-supplied private path escaped redaction")
	}
}

func TestInputStagingPathDiagnosticRequiresDeclaredNASInput(t *testing.T) {
	for _, name := range []string{"UNDECLARED_SECRET", "stored"} {
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			s := submitVerificationInput(t, q)
			s.SharedInputs = map[string]SharedInput{
				"movie":  {NAS: "movie", SHA256: s.Files["input"].SHA256, SizeBytes: s.Files["input"].SizeBytes},
				"stored": {SHA256: s.Files["input"].SHA256, SizeBytes: s.Files["input"].SizeBytes},
			}
			m := cacheCapacityMachine("nas-reader", 1<<20)
			check(t, q, m, nil)
			submitted := submit(t, q, s)
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("not assigned")
			}
			field := "shared_inputs." + name + ".nas"
			cause := &RuntimeCause{Operation: "input_staging", Class: "not_found", PathDiagnostic: &PathDiagnostic{Field: field, Side: "worker", Path: "input", ResolutionBase: "worker-checkpoint-root", Cause: "missing"}}
			r := result(assigned, false)
			r.ExecutionCause, r.Error = cause, cause.String()
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			database, root := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			q, err := Open(database, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			// Check durable state too: public redaction alone must not hide a
			// worker-supplied name already saved into reasons/history/ledger.
			raw, err := json.Marshal(q.s.Jobs[submitted.ID])
			if err != nil {
				t.Fatal(err)
			}
			got := job(t, q, submitted.ID)
			wantDiagnostic := name == "movie"
			if got.ExecutionCause == nil || (got.ExecutionCause.PathDiagnostic != nil) != wantDiagnostic || got.State != "failed" || len(got.AttemptHistory) != 1 {
				t.Fatalf("unexpected completion: %+v", got)
			}
			if strings.Contains(string(raw), field) != wantDiagnostic || strings.Contains(got.Reason, field) != wantDiagnostic || strings.Contains(got.AttemptHistory[0].Reason, field) != wantDiagnostic {
				t.Fatalf("diagnostic was not bound to declared NAS inputs: %s", raw)
			}
		})
	}
}
