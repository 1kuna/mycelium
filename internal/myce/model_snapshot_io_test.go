package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Exercise real snapshot resolution, copying/hash verification, worker outcome
// recording and queue completion. No payload has been authorized to start.
func TestModelSnapshotIOFailureBeforeStart(t *testing.T) {
	for _, tc := range []struct {
		name, class string
		fault       error
	}{
		{"read_EIO", "io_error", syscall.EIO},
		{"read_timeout", "timeout", errCheckpointIOTimeout},
		{"read_deadline", "timeout", context.DeadlineExceeded},
		{"wrong_hash", "invalid_input", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			root := t.TempDir()
			repo := tinyModel(t, root)
			if tc.fault == nil {
				blob, err := filepath.EvalSymlinks(filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(blob, []byte(strings.Repeat("x", len(tinyWeights))), 0644); err != nil {
					t.Fatal(err)
				}
			}
			w := snapshotWorker(t, root, clock)
			w.StateDir = newTestStateRoot(t)
			m := machine("snapshot-reader", "fake")
			m.Capabilities = append(m.Capabilities, modelSnapshotsCapability)
			w.Machine = m
			store := &workerStore{dir: t.TempDir()}
			bytes := 0
			w.modelSnapshots.readNAS = func(f *os.File, p []byte) (int, error) {
				if tc.fault != nil {
					return 0, &os.PathError{Op: "read", Path: f.Name(), Err: tc.fault}
				}
				n, err := f.Read(p)
				bytes += n
				return n, err
			}
			s := spec("experiment")
			s.ModelSnapshots = snapshotJob("org/tiny").Spec.ModelSnapshots
			s.ModelSnapshots["base"] = ModelSnapshot{Repo: "org/tiny", Revision: tinyRevision, Files: []string{"model.safetensors"}}
			j := submit(t, q, s)
			for attempt := 1; attempt <= 4; attempt++ {
				assigned := check(t, q, m, nil).Job
				if assigned == nil || assigned.Attempt != attempt {
					t.Fatalf("missing attempt %d: %+v", attempt, assigned)
				}
				store.state.Job, store.state.Session = assigned, assigned.Session
				paths, releases, err := w.acquireModelSnapshots(context.Background(), *assigned)
				for _, release := range releases {
					release()
				}
				if err == nil || len(paths) != 0 {
					t.Fatal("unverified model exposed", paths, err)
				}
				if tc.fault != nil && !errors.Is(err, tc.fault) {
					t.Fatal("lost I/O error identity", err)
				}
				r := startFailureOutcome(result(assigned, false), w.stagingFailure(store, *assigned, err))
				if r.ExecutionCause == nil || r.ExecutionCause.Class != tc.class || r.Interrupted != (tc.fault != nil) {
					t.Fatalf("wrong outcome: %+v", r)
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, j.ID)
				if got.Losses != 0 || got.InfrastructureRetries != 0 || got.StartedAt != nil {
					t.Fatal("staging spent command budget", got)
				}
				finding := statusJobValue(got).Finding
				if finding == nil {
					t.Fatal("missing staging finding")
				}
				if tc.fault == nil {
					if bytes != len(tinyWeights) || got.State != "failed" || got.ExecutionCause.Predicate != "model_snapshot_mismatch" || finding.Code != "model_snapshot_mismatch" || finding.Owner != findingOwnerJob {
						t.Fatalf("wrong content mismatch: bytes=%d job=%+v finding=%+v", bytes, got, finding)
					}
					break
				}
				if got.State != "queued" || got.ExecutionCause.Predicate != "" || finding.Code != "input_staging_infrastructure" || finding.Owner != findingOwnerMycelium {
					t.Fatalf("wrong infrastructure retry: %+v finding=%+v", got, finding)
				}
				wire, _ := json.Marshal(statusJobValue(got))
				if strings.Contains(string(wire), root) {
					t.Fatal("source path leaked")
				}
				if err := q.Complete(r); !errors.Is(err, ErrStale) {
					t.Fatal("stale result accepted", err)
				}
				clock.Advance(time.Second)
			}
		})
	}
}

func TestModelSnapshotOperationalErrors(t *testing.T) {
	for _, cause := range []error{syscall.EIO, syscall.ETIMEDOUT, os.ErrDeadlineExceeded, errCheckpointIOTimeout, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			for _, predicate := range []string{"model_snapshot_missing", "model_snapshot_revision", "model_snapshot_mismatch"} {
				err := modelSnapshotError(predicate, "io_error", &os.PathError{Op: "read", Path: "/private/source", Err: cause})
				d := runtimeDiagnostic(err)
				if !errors.Is(err, cause) || !stagingRetryable(err) || d.Predicate != "" || d.Staging == nil {
					t.Fatalf("operational error became content refusal: %+v", d)
				}
				if cause != syscall.EIO && d.Class != "timeout" {
					t.Fatalf("timeout class lost: %+v", d)
				}
			}
		})
	}
}
