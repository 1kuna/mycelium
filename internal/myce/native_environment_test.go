package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
)

func TestEnvironmentRecipeStoreSubmissionAndSweep(t *testing.T) {
	for _, kind := range []string{"inline", "catalog", "execution-sidecar"} {
		t.Run(kind, func(t *testing.T) {
			q, clock := fixture(t)
			lock := filepath.Join(t.TempDir(), "requirements.txt")
			want := []byte("example==1.0\n")
			if err := os.WriteFile(lock, want, 0600); err != nil {
				t.Fatal(err)
			}
			r, err := snapshotEnvironment(EnvironmentRecipe{LockFile: lock, Python: "3.12"}, "fake")
			if err != nil {
				t.Fatal(err)
			}
			if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"fake": r}}}); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.SubmissionKey = "recipe"
			s.Environment = Environment{Name: "tools"}
			switch kind {
			case "inline":
				s.Environment = Environment{LockFile: lock, Python: "3.12", Platforms: []string{"fake"}}
			case "execution-sidecar":
				s.Environment = Environment{}
				s.Run = RunSpec{}
				s.Execution = &ExecutionSpec{Main: ExecutionMember{Command: []string{"true"}, Environment: "tools"}, Sidecars: []NativeSidecar{{Name: "helper", Command: []string{"true"}, Environment: "tools"}}}
			}
			j := submit(t, q, s)
			ref := q.s.Jobs[j.ID].EnvironmentRecipes["fake"].FilesRef
			if ref == nil {
				t.Fatal("submitted recipe has no blob reference")
			}
			for _, key := range []string{"jobs/" + j.ID, "submissions/" + submissionDigest(s.Project, s.SubmissionKey)} {
				var data []byte
				if err := q.db.QueryRow("SELECT data FROM queue_state WHERE key=?", key).Scan(&data); err != nil {
					t.Fatal(err)
				}
				var stored Job
				if key == "jobs/"+j.ID {
					err = json.Unmarshal(data, &stored)
				} else {
					var answer submissionRecord
					err = json.Unmarshal(data, &answer)
					if err != nil || answer.Job == nil {
						t.Fatal("missing keyed recipe job", err)
					}
					stored = *answer.Job
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, recipes := range append([]map[string]EnvironmentRecipe{stored.EnvironmentRecipes}, stored.SidecarEnvironmentRecipes["helper"]) {
					for _, recipe := range recipes {
						if len(recipe.Files) != 0 || recipe.FilesRef == nil || *recipe.FilesRef != *ref {
							t.Fatalf("embedded or missing recipe in %s: %+v", key, recipe)
						}
					}
				}
			}
			// The catalog and delivered answer still own their original bytes.
			if !reflect.DeepEqual(j.EnvironmentRecipes["fake"].Files, r.Files) || !reflect.DeepEqual(q.environments.Environments["tools"]["fake"].Files, r.Files) {
				t.Fatal("freezing mutated catalog or response bytes")
			}
			m := machine("builder", "fake")
			m.EnvironmentBuilder = true
			reply := check(t, q, m, nil)
			if len(reply.EnvironmentPlan) != 1 || !reflect.DeepEqual(reply.EnvironmentPlan[0].Files, r.Files) {
				t.Fatal("worker preparation lost recipe bytes", reply.EnvironmentPlan)
			}
			m.Environments = []string{r.Key}
			reply = check(t, q, m, nil)
			if reply.Job == nil || !reflect.DeepEqual(reply.Job.EnvironmentRecipes["fake"].Files, r.Files) {
				t.Fatal("worker assignment lost recipe bytes")
			}
			if kind == "execution-sidecar" && !reflect.DeepEqual(reply.Job.SidecarEnvironmentRecipes["helper"]["fake"].Files, r.Files) {
				t.Fatal("worker sidecar lost recipe bytes")
			}
			entries, err := os.ReadDir(filepath.Join(q.files.root, "blobs"))
			if err != nil || len(entries) != 1 {
				t.Fatalf("recipe not stored once: %d blobs, %v", len(entries), err)
			}
			// The same bytes are eligible for reclamation as an expired job's
			// input, but the submitted recipe retains independent custody.
			if err := q.update(func() error {
				finished := clock.Now()
				q.s.Jobs["expired-input"] = &Job{ID: "expired-input", State: "succeeded", FinishedAt: &finished, Spec: Spec{Files: FileManifest{"old-input": *ref}}}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(DefaultFileRetention + time.Nanosecond)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if err := q.files.verify(context.Background(), *ref); err != nil {
				t.Fatal("sweep collected referenced recipe", err)
			}
			if !q.s.Jobs["expired-input"].FilesExpired {
				t.Fatal("sweep did not expire the old input")
			}
			before := q.persisted
			if err := q.migrateBundleFiles(); err != nil || !reflect.DeepEqual(before, q.persisted) {
				t.Fatal("startup safety net found new embedded recipe bytes", err)
			}
		})
	}
}

func TestEnvironmentRecipeStorePublicationFence(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	catalog := EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"fake": r}}}
	if err := q.ConfigureEnvironments(catalog); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "tools"
	entered, unblock := gateSubmitVerification(t, q, nil)
	done := startSubmitVerification(q, context.Background(), s)
	submitVerificationWait(t, "recipe publication", entered)
	// Configuration must remain responsive while blob durability is blocked,
	// and its replacement must fence the stale recipe before commit.
	submitVerificationProgress(t, "catalog replacement", func() error { return q.ConfigureEnvironments(catalog) })
	unblock()
	got := submitVerificationWait(t, "submission", done)
	if !errors.Is(got.err, ErrStale) {
		t.Fatalf("catalog replacement did not fence submission: %v", got.err)
	}
	requireNoSubmission(t, q)
}

func TestEnvironmentRecipeStoreLockSnapshotOutsideQueue(t *testing.T) {
	q, _ := fixture(t)
	lock := filepath.Join(t.TempDir(), "requirements.txt")
	if err := syscall.Mkfifo(lock, 0600); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment = Environment{LockFile: lock, Python: "3.12", Platforms: []string{"fake"}}
	// Opening the write end proves the snapshot reader reached the FIFO. Keep
	// its read blocked until a separate queue transaction has completed.
	writer := make(chan *os.File, 1)
	go func() {
		f, _ := os.OpenFile(lock, os.O_WRONLY, 0)
		writer <- f
	}()
	done := startSubmitVerification(q, context.Background(), s)
	f := submitVerificationWait(t, "snapshot read", writer)
	if f == nil {
		t.Fatal("could not open snapshot writer")
	}
	defer f.Close()
	submitVerificationProgress(t, "queue transaction during lock-file read", func() error {
		_, err := q.CancelSubmission("test", "unrelated")
		return err
	})
	if _, err := f.Write([]byte("example==1.0\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := submitVerificationWait(t, "snapshot submission", done); got.err != nil {
		t.Fatal(got.err)
	}
}
