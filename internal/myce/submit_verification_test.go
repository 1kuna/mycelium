package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func submitVerificationInput(t *testing.T, q *Queue) Spec {
	t.Helper()
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "input")
	data := []byte("small verified input")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.SubmissionKey = "verified-input"
	s.Files = FileManifest{"input": ref}
	return s
}

type submitVerificationResult struct {
	job *Job
	err error
}

func startSubmitVerification(q *Queue, ctx context.Context, s Spec) <-chan submitVerificationResult {
	done := make(chan submitVerificationResult, 1)
	go func() {
		j, err := q.SubmitContext(ctx, s)
		done <- submitVerificationResult{j, err}
	}()
	return done
}

func requireNoSubmission(t *testing.T, q *Queue) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.s.Jobs) != 0 || len(q.s.Submissions) != 0 {
		t.Fatalf("uncommitted submission retained job/key: %d/%d", len(q.s.Jobs), len(q.s.Submissions))
	}
}

// Gate the actual durability effect after hashing, rather than substituting a
// submission controller or relying on file size and scheduler timing.
func gateSubmitVerification(t *testing.T, q *Queue, barrierError error) (<-chan struct{}, func()) {
	t.Helper()
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, enterOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	syncDir := q.files.syncDir
	q.files.syncDir = func(path string) error {
		enterOnce.Do(func() {
			close(entered)
			<-release
			close(exited)
		})
		if barrierError != nil {
			return barrierError
		}
		return syncDir(path)
	}
	t.Cleanup(func() {
		unblock()
		select {
		case <-entered:
			submitVerificationWait(t, "durability effect cleanup", exited)
		default:
		}
	})
	return entered, unblock
}

func submitVerificationWait[T any](t *testing.T, name string, done <-chan T) T {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatalf("%s blocked behind input verification", name)
		var zero T
		return zero
	}
}

func submitVerificationProgress(t *testing.T, name string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	if err := submitVerificationWait(t, name, done); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func TestSubmitVerificationDoesNotBlockQueue(t *testing.T) {
	q, clock := fixture(t)
	s := submitVerificationInput(t, q)
	m := machine("other", "fake")
	m.HostID = "other-host"
	check(t, q, m, nil)
	other := spec("experiment")
	other.AllowedMachines = []string{m.ID}
	submit(t, q, other)
	active := check(t, q, m, nil).Job
	if active == nil {
		t.Fatal("missing other-host assignment")
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(server.Close)
	entered, unblock := gateSubmitVerification(t, q, nil)
	done := make(chan error, 1)
	var submitted Job
	go func() {
		done <- (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/jobs", s, &submitted)
	}()
	submitVerificationWait(t, "input durability barrier", entered)
	submitVerificationProgress(t, "other-host check-in", func() error {
		_, err := q.Checkin(Checkin{Machine: machine("third", "fake")})
		return err
	})
	submitVerificationProgress(t, "cancellation", func() error { return q.Cancel(active.ID) })
	submitVerificationProgress(t, "other-host reconciliation", func() error {
		exchange := closeExchange(m.HostID, clock)
		exchange.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
		ack, err := q.ReconcileHost(exchange)
		if err == nil && len(ack.Pending) != 0 {
			return fmt.Errorf("session discharge remains pending: %v", ack.Pending)
		}
		return err
	})
	unblock()
	if err := submitVerificationWait(t, "HTTP submission", done); err != nil {
		t.Fatal(err)
	}
	if submitted.ID == "" || submitted.Spec.Files["input"] != s.Files["input"] {
		t.Fatal("verified submission was not committed", submitted)
	}
	if got := job(t, q, active.ID); got.State != "cancelled" || !got.ReleaseConfirmed {
		t.Fatal("other-host cancellation/reconciliation did not settle", got)
	}
}

func TestSubmitVerificationCancellationAndRecheck(t *testing.T) {
	t.Run("request-cancel", func(t *testing.T) {
		q, _ := fixture(t)
		s := submitVerificationInput(t, q)
		entered, unblock := gateSubmitVerification(t, q, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		body, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(body)).WithContext(ctx)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		done := make(chan struct{})
		go func() { API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r); close(done) }()
		submitVerificationWait(t, "durability barrier", entered)
		cancel()
		// The handler must finish BEFORE releasing the non-cancellable syscall.
		submitVerificationWait(t, "cancelled HTTP request", done)
		if w.Code < http.StatusBadRequest {
			t.Fatal("cancelled request accepted", w.Code)
		}
		requireNoSubmission(t, q)
		// A second caller waiting for the occupied I/O slot is cancellable too.
		waiting, stopWaiting := context.WithCancel(context.Background())
		second := startSubmitVerification(q, waiting, s)
		stopWaiting()
		if got := submitVerificationWait(t, "cancelled slot acquisition", second); !errors.Is(got.err, context.Canceled) {
			t.Fatal(got)
		}
		unblock()
		// Later completion of the old I/O can never create its job.
		requireNoSubmission(t, q)
	})
	for _, deadline := range []string{"request-deadline", "default-bound"} {
		t.Run(deadline, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q, _ := fixture(t)
				s := submitVerificationInput(t, q)
				entered, unblock := gateSubmitVerification(t, q, nil)
				ctx := context.Background()
				budget := submitVerificationTimeout
				if deadline == "request-deadline" {
					var cancel context.CancelFunc
					budget = time.Second
					ctx, cancel = context.WithTimeout(ctx, budget)
					defer cancel()
				}
				start := time.Now()
				done := startSubmitVerification(q, ctx, s)
				submitVerificationWait(t, "durability barrier", entered)
				// synctest advances to the actual context deadline only after all
				// actors block on effects; no wall sleeps or large fixture required.
				got := <-done
				if !errors.Is(got.err, context.DeadlineExceeded) || time.Since(start) != budget {
					t.Fatal("request deadline not preserved", got, time.Since(start), budget)
				}
				requireNoSubmission(t, q)
				unblock()
				synctest.Wait()
				requireNoSubmission(t, q)
			})
		})
	}
	t.Run("http-request-deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q, _ := fixture(t)
			s := submitVerificationInput(t, q)
			entered, unblock := gateSubmitVerification(t, q, nil)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			body, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(body)).WithContext(ctx)
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			done := make(chan struct{})
			start := time.Now()
			go func() { API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r); close(done) }()
			submitVerificationWait(t, "durability barrier", entered)
			<-done
			if w.Code < http.StatusBadRequest || time.Since(start) != time.Second {
				t.Fatal("HTTP request deadline not propagated", w.Code, time.Since(start))
			}
			requireNoSubmission(t, q)
			unblock()
			synctest.Wait()
			requireNoSubmission(t, q)
		})
	})
	for _, change := range []string{"client-inputs", "file-store", "checkpoint-mode", "placement", "host-close", "queue-close"} {
		t.Run(change, func(t *testing.T) {
			q, clock := fixture(t)
			s := submitVerificationInput(t, q)
			s.AllowedMachines = []string{"target"}
			s.Run.Env = map[string]string{"VALUE": "original"}
			original := cloneJob(&Job{Spec: s}).Spec
			m := machine("target", "fake")
			m.HostID = "target-host"
			check(t, q, m, nil)
			entered, unblock := gateSubmitVerification(t, q, nil)
			done := startSubmitVerification(q, context.Background(), s)
			submitVerificationWait(t, "durability barrier", entered)
			switch change {
			case "client-inputs":
				// Mutate only after the effect proves the snapshot is owned.
				s.Files["input"] = FileRef{SHA256: "unverified", SizeBytes: 1}
				s.Run.Command[0], s.Run.Env["VALUE"], s.AllowedMachines[0] = "changed", "changed", "changed"
			case "file-store", "checkpoint-mode":
				root := t.TempDir()
				checkpoints := true
				if change == "checkpoint-mode" {
					root, checkpoints = q.files.root, false
				}
				if err := q.ConfigureFileStore(root, 1<<20, checkpoints); err != nil {
					t.Fatal(err)
				}
			case "placement":
				if err := q.update(func() error {
					q.s.Policies = map[string]ProjectPolicy{s.Project: {AllowedMachines: []string{"elsewhere"}}}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "host-close":
				exchange := closeExchange(m.HostID, clock)
				exchange.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
				if _, err := q.ReconcileHost(exchange); err != nil {
					t.Fatal(err)
				}
			case "queue-close":
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
			}
			unblock()
			got := submitVerificationWait(t, "submission recheck", done)
			if change != "client-inputs" && change != "host-close" {
				if got.err == nil {
					t.Fatal("changed custody/placement was accepted", got)
				}
				requireNoSubmission(t, q)
				return
			}
			if got.err != nil || got.job.ID == "" || got.job.Spec.Files["input"] != original.Files["input"] || got.job.Spec.Run.Env["VALUE"] != "original" || got.job.Spec.Run.Command[0] != original.Run.Command[0] {
				t.Fatal("submission snapshot changed", got)
			}
			if replay := submit(t, q, original); replay.ID != got.job.ID {
				t.Fatal("snapshot fingerprint changed", replay)
			}
			if change == "host-close" {
				if reply := check(t, q, m, nil); reply.Job != nil || !reply.Leave {
					t.Fatal("closed host admitted verified submission", reply)
				}
				if j := job(t, q, got.job.ID); j.Attempt != 0 || j.State != "queued" {
					t.Fatal("close defeated by in-flight submit", j)
				}
			}
		})
	}
}

func TestSubmitVerificationKeyAtomicity(t *testing.T) {
	t.Run("concurrent-identical", func(t *testing.T) {
		q, _ := fixture(t)
		s := submitVerificationInput(t, q)
		entered, unblock := gateSubmitVerification(t, q, nil)
		const count = 12
		pending := make([]<-chan submitVerificationResult, count)
		for i := range pending {
			pending[i] = startSubmitVerification(q, context.Background(), s)
		}
		submitVerificationWait(t, "durability barrier", entered)
		unblock()
		id := ""
		for _, done := range pending {
			got := submitVerificationWait(t, "concurrent submit", done)
			if got.err != nil || got.job.ID == "" || id != "" && got.job.ID != id {
				t.Fatal("concurrent key duplicated or failed", got)
			}
			id = got.job.ID
		}
		if len(q.s.Jobs) != 1 || len(q.s.Submissions) != 1 {
			t.Fatal("key/job creation was not atomic")
		}
		ack, err := q.CancelSubmission(s.Project, s.SubmissionKey)
		if err != nil || ack.JobID == nil || *ack.JobID != id || !ack.WillNotRun {
			t.Fatal("submit-first cancellation lost binding", ack, err)
		}
		if replay := submit(t, q, s); replay.ID != id || replay.State != "cancelled" {
			t.Fatal("cancelled job resurrected", replay)
		}
	})
	for _, outcome := range []string{"cancel-first", "cancel-before-error", "expiry", "duplicate-conflict", "write-failure", "cancel-write-failure"} {
		t.Run(outcome, func(t *testing.T) {
			q, clock := fixture(t)
			s := submitVerificationInput(t, q)
			var barrierError error
			if outcome == "cancel-before-error" {
				barrierError = errors.New("injected directory sync failure")
			}
			entered, unblock := gateSubmitVerification(t, q, barrierError)
			done := startSubmitVerification(q, context.Background(), s)
			submitVerificationWait(t, "durability barrier", entered)
			var reserved Cancellation
			var duplicate *Job
			switch outcome {
			case "cancel-first", "cancel-before-error", "expiry":
				var err error
				reserved, err = q.CancelSubmission(s.Project, s.SubmissionKey)
				if err != nil || reserved.State != "absent" || !reserved.WillNotRun {
					t.Fatal(reserved, err)
				}
				if outcome == "expiry" {
					clock.Advance(submissionRetention)
					if err := q.Tick(); err != nil {
						t.Fatal(err)
					}
				}
			case "duplicate-conflict":
				changed := s
				changed.Files = nil
				duplicate = submit(t, q, changed)
			case "write-failure", "cancel-write-failure":
				// The empty fixture has no queue row yet. Reject INSERT so the
				// upsert fails on both its first-write and conflict/update paths.
				if _, err := q.db.Exec(`CREATE TRIGGER reject_queue BEFORE INSERT ON queue BEGIN SELECT RAISE(ABORT,'test write failure'); END`); err != nil {
					t.Fatal(err)
				}
				if outcome == "cancel-write-failure" {
					if _, err := q.CancelSubmission(s.Project, s.SubmissionKey); err == nil {
						t.Fatal("failed cancellation acknowledged")
					}
					requireNoSubmission(t, q)
					if _, err := q.db.Exec(`DROP TRIGGER reject_queue`); err != nil {
						t.Fatal(err)
					}
				}
			}
			unblock()
			got := submitVerificationWait(t, "key recheck", done)
			switch outcome {
			case "expiry":
				submissionError(t, got.err, 410, "submission_key_expired")
				ack, err := q.CancelSubmission(s.Project, s.SubmissionKey)
				if err != nil || ack.State != "unknown" || ack.Release != nil || len(q.s.Jobs) != 0 {
					t.Fatal("expired reservation created work", ack, err)
				}
			case "duplicate-conflict":
				submissionError(t, got.err, 409, "submission_key_conflict")
				if len(q.s.Jobs) != 1 || q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)].JobID != duplicate.ID {
					t.Fatal("concurrent key binding overwritten")
				}
			case "write-failure":
				if got.err == nil {
					t.Fatal("failed SQLite commit accepted")
				}
				requireNoSubmission(t, q)
				if _, err := q.db.Exec(`DROP TRIGGER reject_queue`); err != nil {
					t.Fatal(err)
				}
				submit(t, q, s)
			case "cancel-write-failure":
				if got.err != nil || got.job.State != "queued" || got.job.CancelledAt != nil || len(q.s.Jobs) != 1 || len(q.s.Submissions) != 1 {
					t.Fatal("unacknowledged cancellation leaked", got)
				}
			default:
				if got.err != nil || got.job.State != "cancelled" || got.job.Attempt != 0 || got.job.CancelledAt == nil || !got.job.CancelledAt.Equal(*reserved.AcknowledgedAt) || len(q.s.Jobs) != 1 || len(q.s.Submissions) != 1 {
					t.Fatal("cancel-first ordering lost", got)
				}
				changed := s
				changed.Run.Command, changed.Files = nil, FileManifest{"unavailable": {}}
				if replay := submit(t, q, changed); replay.ID != got.job.ID || replay.State != "cancelled" {
					t.Fatal("reservation replay lost precedence", replay)
				}
			}
		})
	}
	t.Run("replay-bypasses-blocked-verifier", func(t *testing.T) {
		q, _ := fixture(t)
		s := submitVerificationInput(t, q)
		first := submit(t, q, s)
		entered, unblock := gateSubmitVerification(t, q, nil)
		other := s
		other.SubmissionKey = "other-key"
		done := startSubmitVerification(q, context.Background(), other)
		submitVerificationWait(t, "durability barrier", entered)
		replay := submitVerificationWait(t, "key replay", startSubmitVerification(q, context.Background(), s))
		if replay.err != nil || replay.job.ID != first.ID {
			t.Fatal("retained key entered blocked verification", replay)
		}
		changed := s
		changed.Name = "changed"
		conflict := submitVerificationWait(t, "key conflict", startSubmitVerification(q, context.Background(), changed))
		submissionError(t, conflict.err, 409, "submission_key_conflict")
		unblock()
		if got := submitVerificationWait(t, "other submit", done); got.err != nil {
			t.Fatal(got)
		}
	})
	t.Run("corrupt-input", func(t *testing.T) {
		q, _ := fixture(t)
		s := submitVerificationInput(t, q)
		if err := os.WriteFile(q.files.blob(s.Files["input"]), []byte("corrupt"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Submit(s); err == nil {
			t.Fatal("corrupt input accepted")
		}
		requireNoSubmission(t, q)
	})
}
