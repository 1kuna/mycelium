package myce

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestQueueSQLiteWriteBlocksOtherWritersButServesCommittedRead(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint("rollback=", rollback), func(t *testing.T) {
			q, clock := fixture(t)
			q.inLockCommit = rollback
			m := machine("writer", "fake")
			check(t, q, m, nil)
			j := submit(t, q, spec("experiment"))
			drainAdmissionWait(t, q)
			entered, resume := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			barrier := sync.OnceFunc(func() {
				close(entered)
				<-resume
			})
			q.observeQueueRow = func(query string, args []any) {
				if query == upsertQueueState && args[0] == "jobs/"+j.ID {
					barrier()
				}
			}
			var reply Reply
			checkin := make(chan error, 1)
			go func() {
				var err error
				reply, err = q.Checkin(Checkin{Machine: m, period: time.Second})
				checkin <- err
			}()
			awaitQueueWrite(t, entered)
			if free := q.mu.mu.TryLock(); free {
				q.mu.mu.Unlock()
				if rollback {
					t.Fatal("rollback released the physical lock")
				}
			} else if !rollback {
				t.Fatal("SQLite was called under the physical lock")
			}
			if q.mu.TryRLock() {
				q.mu.RUnlock()
				t.Fatal("a mutable reader entered a speculative write")
			}
			before := make(chan error, 1)
			go func() {
				got, err := q.jobSnapshot(j.ID)
				if err == nil && (got.State != "queued" || got.Attempt != 0) {
					err = fmt.Errorf("uncommitted assignment visible: %s attempt %d", got.State, got.Attempt)
				}
				before <- err
			}()
			if err := awaitQueueWrite(t, before); err != nil {
				t.Fatal(err)
			}
			cancelled := make(chan error, 1)
			go func() { cancelled <- q.Cancel(j.ID) }()
			await(t, func() bool { return q.mu.progress().Waiting > 0 })
			select {
			case err := <-checkin:
				t.Fatalf("check-in replied before SQLite commit: %v", err)
			case err := <-cancelled:
				t.Fatalf("second writer passed the first: %v", err)
			default:
			}
			clock.Advance(time.Second)
			unblock()
			if err := awaitQueueWrite(t, checkin); err != nil || reply.Job == nil || reply.Job.Attempt != 1 {
				t.Fatalf("first writer lost its assignment: %+v %v", reply.Job, err)
			}
			if err := awaitQueueWrite(t, cancelled); err != nil {
				t.Fatal(err)
			}
			requireState(t, q, j.ID, "stopping")
			q = reopenQueue(t, q, clock)
			got := job(t, q, j.ID)
			if got.State != "stopping" || got.Attempt != 1 || got.Session != m.Session {
				t.Fatalf("writer order did not survive reopen: %+v", got)
			}
		})
	}
}

func TestQueueSQLiteCommitFailureDoesNotPublishAssignment(t *testing.T) {
	q, _ := fixture(t)
	m := machine("writer", "fake")
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	drainAdmissionWait(t, q)
	// Accept all row statements and fail at COMMIT. The SQL connection must
	// also be reusable after database/sql ends the failed transaction.
	if _, err := q.db.Exec(`PRAGMA foreign_keys=ON;
CREATE TABLE commit_parent(id INTEGER PRIMARY KEY);
CREATE TABLE commit_child(id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TEMP TRIGGER fail_assignment_commit BEFORE INSERT ON queue_state
WHEN NEW.key = '` + "jobs/" + j.ID + `'
BEGIN INSERT INTO commit_child VALUES(1); END;`); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := (Client{URL: server.URL, Token: "test"}).WithoutRetries()
	var reply Reply
	checkin := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}
	err := client.Call(context.Background(), "POST", "/api/workers/checkin", checkin, &reply)
	var api *HTTPError
	if !errors.As(err, &api) || api.Status != http.StatusServiceUnavailable || !retryTransient(err) ||
		reply.Job != nil || len(reply.Attempts) > 0 {
		t.Fatalf("failed commit released a reply or a permanent error: %+v %v", reply, err)
	}
	if strings.Contains(err.Error(), "commit_child") {
		t.Fatal("SQLite details reached the worker")
	}
	requireState(t, q, j.ID, "queued")
	if _, err := q.db.Exec("DROP TRIGGER fail_assignment_commit"); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(context.Background(), "POST", "/api/workers/checkin", checkin, &reply); err != nil ||
		len(reply.Attempts) != 1 || reply.Attempts[0].Job == nil || reply.Attempts[0].Job.Attempt != 1 {
		t.Fatalf("retry lost the uncommitted attempt number: %+v %v", reply, err)
	}
}

func TestQueueSQLiteContactCheckpoint(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint("rollback=", rollback), func(t *testing.T) {
			q, clock := fixture(t)
			q.inLockCommit = rollback
			m := machine("writer", "fake")
			check(t, q, m, nil)
			initial := q.histories.contacts[m.ID].Seen
			for range 3 {
				clock.Advance(time.Second)
				check(t, q, m, nil)
			}
			want := initial
			if rollback {
				want = clock.Now()
			}
			if got := q.histories.contacts[m.ID].Seen; !got.Equal(want) || !q.s.Machines[m.ID].LastSeen.Equal(clock.Now()) {
				t.Fatalf("steady check-ins saved contact at %v, want %v", got, want)
			}
			clock.Advance(RestartGrace)
			check(t, q, m, nil)
			if got := q.histories.contacts[m.ID].Seen; !got.Equal(clock.Now()) {
				t.Fatal("contact history missed the restart-window checkpoint")
			}
			clock.Advance(time.Second)
			check(t, q, m, nil)
			q = reopenQueue(t, q, clock)
			if !q.unreported[m.ID] || !q.s.Machines[m.ID].LastSeen.Equal(clock.Now()) {
				t.Fatal("clean close did not preserve the latest contact, or made it live after restart")
			}
		})
	}
}

func openContactCrashQueue(t *testing.T, dir string, now time.Time, rollback bool) (*Queue, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: now}
	q, err := Open(filepath.Join(dir, "queue.db"), filepath.Join(dir, "checkpoints"), clock, WithQueueCommitUnderLock(rollback))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	return q, clock
}

func killContactCrashQueue(t *testing.T, receipt string) {
	t.Helper()
	fmt.Println("contact crash receipt: " + receipt)
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {}
}

func runContactCrash(t *testing.T, testName, dir, phase string, rollback bool) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+testName+"$")
	child.Env = []string{
		"MYCE_TEST_CONTACT_CRASH=" + phase,
		"MYCE_TEST_CONTACT_CRASH_DIR=" + dir,
		fmt.Sprintf("MYCE_TEST_CONTACT_CRASH_ROLLBACK=%t", rollback),
	}
	out, err := child.CombinedOutput()
	var exit *exec.ExitError
	_, receipt, found := strings.Cut(string(out), "contact crash receipt: ")
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL || !found {
		t.Fatalf("fixture missed the crash after accepted contact: %v\n%s", err, out)
	}
	value, _, _ := strings.Cut(receipt, "\n")
	return value
}

func TestQueueSQLiteIdleSessionSurvivesUncleanRestart(t *testing.T) {
	const period = 4 * time.Minute
	start := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	worker, peer := machine("worker", "fake"), machine("peer", "fake")
	if phase := os.Getenv("MYCE_TEST_CONTACT_CRASH"); phase != "" {
		q, clock := openContactCrashQueue(t, os.Getenv("MYCE_TEST_CONTACT_CRASH_DIR"), start, os.Getenv("MYCE_TEST_CONTACT_CRASH_ROLLBACK") == "true")
		if phase != "join" {
			check(t, q, worker, nil) // Save short-cadence evidence for the existing worker.
		}
		check(t, q, peer, nil)
		clock.Advance(time.Second)
		report := Checkin{Machine: worker, period: period}
		if phase == "replace" {
			report.Fresh, report.PreviousSession = true, worker.Session
			report.Machine.Session += "-replacement"
		}
		if reply, err := q.Checkin(report); err != nil || reply.Stop || reply.Job != nil {
			t.Fatalf("fixture did not accept an idle session: %+v, %v", reply, err)
		}
		killContactCrashQueue(t, report.Machine.Session)
	}
	for _, rollback := range []bool{false, true} {
		for _, phase := range []string{"join", "increase", "replace"} {
			t.Run(fmt.Sprintf("rollback=%t/%s", rollback, phase), func(t *testing.T) {
				dir := t.TempDir()
				session := runContactCrash(t, "TestQueueSQLiteIdleSessionSurvivesUncleanRestart", dir, phase, rollback)
				q, clock := openContactCrashQueue(t, dir, start.Add(2*time.Second), rollback)
				m := q.s.Machines[worker.ID]
				if m == nil || m.Session != session || m.Gone || !q.unreported[worker.ID] {
					t.Fatal("reopen did not restore the accepted session as unreported")
				}
				evidence := q.s.ContactEvidence[worker.ID]
				// The peer spends the restart service window while the slow worker
				// has yet to reach its first scheduled post-restart heartbeat.
				q.mu.control.begin()
				defer q.mu.control.end(func() {})
				check(t, q, peer, nil)
				advanceReportingPeer(t, q, clock, peer, RestartGrace)
				if q.s.Machines[worker.ID].Gone || !q.unreported[worker.ID] {
					t.Fatalf("idle session expired before its next heartbeat: evidence=%+v", evidence)
				}
				if evidence.Session != session || evidence.ArrivalAllowance < 2*period || !evidence.Seen.Equal(start.Add(time.Second)) {
					t.Fatalf("idle session reopened without its latest recovery evidence: %+v", evidence)
				}
				advanceReportingPeer(t, q, clock, peer, evidence.Seen.Add(period).Sub(clock.Now()))
				worker := worker
				worker.Session = session
				if reply, err := q.Checkin(Checkin{Machine: worker, period: period}); err != nil || reply.Stop || q.unreported[worker.ID] {
					t.Fatalf("idle session failed to reconcile: stop=%t unreported=%t, %v", reply.Stop, q.unreported[worker.ID], err)
				}
			})
		}
	}
}

func TestQueueSQLiteCustodySurvivesUncleanRestart(t *testing.T) {
	const period = 4 * time.Minute
	start := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	worker, peer := machine("worker", "fake"), machine("peer", "fake")
	if phase := os.Getenv("MYCE_TEST_CONTACT_CRASH"); phase != "" {
		q, clock := openContactCrashQueue(t, os.Getenv("MYCE_TEST_CONTACT_CRASH_DIR"), start, os.Getenv("MYCE_TEST_CONTACT_CRASH_ROLLBACK") == "true")
		check(t, q, peer, nil) // Set the periodic checkpoint before the worker arrives.
		clock.Advance(time.Second)
		report := Checkin{Machine: worker, period: period}
		if phase == "growing-allowance" {
			report.period = time.Second
		}
		s := spec("production")
		s.AllowedMachines = []string{worker.ID}
		submitted := submit(t, q, s)
		reply, err := q.Checkin(report)
		if err != nil || reply.Job == nil || reply.Job.ID != submitted.ID || reply.Job.Attempt != 1 {
			t.Fatalf("fixture did not assign the production attempt: %+v, %v", reply, err)
		}
		if phase == "new-custody" {
			// Give the start its own accepted contact to checkpoint. Replaying
			// the assignment leaves the job unstarted and the cadence unchanged.
			clock.Advance(time.Second)
			if next, err := q.Checkin(report); err != nil || next.Job == nil || next.Job.ID != reply.Job.ID || next.Job.StartedAt != nil {
				t.Fatalf("fixture did not replay the unstarted assignment: %+v, %v", next, err)
			}
		}
		if phase != "assignment" {
			if err := q.Started(result(reply.Job, true)); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "growing-allowance" {
			clock.Advance(time.Second)
			report.period = period
			report.Running = &Execution{ID: reply.Job.ID, Attempt: reply.Job.Attempt}
			report.Machine.FreeMemoryGB = 0
			if _, err := q.Checkin(report); err != nil {
				t.Fatal(err)
			}
		}
		killContactCrashQueue(t, reply.Job.ID)
	}
	for _, rollback := range []bool{false, true} {
		for _, phase := range []string{"assignment", "new-custody", "growing-allowance"} {
			t.Run(fmt.Sprintf("rollback=%t/%s", rollback, phase), func(t *testing.T) {
				dir := t.TempDir()
				id := runContactCrash(t, "TestQueueSQLiteCustodySurvivesUncleanRestart", dir, phase, rollback)
				q, clock := openContactCrashQueue(t, dir, start.Add(3*time.Second), rollback)
				before := job(t, q, id)
				if before.State != "running" || (before.StartedAt == nil) != (phase == "assignment") || !q.unreported[worker.ID] {
					t.Fatalf("reopen did not restore the unreported attempt: state=%s started=%v unreported=%t",
						before.State, before.StartedAt, q.unreported[worker.ID])
				}
				evidence := q.s.ContactEvidence[worker.ID]
				if phase == "new-custody" && !evidence.Seen.Equal(start.Add(2*time.Second)) {
					t.Fatalf("start did not checkpoint the contact after assignment: %+v", evidence)
				}
				if phase == "assignment" {
					if evidence.Session != worker.Session || evidence.ArrivalAllowance < 2*period {
						t.Fatalf("assignment committed without recovery evidence: %+v", evidence)
					}
					return
				}
				// A busy queue can accrue service through the reporting peer
				// while making no idle control opportunities.
				q.mu.control.begin()
				defer q.mu.control.end(func() {})
				check(t, q, peer, nil)
				advanceReportingPeer(t, q, clock, peer, RestartGrace)
				got := job(t, q, id)
				if got.State != "running" || got.Attempt != before.Attempt || got.Losses != 0 || got.InfrastructureRetries != 0 ||
					evidence.Session != worker.Session || evidence.ArrivalAllowance < 2*period {
					t.Fatalf("restart lost production custody before the next heartbeat: state=%s attempt=%d losses=%d retries=%d evidence=%+v",
						got.State, got.Attempt, got.Losses, got.InfrastructureRetries, evidence)
				}
				advanceReportingPeer(t, q, clock, peer, period-RestartGrace)
				reply, err := q.Checkin(Checkin{Machine: worker, period: period, Running: &Execution{ID: id, Attempt: before.Attempt}})
				if err != nil || reply.Stop || q.unreported[worker.ID] {
					t.Fatalf("worker could not reconcile the same attempt: stop=%t unreported=%t, %v", reply.Stop, q.unreported[worker.ID], err)
				}
				got = job(t, q, id)
				if got.State != "running" || got.Attempt != before.Attempt || got.Losses != 0 || got.InfrastructureRetries != 0 {
					t.Fatalf("reconciliation changed production custody: state=%s attempt=%d losses=%d retries=%d",
						got.State, got.Attempt, got.Losses, got.InfrastructureRetries)
				}
			})
		}
	}
}

func TestQueueSQLiteCancelledHistorySweepSkipsWrite(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint("rollback=", rollback), func(t *testing.T) {
			q, clock := fixture(t)
			q.inLockCommit = rollback
			m := machine("worker", "fake")
			if err := q.ConfigureLogRetention(0); err != nil {
				t.Fatal(err)
			}
			j := submit(t, q, spec("experiment"))
			if err := q.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			clock.Advance(jobRecordRetention + time.Second)
			check(t, q, m, nil)
			clock.Advance(time.Second)
			check(t, q, m, nil) // A fresh report for clean close to flush in batched mode.
			drainAdmissionWait(t, q)

			q.mu.Lock()
			blocked := q.expiryBlocked(j.ID, q.s.Jobs[j.ID], clock.Now())
			q.mu.Unlock()
			if blocked {
				t.Fatal("fixture did not make the completed job eligible for expiry")
			}
			q.initShutdown()
			span := &requestSpan{}
			ctx := context.WithValue(q.shutdownContext, requestSpanKey{}, span)
			span.mu.Lock()
			resumeSweep := sync.OnceFunc(span.mu.Unlock)
			defer resumeSweep()
			sweep := make(chan error, 1)
			releases := q.mu.progress().Releases
			go func() { sweep <- q.pruneJobHistory(ctx) }()
			// Park the sweep immediately after it snapshots the candidate.
			await(t, func() bool { return q.mu.progress().Releases > releases })

			entered, resume := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			hold := sync.OnceFunc(func() {
				close(entered)
				<-resume
			})
			q.mu.Lock()
			q.observeQueueRow = func(query string, args []any) {
				if query == upsertQueueState && strings.HasPrefix(args[0].(string), "jobs/") && args[0] != "jobs/"+j.ID {
					hold()
				}
			}
			q.mu.Unlock()
			writer := make(chan error, 1)
			go func() {
				_, err := q.Submit(spec("experiment"))
				writer <- err
			}()
			awaitQueueWrite(t, entered)
			resumeSweep()
			await(t, func() bool { return q.mu.progress().Waiting > 0 })
			q.BeginShutdown()
			// Only the already-started write may delay storage close.
			select {
			case err := <-sweep:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("cancelled sweep returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("cancelled history sweep still waited for the active writer")
				unblock()
				awaitQueueWrite(t, sweep)
			}
			unblock()
			if err := awaitQueueWrite(t, writer); err != nil {
				t.Fatal(err)
			}
			var summaries int
			// Take a turn after the cancelled lock waiter has drained.
			q.mu.Lock()
			q.mu.Unlock()
			if err := q.db.QueryRow("SELECT count(*) FROM job_summaries WHERE id=?", j.ID).Scan(&summaries); err != nil || summaries != 0 {
				t.Fatalf("cancelled sweep wrote a history summary: count=%d, %v", summaries, err)
			}
			q = reopenQueue(t, q, clock)
			if !q.s.Machines[m.ID].LastSeen.Equal(clock.Now()) {
				t.Fatal("clean close did not checkpoint the latest contact")
			}
		})
	}
}

func awaitQueueWrite[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case x := <-ch:
		return x
	case <-time.After(testWaitTimeout):
		t.Fatal("queue write missed its test barrier")
		var zero T
		return zero
	}
}

func (e *memoryEvidence) persist(tx *sql.Tx) error {
	var batch queueWriteBatch
	if err := e.prepareWrite(&batch); err != nil {
		return err
	}
	return batch.applyRows(context.Background(), tx, nil)
}
