package myce

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	q, clock := fixture(t)
	m := machine("writer", "fake")
	check(t, q, m, nil)
	initial := q.histories.contacts[m.ID].Seen
	for range 3 {
		clock.Advance(time.Second)
		check(t, q, m, nil)
	}
	if got := q.histories.contacts[m.ID].Seen; !got.Equal(initial) || !q.s.Machines[m.ID].LastSeen.Equal(clock.Now()) {
		t.Fatal("steady check-ins did not keep durable and live contact distinct")
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
	return batch.applyRows(tx, nil)
}
