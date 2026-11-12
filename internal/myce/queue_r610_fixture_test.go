package myce

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// MY-42: main must open the previous release's database, and that release must
// still be able to read what main writes back (rollback). testdata/r6.10-queue.sql
// was written by exact r6.10 source; see its generator beside it. Advance both
// files with the release pair, as scripts/upgrade-compat.py does.
func TestQueuePersistenceOpensPreviousReleaseDatabase(t *testing.T) {
	dump, err := os.ReadFile("testdata/r6.10-queue.sql")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = PrepareStateRoots(dir); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(dir, "queue.db")
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(dump)); err != nil {
		t.Fatal(err)
	}
	db.Close()
	clock := &fakeClock{now: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)}
	q, err := Open(database, t.TempDir(), clock)
	if err != nil {
		t.Fatal("main refused the previous release database:", err)
	}
	check := func(q *Queue, reason string) {
		t.Helper()
		for _, state := range []string{"queued", "paused", "running", "succeeded", "failed"} {
			if j := q.s.Jobs["fixture-"+state]; j == nil || j.State != state {
				t.Fatal("lost job or decision", state, j)
			}
		}
		done := q.s.Jobs["fixture-succeeded"]
		if done.FinishedAt == nil || done.Attempt != 3 || done.Session != "fixture-session" || done.ReleaseConfirmed || len(q.s.HostReleases) != 1 {
			t.Fatal("lost terminal timestamp or pending custody", done, q.s.HostReleases)
		}
		if got := q.s.Jobs["fixture-failed"].Reason; got != reason {
			t.Fatalf("split field record changed: %d bytes", len(got))
		}
		if m := q.s.Machines["fixture-device"]; m == nil || m.Session != "fixture-session" {
			t.Fatal("lost per-machine record", m)
		}
		if len(q.s.Submissions) != 1 {
			t.Fatal("lost keyed submission", len(q.s.Submissions))
		}
		for _, r := range q.s.Submissions {
			if j := q.s.Jobs[r.JobID]; j == nil || j.Spec.SubmissionKey != "fixture-key" {
				t.Fatal("keyed answer no longer resolves to its job", r.JobID)
			}
		}
		if len(q.s.Health.Events) == 0 || q.s.Health.Events[0].DeliveryID == "" {
			t.Fatal("lost health delivery evidence")
		}
	}
	check(q, strings.Repeat("retained failure evidence ", 1024))
	// Upgrade: main writes a new decision into the same layout and reopens it.
	if err = q.update(func() error {
		q.touchJob("fixture-failed").Reason = "decided on main"
		q.touchMachine("fixture-device").Leaving = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	defer q.Close()
	check(q, "decided on main")
	if !q.s.Machines["fixture-device"].Leaving {
		t.Fatal("restart lost machine decision")
	}
	// Rollback stays a plain restart only while main keeps the r6.10 marker.
	var marker []byte
	if err = q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err != nil || !bytes.Equal(marker, normalizedQueueMarker) {
		t.Fatalf("main changed the persistence marker to %s; r6.10 rollback would need a conversion", marker)
	}
	// Every release since r6.11 reads shared-value rows, so main converted the
	// settled rows of this database when it opened it.
	shared := func(q *Queue) int {
		t.Helper()
		records, err := readQueueRecords(q.db)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for key, data := range records {
			if strings.HasPrefix(key, "jobs/") && bytes.HasPrefix(data, jobRecordRefsPrefix) {
				n++
			}
		}
		return n
	}
	if shared(q) == 0 {
		t.Fatal("opening with the writer on converted no retained job")
	}
	check(q, "decided on main")
	q = reopenQueue(t, q, clock)
	check(q, "decided on main")
}
