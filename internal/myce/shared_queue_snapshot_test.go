package myce

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// proofCopy is one member's SQLite copy, bound to the authority.
func proofCopy(t *testing.T, a *sharedQueueAuthority) (*sharedQueueMember, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.db")
	db, err := openQueueSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &sharedQueueMember{authority: a, replica: db, timeout: 5 * time.Second}
	if err := m.bindReplica(t.Context()); err != nil {
		t.Fatal(err)
	}
	return m, path
}

// proofCoordinator is a writer on the leader's copy and the compactor that
// reads that copy through its own handle, as a coordinating member does.
func proofCoordinator(t *testing.T, a *sharedQueueAuthority) (*sharedQueueMember, *sharedQueueWriter, *sharedQueueCompactor) {
	t.Helper()
	turn, err := a.acquire(t.Context(), ID(), "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leader, path := proofCopy(t, a)
	source, err := openQueueSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	w := &sharedQueueWriter{authority: a, turn: turn, timeout: 5 * time.Second}
	return leader, w, &sharedQueueCompactor{authority: a, source: source, writer: w}
}

// proofSeed writes 20 KiB of steady state the churn never changes.
func proofSeed(t *testing.T, w *sharedQueueWriter, db *sql.DB) {
	t.Helper()
	var b queueWriteBatch
	for i := range 20 {
		b.add(upsertQueueState, fmt.Sprintf("static/%02d", i), bytes.Repeat([]byte{byte(i)}, 1024))
		b.add("INSERT INTO logs(job,attempt,text) VALUES(?,?,?)", "seed", 1, fmt.Sprintf("seed line %d", i))
	}
	if err := w.apply(context.Background(), db, &b, nil); err != nil {
		t.Fatal(err)
	}
}

// proofChurn rewrites one state row, as running jobs and the global queue
// record do, and adds then removes the newest log line, so AUTOINCREMENT
// state is ahead of every remaining row.
func proofChurn(t *testing.T, w *sharedQueueWriter, db *sql.DB, i int) {
	t.Helper()
	var b queueWriteBatch
	b.add(upsertQueueState, "state", bytes.Repeat([]byte{byte(i)}, 1024))
	b.add("INSERT INTO logs(job,attempt,text) VALUES(?,?,?)", "churn", 1, fmt.Sprintf("line %d", i))
	b.add("DELETE FROM logs WHERE seq=(SELECT max(seq) FROM logs)")
	if err := w.apply(context.Background(), db, &b, nil); err != nil {
		t.Fatal(err)
	}
}

// proofSharedDump renders every shared row, the log AUTOINCREMENT state and
// the cursor, in key order.
func proofSharedDump(t *testing.T, db *sql.DB) string {
	t.Helper()
	names := make([]string, 0, len(sharedQueueTables))
	for name := range sharedQueueTables {
		names = append(names, name)
	}
	slices.Sort(names)
	var out strings.Builder
	for _, name := range names {
		table := sharedQueueTables[name]
		rows, err := db.Query("SELECT " + strings.Join(table.columns, ",") + " FROM " + name + " ORDER BY " + strings.Join(table.keys, ","))
		if err != nil {
			t.Fatal(err)
		}
		values := make([]any, len(table.columns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&out, "%s %#v\n", name, values)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	var logs sql.NullInt64
	if err := db.QueryRow("SELECT seq FROM sqlite_sequence WHERE name='logs'").Scan(&logs); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	fmt.Fprintf(&out, "logs autoincrement %v\n", logs)
	var cursor int64
	err := db.QueryRow("SELECT sequence FROM myce_shared_cursor WHERE id=1").Scan(&cursor)
	if err != nil && !strings.Contains(err.Error(), "no such table") {
		t.Fatal(err)
	}
	if err == nil {
		// A rollback restore drops the cursor; compare the rest.
		fmt.Fprintf(&out, "cursor %d\n", cursor)
	}
	return out.String()
}

type proofLogSize struct{ rows, bytes, snapshots, chunks, floor int64 }

func proofRetained(t *testing.T, a *sharedQueueAuthority) proofLogSize {
	t.Helper()
	var s proofLogSize
	if err := a.db.QueryRow(`SELECT (SELECT count(*) FROM myce_changes_v1),
		(SELECT COALESCE(sum(octet_length(payload)),0)::bigint FROM myce_changes_v1),
		(SELECT count(*) FROM myce_snapshots_v1), (SELECT count(*) FROM myce_snapshot_chunks_v1),
		floor FROM myce_coordinator_v1 WHERE id=1`).Scan(&s.rows, &s.bytes, &s.snapshots, &s.chunks, &s.floor); err != nil {
		t.Fatal(err)
	}
	return s
}

func proofPullHead(t *testing.T, m *sharedQueueMember) error {
	t.Helper()
	turn, err := m.authority.current(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return m.pull(t.Context(), turn)
}

func proofSentinel(t *testing.T, db *sql.DB, write bool) bool {
	t.Helper()
	if write {
		if _, err := db.Exec(upsertQueueState, "test/sentinel", []byte("local only")); err != nil {
			t.Fatal(err)
		}
		return true
	}
	var found bool
	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM queue_state WHERE key='test/sentinel')").Scan(&found); err != nil {
		t.Fatal(err)
	}
	return found
}

// Sustained changes leave about two snapshots' worth of changes, not the
// whole history (#2157).
func TestSharedQueuePostgresChangeLogStaysBounded(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	leader, w, c := proofCoordinator(t, a)
	proofSeed(t, w, leader.replica)
	var peak proofLogSize
	var largest, change int64
	const batches = 600
	for i := range batches {
		before := w.appended.Load()
		proofChurn(t, w, leader.replica, i)
		change = max(change, w.appended.Load()-before)
		if err := c.step(t.Context()); err != nil {
			t.Fatal(err)
		}
		largest = max(largest, c.latest.bytes)
		if s := proofRetained(t, a); s.floor > 0 {
			peak.rows, peak.bytes = max(peak.rows, s.rows), max(peak.bytes, s.bytes)
			peak.snapshots, peak.chunks = max(peak.snapshots, s.snapshots), max(peak.chunks, s.chunks)
		}
	}
	final := proofRetained(t, a)
	if final.floor == 0 || peak.rows == 0 {
		t.Fatalf("nothing was pruned under %d changes: %+v", batches, final)
	}
	// Two generations, each closed at its snapshot's size, plus the change
	// that crossed each threshold.
	if limit := 2*largest + 2*change; peak.bytes > limit {
		t.Fatalf("retained %d change bytes, above two snapshots (%d) plus two changes (%d)", peak.bytes, 2*largest, 2*change)
	}
	if peak.snapshots > 2 {
		t.Fatalf("kept %d snapshots", peak.snapshots)
	}
	if total := w.appended.Load(); total < 5*peak.bytes {
		t.Fatalf("fixture too small to show a bound: %d committed, %d retained", total, peak.bytes)
	}
	// The pruned authority still yields the leader's exact state.
	fresh, _ := proofCopy(t, a)
	if err := proofPullHead(t, fresh); err != nil {
		t.Fatal(err)
	}
	if got, want := proofSharedDump(t, fresh.replica), proofSharedDump(t, leader.replica); got != want {
		t.Fatalf("rebuilt copy differs:\n%s\nwant:\n%s", got, want)
	}
	t.Logf("committed %d changes (%d B); retained at most %d rows (%d B); largest snapshot %d B",
		w.sequence, w.appended.Load(), peak.rows, peak.bytes, largest)
}

// A copy below the floor rebuilds from the snapshot and matches the leader.
// A copy exactly at the floor replays without a rebuild.
func TestSharedQueuePostgresSnapshotRebuildAndFloorReplay(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	leader, w, c := proofCoordinator(t, a)
	behind, _ := proofCopy(t, a)
	fresh, _ := proofCopy(t, a)
	atFloor, _ := proofCopy(t, a)
	proofSeed(t, w, leader.replica)
	for i := range 5 {
		proofChurn(t, w, leader.replica, i)
	}
	if err := proofPullHead(t, behind); err != nil {
		t.Fatal(err)
	}
	var snapshots []int64
	c.snapshotted = func() error {
		snapshots = append(snapshots, c.latest.sequence)
		if len(snapshots) == 2 {
			// The prune about to run raises the floor to the first
			// snapshot. Leave this copy exactly there first.
			if err := atFloor.pull(t.Context(), sharedQueueTurn{head: snapshots[0]}); err != nil {
				return err
			}
			proofSentinel(t, atFloor.replica, true)
		}
		return nil
	}
	for i := 5; len(snapshots) < 2; i++ {
		if i > 500 {
			t.Fatal("no second snapshot")
		}
		proofChurn(t, w, leader.replica, i)
		if err := c.step(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		proofChurn(t, w, leader.replica, 1000+i)
	}
	turn, err := a.current(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if turn.floor != snapshots[0] || atFloor.sequence != turn.floor || behind.sequence >= turn.floor {
		t.Fatalf("floor %d, at-floor copy %d, behind copy %d", turn.floor, atFloor.sequence, behind.sequence)
	}
	if err := atFloor.pull(t.Context(), turn); err != nil {
		t.Fatal(err)
	}
	if !proofSentinel(t, atFloor.replica, false) {
		t.Fatal("the copy at the floor was rebuilt instead of replaying")
	}
	if _, err := atFloor.replica.Exec("DELETE FROM queue_state WHERE key='test/sentinel'"); err != nil {
		t.Fatal(err)
	}
	// End on a snapshot at the head, so the rebuilt copies replay nothing
	// and their log AUTOINCREMENT state comes only from the snapshot.
	if s, err := a.writeSnapshot(t.Context(), c.source); err != nil || s.sequence != w.sequence {
		t.Fatalf("snapshot at head = %+v, %v", s, err)
	}
	proofSentinel(t, behind.replica, true)
	want := proofSharedDump(t, leader.replica)
	for name, m := range map[string]*sharedQueueMember{"behind": behind, "fresh": fresh, "at floor": atFloor} {
		if err := proofPullHead(t, m); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := proofSharedDump(t, m.replica); got != want {
			t.Fatalf("%s copy differs:\n%s\nwant:\n%s", name, got, want)
		}
	}
}

// Stopping after the snapshot commits and before the prune loses nothing:
// every change is still there, and the next coordinator finishes the prune.
func TestSharedQueuePostgresCrashBetweenSnapshotAndPrune(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	leader, w, c := proofCoordinator(t, a)
	behind, _ := proofCopy(t, a)
	proofSeed(t, w, leader.replica)
	proofChurn(t, w, leader.replica, 0)
	if err := proofPullHead(t, behind); err != nil {
		t.Fatal(err)
	}
	snapshots := 0
	crash := errors.New("coordinator stopped before pruning")
	c.snapshotted = func() error {
		if snapshots++; snapshots == 2 {
			return crash
		}
		return nil
	}
	for i := 1; ; i++ {
		if i > 500 {
			t.Fatal("no second snapshot")
		}
		proofChurn(t, w, leader.replica, i)
		if err := c.step(t.Context()); errors.Is(err, crash) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	pg.stop()
	pg.start()
	s := proofRetained(t, a)
	if s.floor != 0 || s.rows != w.sequence || s.snapshots != 2 {
		t.Fatalf("after the crash: %+v, head %d", s, w.sequence)
	}
	want := proofSharedDump(t, leader.replica)
	replayed, _ := proofCopy(t, a)
	if err := proofPullHead(t, replayed); err != nil {
		t.Fatal(err)
	}
	if got := proofSharedDump(t, replayed.replica); got != want {
		t.Fatalf("full replay after the crash differs:\n%s\nwant:\n%s", got, want)
	}
	// The next coordinator finishes the prune before its first snapshot.
	next := &sharedQueueCompactor{authority: a, source: c.source, writer: &sharedQueueWriter{}}
	if err := next.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	s = proofRetained(t, a)
	if s.floor == 0 || s.rows != w.sequence-s.floor || s.snapshots != 2 {
		t.Fatalf("after the next coordinator: %+v, head %d", s, w.sequence)
	}
	// A copy with a stale view of the floor fails closed rather than skip.
	position := behind.sequence
	if err := behind.pull(t.Context(), sharedQueueTurn{head: w.sequence}); !errors.Is(err, errSharedHead) || behind.sequence != position {
		t.Fatalf("stale floor pull = %v at %d, want a refusal at %d", err, behind.sequence, position)
	}
	for name, m := range map[string]*sharedQueueMember{"behind": behind, "replayed": replayed} {
		if err := proofPullHead(t, m); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := proofSharedDump(t, m.replica); got != want {
			t.Fatalf("%s copy differs:\n%s\nwant:\n%s", name, got, want)
		}
	}
	// Rollback restores the pruned authority from its snapshot.
	path := filepath.Join(t.TempDir(), "restored.db")
	if err := a.restore(t.Context(), path, w.sequence); err != nil {
		t.Fatal(err)
	}
	restored, err := openQueueSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if got, want := proofSharedDump(t, restored), strings.TrimSuffix(want, fmt.Sprintf("cursor %d\n", w.sequence)); got != want {
		t.Fatalf("rollback restore differs:\n%s\nwant:\n%s", got, want)
	}
}

// A corrupt snapshot changes nothing in the copy.
func TestSharedQueuePostgresSnapshotVerifiedBeforeUse(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	leader, w, c := proofCoordinator(t, a)
	proofSeed(t, w, leader.replica)
	s, err := a.writeSnapshot(t.Context(), c.source)
	if err != nil || s.sequence != 1 {
		t.Fatalf("snapshot = %+v, %v", s, err)
	}
	proofChurn(t, w, leader.replica, 1)
	if _, err := a.writeSnapshot(t.Context(), c.source); err != nil {
		t.Fatal(err)
	}
	if _, err := a.prune(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Replace the newest snapshot's last chunk with valid rows that differ.
	if _, err := a.db.Exec(`UPDATE myce_snapshot_chunks_v1 SET payload=convert_to('{"version":1,"rows":[]}','UTF8')
		WHERE sequence=2 AND chunk=(SELECT max(chunk) FROM myce_snapshot_chunks_v1 WHERE sequence=2)`); err != nil {
		t.Fatal(err)
	}
	m, _ := proofCopy(t, a)
	before := proofSharedDump(t, m.replica)
	if err := proofPullHead(t, m); !errors.Is(err, errSharedSnapshot) {
		t.Fatalf("corrupt snapshot pull = %v", err)
	}
	if got := proofSharedDump(t, m.replica); got != before || m.sequence != 0 {
		t.Fatalf("a refused snapshot changed the copy to sequence %d:\n%s", m.sequence, got)
	}
}

// A member that joins after the log was pruned rebuilds from the snapshot,
// then coordinates and serves records whose changes no longer exist.
func TestSharedQueueMemberJoinsFromSnapshot(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	one := startProofMember(t, dir, "one", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, one, 0)
	seed := proofSubmit(t, one.client, proofSpec("before-prune"))
	submitted, err := a.current(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var floor int64
	for i := 0; floor <= submitted.head; i++ {
		if i > 400 {
			t.Fatalf("the coordinator never pruned past the first submit: floor %d", floor)
		}
		proofSubmit(t, one.client, proofSpec(fmt.Sprintf("churn-%d", i)))
		turn, err := a.current(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		floor = turn.floor
	}
	var stored int64
	if err := a.db.QueryRow("SELECT COALESCE(min(sequence),0) FROM myce_changes_v1").Scan(&stored); err != nil || stored <= 1 {
		t.Fatalf("oldest retained change %d, %v", stored, err)
	}
	two := startProofMember(t, dir, "two", "127.0.0.1:0", pg.dsn)
	proofEventually(t, "joining member rebuilds past the floor", func() bool {
		sequence, j := proofReplica(t, two, seed.ID)
		return sequence >= floor && j != nil && j.State == "queued"
	})
	if log, _ := os.ReadFile(two.log); !strings.Contains(string(log), "event=shared_queue_replica_rebuilt from=0") {
		t.Fatalf("joining member did not rebuild from the snapshot:\n%s", log)
	}
	one.stop(t, syscall.SIGKILL)
	proofLeader(t, a, two, first.number)
	j, err := two.client.GetJob(t.Context(), seed.ID)
	if err != nil || j.State != "queued" {
		t.Fatalf("rebuilt coordinator lost a pruned submit: %+v, %v", j, err)
	}
}
