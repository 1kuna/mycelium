package myce

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type proofPostgres struct {
	t         *testing.T
	root, dsn string
	pgCtl     string
	port      string
	running   bool
}

func proofPostgresUnavailable(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("MYCE_GATE_REQUIRE_POSTGRES") == "1" {
		t.Fatal(reason)
	}
	t.Skip(reason)
}

func startProofPostgres(t *testing.T) *proofPostgres {
	t.Helper()
	initdb, initErr := exec.LookPath("initdb")
	pgCtl, ctlErr := exec.LookPath("pg_ctl")
	if initErr != nil || ctlErr != nil {
		proofPostgresUnavailable(t, "shared queue proof requires PostgreSQL initdb and pg_ctl on PATH")
	}
	if os.Geteuid() == 0 {
		proofPostgresUnavailable(t, "initdb requires an unprivileged OS user")
	}
	// Darwin's test TMPDIR can exceed the UNIX socket path limit.
	root, err := os.MkdirTemp("/tmp", "myce-pg-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(probe.Addr().(*net.TCPAddr).Port)
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	pg := &proofPostgres{t: t, root: root, pgCtl: pgCtl, port: port,
		dsn: "host=" + root + " port=" + port + " dbname=postgres user=proof sslmode=disable"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// This private socket is inside a mode-0700 directory. TCP is disabled.
	out, err := exec.CommandContext(ctx, initdb, "-D", pg.data(), "-U", "proof", "-A", "trust", "--no-locale", "-E", "UTF8").CombinedOutput()
	if err != nil {
		t.Fatalf("initdb: %v\n%s", err, out)
	}
	t.Cleanup(pg.stop)
	pg.start()
	return pg
}

func (pg *proofPostgres) data() string { return filepath.Join(pg.root, "data") }

func (pg *proofPostgres) start() {
	pg.t.Helper()
	pg.ctl("-l", filepath.Join(pg.root, "postgres.log"), "-o", "-p "+pg.port+" -c listen_addresses='' -c unix_socket_directories="+pg.root, "start")
	pg.running = true
}

func (pg *proofPostgres) stop() {
	pg.t.Helper()
	if pg.running {
		pg.ctl("-m", "fast", "stop")
		pg.running = false
	}
}

func (pg *proofPostgres) ctl(args ...string) {
	pg.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, pg.pgCtl, append([]string{"-D", pg.data(), "-w", "-t", "30"}, args...)...).CombinedOutput()
	if err != nil {
		pg.t.Fatalf("pg_ctl: %v\n%s", err, out)
	}
}

func (pg *proofPostgres) authority(t *testing.T) *sharedQueueAuthority {
	t.Helper()
	a, err := openSharedQueueAuthority(t.Context(), pg.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.db.Close() })
	return a
}

func TestSharedQueuePostgresExpiryIsCheckedAfterLock(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	turn, err := a.acquire(ctx, "first", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, _, err := lockedSharedTurn(ctx, lock); err != nil {
		t.Fatal(err)
	}
	appended := make(chan error, 1)
	go func() {
		_, err := a.append(ctx, turn, sharedQueueChange{sequence: 1, handover: turn.number, operation: ID(), payload: []byte("{}")})
		appended <- err
	}()
	proofEventually(t, "append reached the locked row", func() bool {
		var waiting bool
		err := a.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE wait_event_type='Lock' AND query LIKE 'SELECT owner,endpoint,% FOR UPDATE')`).Scan(&waiting)
		return err == nil && waiting
	})
	if _, err := lock.ExecContext(ctx, "UPDATE myce_coordinator_v1 SET expires='epoch' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := lock.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-appended; !errors.Is(err, errSharedTurn) {
		t.Fatalf("expired append = %v", err)
	}
	next, err := a.acquire(ctx, "next", "", time.Minute)
	if err != nil || next.number != turn.number+1 || next.head != 0 {
		t.Fatalf("takeover = %+v, %v", next, err)
	}
	if err := a.renew(ctx, turn, 0, false); !errors.Is(err, errSharedTurn) {
		t.Fatalf("stale release = %v", err)
	}
	if _, err := a.append(ctx, turn, sharedQueueChange{sequence: 1, handover: turn.number, operation: ID(), payload: []byte("{}")}); !errors.Is(err, errSharedTurn) {
		t.Fatalf("stale append = %v", err)
	}
}

func TestSharedQueueRowsReplayLostCommitReply(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	ctx := t.Context()
	turn, err := a.acquire(ctx, "first", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	openCopy := func() *sql.DB {
		db, err := openQueueSQLite(filepath.Join(t.TempDir(), "records.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		if err := (&sharedQueueMember{replica: db, authority: a}).bindReplica(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	leader, follower := openCopy(), openCopy()

	// This failure is injected after a real synchronous PostgreSQL commit.
	discarded := 0
	a.commit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		discarded++
		return errors.New("commit reply lost")
	}
	w := &sharedQueueWriter{authority: a, turn: turn, timeout: 5 * time.Second}
	var batch queueWriteBatch
	batch.add(upsertQueueState, "sample", []byte("first value"))
	for _, offset := range []int64{1, 1, 2} {
		batch.add("INSERT OR IGNORE INTO log_offsets(job,attempt,offset) VALUES(?,?,?)", "example", 1, offset)
		batch.add("INSERT INTO logs(job,attempt,text) SELECT ?,?,? WHERE changes()=1", "example", 1, "log"+string([]byte{255}))
	}
	if err := w.apply(ctx, leader, &batch, nil); err != nil {
		t.Fatal(err)
	}
	if discarded != 1 || w.sequence != 1 {
		t.Fatalf("lost commit reply resolved more than once: %d, sequence %d", discarded, w.sequence)
	}
	a.commit = (*sql.Tx).Commit
	var next queueWriteBatch
	next.add("DELETE FROM logs WHERE seq=?", int64(1))
	next.add("DELETE FROM queue_state WHERE key=?", "sample")
	next.add("INSERT INTO logs(job,attempt,text) VALUES(?,?,?)", "example", 1, "third")
	if err := w.apply(ctx, leader, &next, nil); err != nil {
		t.Fatal(err)
	}
	// A missing reply without a matching commit must close the writer turn.
	a.commit = func(tx *sql.Tx) error {
		return errors.Join(tx.Rollback(), context.DeadlineExceeded)
	}
	var unconfirmed queueWriteBatch
	unconfirmed.add(upsertQueueState, "sample", []byte("unconfirmed"))
	// The turn ends, so callers retry under the next one instead of
	// treating the commit deadline as a terminal request timeout.
	if err := w.apply(ctx, leader, &unconfirmed, nil); !errors.Is(err, errSharedTurn) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unconfirmed write = %v", err)
	}
	a.commit = (*sql.Tx).Commit
	if err := w.apply(ctx, leader, &unconfirmed, nil); !errors.Is(err, errSharedTurn) {
		t.Fatalf("grant after an unconfirmed write = %v", err)
	}
	// Stop and restart the same PostgreSQL authority before replay.
	pg.stop()
	pg.start()
	err = a.changes(ctx, 0, 2, func(change sharedQueueChange) error {
		return replaySharedQueueChange(ctx, follower, change)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, db := range []*sql.DB{leader, follower} {
		var seq int64
		var logs string
		var deleted bool
		if err := db.QueryRow(`SELECT sequence, (SELECT group_concat(seq||':'||hex(text), ',') FROM (SELECT seq,text FROM logs ORDER BY seq)),
			NOT EXISTS(SELECT 1 FROM queue_state WHERE key='sample') FROM myce_shared_cursor WHERE id=1`).Scan(&seq, &logs, &deleted); err != nil {
			t.Fatal(err)
		}
		if seq != 2 || !deleted || logs != "2:6C6F67FF,3:7468697264" {
			t.Fatalf("copy sequence=%d logs=%q deleted=%v", seq, logs, deleted)
		}
	}
	var versions strings.Builder
	if err := a.changes(ctx, 0, 2, func(change sharedQueueChange) error {
		versions.WriteString(change.operation)
		return nil
	}); err != nil || versions.Len() != 64 {
		t.Fatalf("stable operations length=%d error=%v", versions.Len(), err)
	}
}

func TestSharedQueuePostgresGateFailsWithoutFixture(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("MYCE_GATE_REQUIRE_POSTGRES", "1")
	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestSharedQueuePostgresExpiryIsCheckedAfterLock$")
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 ||
		!strings.Contains(string(out), "shared queue proof requires PostgreSQL initdb and pg_ctl on PATH") {
		t.Fatalf("missing tools did not fail the selected PostgreSQL proof: %v\n%s", err, out)
	}
}
