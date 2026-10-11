package myce

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCrashDurableSubmission(t *testing.T) {
	if path := os.Getenv("MYCE_TEST_CRASH_DB"); path != "" {
		c := &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
		q, err := Open(path, os.Getenv("MYCE_TEST_CRASH_ROOT"), c)
		if err != nil {
			panic(err)
		}
		if _, err = q.Submit(spec("experiment")); err != nil {
			panic(err)
		}
		os.Exit(0) // Deliberately skip SQLite Close and all deferred cleanup.
	}
	path := filepath.Join(t.TempDir(), "queue.db")
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashDurableSubmission$")
	cmd.Env = append(os.Environ(), "MYCE_TEST_CRASH_DB="+path, "MYCE_TEST_CRASH_ROOT="+root)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v %s", err, out)
	}
	c := &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var mode string
	var full int
	if err = q.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if err = q.db.QueryRow("PRAGMA synchronous").Scan(&full); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" || full != 2 {
		t.Fatalf("durability: %s %d", mode, full)
	}
	snap, err := q.Snapshot()
	if err != nil || len(snap.Jobs) != 1 || snap.Jobs[0].State != "queued" {
		t.Fatalf("lost submitted job: %+v %v", snap, err)
	}
}

func TestRestartGraceReportsAndExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	root := t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	submit(t, q, spec("experiment"))
	a := check(t, q, machine("a", "fake"), nil).Job
	submit(t, q, spec("experiment"))
	b := check(t, q, machine("b", "fake"), nil).Job
	submit(t, q, spec("experiment"))
	d := check(t, q, machine("d", "fake"), nil).Job
	q.Close()
	c.Advance(10 * time.Minute)
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.SetRestartGrace(2 * time.Minute)
	c.Advance(119 * time.Second)
	requireState(t, q, a.ID, "running")
	requireState(t, q, b.ID, "running")
	if check(t, q, machine("a", "fake"), a).Stop {
		t.Fatal("reported job stopped during recovery")
	}
	finish(t, q, b, true) // Buffered finished result is itself a report.
	c.Advance(time.Second)
	requireState(t, q, b.ID, "succeeded")
	requireState(t, q, a.ID, "running")
	requireState(t, q, d.ID, "queued")
}

func TestWholeNetworkSilenceWaitsAndReconnects(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	a := check(t, q, machine("a", "fake"), nil).Job
	submit(t, q, spec("experiment"))
	b := check(t, q, machine("b", "fake"), nil).Job
	c.Advance(10 * time.Minute)
	requireState(t, q, a.ID, "running")
	requireState(t, q, b.ID, "running")
	snap, _ := q.Snapshot()
	if !strings.Contains(snap.Warning, "network") {
		t.Fatal(snap.Warning)
	}
	if check(t, q, machine("a", "fake"), a).Stop {
		t.Fatal("first reconnect stopped")
	}
	c.Advance(59 * time.Second)
	requireState(t, q, b.ID, "running")
	if check(t, q, machine("b", "fake"), b).Stop {
		t.Fatal("second reconnect stopped")
	}
}

func TestWorkerFreshRestartRetriesCleanedSession(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("a", "fake")
	r := check(t, q, m, nil).Job
	c.Advance(time.Second)
	m.Session = "fresh"
	reply, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: r.Session})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Job == nil || reply.Job.Attempt != 2 {
		t.Fatalf("fresh worker: %+v", reply)
	}
	if !errors.Is(q.Complete(result(r, true)), ErrStale) {
		t.Fatal("orphan result accepted")
	}
}

func TestLateReturnCheckpointWinner(t *testing.T) {
	for _, olderWins := range []bool{false, true} {
		t.Run(stringBool(olderWins), func(t *testing.T) {
			q, c := fixture(t)
			submit(t, q, spec("experiment"))
			m := machine("a", "fake")
			old := check(t, q, m, nil).Job
			c.Advance(LossAfter)
			check(t, q, observer(), nil)
			n := machine("b", "fake")
			next := check(t, q, n, nil).Job
			if next == nil {
				t.Fatal("lost job not reassigned")
			}
			dir := filepath.Join(q.root, next.Checkpoint, "attempt-2")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":20},"value":20}`), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := q.Checkin(Checkin{Machine: n, Running: &Execution{ID: next.ID, Attempt: next.Attempt, Progress: 20}})
			if err != nil {
				t.Fatal(err)
			}
			progress := int64(10)
			if olderWins {
				progress = 30
			}
			reply, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: old.ID, Attempt: old.Attempt, Progress: progress}})
			if err != nil {
				t.Fatal(err)
			}
			if reply.Kill == olderWins {
				t.Fatalf("wrong winner: %+v", reply)
			}
			if olderWins {
				loser := check(t, q, n, next)
				if !loser.Kill {
					t.Fatal("newer attempt loser continues")
				}
				finish(t, q, old, true)
			} else {
				finish(t, q, next, true)
			}
			requireState(t, q, old.ID, "succeeded")
		})
	}
}

func TestNASUnavailableBlocksAndRecovers(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	root := q.root
	away := root + "-away"
	if err := os.Rename(root, away); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(away, root)
	c.Advance(time.Second)
	if check(t, q, machine("a", "fake"), nil).Job != nil {
		t.Fatal("started without checkpoint root")
	}
	snap, _ := q.Snapshot()
	if !strings.Contains(snap.Warning, "NAS") || !strings.Contains(job(t, q, j.ID).Reason, "NAS") {
		t.Fatal("NAS missing status")
	}
	if err := os.Rename(away, root); err != nil {
		t.Fatal(err)
	}
	m := machine("a", "fake")
	m.CheckpointUnavailable = true
	if check(t, q, m, nil).Job != nil {
		t.Fatal("started on worker without NAS")
	}
	m.CheckpointUnavailable = false
	if check(t, q, m, nil).Job == nil {
		t.Fatal("did not recover")
	}
}

func TestFailedWriteRestoresStateAndExternalCopies(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	j.Spec.Run.Command[0] = "mutated"
	if job(t, q, j.ID).Spec.Run.Command[0] == "mutated" {
		t.Fatal("external job aliases queue state")
	}
	before := len(q.s.Jobs)
	q.db.Close()
	if _, err := q.Submit(spec("experiment")); err == nil {
		t.Fatal("closed DB accepted write")
	}
	if len(q.s.Jobs) != before {
		t.Fatal("failed submit retained phantom job")
	}
}

func TestLogAndCompletionRedelivery(t *testing.T) {
	q, _ := fixture(t)
	submit(t, q, spec("experiment"))
	r := check(t, q, machine("a", "fake"), nil).Job
	l := LogChunk{Result: result(r, true), Offset: 1, Text: "once"}
	for i := 0; i < 2; i++ {
		if err := q.AppendLog(l); err != nil {
			t.Fatal(err)
		}
	}
	ls, err := q.Logs(r.ID, 0)
	if err != nil || len(ls) != 1 {
		t.Fatalf("duplicated delivery %+v %v", ls, err)
	}
	finish(t, q, r, true)
	finish(t, q, r, true)
}

func TestPlannedPreemptionNotLimitedToThreeResumes(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("a", "fake")
	for i := 0; i < 5; i++ {
		r := check(t, q, m, nil).Job
		if r == nil || r.ID != j.ID {
			t.Fatal("resume exhausted by planned pause")
		}
		if err := q.Leave(m.ID, false); err != nil {
			t.Fatal(err)
		}
		finish(t, q, r, true)
		check(t, q, m, nil)
		m.Session = ID()
	}
	requireState(t, q, j.ID, "queued")
}

func TestQueueSingleWriterAndMachineNameOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	root := t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if other, err := Open(path, root, c); err == nil {
		other.Close()
		t.Fatal("second writer accepted")
	}
	m := machine("a", "fake")
	check(t, q, m, nil)
	m.Session = "unrelated"
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true}); err == nil {
		t.Fatal("unrelated fresh worker stole live machine name")
	}
}

func TestCheckpointReconciliationWaitsForNAS(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("a", "fake")
	old := check(t, q, m, nil).Job
	c.Advance(LossAfter)
	check(t, q, observer(), nil)
	n := machine("b", "fake")
	next := check(t, q, n, nil).Job
	root := q.root
	away := root + "-unavailable"
	if err := os.Rename(root, away); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(away, root)
	c.Advance(time.Second)
	reply := check(t, q, m, old)
	if reply.Kill || !reply.Hold {
		t.Fatalf("decided without NAS: %+v", reply)
	}
	if job(t, q, next.ID).Machine != n.ID {
		t.Fatal("changed owner without checkpoint proof")
	}
}
