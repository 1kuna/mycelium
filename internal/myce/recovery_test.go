package myce

import (
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

func TestLateReturnCheckpointWinner(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, keyed := range []bool{false, true} {
			for _, olderWins := range []bool{false, true} {
				t.Run("keyed="+stringBool(keyed)+"/olderWins="+stringBool(olderWins), func(t *testing.T) {
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
					nextStaged := c.Now()
					oldStaged := nextStaged.Add(-time.Minute)
					_, err := q.Checkin(Checkin{Machine: n, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: next.ID, Attempt: next.Attempt, Progress: 20}, Session: next.Session, InputsStagedAt: &nextStaged}}})
					if err != nil {
						t.Fatal(err)
					}
					progress := int64(10)
					if olderWins {
						progress = 30
					}
					report := Checkin{Machine: m, Running: &Execution{ID: old.ID, Attempt: old.Attempt, Progress: progress}}
					if keyed {
						report.AttemptProtocol = keyedAttemptProtocol
						report.Attempts = []AttemptReport{{Execution: *report.Running, Session: old.Session, InputsStagedAt: &oldStaged}}
						report.Running = nil
					}
					reply, err := q.Checkin(report)
					if err != nil {
						t.Fatal(err)
					}
					kill := reply.Kill
					if keyed {
						kill = reply.Attempts[0].Kill
					}
					if kill == olderWins {
						t.Fatalf("wrong winner: %+v", reply)
					}
					wantStaged := &nextStaged
					if olderWins {
						wantStaged = nil // Legacy reports cannot supply staging evidence.
						if keyed {
							wantStaged = &oldStaged
						}
					}
					gotStaged := job(t, q, old.ID).InputsStagedAt
					if (gotStaged == nil) != (wantStaged == nil) || gotStaged != nil && !gotStaged.Equal(*wantStaged) {
						t.Fatalf("winner staging time = %v, want %v", gotStaged, wantStaged)
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

	})

	t.Run("NASUnavailableBlocksAndRecovers", func(t *testing.T) {
		q, c := fixture(t)
		j := submit(t, q, spec("experiment"))
		fakeCheckpointAvailability(t, q, false)
		c.Advance(time.Second)
		if check(t, q, machine("a", "fake"), nil).Job != nil {
			t.Fatal("started without checkpoint root")
		}
		snap, _ := q.Snapshot()
		if !strings.Contains(snap.Warning, "NAS") || !strings.Contains(job(t, q, j.ID).Reason, "NAS") {
			t.Fatal("NAS missing status")
		}
		fakeCheckpointAvailability(t, q, true)
		m := machine("a", "fake")
		m.CheckpointUnavailable = true
		if check(t, q, m, nil).Job != nil {
			t.Fatal("started on worker without NAS")
		}
		m.CheckpointUnavailable = false
		if check(t, q, m, nil).Job == nil {
			t.Fatal("did not recover")
		}

	})

	t.Run("CheckpointReconciliationWaitsForNAS", func(t *testing.T) {
		q, c := fixture(t)
		submit(t, q, spec("experiment"))
		m := machine("a", "fake")
		old := check(t, q, m, nil).Job
		c.Advance(LossAfter)
		check(t, q, observer(), nil)
		n := machine("b", "fake")
		next := check(t, q, n, nil).Job
		fakeCheckpointAvailability(t, q, false)
		c.Advance(time.Second)
		reply := check(t, q, m, old)
		if reply.Kill || !reply.Hold {
			t.Fatalf("decided without NAS: %+v", reply)
		}
		if job(t, q, next.ID).Machine != n.ID {
			t.Fatal("changed owner without checkpoint proof")
		}

	})
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
