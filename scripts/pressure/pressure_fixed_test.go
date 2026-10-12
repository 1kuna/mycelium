package myce

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// These tests assert the pressure plan, rather than the current implementation.
func TestVersoPressureR2CheckpointSkew(t *testing.T) {
	for _, offset := range []int{-120, -30, -5, 5, 30, 120} {
		t.Run(strconv.Itoa(offset), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "job")
			now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			for attempt, unit := range map[int]string{1: "2", 2: "9"} {
				dir := filepath.Join(root, "attempt-"+strconv.Itoa(attempt))
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				p := filepath.Join(dir, "checkpoint.json")
				if err := os.WriteFile(p, []byte(unit), 0600); err != nil {
					t.Fatal(err)
				}
				stamp := now
				if offset > 0 && attempt == 1 || offset < 0 && attempt == 2 {
					stamp = now.Add(time.Duration(offset) * time.Second)
				}
				if err := os.Chtimes(p, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			}
			dest, err := prepareCheckpoint(Job{ID: "synthetic-skew", Attempt: 3, Checkpoint: root})
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dest, "checkpoint.json"))
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != "9" {
				t.Fatalf("completed progress regressed: resumed=%s want=9 skew_seconds=%d", data, offset)
			}
		})
	}
}

func TestVersoPressureProductionDuringMissingNAS(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	s.Resumable = false
	j := submit(t, q, s)
	root, away := q.root, q.root+"-unavailable"
	if err := os.Rename(root, away); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(away, root)
	m := machine("synthetic-only", "fake")
	if reply := check(t, q, m, nil); reply.Job != nil {
		return
	}
	clock.Advance(31 * time.Second)
	if reply := check(t, q, m, nil); reply.Job == nil {
		t.Fatalf("production missed plan start bound: state=%s elapsed=31s no successful start or fallback", job(t, q, j.ID).State)
	}
}
