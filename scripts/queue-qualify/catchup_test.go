package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestCopySnapshotHashesAndProjects(t *testing.T) {
	share, local := t.TempDir(), t.TempDir()
	src := filepath.Join(share, snapshotPrefix+".bin")
	created, err := ensureSnapshot(context.Background(), src, 8<<20, 0)
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	if again, err := ensureSnapshot(context.Background(), src, 8<<20, 0); err != nil || again {
		t.Fatalf("an existing snapshot is reused: %v %v", again, err)
	}
	b, _ := os.ReadFile(src)
	sum := sha256.Sum256(b)
	s, err := copySnapshot(context.Background(), src, local, 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Bytes != 8<<20 || s.SHA256 != hex.EncodeToString(sum[:]) || s.ProjectedLiveS <= 0 {
		t.Fatalf("%+v", s)
	}
	if entries, _ := os.ReadDir(local); len(entries) != 0 {
		t.Fatalf("local copy kept: %v", entries)
	}
	if _, err := copySnapshot(context.Background(), src, local, 1<<62); err == nil {
		t.Fatal("copy ignored the local free-space floor")
	}
}

func TestCatchupChecksNeedTenTimesLive(t *testing.T) {
	prof, _ := parseProfile("queue")
	ok := &catchupResults{Replay: &replayResults{ChangesPerSec: 200, MBps: 2, SpeedupOnLive: 17}}
	if v := verdict(catchupChecks(ok, prof, defaultRate)); v != statusPass {
		t.Fatalf("%+v", catchupChecks(ok, prof, defaultRate))
	}
	slow := &catchupResults{Replay: &replayResults{ChangesPerSec: 100, MBps: 2}}
	if v := verdict(catchupChecks(slow, prof, defaultRate)); v != statusFail {
		t.Fatalf("100 changes/s is under 10x of 11.6/s: %+v", catchupChecks(slow, prof, defaultRate))
	}
}

func TestReplayAgainstDisposablePostgres(t *testing.T) {
	pg := startPostgres(t)
	tg := pg.target(t)
	prof, _ := parseProfile("queue")
	local := t.TempDir()
	res, err := replayLog(context.Background(), tg, local, 300, prof, defaultRate)
	if err != nil {
		t.Fatal(err)
	}
	if res.Changes != 300 || res.Bytes < 300*3000 || res.ChangesPerSec <= 0 || res.ApplySeconds <= 0 {
		t.Fatalf("%+v", res)
	}
	if entries, _ := os.ReadDir(local); len(entries) != 0 {
		t.Fatalf("replica left behind: %v", entries)
	}
	conn, err := tg.connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var left int
	conn.QueryRow(context.Background(), "SELECT count(*) FROM myce_qualification.catchup_changes_v1").Scan(&left)
	if left != 0 {
		t.Fatalf("seeded log kept %d rows", left)
	}
}
