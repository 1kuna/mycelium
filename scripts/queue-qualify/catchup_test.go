package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// A created snapshot goes into a new run directory of its own, is reported
// for NAS-local removal, and is never deleted; the local copy, on the
// measuring machine's own disk, is.
func TestSnapshotIsWriteOnlyAndCopyHashesAndProjects(t *testing.T) {
	share, local := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(share, "snapshot.bin"), []byte("someone else's"), 0o644)
	made, err := createSnapshot(context.Background(), share, 8<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(made.path, snapshotFile)
	if filepath.Dir(made.path) != filepath.Clean(share) || made.BytesWritten != 8<<20 || made.FilesComplete != 1 {
		t.Fatalf("%+v", made)
	}
	if b, _ := os.ReadFile(filepath.Join(share, "snapshot.bin")); string(b) != "someone else's" {
		t.Fatal("snapshot creation touched a file outside its run directory")
	}
	again, err := createSnapshot(context.Background(), share, 8<<20, 0)
	if err != nil || again.path == made.path {
		t.Fatalf("a second snapshot must get its own directory: %v", err)
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
	if _, err := os.Stat(src); err != nil {
		t.Fatal("the snapshot on the share was removed")
	}
	if _, err := copySnapshot(context.Background(), src, local, 1<<62); err == nil {
		t.Fatal("copy ignored the local free-space floor")
	}
	if err := requireLocalDisk(local); err != nil {
		t.Fatalf("a temporary directory is a local disk: %v", err)
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

// Each replay seeds its own rows under a fresh run ID and leaves them; a
// second run does not collide with the first.
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
	second, err := replayLog(context.Background(), tg, local, 100, prof, defaultRate)
	if err != nil || second.Run == res.Run {
		t.Fatalf("second run: %v %+v", err, second)
	}
	conn, err := tg.connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var kept int
	conn.QueryRow(context.Background(), "SELECT count(*) FROM myce_qualification.catchup_changes_v1").Scan(&kept)
	if kept != 400 {
		t.Fatalf("seeded rows kept: %d, want 400", kept)
	}
}
