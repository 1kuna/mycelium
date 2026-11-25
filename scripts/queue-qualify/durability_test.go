package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadLedgerSkipsATornFinalLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	body := `H {"run":"r1","started":"2026-11-25T00:00:00Z","system_identifier":"42","timeline":1}
A 1 aa 2026-11-25T00:00:01.5Z 0/16B3A28
U 2 bb
F 3
E 0/26B3A28
A 4 cc 2026-11-25T00`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := readLedger(path)
	if err != nil || l.header.Run != "r1" || l.header.SystemIdentifier != "42" || len(l.entries) != 3 || l.entries[2].state != "U" || l.entries[1].sha != "aa" {
		t.Fatalf("%+v %v", l, err)
	}
	if l.entries[1].lsn != 0x16B3A28 || l.entries[1].started.Second() != 1 || l.endLSN != 0x26B3A28 {
		t.Fatalf("acknowledgement binding lost: %+v end %X", l.entries[1], l.endLSN)
	}
	if err := os.WriteFile(path, []byte("A 1 aa 2026-11-25T00:00:01Z 0/1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readLedger(path); err == nil {
		t.Fatal("headless ledger accepted")
	}
}

// evaluateLedger judges a ledger against the server's state. Every failure
// below is one a pass would hide.
func TestVerifyFailsWithoutAnAcknowledgementBeforeTheRestart(t *testing.T) {
	t0 := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	t1, t2 := t0.Add(time.Minute), t0.Add(2*time.Minute)
	ledgerAt := func(started ...time.Time) *ledger {
		l := &ledger{header: ledgerHeader{Run: "r", SystemIdentifier: "42", Timeline: 1, ServerStarted: t0}, entries: map[int64]ledgerEntry{}}
		for i, s := range started {
			l.entries[int64(i+1)] = ledgerEntry{state: "A", sha: "x", started: s, lsn: 100}
		}
		return l
	}
	present := map[int64]storedRow{1: {"x", "x"}, 2: {"x", "x"}}
	now := &serverInfo{SystemIdentifier: "42", Timeline: 1, StartedAt: t2}
	failed := func(cs []check) []string {
		var names []string
		for _, c := range cs {
			if c.Status == statusFail {
				names = append(names, c.Name)
			}
		}
		return names
	}
	if f := failed(verifyChecks(evaluateLedger(ledgerAt(t1, t2), present, now, 200))); len(f) != 0 {
		t.Fatalf("one acknowledgement before the restart and all present should pass: %v", f)
	}
	// The write began under t0, the server restarted at t1 before the first
	// acknowledgement, and verify runs under t1: nothing acknowledged ever
	// went through a restart. A check on the header's start time passes this.
	after := &serverInfo{SystemIdentifier: "42", Timeline: 1, StartedAt: t1}
	if f := failed(verifyChecks(evaluateLedger(ledgerAt(t1, t1), present, after, 200))); len(f) != 1 || f[0] != "acknowledged before a server restart" {
		t.Fatalf("every acknowledgement after the restart must fail exactly that check: %v", f)
	}
	for name, server := range map[string]*serverInfo{
		"changed identifier":    {SystemIdentifier: "43", Timeline: 1, StartedAt: t2},
		"unreadable identifier": {Timeline: 1, StartedAt: t2},
	} {
		if f := failed(verifyChecks(evaluateLedger(ledgerAt(t1, t2), present, server, 200))); len(f) != 1 || f[0] != "same data directory" {
			t.Fatalf("%s must fail the data directory check: %v", name, f)
		}
	}
	if f := failed(verifyChecks(evaluateLedger(ledgerAt(t1, t2), present, now, 99))); len(f) != 1 || f[0] != "not an older copy" {
		t.Fatalf("WAL behind an acknowledgement must fail: %v", f)
	}
	promoted := &serverInfo{SystemIdentifier: "42", Timeline: 2, StartedAt: t2}
	if f := failed(verifyChecks(evaluateLedger(ledgerAt(t1, t2), present, promoted, 200))); len(f) != 1 || f[0] != "not an older copy" {
		t.Fatalf("a new timeline must fail: %v", f)
	}
}

// The writer spans an immediate (crash) shutdown of PostgreSQL; verify then
// finds every acknowledged row. Deleting one row afterwards must fail verify.
func TestDurabilityAcrossCrashRestart(t *testing.T) {
	pg := startPostgres(t)
	tg := pg.target(t)
	ctx := context.Background()
	ledgerPath := filepath.Join(t.TempDir(), "ledger")
	ledgerFile, err := os.OpenFile(ledgerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer ledgerFile.Close()
	prof, _ := parseProfile("queue")
	restarted := make(chan error, 1)
	go func() {
		time.Sleep(time.Second)
		restarted <- pg.ctl("restart", "-m", "immediate")
	}()
	res, _, err := durabilityWrite(ctx, tg, ledgerFile, durabilityOptions{run: "crash", duration: 4 * time.Second, rate: 40, maxOutage: time.Minute, profile: prof})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-restarted; err != nil {
		t.Fatal(err)
	}
	if res.StoppedBy != "duration" || res.Acknowledged < 60 || res.ServerRestarts != 1 || res.IdentityChanged || len(res.Outages) == 0 || !res.EndMarker {
		t.Fatalf("writer %+v", res)
	}
	conn, err := tg.open(ctx, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	l, err := readLedger(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := verifyLedger(ctx, conn, tg.class, l)
	if err != nil {
		t.Fatal(err)
	}
	if verdict(verifyChecks(v)) != statusPass || v.Acknowledged != res.Acknowledged || v.AckedBeforeRestart == 0 || v.AckedBeforeRestart == v.Acknowledged {
		t.Fatalf("verify %+v %+v", v, verifyChecks(v))
	}
	if _, err := conn.Exec(ctx, "DELETE FROM myce_qualification.durability_v1 WHERE run='crash' AND seq=(SELECT min(seq) FROM myce_qualification.durability_v1 WHERE run='crash' AND seq > 0)"); err != nil {
		t.Fatal(err)
	}
	v, _, err = verifyLedger(ctx, conn, tg.class, l)
	if err != nil || len(v.MissingAcknowledged) != 1 || verdict(verifyChecks(v)) != statusFail {
		t.Fatalf("a lost row must fail: %+v %v", v, err)
	}
}

// A verify that finds every row proves nothing unless the server restarted
// between an acknowledgement and the verify.
func TestDurabilityVerifyFailsWithoutARestart(t *testing.T) {
	pg := startPostgres(t)
	pg.target(t)
	ctx := context.Background()
	dir := t.TempDir()
	ledger := filepath.Join(dir, "ledger")
	var out strings.Builder
	if err := cmdDurabilityWrite(ctx, []string{"--dsn-env", "QQ_TEST_DSN", "--out", dir, "--ledger", ledger,
		"--duration", "1s", "--rate", "40"}, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	err := cmdDurabilityVerify(ctx, []string{"--dsn-env", "QQ_TEST_DSN", "--out", dir, "--ledger", ledger}, &out)
	if !errors.Is(err, errChecksFailed) || !strings.Contains(out.String(), "restart") {
		t.Fatalf("verify without a restart: %v\n%s", err, out.String())
	}
	if err := pg.ctl("restart", "-m", "fast"); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := cmdDurabilityVerify(ctx, []string{"--dsn-env", "QQ_TEST_DSN", "--out", dir, "--ledger", ledger}, &out); err != nil {
		t.Fatalf("verify after a restart: %v\n%s", err, out.String())
	}
}

// A copy of the data directory taken after every acknowledgement but before
// the writer's end marker holds every row, the same identifier and a fresh
// start time. Verify against it must still fail, on its WAL position.
func TestDurabilityVerifyFailsOnAnOlderCopyOfTheDataDirectory(t *testing.T) {
	pg := startPostgres(t)
	tg := pg.target(t)
	ctx := context.Background()
	ledgerPath := filepath.Join(t.TempDir(), "ledger")
	ledgerFile, err := os.OpenFile(ledgerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer ledgerFile.Close()
	prof, _ := parseProfile("queue")
	copyDir := pg.data + "-copy"
	var copyErr error
	res, _, err := durabilityWrite(ctx, tg, ledgerFile, durabilityOptions{run: "copied", duration: time.Second, rate: 40, maxOutage: time.Minute, profile: prof,
		beforeEnd: func() {
			if copyErr = pg.ctl("stop", "-m", "fast"); copyErr != nil {
				return
			}
			if out, err := exec.Command("cp", "-Rp", pg.data, copyDir).CombinedOutput(); err != nil {
				copyErr = errors.New(string(out))
				return
			}
			copyErr = pg.ctl("start")
		}})
	if err != nil || copyErr != nil {
		t.Fatal(err, copyErr)
	}
	if !res.EndMarker || res.Acknowledged == 0 {
		t.Fatalf("writer %+v", res)
	}
	// Swap the copy in where the original was, as a recreated container on a
	// copied folder would.
	if err := pg.ctl("stop", "-m", "fast"); err != nil {
		t.Fatal(err)
	}
	pg.data = copyDir
	if err := pg.ctl("start"); err != nil {
		t.Fatal(err)
	}
	l, err := readLedger(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tg.open(ctx, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	v, _, err := verifyLedger(ctx, conn, tg.class, l)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range verifyChecks(v) {
		wantFail := c.Name == "not an older copy" || c.Name == "every acknowledged write survived" // the end marker is missing too
		if (c.Status == statusFail) != wantFail {
			t.Errorf("%s: %s (%s)", c.Name, c.Status, c.Measured)
		}
	}
	if len(v.MissingAcknowledged) != 1 || v.MissingAcknowledged[0] != endMarkerSeq || !v.WALBehind || v.AckedBeforeRestart != v.Acknowledged {
		t.Fatalf("copy not caught: %+v", v)
	}
}
