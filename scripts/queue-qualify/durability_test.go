package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadLedgerSkipsATornFinalLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	body := `H {"run":"r1","started":"2026-11-25T00:00:00Z","system_identifier":"42"}
A 1 aa
U 2 bb
F 3
A`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	h, entries, err := readLedger(path)
	if err != nil || h.Run != "r1" || h.SystemIdentifier != "42" || len(entries) != 3 || entries[2].state != "U" || entries[1].sha != "aa" {
		t.Fatalf("%+v %+v %v", h, entries, err)
	}
	if err := os.WriteFile(path, []byte("A 1 aa\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readLedger(path); err == nil {
		t.Fatal("headless ledger accepted")
	}
}

// The writer spans an immediate (crash) shutdown of PostgreSQL; verify then
// finds every acknowledged row. Deleting one row afterwards must fail verify.
func TestDurabilityAcrossCrashRestart(t *testing.T) {
	pg := startPostgres(t)
	tg := pg.target(t)
	ctx := context.Background()
	ledgerPath := filepath.Join(t.TempDir(), "ledger")
	ledger, err := os.OpenFile(ledgerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	prof, _ := parseProfile("queue")
	restarted := make(chan error, 1)
	go func() {
		time.Sleep(time.Second)
		restarted <- pg.ctl("restart", "-m", "immediate")
	}()
	res, _, err := durabilityWrite(ctx, tg, ledger, "crash", 4*time.Second, 40, time.Minute, prof)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-restarted; err != nil {
		t.Fatal(err)
	}
	if res.StoppedBy != "duration" || res.Acknowledged < 60 || res.ServerRestarts != 1 || res.IdentityChanged || len(res.Outages) == 0 {
		t.Fatalf("writer %+v", res)
	}
	conn, err := tg.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	h, entries, err := readLedger(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := readServerInfo(ctx, conn, tg.class, 0)
	if err != nil {
		t.Fatal(err)
	}
	v, err := verifyLedger(ctx, conn, h, entries, info.SystemIdentifier)
	if err != nil {
		t.Fatal(err)
	}
	if verdict(verifyChecks(v)) != statusPass || v.Acknowledged != res.Acknowledged {
		t.Fatalf("verify %+v %+v", v, verifyChecks(v))
	}
	if _, err := conn.Exec(ctx, "DELETE FROM myce_qualification.durability_v1 WHERE run='crash' AND seq=(SELECT min(seq) FROM myce_qualification.durability_v1 WHERE run='crash')"); err != nil {
		t.Fatal(err)
	}
	v, err = verifyLedger(ctx, conn, h, entries, info.SystemIdentifier)
	if err != nil || len(v.MissingAcknowledged) != 1 || verdict(verifyChecks(v)) != statusFail {
		t.Fatalf("a lost row must fail: %+v %v", v, err)
	}
	if v, _ := verifyLedger(ctx, conn, h, entries, "other"); verdict(verifyChecks(v)) != statusFail || *v.IdentityMatches {
		t.Fatal("a replaced data directory must fail")
	}
}
