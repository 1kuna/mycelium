package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"mycelium/internal/myce"
)

// myce queue cutover|rollback moves queue authority between the single SQLite
// queue and the shared PostgreSQL authority (docs/operations.md#shared-queue-cutover-and-rollback).
func sharedQueueMigrationCommand(ctx context.Context, action string, args []string, out io.Writer) error {
	f := flag.NewFlagSet("queue "+action, flag.ContinueOnError)
	db := f.String("db", "", "queue SQLite database (cutover: the stopped single queue; rollback: a new path)")
	checkpoints := f.String("checkpoints", "", "queue checkpoint root")
	files := f.String("file-store", "", "shared file store the members use")
	acknowledge := f.String("acknowledge", "", "accept the file problems reported with this digest")
	authority := f.String("shared-authority", os.Getenv("MYCE_SHARED_QUEUE_PG_DSN"), "PostgreSQL authority DSN without a password (default MYCE_SHARED_QUEUE_PG_DSN)")
	f.Lookup("shared-authority").DefValue = ""
	passwordFile := f.String("shared-authority-password-file", "", "private file (chmod 600) holding the authority password")
	verifyOnly := new(bool)
	if action == "cutover" {
		f.BoolVar(verifyOnly, "verify-only", false, "only verify file references; change nothing")
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return &exitError{2, fmt.Errorf("usage: myce queue %s --db DB --checkpoints DIR --file-store DIR", action)}
	}
	cfg := myce.SharedQueueMigration{
		Postgres: *authority, PostgresPasswordFile: *passwordFile, Database: *db, Checkpoints: *checkpoints, FileStore: *files,
		Clock: myce.RealClock{}, Acknowledge: *acknowledge, VerifyOnly: *verifyOnly,
	}
	run := myce.SharedQueueCutover
	if action == "rollback" {
		run = myce.SharedQueueRollback
	}
	result, err := run(ctx, cfg)
	if !result.Already && (err == nil || errors.Is(err, myce.ErrSharedFilesUnacknowledged)) {
		fmt.Fprintf(out, "files: checked %d references, %d bytes, %d problems\n", result.Files.Checked, result.Files.Bytes, len(result.Files.Problems))
	}
	for _, p := range result.Files.Problems {
		fmt.Fprintf(out, "file %s: sha256=%s size=%d kind=%s job=%s name=%s\n", p.Problem, p.SHA256, p.SizeBytes, p.Holder.Kind, p.Holder.JobID, p.Holder.Name)
	}
	if errors.Is(err, myce.ErrSharedFilesUnacknowledged) {
		return &exitError{3, fmt.Errorf("%w; restore the bytes and rerun, or accept exactly this report with --acknowledge %s", err, result.Files.Digest)}
	}
	if err != nil {
		return err
	}
	switch {
	case *verifyOnly:
		fmt.Fprintln(out, "verify-only: nothing changed")
	case result.Already && action == "cutover":
		fmt.Fprintf(out, "cutover already complete: authority holds the import through sequence %d\n", result.Sequence)
	case action == "cutover":
		fmt.Fprintf(out, "cutover complete: imported sequence 1..%d; the single queue database is fenced; start the shared members\n", result.Sequence)
	default:
		fmt.Fprintf(out, "rollback complete: shared writers fenced; restored sequence 1..%d into %s; start the single queue on it\n", result.Sequence, *db)
	}
	return nil
}
