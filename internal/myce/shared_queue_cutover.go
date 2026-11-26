package myce

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Stage 3 moves authority between the single SQLite queue and the PostgreSQL
// authority without a second state format. Cutover appends one consistent
// snapshot of every shared table as ordinary stage 1 row changes. Rollback
// fences the shared writers and replays the committed changes into a new
// single-queue database with the same replay code every member uses.

var (
	errSharedCopy = errors.New("this database is a shared queue copy or was cut over to the shared queue; " +
		"starting it as a single queue would lose acknowledged shared work (restore with myce queue rollback)")
	// ErrSharedFilesUnacknowledged means file verification found problems and
	// the caller did not acknowledge that exact report.
	ErrSharedFilesUnacknowledged = errors.New("file references are missing or do not match their bytes; nothing was changed")
)

// SharedQueueMigration configures myce queue cutover and rollback. The paths
// are the queue's own database, checkpoint root and file store.
type SharedQueueMigration struct {
	Postgres, Database, Checkpoints, FileStore string
	// PostgresPasswordFile is a private file holding the authority password.
	PostgresPasswordFile string
	Clock                Clock
	// Acknowledge accepts exactly the reported set of file problems.
	Acknowledge string
	// VerifyOnly opens the database, reports file problems and changes no
	// shared state. Use it on a stopped queue or a consistent copy.
	VerifyOnly bool
}

// SharedFileProblem is one durable reference whose bytes are missing from the
// file store or do not match its SHA-256 and size. Verification never deletes.
type SharedFileProblem struct {
	Problem string `json:"problem"`
	FileRef
	Holder FileKeepingReference `json:"holder"`
}

type SharedFileReport struct {
	Checked  int                 `json:"checked"`
	Bytes    int64               `json:"bytes"`
	Problems []SharedFileProblem `json:"problems,omitempty"`
	// Digest identifies the problem set for --acknowledge.
	Digest string `json:"digest,omitempty"`
}

type SharedQueueMigrationResult struct {
	Files SharedFileReport `json:"files"`
	// Sequence is the last change imported by cutover or restored by rollback.
	Sequence int64 `json:"sequence"`
	// Already reports a repeated command that found its work complete.
	Already bool `json:"already,omitempty"`
}

const (
	sharedImportChunkBytes = 1 << 20 // Keeps catch-up progress per change small.
	sharedCutoverOwner     = "cutover"
	sharedRollbackOwner    = "rollback:"
)

// A fenced turn never expires, so no member can take it or renew its own.
var sharedFenceExpiry = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

func (cfg SharedQueueMigration) validate() error {
	if cfg.Database == "" || cfg.Checkpoints == "" || cfg.FileStore == "" || cfg.Clock == nil {
		return errors.New("shared queue migration requires --db, --checkpoints and --file-store")
	}
	if !cfg.VerifyOnly && cfg.Postgres == "" {
		return errors.New("set --shared-authority or MYCE_SHARED_QUEUE_PG_DSN to the PostgreSQL authority")
	}
	return nil
}

// SharedQueueCutover imports the stopped single queue into a fresh PostgreSQL
// authority and fences the old database. Holding the database lock proves the
// old writer is gone. Rerunning after an interruption completes the same
// import, or reports that it already finished.
func SharedQueueCutover(ctx context.Context, cfg SharedQueueMigration) (SharedQueueMigrationResult, error) {
	var out SharedQueueMigrationResult
	if err := cfg.validate(); err != nil {
		return out, err
	}
	if _, err := os.Stat(cfg.Database); err != nil {
		return out, err
	}
	lock, err := lockQueueDatabase(cfg.Database)
	if err != nil {
		return out, fmt.Errorf("stop the single queue before cutover: %w", err)
	}
	defer lock.Close()
	fenced, identity, sequence, err := sharedCutoverState(cfg.Database)
	if err != nil {
		return out, err
	}
	if cfg.VerifyOnly {
		if fenced {
			return out, errSharedCopy
		}
		out.Files, err = verifySharedFileReferences(ctx, cfg, lock)
		return out, err
	}
	authority, err := openSharedQueueAuthority(ctx, cfg.Postgres, cfg.PostgresPasswordFile)
	if err != nil {
		return out, err
	}
	defer authority.db.Close()
	if identity != "" {
		if identity != authority.identity {
			return out, errors.New("this database was cut over to a different PostgreSQL authority")
		}
		out.Sequence, out.Already = sequence, true
		return out, nil
	}
	if !fenced {
		turn, err := authority.current(ctx)
		if err != nil {
			return out, err
		}
		if turn.head != 0 {
			return out, errors.New("the PostgreSQL authority already holds queue changes; cut over into a fresh database")
		}
		out.Files, err = verifySharedFileReferences(ctx, cfg, lock)
		if err != nil {
			return out, err
		}
		if len(out.Files.Problems) != 0 && cfg.Acknowledge != out.Files.Digest {
			return out, ErrSharedFilesUnacknowledged
		}
	}
	db, err := openQueueSQLite(cfg.Database)
	if err != nil {
		return out, err
	}
	defer db.Close()
	// The cursor table alone makes single-queue startup refuse this database,
	// and an empty one makes a shared member refuse it too until bound below.
	if _, err := db.ExecContext(ctx, sharedQueueCursorSchema); err != nil {
		return out, err
	}
	out.Sequence, out.Already, err = authority.importSnapshot(ctx, db)
	if err != nil {
		return out, err
	}
	// Bound at the imported head, the old database is a valid member copy.
	_, err = db.ExecContext(ctx, "INSERT INTO myce_shared_cursor(id,identity,sequence) VALUES(1,?,?)", authority.identity, out.Sequence)
	return out, err
}

func sharedCutoverState(path string) (fenced bool, identity string, sequence int64, err error) {
	db, err := openQueueSQLite(path)
	if err != nil {
		return false, "", 0, err
	}
	defer db.Close()
	if err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='myce_shared_cursor')").Scan(&fenced); err != nil || !fenced {
		return fenced, "", 0, err
	}
	err = db.QueryRow("SELECT identity,sequence FROM myce_shared_cursor WHERE id=1").Scan(&identity, &sequence)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return fenced, identity, sequence, err
}

// importSnapshot commits the whole snapshot in one PostgreSQL transaction
// while holding the coordinator row, so members see none of it or all of it.
func (a *sharedQueueAuthority) importSnapshot(ctx context.Context, db *sql.DB) (int64, bool, error) {
	read, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, false, err
	}
	defer read.Rollback()
	tx, err := a.begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	turn, now, err := lockedSharedTurn(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	number := turn.number + 1
	var sequence int64
	already := false
	err = exportSharedSnapshot(ctx, read, func(payload []byte) error {
		sequence++
		digest := sha256.Sum256(payload)
		operation := fmt.Sprintf("%s:%d:%x", sharedCutoverOwner, sequence, digest)
		var found int64
		switch err := tx.QueryRowContext(ctx, "SELECT sequence FROM myce_changes_v1 WHERE operation=$1", operation).Scan(&found); {
		case err == nil && found == sequence:
			if sequence == 1 {
				already = true
			}
			if already {
				return nil
			}
			return errSharedHead
		case err == nil:
			return errSharedHead
		case !errors.Is(err, sql.ErrNoRows):
			return err
		case already:
			// The earlier import committed in one transaction, so any
			// difference means the database changed after its fence.
			return errSharedHead
		}
		if sequence == 1 {
			if turn.head != 0 {
				return errors.New("the PostgreSQL authority already holds queue changes; cut over into a fresh database")
			}
			if turn.expires.After(now) {
				return errors.New("a shared member holds the coordinator turn; stop shared members before cutover")
			}
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO myce_changes_v1(sequence,handover,operation,payload) VALUES($1,$2,$3,$4)",
			sequence, number, operation, payload)
		return err
	})
	if err != nil || already {
		return sequence, already, err
	}
	// Leave the turn expired and unready, so the first member takes it at once.
	if _, err := tx.ExecContext(ctx, `UPDATE myce_coordinator_v1 SET owner=$1,endpoint='',handover=$2,head=$3,expires=$4,ready=false WHERE id=1`,
		sharedCutoverOwner, number, sequence, now); err != nil {
		return 0, false, err
	}
	return sequence, false, tx.Commit()
}

// exportSharedSnapshot reads every shared table in key order inside one read
// transaction. The same database always yields the same payloads.
func exportSharedSnapshot(ctx context.Context, tx *sql.Tx, emit func([]byte) error) error {
	names := make([]string, 0, len(sharedQueueTables))
	for name := range sharedQueueTables {
		names = append(names, name)
	}
	slices.Sort(names)
	chunk := sharedQueueRows{Version: 1}
	size, emitted := 0, 0
	flush := func() error {
		payload, err := json.Marshal(chunk)
		if err != nil {
			return err
		}
		chunk.Rows, size = nil, 0
		emitted++
		return emit(payload)
	}
	for _, name := range names {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?)", name).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			continue
		}
		table := sharedQueueTables[name]
		// Fixed table and column names, never values.
		rows, err := tx.QueryContext(ctx, "SELECT "+strings.Join(table.columns, ",")+" FROM "+name+" ORDER BY "+strings.Join(table.keys, ","))
		if err != nil {
			return err
		}
		src := make([]any, len(table.columns))
		dest := make([]any, len(src))
		for i := range src {
			dest[i] = &src[i]
		}
		for rows.Next() {
			if err = rows.Scan(dest...); err != nil {
				break
			}
			var values []sharedQueueValue
			if values, err = sharedQueueValues(src); err != nil {
				break
			}
			chunk.Rows = append(chunk.Rows, sharedQueueRow{Table: name, Values: values})
			for _, v := range values {
				size += 16 + len(v.Bytes)
			}
			if size >= sharedImportChunkBytes {
				if err = flush(); err != nil {
					break
				}
			}
		}
		if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
			return err
		}
	}
	if len(chunk.Rows) != 0 || emitted == 0 {
		return flush()
	}
	return nil
}

// SharedQueueRollback fences every shared writer, then restores the latest
// committed shared state into a new single-queue database at cfg.Database.
// It never reads an older database or backup.
func SharedQueueRollback(ctx context.Context, cfg SharedQueueMigration) (SharedQueueMigrationResult, error) {
	var out SharedQueueMigrationResult
	if err := cfg.validate(); err != nil {
		return out, err
	}
	if cfg.VerifyOnly {
		return out, errors.New("rollback has no verify-only mode")
	}
	for _, path := range []string{cfg.Database, cfg.Database + "-wal"} {
		if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
			return out, fmt.Errorf("%s already exists; rollback restores the latest shared commit into a new database path. "+
				"Never start an older pre-cutover database or backup: it would lose acknowledged shared work", path)
		}
	}
	authority, err := openSharedQueueAuthority(ctx, cfg.Postgres, cfg.PostgresPasswordFile)
	if err != nil {
		return out, err
	}
	defer authority.db.Close()
	head, err := authority.fenceShared(ctx)
	if err != nil {
		return out, err
	}
	if head == 0 {
		return out, errors.New("the PostgreSQL authority holds no committed queue changes")
	}
	out.Sequence = head
	// The working directory is this command's own rebuildable output.
	work := cfg.Database + ".rollback"
	if err := os.RemoveAll(work); err != nil {
		return out, err
	}
	// The single queue starts only in marked state roots.
	if err := PrepareStateRoots(filepath.Dir(cfg.Database), work); err != nil {
		return out, err
	}
	defer os.RemoveAll(work)
	staged := filepath.Join(work, filepath.Base(cfg.Database))
	if err := authority.restore(ctx, staged, head); err != nil {
		return out, err
	}
	verify := cfg
	verify.Database = staged
	if out.Files, err = verifySharedFileReferences(ctx, verify, nil); err != nil {
		return out, err
	}
	if len(out.Files.Problems) != 0 && cfg.Acknowledge != out.Files.Digest {
		return out, ErrSharedFilesUnacknowledged
	}
	// One self-contained file, published by an atomic rename.
	db, err := openQueueSQLite(staged)
	if err != nil {
		return out, err
	}
	_, err = db.ExecContext(ctx, "PRAGMA journal_mode=DELETE")
	if err = errors.Join(err, db.Close()); err != nil {
		return out, err
	}
	if err := os.Rename(staged, cfg.Database); err != nil {
		return out, err
	}
	return out, syncWorkerDir(filepath.Dir(cfg.Database))
}

// fenceShared takes the coordinator turn permanently. The current owner can
// no longer renew or append, and no member can acquire it again.
func (a *sharedQueueAuthority) fenceShared(ctx context.Context) (int64, error) {
	tx, err := a.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	turn, _, err := lockedSharedTurn(ctx, tx)
	if err != nil {
		return 0, err
	}
	if !strings.HasPrefix(turn.owner, sharedRollbackOwner) {
		if _, err := tx.ExecContext(ctx, `UPDATE myce_coordinator_v1 SET owner=$1,endpoint='',handover=handover+1,expires=$2,ready=false WHERE id=1`,
			sharedRollbackOwner+ID(), sharedFenceExpiry); err != nil {
			return 0, err
		}
	}
	return turn.head, tx.Commit()
}

// restore builds a single-queue database at path from the newest snapshot,
// if changes were pruned, and the changes after it through head. While
// the replica cursor exists, single-queue startup refuses the partial copy.
func (a *sharedQueueAuthority) restore(ctx context.Context, path string, head int64) error {
	db, err := openQueueSQLite(path)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, schema := range []string{sharedQueueCursorSchema, memoryEvidenceSchema, gpuSharingSchema} {
		if _, err := db.ExecContext(ctx, schema); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO myce_shared_cursor(id,identity,sequence) VALUES(1,?,0)", a.identity); err != nil {
		return err
	}
	// Changes at or below the floor were pruned; start from the snapshot.
	turn, err := a.current(ctx)
	if err != nil {
		return err
	}
	var start int64
	if turn.floor > 0 {
		if start, err = a.rebuild(ctx, db, 0); err != nil {
			return err
		}
	}
	if err := a.changes(ctx, start, head, func(c sharedQueueChange) error {
		return replaySharedQueueChange(ctx, db, c)
	}); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "DROP TABLE myce_shared_cursor")
	return err
}

// verifySharedFileReferences opens the queue database once to read every
// retained file reference, then hashes the store's bytes outside any lock.
// Worker-local custody and the configured model catalog are not store bytes.
func verifySharedFileReferences(ctx context.Context, cfg SharedQueueMigration, lock *os.File) (SharedFileReport, error) {
	var report SharedFileReport
	q, err := openQueue(cfg.Database, cfg.Checkpoints, cfg.Clock, nil, lock, WithFileStore(cfg.FileStore, 100<<30, true))
	if err != nil {
		return report, err
	}
	holders := map[FileRef][]FileKeepingReference{}
	q.mu.RLock()
	q.collectFileReferences(false, nil, func(ref FileRef, holder FileKeepingReference) {
		if holder.Kind != "worker_local_custody" && holder.Kind != "model_catalog" {
			holders[ref] = append(holders[ref], holder)
		}
	})
	store := q.files
	q.mu.RUnlock()
	if err := q.Close(); err != nil {
		return report, err
	}
	refs := make([]FileRef, 0, len(holders))
	for ref := range holders {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b FileRef) int {
		return strings.Compare(a.SHA256+fmt.Sprint(a.SizeBytes), b.SHA256+fmt.Sprint(b.SizeBytes))
	})
	digest := sha256.New()
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.Checked++
		report.Bytes += ref.SizeBytes
		err := store.verify(ctx, ref)
		if err == nil {
			continue
		}
		problem := "mismatch"
		if errors.Is(err, fs.ErrNotExist) {
			problem = "missing"
		}
		owners := holders[ref]
		slices.SortFunc(owners, func(a, b FileKeepingReference) int {
			return strings.Compare(a.JobID+"\x00"+a.Kind+"\x00"+a.Name, b.JobID+"\x00"+b.Kind+"\x00"+b.Name)
		})
		for _, holder := range slices.Compact(owners) {
			report.Problems = append(report.Problems, SharedFileProblem{Problem: problem, FileRef: ref, Holder: holder})
			fmt.Fprintf(digest, "%s %s %d %s %s %s\n", problem, ref.SHA256, ref.SizeBytes, holder.Kind, holder.JobID, holder.Name)
		}
	}
	if len(report.Problems) != 0 {
		report.Digest = hex.EncodeToString(digest.Sum(nil))[:16]
	}
	return report, nil
}
