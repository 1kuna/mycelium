package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "modernc.org/sqlite"
)

// liveDatabaseBytes is the queue database size read on 2026-11-25
// (docs/design-queue-beyond-one-host.md, "What we measured").
const liveDatabaseBytes = 8_157_110_272

// replayBatch matches the stage 2 member's pull batch.
const replayBatch = 64

const snapshotPrefix = "myce-qual-snapshot"

// A snapshot this tool creates gets an ownership record beside it, written
// before the snapshot exists. The record holds the snapshot's first bytes,
// which come from a fresh random seed, so cleanup removes a snapshot only
// when its content proves this tool wrote it.
const (
	snapshotOwnerSuffix = ".queue-qualify-owner"
	snapshotOwnerHeader = "queue-qualify snapshot "
	snapshotTokenBytes  = 32
)

// snapshotToken is the first bytes writeLoadFile produces from this seed.
func snapshotToken(seed uint64) []byte {
	rng := rand.New(rand.NewPCG(seed, 5))
	token := make([]byte, 0, snapshotTokenBytes)
	for len(token) < snapshotTokenBytes {
		v := rng.Uint64()
		for i := 0; i < 8; i++ {
			token = append(token, byte(v>>(8*i)))
		}
	}
	return token
}

// removeSnapshot deletes a snapshot and its ownership record when the record
// names it and its first bytes match. It reports false for anything else.
func removeSnapshot(path string) (bool, error) {
	owner := path + snapshotOwnerSuffix
	record, err := os.ReadFile(owner)
	if err != nil {
		return false, nil
	}
	fields := strings.Fields(strings.TrimPrefix(string(record), snapshotOwnerHeader))
	if !strings.HasPrefix(string(record), snapshotOwnerHeader) || len(fields) != 2 || fields[0] != filepath.Base(path) {
		return false, nil
	}
	token, err := hex.DecodeString(fields[1])
	if err != nil || len(token) != snapshotTokenBytes {
		return false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	head := make([]byte, snapshotTokenBytes)
	n, _ := io.ReadFull(f, head)
	f.Close()
	// A run killed before its first block leaves an empty file; that file was
	// still created by the run that wrote the record.
	if string(head[:n]) != string(token[:n]) {
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		return false, err
	}
	return true, os.Remove(owner)
}

type snapshotResults struct {
	Bytes          int64   `json:"bytes"`
	Seconds        float64 `json:"seconds"`
	MiBps          float64 `json:"MiBps"`
	SHA256         string  `json:"sha256"`
	CreatedInRun   bool    `json:"created_in_run"`
	ProjectedLiveS float64 `json:"projected_seconds_for_live_database"`
}

type replayResults struct {
	Changes        int     `json:"changes"`
	Bytes          int64   `json:"bytes"`
	SeedSeconds    float64 `json:"seed_seconds"`
	FetchSeconds   float64 `json:"fetch_seconds"`
	ApplySeconds   float64 `json:"apply_seconds"`
	Seconds        float64 `json:"seconds"`
	ChangesPerSec  float64 `json:"changes_per_second"`
	MBps           float64 `json:"MBps"`
	LiveRate       float64 `json:"live_rate"`
	SpeedupOnLive  float64 `json:"speedup_over_live"`
	OneHourReplayS float64 `json:"seconds_to_replay_one_hour_of_changes"`
}

type catchupResults struct {
	Snapshot *snapshotResults `json:"snapshot,omitempty"`
	Replay   *replayResults   `json:"replay,omitempty"`
}

func cmdCatchup(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("catchup", out)
	cf := addConnFlags(fs)
	o := addOutputFlags(fs)
	snapshot := fs.String("snapshot", "", "snapshot file on the NAS share to copy and hash (named myce-qual-snapshot*)")
	createBytes := fs.String("create-bytes", "", "create the snapshot file with this many random bytes if it is missing")
	createOnly := fs.Bool("create-only", false, "create the snapshot file and stop; measure from a different machine so no cache helps")
	minFree := fs.String("min-free", "100GiB", "free space the snapshot creation must leave on the share")
	localDir := fs.String("local-dir", os.TempDir(), "local directory for the copied snapshot and the SQLite copy")
	localMinFree := fs.String("local-min-free", "40GiB", "free space to leave on the local disk")
	changes := fs.Int("changes", 6960, "change-log entries to replay (6960 is ten minutes at 11.6/s; 0 skips)")
	rate := fs.Float64("rate", defaultRate, "live append rate the replay must outrun")
	profile := fs.String("profile", "queue", "payload sizes: queue or fixed:BYTES")
	if err := fs.Parse(args); err != nil {
		return err
	}
	prof, err := parseProfile(*profile)
	if err != nil {
		return err
	}
	localFloor, err := parseBytes(*localMinFree)
	if err != nil {
		return err
	}
	shareFloor, err := parseBytes(*minFree)
	if err != nil {
		return err
	}
	res := &catchupResults{}
	r := newReport("catchup", o)
	if *snapshot != "" {
		if !strings.HasPrefix(filepath.Base(*snapshot), snapshotPrefix) {
			return fmt.Errorf("--snapshot must be named %s* so cleanup can find it", snapshotPrefix)
		}
		var size int64
		if *createBytes != "" {
			if size, err = parseBytes(*createBytes); err != nil {
				return err
			}
		}
		created, err := ensureSnapshot(ctx, *snapshot, size, uint64(shareFloor))
		if err != nil {
			return err
		}
		if *createOnly {
			fmt.Fprintf(out, "snapshot ready (created: %v)\n", created)
			return nil
		}
		if res.Snapshot, err = copySnapshot(ctx, *snapshot, *localDir, uint64(localFloor)); err != nil {
			return err
		}
		res.Snapshot.CreatedInRun = created
	}
	if *changes > 0 {
		t, err := cf.load()
		if err != nil {
			return err
		}
		if res.Replay, err = replayLog(ctx, t, *localDir, *changes, prof, *rate); err != nil {
			return err
		}
	}
	if res.Snapshot == nil && res.Replay == nil {
		return errors.New("nothing to measure: pass --snapshot, --changes or both")
	}
	r.Params["profile"] = prof.Name
	r.Params["live_rate"] = *rate
	r.Results = res
	r.Checks = catchupChecks(res, prof, *rate)
	r.md = func(b *strings.Builder) { catchupMarkdown(b, res) }
	return r.finish(out)
}

func ensureSnapshot(ctx context.Context, path string, size int64, floor uint64) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, probeErr(err)
	}
	if size <= 0 {
		return false, errors.New("the snapshot is missing; pass --create-bytes to create it")
	}
	avail, total, err := statfsFree(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	floor = max(floor, uint64(0.10*float64(total)))
	if avail < floor+uint64(size) {
		return false, fmt.Errorf("refusing: %.1f GiB free, need %.1f GiB above a %.1f GiB floor",
			float64(avail)/(1<<30), float64(size)/(1<<30), float64(floor)/(1<<30))
	}
	seed := rand.Uint64()
	owner := path + snapshotOwnerSuffix
	record, err := os.OpenFile(owner, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return false, probeErr(err)
	}
	_, err = fmt.Fprintf(record, "%s%s %s\n", snapshotOwnerHeader, filepath.Base(path), hex.EncodeToString(snapshotToken(seed)))
	if err == nil {
		err = record.Sync()
	}
	record.Close()
	if err == nil {
		err = writeLoadFile(ctx, path, size, make([]byte, loadBlock), rand.New(rand.NewPCG(seed, 5)), newRateLimiter(0), func(int64) {})
		// O_EXCL failing means someone else's file appeared; never remove it.
		if err != nil && !errors.Is(err, fs.ErrExist) {
			os.Remove(path)
		}
	}
	if err != nil {
		os.Remove(owner)
		return false, probeErr(err)
	}
	return true, nil
}

func copySnapshot(ctx context.Context, src, localDir string, floor uint64) (*snapshotResults, error) {
	info, err := os.Stat(src)
	if err != nil {
		return nil, probeErr(err)
	}
	avail, _, err := statfsFree(localDir)
	if err != nil {
		return nil, err
	}
	if avail < floor+uint64(info.Size()) {
		return nil, fmt.Errorf("refusing: the local disk has %.1f GiB free, need %.1f GiB above %.1f GiB",
			float64(avail)/(1<<30), float64(info.Size())/(1<<30), float64(floor)/(1<<30))
	}
	in, err := os.Open(src)
	if err != nil {
		return nil, probeErr(err)
	}
	defer in.Close()
	outFile, err := os.CreateTemp(localDir, snapshotPrefix+"-copy-")
	if err != nil {
		return nil, probeErr(err)
	}
	defer os.Remove(outFile.Name())
	defer outFile.Close()
	h := sha256.New()
	start := time.Now()
	n, err := io.CopyBuffer(io.MultiWriter(outFile, h), readerCtx{ctx, in}, make([]byte, loadBlock))
	if err == nil {
		err = outFile.Sync()
	}
	if err != nil {
		return nil, probeErr(err)
	}
	s := &snapshotResults{Bytes: n, Seconds: time.Since(start).Seconds(), SHA256: hex.EncodeToString(h.Sum(nil))}
	if s.Seconds > 0 {
		s.MiBps = float64(n) / (1 << 20) / s.Seconds
		s.ProjectedLiveS = float64(liveDatabaseBytes) / (float64(n) / s.Seconds)
	}
	return s, nil
}

type readerCtx struct {
	ctx context.Context
	r   io.Reader
}

func (r readerCtx) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// replayLog seeds a copy of the change log, then times what a returning
// member does: fetch ordered batches and apply each change in its own SQLite
// transaction with the cursor, as stage 2's replay does.
func replayLog(ctx context.Context, t *target, localDir string, n int, prof payloadProfile, rate float64) (*replayResults, error) {
	conn, err := t.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	if err := prepare(ctx, conn); err != nil {
		return nil, err
	}
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", catchupLockKey).Scan(&locked); err != nil {
		return nil, err
	}
	if !locked {
		return nil, errors.New("another queue-qualify catchup run holds this database")
	}
	defer conn.Exec(context.Background(), "TRUNCATE myce_qualification.catchup_changes_v1")
	res := &replayResults{Changes: n, LiveRate: rate}
	pool := newPayloadPool(prof, 42)
	seedStart := time.Now()
	if _, err := conn.Exec(ctx, "TRUNCATE myce_qualification.catchup_changes_v1"); err != nil {
		return nil, err
	}
	seq := 0
	_, err = conn.CopyFrom(ctx, pgx.Identifier{qualSchema, "catchup_changes_v1"}, []string{"sequence", "handover", "operation", "payload"},
		pgx.CopyFromFunc(func() ([]any, error) {
			if seq == n {
				return nil, nil
			}
			seq++
			return []any{int64(seq), int64(1), fmt.Sprintf("catchup-%d", seq), pool.next()}, nil
		}))
	if err != nil {
		return nil, err
	}
	res.SeedSeconds = time.Since(seedStart).Seconds()

	dbPath := filepath.Join(localDir, fmt.Sprintf("queue-qualify-replica-%d.sqlite", time.Now().UnixNano()))
	defer func() {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			os.Remove(dbPath + suffix)
		}
	}()
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `CREATE TABLE cursor(id INTEGER PRIMARY KEY CHECK (id = 1), sequence INTEGER NOT NULL);
		INSERT INTO cursor VALUES (1, 0);
		CREATE TABLE rows(tbl TEXT NOT NULL, key INTEGER NOT NULL, value BLOB NOT NULL, PRIMARY KEY (tbl, key)) WITHOUT ROWID;`); err != nil {
		return nil, err
	}
	start := time.Now()
	var fetch, apply time.Duration
	for after := 0; after < n; {
		through := min(n, after+replayBatch)
		began := time.Now()
		rows, err := conn.Query(ctx, `SELECT sequence,payload FROM myce_qualification.catchup_changes_v1
			WHERE sequence > $1 AND sequence <= $2 ORDER BY sequence`, after, through)
		if err != nil {
			return nil, err
		}
		type change struct {
			seq     int64
			payload []byte
		}
		var batch []change
		for rows.Next() {
			var c change
			if err := rows.Scan(&c.seq, &c.payload); err != nil {
				rows.Close()
				return nil, err
			}
			batch = append(batch, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		fetch += time.Since(began)
		began = time.Now()
		for _, c := range batch {
			if c.seq != int64(after)+1 {
				return nil, errHead
			}
			if err := applyChange(ctx, db, c.seq, c.payload); err != nil {
				return nil, err
			}
			res.Bytes += int64(len(c.payload))
			after++
		}
		apply += time.Since(began)
		if len(batch) == 0 {
			return nil, errHead
		}
	}
	res.Seconds = time.Since(start).Seconds()
	res.FetchSeconds, res.ApplySeconds = fetch.Seconds(), apply.Seconds()
	if res.Seconds > 0 {
		res.ChangesPerSec = float64(n) / res.Seconds
		res.MBps = float64(res.Bytes) / 1e6 / res.Seconds
		res.SpeedupOnLive = res.ChangesPerSec / rate
		res.OneHourReplayS = 3600 * rate / res.ChangesPerSec
	}
	return res, nil
}

func applyChange(ctx context.Context, db *sql.DB, seq int64, payload []byte) error {
	var doc rowsDoc
	if err := json.Unmarshal(payload, &doc); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE cursor SET sequence=? WHERE id=1 AND sequence=?", seq, seq-1); err != nil {
		return err
	}
	for _, row := range doc.Rows {
		var key int64
		var value []byte
		for _, v := range row.Values {
			if v.Kind == "int" {
				key = v.Int % 50000 // mostly updates of existing rows, as in a live queue
			} else {
				value = v.Bytes
			}
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO rows(tbl,key,value) VALUES(?,?,?) ON CONFLICT(tbl,key) DO UPDATE SET value=excluded.value", row.Table, key, value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// replaySpeedup is the margin a returning member needs over live appends:
// at 10x, an hour asleep costs six minutes of replay, and a member that has
// fallen behind always converges.
const replaySpeedup = 10

func catchupChecks(res *catchupResults, prof payloadProfile, rate float64) []check {
	var cs []check
	if r := res.Replay; r != nil {
		liveBytes := rate * prof.mean() / 1e6
		ok := r.ChangesPerSec >= replaySpeedup*rate && r.MBps >= replaySpeedup*liveBytes
		st := statusFail
		if ok {
			st = statusPass
		}
		cs = append(cs, check{Name: "replay outruns live appends", Status: st,
			Measured: fmt.Sprintf("%.0f changes/s, %.2f MB/s (%.1fx live)", r.ChangesPerSec, r.MBps, r.SpeedupOnLive),
			Limit:    fmt.Sprintf(">= %dx live: %.0f changes/s and %.2f MB/s", replaySpeedup, replaySpeedup*rate, replaySpeedup*liveBytes),
			Prevents: "a returning member that never converges, or a sole survivor that waits hours of replay before it can coordinate"})
	}
	if s := res.Snapshot; s != nil {
		cs = append(cs, check{Name: "full snapshot transfer at today's size", Status: statusInfo,
			Measured: fmt.Sprintf("%.0f s projected (%.0f MiB/s)", s.ProjectedLiveS, s.MiBps), Limit: "decision input: snapshot cadence and cold-start pause",
			Prevents: "choosing snapshot cadence without the cost of a cold copy"})
		if s.CreatedInRun {
			cs = append(cs, check{Name: "snapshot read was cold", Status: statusWarn, Measured: "created by this run",
				Limit: "create with --create-only on another machine", Prevents: "a client cache inflating the transfer rate"})
		}
	}
	return cs
}

func catchupMarkdown(b *strings.Builder, res *catchupResults) {
	if s := res.Snapshot; s != nil {
		fmt.Fprintf(b, "\n## Snapshot transfer\n\n%.2f GiB in %.1f s (%.1f MiB/s), hashed while copying. Projected for the %.2f GiB live database: %.0f s.\n",
			float64(s.Bytes)/(1<<30), s.Seconds, s.MiBps, float64(liveDatabaseBytes)/(1<<30), s.ProjectedLiveS)
	}
	if r := res.Replay; r != nil {
		fmt.Fprintf(b, "\n## Change-log replay\n\n%d changes (%.1f MB) in %.2f s: fetch %.2f s, apply %.2f s; %.0f changes/s, %.2f MB/s, %.1fx the live rate. One hour of live changes replays in %.0f s. Seeding took %.1f s (not counted).\n",
			r.Changes, float64(r.Bytes)/1e6, r.Seconds, r.FetchSeconds, r.ApplySeconds, r.ChangesPerSec, r.MBps, r.SpeedupOnLive, r.OneHourReplayS, r.SeedSeconds)
		b.WriteString("\nApply cost approximates stage 2: one SQLite FULL transaction per change with the cursor, JSON decode and keyed upserts, not the real table set.\n")
	}
}
