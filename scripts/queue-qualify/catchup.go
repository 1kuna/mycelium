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

// snapshotFile is the name of a created snapshot inside its run directory.
const snapshotFile = "snapshot.bin"

type snapshotResults struct {
	Bytes          int64   `json:"bytes"`
	Seconds        float64 `json:"seconds"`
	MiBps          float64 `json:"MiBps"`
	SHA256         string  `json:"sha256"`
	CreatedInRun   bool    `json:"created_in_run"`
	ProjectedLiveS float64 `json:"projected_seconds_for_live_database"`
}

type replayResults struct {
	Run            string  `json:"run"`
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
	snapshot := fs.String("snapshot", "", "existing snapshot file on the NAS share to copy and hash (read only)")
	createIn := fs.String("create-in", "", "create a snapshot in a new run directory inside this share directory; the storage owner removes it NAS-local")
	createBytes := fs.String("create-bytes", "", "size of the snapshot --create-in writes")
	createOnly := fs.Bool("create-only", false, "create the snapshot and stop; measure from a different machine so no cache helps")
	minFree := fs.String("min-free", "100GiB", "free space the snapshot creation must leave on the share")
	localDir := fs.String("local-dir", os.TempDir(), "local directory for the copied snapshot and the SQLite copy; it must be a local disk, since these scratch files are removed")
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
	if *snapshot != "" && *createIn != "" {
		return errors.New("pass --snapshot to measure an existing snapshot or --create-in to create one, not both")
	}
	if *createIn == "" && (*createBytes != "" || *createOnly) {
		return errors.New("--create-bytes and --create-only need --create-in")
	}
	if *createOnly && *changes > 0 {
		*changes = 0
	}
	// Scratch files on the measuring machine are removed after use, so they
	// must never land on the share: the harness deletes nothing on the NAS.
	if err := requireLocalDisk(*localDir); err != nil {
		return err
	}
	res := &catchupResults{}
	r := newReport("catchup", o)
	created := false
	if *createIn != "" {
		size, err := parseBytes(*createBytes)
		if err != nil || size <= 0 {
			return errors.New("--create-in needs a positive --create-bytes")
		}
		made, err := createSnapshot(ctx, *createIn, size, shareFloor)
		if made != nil {
			fmt.Fprintln(out, made.leftover())
		}
		if err != nil {
			return err
		}
		*snapshot, created = filepath.Join(made.path, snapshotFile), true
		fmt.Fprintf(out, "snapshot: %s\n", *snapshot)
		if *createOnly {
			return nil
		}
	}
	if *snapshot != "" {
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

// createSnapshot writes one file of random bytes into a new run directory
// with every bound the load generator has: the byte cap, the free-space
// floor watched during the write, and a bounded stop. It never deletes; a
// partial snapshot stays for the storage owner.
func createSnapshot(ctx context.Context, parent string, size, floor int64) (*loadResults, error) {
	res, err := runLoad(ctx, loadOptions{dir: parent, kind: "snapshot", oneFile: snapshotFile, maxBytes: size,
		minFree: floor, minFreePct: 10, duration: maxLoadRuntime, writers: 1, free: statfsFree, create: createExclusive})
	if err != nil {
		return res, err
	}
	if res.StoppedBy != "max-bytes" || res.FilesComplete != 1 {
		return res, fmt.Errorf("snapshot creation stopped by %s before it finished", res.StoppedBy)
	}
	return res, nil
}

// requireLocalDisk refuses a scratch directory on a network mount. The
// harness removes its own scratch files after a measurement, and it must
// never delete anything on the NAS.
func requireLocalDisk(dir string) error {
	local, err := isLocalDisk(dir)
	if err != nil {
		return fmt.Errorf("--local-dir: %w", err)
	}
	if !local {
		return errors.New("--local-dir is on a network mount; use a directory on the measuring machine's own disk")
	}
	return nil
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
	outFile, err := os.CreateTemp(localDir, "queue-qualify-snapshot-copy-")
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
	conn, err := t.open(ctx, true, catchupLockKey)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	// Each run seeds its own rows under a fresh run ID and never deletes them;
	// the storage owner removes the whole qualification database afterwards.
	run := "cu-" + newRunID()
	res := &replayResults{Run: run, Changes: n, LiveRate: rate}
	pool := newPayloadPool(prof, 42)
	seedStart := time.Now()
	seq := 0
	_, err = conn.CopyFrom(ctx, pgx.Identifier{qualSchema, "catchup_changes_v1"}, []string{"run", "sequence", "handover", "operation", "payload"},
		pgx.CopyFromFunc(func() ([]any, error) {
			if seq == n {
				return nil, nil
			}
			seq++
			return []any{run, int64(seq), int64(1), fmt.Sprintf("catchup-%d", seq), pool.next()}, nil
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
			WHERE run = $1 AND sequence > $2 AND sequence <= $3 ORDER BY sequence`, run, after, through)
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
