package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// The ledger is the client's record of what the server acknowledged. Each
// line is written and synced only after COMMIT returns:
//
//	H {json}                          header: run, system identifier, timeline
//	A <seq> <sha> <started> <lsn>     acknowledged under the server start time
//	                                  <started>, at or after WAL position <lsn>
//	U <seq> <sha>                     COMMIT failed with an unknown outcome
//	F <seq>                           failed before COMMIT; cannot exist
//	E <lsn>                           the end marker, acknowledged after every
//	                                  write, at or after WAL position <lsn>
//
// Binding each acknowledgement to the server start it was written under lets
// verify require that at least one came before the restart it observes. The
// WAL positions let verify refuse a server whose WAL is behind what was
// acknowledged: a restored backup or an older copy of the data directory.
type ledgerHeader struct {
	Run              string    `json:"run"`
	Started          time.Time `json:"started"`
	SystemIdentifier string    `json:"system_identifier,omitempty"`
	Timeline         int       `json:"timeline,omitempty"`
	ServerStarted    time.Time `json:"server_started,omitzero"`
}

// endMarkerSeq is the end marker's row. Its payload is large enough that the
// WAL it writes cannot be covered by the few records a restart adds, so a
// copy taken before it is always behind its position.
const (
	endMarkerSeq   = 0
	endMarkerBytes = 1 << 20
)

type outage struct {
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Seconds float64   `json:"seconds"`
	Kind    string    `json:"first_error"`
}

type writeResults struct {
	Run              string       `json:"run"`
	Acknowledged     int          `json:"acknowledged"`
	Uncertain        int          `json:"uncertain"`
	Failed           int          `json:"failed"`
	EndMarker        bool         `json:"end_marker_acknowledged"`
	Outages          []outage     `json:"outages"`
	Commit           latencyStats `json:"commit"`
	IdentityChanged  bool         `json:"system_identifier_changed"`
	ServerRestarts   int          `json:"server_restarts_seen"`
	StoppedBy        string       `json:"stopped_by"`
	LongestOutageSec float64      `json:"longest_outage_seconds"`
}

type durabilityOptions struct {
	run                 string
	duration, maxOutage time.Duration
	rate                float64
	profile             payloadProfile
	// beforeEnd runs after the last write and before the end marker. Tests
	// use it to copy the data directory at that point.
	beforeEnd func()
}

func cmdDurabilityWrite(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("durability-write", out)
	cf := addConnFlags(fs)
	o := addOutputFlags(fs)
	ledgerPath := fs.String("ledger", "", "local ledger file to create (required; keep it for durability-verify)")
	var opt durabilityOptions
	fs.StringVar(&opt.run, "run", "", "run identifier (default: time-based)")
	fs.DurationVar(&opt.duration, "duration", 15*time.Minute, "how long to keep writing, including outages (max 2h)")
	fs.Float64Var(&opt.rate, "rate", defaultRate, "writes per second")
	fs.DurationVar(&opt.maxOutage, "max-outage", 20*time.Minute, "give up if the server stays unreachable this long")
	profile := fs.String("profile", "queue", "payload sizes: queue or fixed:BYTES")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ledgerPath == "" || opt.duration <= 0 || opt.duration > 2*time.Hour || opt.rate <= 0 || opt.rate > 1000 {
		return errors.New("--ledger is required, --duration must be in (0, 2h] and --rate in (0, 1000]")
	}
	var err error
	if opt.profile, err = parseProfile(*profile); err != nil {
		return err
	}
	if opt.run == "" {
		opt.run = "dw-" + time.Now().UTC().Format("20060102T150405Z") + "-" + newRunID()
	}
	t, err := cf.load()
	if err != nil {
		return err
	}
	ledger, err := os.OpenFile(*ledgerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create ledger: %w", err)
	}
	defer ledger.Close()
	r := newReport("durability-write", o)
	r.Params["run"] = opt.run
	r.Params["rate"] = opt.rate
	r.Params["duration"] = opt.duration.String()
	r.Params["max_outage"] = opt.maxOutage.String()
	res, info, err := durabilityWrite(ctx, t, ledger, opt)
	if err != nil {
		return err
	}
	r.Server = info
	r.Results = res
	r.Checks = []check{
		{Name: "data directory kept its identity", Status: map[bool]string{false: statusPass, true: statusFail}[res.IdentityChanged],
			Measured: fmt.Sprint(!res.IdentityChanged), Limit: "same server and database on every reconnect",
			Prevents: "a container recreated on an empty data directory silently starting a new, empty authority"},
		{Name: "writer outlived every outage", Status: map[bool]string{true: statusPass, false: statusFail}[res.StoppedBy == "duration"],
			Measured: res.StoppedBy, Limit: "duration", Prevents: "an outage longer than --max-outage hiding a server that never came back on its own"},
		{Name: "longest outage", Status: statusInfo, Measured: fmt.Sprintf("%.1f s", res.LongestOutageSec),
			Limit: "context: new starts wait while the database is down", Prevents: "planning restarts without knowing their pause"},
	}
	r.Notes = append(r.Notes, "Run durability-verify with the same ledger after the restart or reboot; this report alone proves nothing about loss.")
	r.md = func(b *strings.Builder) {
		fmt.Fprintf(b, "\n## Writes\n\nRun %s: %d acknowledged, %d uncertain, %d failed before commit; %d server restarts seen; end marker acknowledged: %v.\n",
			res.Run, res.Acknowledged, res.Uncertain, res.Failed, res.ServerRestarts, res.EndMarker)
		latencyTable(b, [][2]any{{"commit", res.Commit}})
		if len(res.Outages) > 0 {
			b.WriteString("\n| Outage start | Seconds | First error |\n| --- | ---: | --- |\n")
			for _, o := range res.Outages {
				fmt.Fprintf(b, "| %s | %.1f | %s |\n", o.Start.Format(time.RFC3339), o.Seconds, o.Kind)
			}
		}
	}
	return r.finish(out)
}

func serverStarted(ctx context.Context, conn *pgx.Conn) (time.Time, error) {
	var at time.Time
	err := conn.QueryRow(ctx, "SELECT pg_catalog.pg_postmaster_start_time()").Scan(&at)
	return at, err
}

func durabilityWrite(ctx context.Context, t *target, ledger *os.File, opt durabilityOptions) (*writeResults, *serverInfo, error) {
	conn, err := t.open(ctx, true, 0)
	if err != nil {
		return nil, nil, err
	}
	defer func() { conn.Close(context.Background()) }()
	info, err := readServerInfo(ctx, conn, t.class, 20)
	if err != nil {
		return nil, nil, err
	}
	// Verify requires the identifier, so a run that cannot read it could
	// never pass; say so before writing.
	if info.SystemIdentifier == "" || info.Timeline == 0 {
		return nil, nil, errors.New("this role cannot read pg_control_system() or pg_control_checkpoint(); connect as the qualification database's owner so verify can prove the same data directory came back")
	}
	w := bufio.NewWriter(ledger)
	header, _ := json.Marshal(ledgerHeader{Run: opt.run, Started: time.Now().UTC(), SystemIdentifier: info.SystemIdentifier,
		Timeline: info.Timeline, ServerStarted: info.StartedAt})
	if err := writeLedger(w, ledger, "H "+string(header)); err != nil {
		return nil, nil, err
	}
	res := &writeResults{Run: opt.run}
	pool := newPayloadPool(opt.profile, uint64(time.Now().UnixNano()))
	gap := rand.New(rand.NewPCG(7, 11))
	started := info.StartedAt
	var lat []float64
	var down *outage
	closeOutage := func() {
		if down != nil {
			down.End = time.Now().UTC()
			down.Seconds = down.End.Sub(down.Start).Seconds()
			res.Outages = append(res.Outages, *down)
			res.LongestOutageSec = max(res.LongestOutageSec, down.Seconds)
			down = nil
		}
	}
	// reopen replaces a closed session with a vetted one on the same server
	// and database, and notes a restart by its new start time.
	reopen := func(ctx context.Context) error {
		fresh, err := t.open(ctx, true, 0)
		if err != nil {
			return err
		}
		at, err := serverStarted(ctx, fresh)
		if err != nil {
			fresh.Close(context.Background())
			return err
		}
		if !at.Equal(started) {
			res.ServerRestarts++
			started = at
		}
		conn = fresh
		return nil
	}
	deadline := time.Now().Add(opt.duration)
	next := time.Now()
	for seq := int64(1); ; seq++ {
		next = next.Add(time.Duration(gap.ExpFloat64() / opt.rate * float64(time.Second)))
		if next.After(deadline) {
			res.StoppedBy = "duration"
			break
		}
		if !sleepUntil(ctx, next) {
			res.StoppedBy = "interrupted"
			break
		}
		if conn.IsClosed() {
			call, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := reopen(call)
			cancel()
			if errors.Is(err, errRefused) {
				// Never write to a server or database the run did not start on.
				res.IdentityChanged = errors.Is(err, errTargetChanged)
				res.StoppedBy = "refused: " + err.Error()
				break
			}
			if err != nil {
				if down == nil {
					down = &outage{Start: time.Now().UTC(), Kind: "connect"}
				}
				if time.Since(down.Start) > opt.maxOutage {
					res.StoppedBy = "max-outage"
					break
				}
				seq--
				sleepCtx(ctx, 500*time.Millisecond)
				continue
			}
		}
		payload := pool.next()
		sum := sha256.Sum256(payload)
		began := time.Now()
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		lsn, uncertain, err := durableInsert(call, conn, opt.run, seq, sum[:], payload)
		cancel()
		if ctx.Err() != nil {
			res.StoppedBy = "interrupted"
			break
		}
		var line string
		switch {
		case err == nil:
			lat = append(lat, ms(time.Since(began)))
			res.Acknowledged++
			line = fmt.Sprintf("A %d %s %s %s", seq, hex.EncodeToString(sum[:]), started.UTC().Format(time.RFC3339Nano), lsn)
			closeOutage()
		case uncertain:
			res.Uncertain++
			line = fmt.Sprintf("U %d %s", seq, hex.EncodeToString(sum[:]))
		default:
			res.Failed++
			line = fmt.Sprintf("F %d", seq)
		}
		if err != nil {
			if down == nil {
				down = &outage{Start: began.UTC(), Kind: errorKind(err)}
			}
			// Reconnect for the next write; that also detects a restart.
			conn.Close(context.Background())
		}
		if err := writeLedger(w, ledger, line); err != nil {
			return nil, nil, err
		}
		if err != nil {
			// Let the server come back without hammering it.
			sleepCtx(ctx, 500*time.Millisecond)
		}
	}
	closeOutage()
	res.Commit = summarize(lat, res.Uncertain+res.Failed)
	if opt.beforeEnd != nil {
		opt.beforeEnd()
	}
	// The end marker is best effort and bounded: the stop guard allows ten
	// seconds after Ctrl-C. Without it verify falls back to the highest
	// acknowledged WAL position.
	if !res.IdentityChanged {
		endCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lsn, err := writeEndMarker(endCtx, conn, opt.run)
		if err != nil {
			// The session may have died with a restart; try once on a fresh,
			// vetted one.
			conn.Close(context.Background())
			if err = reopen(endCtx); err == nil {
				lsn, err = writeEndMarker(endCtx, conn, opt.run)
			}
		}
		cancel()
		if err == nil {
			res.EndMarker = true
			if err := writeLedger(w, ledger, "E "+lsn); err != nil {
				return nil, nil, err
			}
		}
	}
	return res, info, nil
}

// writeEndMarker commits the end marker row with a large random payload.
func writeEndMarker(ctx context.Context, conn *pgx.Conn, run string) (string, error) {
	payload := make([]byte, endMarkerBytes)
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 13))
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	sum := sha256.Sum256(payload)
	lsn, _, err := durableInsert(ctx, conn, run, endMarkerSeq, sum[:], payload)
	return lsn, err
}

func durableInsert(ctx context.Context, conn *pgx.Conn, run string, seq int64, sum, payload []byte) (lsn string, uncertain bool, err error) {
	tx, err := beginDurable(ctx, conn)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)
	// The insert position after the row is a lower bound on the commit's WAL
	// position, read without another round trip.
	if err := tx.QueryRow(ctx, `INSERT INTO myce_qualification.durability_v1(run,seq,sha,payload) VALUES($1,$2,$3,$4)
		RETURNING pg_catalog.pg_current_wal_insert_lsn()::text`, run, seq, sum, payload).Scan(&lsn); err != nil {
		return "", false, err
	}
	err = tx.Commit(ctx)
	return lsn, err != nil, err
}

// writeLedger syncs each line so a client crash cannot lose an
// acknowledgement the server already gave.
func writeLedger(w *bufio.Writer, f *os.File, line string) error {
	if _, err := w.WriteString(line + "\n"); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// parseLSN reads PostgreSQL's "hi/lo" hexadecimal WAL position.
func parseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("bad WAL position %q", s)
	}
	h, err1 := strconv.ParseUint(hi, 16, 32)
	l, err2 := strconv.ParseUint(lo, 16, 32)
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("bad WAL position %q", s)
	}
	return h<<32 | l, nil
}

func formatLSN(v uint64) string { return fmt.Sprintf("%X/%X", v>>32, uint32(v)) }

type ledgerEntry struct {
	state   string
	sha     string
	started time.Time // server start the acknowledgement was written under
	lsn     uint64
}

type ledger struct {
	header  ledgerHeader
	entries map[int64]ledgerEntry
	endLSN  uint64 // 0 when the end marker was not acknowledged
}

func readLedger(path string) (*ledger, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	l := &ledger{entries: map[int64]ledgerEntry{}}
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := sc.Text()
		if rest, ok := strings.CutPrefix(text, "H "); ok {
			if err := json.Unmarshal([]byte(rest), &l.header); err != nil {
				return nil, fmt.Errorf("ledger line %d: %w", line, err)
			}
			continue
		}
		parts := strings.Fields(text)
		if len(parts) == 2 && parts[0] == "E" {
			if l.endLSN, err = parseLSN(parts[1]); err != nil {
				return nil, fmt.Errorf("ledger line %d: %w", line, err)
			}
			continue
		}
		if len(parts) < 2 || parts[0] == "A" && len(parts) < 5 {
			// A torn final line from a client crash is not an acknowledgement.
			continue
		}
		seq, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ledger line %d: bad sequence", line)
		}
		e := ledgerEntry{state: parts[0]}
		if len(parts) > 2 {
			e.sha = parts[2]
		}
		if e.state == "A" {
			if e.started, err = time.Parse(time.RFC3339Nano, parts[3]); err != nil {
				return nil, fmt.Errorf("ledger line %d: bad server start time", line)
			}
			if e.lsn, err = parseLSN(parts[4]); err != nil {
				return nil, fmt.Errorf("ledger line %d: %w", line, err)
			}
		}
		l.entries[seq] = e
	}
	if l.header.Run == "" {
		return nil, errors.New("ledger has no header")
	}
	return l, sc.Err()
}

type verifyResults struct {
	Run                 string  `json:"run"`
	Acknowledged        int     `json:"acknowledged"`
	AckedBeforeRestart  int     `json:"acknowledged_before_the_restart"`
	MissingAcknowledged []int64 `json:"missing_acknowledged"`
	Corrupt             []int64 `json:"corrupt"`
	PresentAfterFailure []int64 `json:"present_after_failure"`
	UncertainPresent    int     `json:"uncertain_present"`
	UncertainAbsent     int     `json:"uncertain_absent"`
	Unlisted            int     `json:"present_not_in_ledger"`
	EndMarker           bool    `json:"end_marker_in_ledger"`

	IdentityAtWrite  string    `json:"system_identifier_at_write"`
	IdentityAtVerify string    `json:"system_identifier_at_verify"`
	TimelineAtWrite  int       `json:"timeline_at_write"`
	TimelineAtVerify int       `json:"timeline_at_verify"`
	WALFloor         string    `json:"wal_acknowledged_through"`
	WALAtVerify      string    `json:"wal_at_verify"`
	WALBehind        bool      `json:"wal_behind_acknowledged"`
	StartedAtVerify  time.Time `json:"server_started_at_verify"`
}

func cmdDurabilityVerify(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("durability-verify", out)
	cf := addConnFlags(fs)
	o := addOutputFlags(fs)
	ledgerPath := fs.String("ledger", "", "ledger written by durability-write (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ledgerPath == "" {
		return errors.New("--ledger is required")
	}
	l, err := readLedger(*ledgerPath)
	if err != nil {
		return err
	}
	t, err := cf.load()
	if err != nil {
		return err
	}
	conn, err := t.open(ctx, false, 0)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	res, info, err := verifyLedger(ctx, conn, t.class, l)
	if err != nil {
		return err
	}
	r := newReport("durability-verify", o)
	r.Params["run"] = l.header.Run
	r.Server = info
	r.Results = res
	r.Checks = verifyChecks(res)
	r.Notes = append(r.Notes, "A byte-for-byte copy of the data directory taken after the end marker cannot be told apart from the original over SQL; the plan's container and mount check before and after covers it.")
	r.md = func(b *strings.Builder) {
		fmt.Fprintf(b, "\n## Ledger\n\nRun %s: %d acknowledged (%d before the restart), %d missing, %d corrupt; uncertain %d present and %d absent; %d failed-before-commit rows present; %d rows not in the ledger. WAL acknowledged through %s, at verify %s.\n",
			res.Run, res.Acknowledged, res.AckedBeforeRestart, len(res.MissingAcknowledged), len(res.Corrupt), res.UncertainPresent, res.UncertainAbsent,
			len(res.PresentAfterFailure), res.Unlisted, res.WALFloor, res.WALAtVerify)
	}
	return r.finish(out)
}

type storedRow struct{ actual, stored string }

// verifyLedger reads the run's rows and the server's state, then judges them.
func verifyLedger(ctx context.Context, conn *pgx.Conn, class string, l *ledger) (*verifyResults, *serverInfo, error) {
	info, err := readServerInfo(ctx, conn, class, 20)
	if err != nil {
		return nil, nil, err
	}
	var walNow uint64
	if !info.InRecovery {
		var s string
		if err := conn.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text").Scan(&s); err != nil {
			return nil, nil, err
		}
		if walNow, err = parseLSN(s); err != nil {
			return nil, nil, err
		}
	}
	rows, err := conn.Query(ctx, `SELECT seq, pg_catalog.encode(pg_catalog.sha256(payload),'hex'), pg_catalog.encode(sha,'hex')
		FROM myce_qualification.durability_v1 WHERE run OPERATOR(pg_catalog.=) $1`, l.header.Run)
	if err != nil {
		return nil, nil, err
	}
	present := map[int64]storedRow{}
	for rows.Next() {
		var seq int64
		var r storedRow
		if err := rows.Scan(&seq, &r.actual, &r.stored); err != nil {
			rows.Close()
			return nil, nil, err
		}
		present[seq] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return evaluateLedger(l, present, info, walNow), info, nil
}

// evaluateLedger compares the ledger with what the server holds now.
func evaluateLedger(l *ledger, present map[int64]storedRow, now *serverInfo, walNow uint64) *verifyResults {
	h := l.header
	res := &verifyResults{Run: h.Run, StartedAtVerify: now.StartedAt, EndMarker: l.endLSN != 0,
		IdentityAtWrite: h.SystemIdentifier, IdentityAtVerify: now.SystemIdentifier,
		TimelineAtWrite: h.Timeline, TimelineAtVerify: now.Timeline}
	floor := l.endLSN
	for seq, r := range present {
		e, ok := l.entries[seq]
		switch {
		case seq == endMarkerSeq:
		case !ok:
			res.Unlisted++
		case e.state == "F":
			res.PresentAfterFailure = append(res.PresentAfterFailure, seq)
		case e.state == "U":
			res.UncertainPresent++
		}
		if r.actual != r.stored || ok && e.sha != "" && e.sha != r.actual {
			res.Corrupt = append(res.Corrupt, seq)
		}
	}
	for seq, e := range l.entries {
		switch e.state {
		case "A":
			res.Acknowledged++
			floor = max(floor, e.lsn)
			// A start time earlier than the one at verify means the server
			// went down after this row was acknowledged.
			if !e.started.IsZero() && e.started.Before(now.StartedAt) {
				res.AckedBeforeRestart++
			}
			if _, ok := present[seq]; !ok {
				res.MissingAcknowledged = append(res.MissingAcknowledged, seq)
			}
		case "U":
			if _, ok := present[seq]; !ok {
				res.UncertainAbsent++
			}
		}
	}
	if _, ok := present[endMarkerSeq]; l.endLSN != 0 && !ok {
		res.MissingAcknowledged = append(res.MissingAcknowledged, endMarkerSeq)
	}
	res.WALFloor, res.WALAtVerify = formatLSN(floor), formatLSN(walNow)
	res.WALBehind = floor == 0 || walNow < floor
	slices.Sort(res.MissingAcknowledged)
	slices.Sort(res.Corrupt)
	slices.Sort(res.PresentAfterFailure)
	return res
}

func verifyChecks(res *verifyResults) []check {
	status := func(ok bool) string {
		if ok {
			return statusPass
		}
		return statusFail
	}
	identity := "same"
	switch {
	case res.IdentityAtWrite == "" || res.IdentityAtVerify == "":
		identity = "unreadable at write or at verify"
	case res.IdentityAtWrite != res.IdentityAtVerify:
		identity = "changed"
	}
	timeline := res.TimelineAtWrite != 0 && res.TimelineAtWrite == res.TimelineAtVerify
	return []check{
		{Name: "acknowledged before a server restart", Status: status(res.AckedBeforeRestart > 0),
			Measured: fmt.Sprintf("%d of %d written under an earlier server start than %s", res.AckedBeforeRestart, res.Acknowledged, res.StartedAtVerify.UTC().Format(time.RFC3339Nano)),
			Limit:    ">= 1",
			Prevents: "a pass from a server that never went down, or went down before the first acknowledgement, which says nothing about losing acknowledged rows"},
		{Name: "every acknowledged write survived", Status: status(res.Acknowledged > 0 && len(res.MissingAcknowledged) == 0),
			Measured: fmt.Sprintf("%d of %d missing", len(res.MissingAcknowledged), res.Acknowledged), Limit: "0 missing, at least 1 acknowledged",
			Prevents: "losing an acknowledged assignment, start or result, which the design promises survives any one machine: duplicate execution or a lost result"},
		{Name: "payloads intact", Status: status(len(res.Corrupt) == 0), Measured: fmt.Sprint(len(res.Corrupt)), Limit: "0 corrupt",
			Prevents: "replaying a damaged change into every member's copy"},
		{Name: "no row from a transaction that failed before COMMIT", Status: status(len(res.PresentAfterFailure) == 0),
			Measured: fmt.Sprint(len(res.PresentAfterFailure)), Limit: "0",
			Prevents: "a failure the client treats as rolled back taking effect anyway, breaking the queue's retry rules"},
		{Name: "same data directory", Status: status(identity == "same"), Measured: identity,
			Limit:    "system identifier readable and unchanged; connect as the database owner",
			Prevents: "an update or redeploy that recreated the cluster and passed only because the ledger was empty"},
		{Name: "not an older copy", Status: status(timeline && !res.WALBehind),
			Measured: fmt.Sprintf("WAL at verify %s, acknowledged through %s; timeline %d at write, %d at verify", res.WALAtVerify, res.WALFloor, res.TimelineAtWrite, res.TimelineAtVerify),
			Limit:    "WAL at or past every acknowledged position and the end marker; timeline unchanged",
			Prevents: "verifying against a restored backup or an older copy of the data directory, which holds rows without proving this server kept them"},
	}
}
