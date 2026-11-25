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
// line is written and synced only after COMMIT returns, so "A" means the
// server acknowledged the row and "U" means COMMIT failed with an unknown
// outcome. "F" means the transaction failed before COMMIT and cannot exist.
type ledgerHeader struct {
	Run              string    `json:"run"`
	Started          time.Time `json:"started"`
	SystemIdentifier string    `json:"system_identifier,omitempty"`
}

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
	Outages          []outage     `json:"outages"`
	Commit           latencyStats `json:"commit"`
	IdentityChanged  bool         `json:"system_identifier_changed"`
	ServerRestarts   int          `json:"server_restarts_seen"`
	StoppedBy        string       `json:"stopped_by"`
	LongestOutageSec float64      `json:"longest_outage_seconds"`
}

func cmdDurabilityWrite(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("durability-write", out)
	cf := addConnFlags(fs)
	o := addOutputFlags(fs)
	ledgerPath := fs.String("ledger", "", "local ledger file to create (required; keep it for durability-verify)")
	run := fs.String("run", "", "run identifier (default: time-based)")
	duration := fs.Duration("duration", 15*time.Minute, "how long to keep writing, including outages (max 2h)")
	rate := fs.Float64("rate", defaultRate, "writes per second")
	maxOutage := fs.Duration("max-outage", 20*time.Minute, "give up if the server stays unreachable this long")
	profile := fs.String("profile", "queue", "payload sizes: queue or fixed:BYTES")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ledgerPath == "" || *duration <= 0 || *duration > 2*time.Hour || *rate <= 0 || *rate > 1000 {
		return errors.New("--ledger is required, --duration must be in (0, 2h] and --rate in (0, 1000]")
	}
	prof, err := parseProfile(*profile)
	if err != nil {
		return err
	}
	if *run == "" {
		*run = "dw-" + time.Now().UTC().Format("20060102T150405Z")
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
	r.Params["run"] = *run
	r.Params["rate"] = *rate
	r.Params["duration"] = duration.String()
	r.Params["max_outage"] = maxOutage.String()
	res, info, err := durabilityWrite(ctx, t, ledger, *run, *duration, *rate, *maxOutage, prof)
	if err != nil {
		return err
	}
	r.Server = info
	r.Results = res
	r.Checks = []check{
		{Name: "data directory kept its identity", Status: map[bool]string{false: statusPass, true: statusFail}[res.IdentityChanged],
			Measured: fmt.Sprint(!res.IdentityChanged), Limit: "same system identifier before and after every outage",
			Prevents: "a container recreated on an empty data directory silently starting a new, empty authority"},
		{Name: "writer outlived every outage", Status: map[bool]string{true: statusPass, false: statusFail}[res.StoppedBy == "duration"],
			Measured: res.StoppedBy, Limit: "duration", Prevents: "an outage longer than --max-outage hiding a server that never came back on its own"},
		{Name: "longest outage", Status: statusInfo, Measured: fmt.Sprintf("%.1f s", res.LongestOutageSec),
			Limit: "context: new starts wait while the database is down", Prevents: "planning restarts without knowing their pause"},
	}
	r.Notes = append(r.Notes, "Run durability-verify with the same ledger after the restart or reboot; this report alone proves nothing about loss.")
	r.md = func(b *strings.Builder) {
		fmt.Fprintf(b, "\n## Writes\n\nRun %s: %d acknowledged, %d uncertain, %d failed before commit; %d server restarts seen.\n",
			res.Run, res.Acknowledged, res.Uncertain, res.Failed, res.ServerRestarts)
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

func durabilityWrite(ctx context.Context, t *target, ledger *os.File, run string, duration time.Duration, rate float64, maxOutage time.Duration, prof payloadProfile) (*writeResults, *serverInfo, error) {
	conn, err := t.connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { conn.Close(context.Background()) }()
	if err := prepare(ctx, conn); err != nil {
		return nil, nil, err
	}
	info, err := readServerInfo(ctx, conn, t.class, 20)
	if err != nil {
		return nil, nil, err
	}
	w := bufio.NewWriter(ledger)
	header, _ := json.Marshal(ledgerHeader{Run: run, Started: time.Now().UTC(), SystemIdentifier: info.SystemIdentifier})
	if err := writeLedger(w, ledger, "H "+string(header)); err != nil {
		return nil, nil, err
	}
	res := &writeResults{Run: run}
	pool := newPayloadPool(prof, uint64(time.Now().UnixNano()))
	gap := rand.New(rand.NewPCG(7, 11))
	identity, started := info.SystemIdentifier, info.StartedAt
	var lat []float64
	var down *outage
	deadline := time.Now().Add(duration)
	next := time.Now()
	for seq := int64(1); ; seq++ {
		next = next.Add(time.Duration(gap.ExpFloat64() / rate * float64(time.Second)))
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
			fresh, err := t.connect(call)
			if err == nil {
				// Restarted or not, the identifier must not change.
				if s, err := readServerInfo(call, fresh, t.class, 0); err == nil {
					if identity != "" && s.SystemIdentifier != "" && s.SystemIdentifier != identity {
						res.IdentityChanged = true
					}
					if !s.StartedAt.Equal(started) {
						res.ServerRestarts++
						started = s.StartedAt
					}
				}
				conn = fresh
			}
			cancel()
			if err != nil {
				if down == nil {
					down = &outage{Start: time.Now().UTC(), Kind: "connect"}
				}
				if time.Since(down.Start) > maxOutage {
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
		uncertain, err := durableInsert(call, conn, run, seq, sum[:], payload)
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
			line = fmt.Sprintf("A %d %s", seq, hex.EncodeToString(sum[:]))
			if down != nil {
				down.End = time.Now().UTC()
				down.Seconds = down.End.Sub(down.Start).Seconds()
				res.Outages = append(res.Outages, *down)
				res.LongestOutageSec = max(res.LongestOutageSec, down.Seconds)
				down = nil
			}
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
	if down != nil {
		down.End = time.Now().UTC()
		down.Seconds = down.End.Sub(down.Start).Seconds()
		res.Outages = append(res.Outages, *down)
		res.LongestOutageSec = max(res.LongestOutageSec, down.Seconds)
	}
	res.Commit = summarize(lat, res.Uncertain+res.Failed)
	return res, info, nil
}

func durableInsert(ctx context.Context, conn *pgx.Conn, run string, seq int64, sum, payload []byte) (uncertain bool, err error) {
	tx, err := beginDurable(ctx, conn)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "INSERT INTO myce_qualification.durability_v1(run,seq,sha,payload) VALUES($1,$2,$3,$4)", run, seq, sum, payload); err != nil {
		return false, err
	}
	err = tx.Commit(ctx)
	return err != nil, err
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

type ledgerEntry struct {
	state string
	sha   string
}

func readLedger(path string) (ledgerHeader, map[int64]ledgerEntry, error) {
	var h ledgerHeader
	f, err := os.Open(path)
	if err != nil {
		return h, nil, err
	}
	defer f.Close()
	entries := map[int64]ledgerEntry{}
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := sc.Text()
		if rest, ok := strings.CutPrefix(text, "H "); ok {
			if err := json.Unmarshal([]byte(rest), &h); err != nil {
				return h, nil, fmt.Errorf("ledger line %d: %w", line, err)
			}
			continue
		}
		parts := strings.Fields(text)
		if len(parts) < 2 {
			// A torn final line from a client crash is not an acknowledgement.
			continue
		}
		seq, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return h, nil, fmt.Errorf("ledger line %d: bad sequence", line)
		}
		e := ledgerEntry{state: parts[0]}
		if len(parts) > 2 {
			e.sha = parts[2]
		}
		entries[seq] = e
	}
	if h.Run == "" {
		return h, nil, errors.New("ledger has no header")
	}
	return h, entries, sc.Err()
}

type verifyResults struct {
	Run                 string  `json:"run"`
	Acknowledged        int     `json:"acknowledged"`
	MissingAcknowledged []int64 `json:"missing_acknowledged"`
	Corrupt             []int64 `json:"corrupt"`
	PresentAfterFailure []int64 `json:"present_after_failure"`
	UncertainPresent    int     `json:"uncertain_present"`
	UncertainAbsent     int     `json:"uncertain_absent"`
	Unlisted            int     `json:"present_not_in_ledger"`
	IdentityMatches     *bool   `json:"system_identifier_matches,omitempty"`
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
	h, entries, err := readLedger(*ledgerPath)
	if err != nil {
		return err
	}
	t, err := cf.load()
	if err != nil {
		return err
	}
	conn, err := t.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	info, err := readServerInfo(ctx, conn, t.class, 20)
	if err != nil {
		return err
	}
	res, err := verifyLedger(ctx, conn, h, entries, info.SystemIdentifier)
	if err != nil {
		return err
	}
	r := newReport("durability-verify", o)
	r.Params["run"] = h.Run
	r.Server = info
	r.Results = res
	r.Checks = verifyChecks(res)
	r.md = func(b *strings.Builder) {
		fmt.Fprintf(b, "\n## Ledger\n\nRun %s: %d acknowledged, %d missing, %d corrupt; uncertain %d present and %d absent; %d failed-before-commit rows present; %d rows not in the ledger.\n",
			res.Run, res.Acknowledged, len(res.MissingAcknowledged), len(res.Corrupt), res.UncertainPresent, res.UncertainAbsent, len(res.PresentAfterFailure), res.Unlisted)
	}
	return r.finish(out)
}

func verifyLedger(ctx context.Context, conn *pgx.Conn, h ledgerHeader, entries map[int64]ledgerEntry, identity string) (*verifyResults, error) {
	res := &verifyResults{Run: h.Run}
	if h.SystemIdentifier != "" && identity != "" {
		same := h.SystemIdentifier == identity
		res.IdentityMatches = &same
	}
	rows, err := conn.Query(ctx, "SELECT seq, encode(sha256(payload),'hex'), encode(sha,'hex') FROM myce_qualification.durability_v1 WHERE run=$1", h.Run)
	if err != nil {
		return nil, err
	}
	present := map[int64]bool{}
	for rows.Next() {
		var seq int64
		var actual, stored string
		if err := rows.Scan(&seq, &actual, &stored); err != nil {
			rows.Close()
			return nil, err
		}
		present[seq] = true
		e, ok := entries[seq]
		switch {
		case !ok:
			res.Unlisted++
		case e.state == "F":
			res.PresentAfterFailure = append(res.PresentAfterFailure, seq)
		case e.state == "U":
			res.UncertainPresent++
		}
		if actual != stored || ok && e.sha != "" && e.sha != actual {
			res.Corrupt = append(res.Corrupt, seq)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for seq, e := range entries {
		switch e.state {
		case "A":
			res.Acknowledged++
			if !present[seq] {
				res.MissingAcknowledged = append(res.MissingAcknowledged, seq)
			}
		case "U":
			if !present[seq] {
				res.UncertainAbsent++
			}
		}
	}
	slices.Sort(res.MissingAcknowledged)
	slices.Sort(res.Corrupt)
	slices.Sort(res.PresentAfterFailure)
	return res, nil
}

func verifyChecks(res *verifyResults) []check {
	status := func(ok bool) string {
		if ok {
			return statusPass
		}
		return statusFail
	}
	cs := []check{
		{Name: "every acknowledged write survived", Status: status(res.Acknowledged > 0 && len(res.MissingAcknowledged) == 0),
			Measured: fmt.Sprintf("%d of %d missing", len(res.MissingAcknowledged), res.Acknowledged), Limit: "0 missing, at least 1 acknowledged",
			Prevents: "losing an acknowledged assignment, start or result, which the design promises survives any one machine: duplicate execution or a lost result"},
		{Name: "payloads intact", Status: status(len(res.Corrupt) == 0), Measured: fmt.Sprint(len(res.Corrupt)), Limit: "0 corrupt",
			Prevents: "replaying a damaged change into every member's copy"},
		{Name: "no row from a transaction that failed before COMMIT", Status: status(len(res.PresentAfterFailure) == 0),
			Measured: fmt.Sprint(len(res.PresentAfterFailure)), Limit: "0",
			Prevents: "a failure the client treats as rolled back taking effect anyway, breaking the queue's retry rules"},
	}
	if res.IdentityMatches != nil {
		cs = append(cs, check{Name: "same data directory", Status: status(*res.IdentityMatches), Measured: fmt.Sprint(*res.IdentityMatches),
			Limit: "system identifier unchanged", Prevents: "an update or redeploy that recreated the cluster and passed only because the ledger was empty"})
	} else {
		cs = append(cs, check{Name: "same data directory", Status: statusNotExercised, Measured: "identifier unreadable",
			Limit: "system identifier unchanged", Prevents: "an update or redeploy that recreated the cluster; connect as the database owner so pg_control_system is readable"})
	}
	return cs
}
