package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Defaults come from measurements and the stage 2 code, not guesses:
//   - 11.6 commits/s is the stage 0 laptop fixture rate (#2092, #1176).
//   - The queue profile matches #2093's sampled commit sizes (check-in median
//     3.9 kB, admission median 13.8 kB) and its 125.6 kB/s record payload.
//   - A 6 s lease with a 1 s renewal tick and a lease/3 per-call timeout are
//     the stage 2 defaults in shared_queue_member.go.
const (
	defaultRate     = 11.6
	defaultLease    = 6 * time.Second
	defaultInterval = time.Second
)

type payloadProfile struct {
	Name    string    `json:"name"`
	Sizes   []int     `json:"sizes"`
	Weights []float64 `json:"weights"`
}

func parseProfile(s string) (payloadProfile, error) {
	if s == "queue" {
		return payloadProfile{Name: s, Sizes: []int{3900, 13800, 96000}, Weights: []float64{0.70, 0.25, 0.05}}, nil
	}
	if n, ok := strings.CutPrefix(s, "fixed:"); ok {
		size, err := strconv.Atoi(n)
		if err != nil || size < 64 || size > 16<<20 {
			return payloadProfile{}, fmt.Errorf("fixed payload size must be 64 bytes to 16 MiB")
		}
		return payloadProfile{Name: s, Sizes: []int{size}, Weights: []float64{1}}, nil
	}
	return payloadProfile{}, fmt.Errorf("unknown payload profile %q (use queue or fixed:BYTES)", s)
}

func (p payloadProfile) mean() float64 {
	var m float64
	for i, s := range p.Sizes {
		m += float64(s) * p.Weights[i]
	}
	return m
}

// payloadPool pre-builds incompressible change payloads in the stage 2
// versioned-rows JSON shape, so generating them costs nothing in the loop.
type payloadPool struct {
	profile payloadProfile
	classes [][][]byte
	rng     *rand.Rand
}

func newPayloadPool(p payloadProfile, seed uint64) *payloadPool {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	pool := &payloadPool{profile: p, rng: rng}
	for _, size := range p.Sizes {
		var class [][]byte
		for range 16 {
			class = append(class, rowsPayload(size, rng))
		}
		pool.classes = append(pool.classes, class)
	}
	return pool
}

func (pp *payloadPool) next() []byte {
	x := pp.rng.Float64()
	for i, w := range pp.profile.Weights {
		if x < w || i == len(pp.profile.Weights)-1 {
			class := pp.classes[i]
			return class[pp.rng.IntN(len(class))]
		}
		x -= w
	}
	panic("unreachable")
}

type rowValue struct {
	Kind  string `json:"kind"`
	Int   int64  `json:"int,omitempty"`
	Bytes []byte `json:"bytes,omitempty"`
}

type rowChange struct {
	Table  string     `json:"table"`
	Values []rowValue `json:"values"`
}

type rowsDoc struct {
	Version int         `json:"version"`
	Rows    []rowChange `json:"rows"`
}

// rowsPayload builds a JSON document of about size bytes. Random bytes are
// the worst case for PostgreSQL's TOAST compression.
func rowsPayload(size int, rng *rand.Rand) []byte {
	doc := rowsDoc{Version: 1}
	budget := size * 3 / 4 // base64 expands 3 bytes to 4
	for budget > 0 {
		n := min(budget, 1536)
		raw := make([]byte, n)
		for i := range raw {
			raw[i] = byte(rng.Uint32())
		}
		doc.Rows = append(doc.Rows, rowChange{Table: "qualification", Values: []rowValue{
			{Kind: "int", Int: rng.Int64N(1 << 40)}, {Kind: "blob", Bytes: raw}}})
		budget -= n + 96 // per-row JSON overhead
	}
	b, _ := json.Marshal(doc)
	return b
}

var (
	errTurn = errors.New("coordinator turn is unavailable")
	errHead = errors.New("change log head does not match")
)

type turn struct {
	owner, endpoint string
	number, head    int64
	expires         time.Time
	ready           bool
}

// The SQL below copies the stage 2 authority statement for statement,
// including SET LOCAL and the separate clock read, so round trips match.
func beginDurable(ctx context.Context, conn *pgx.Conn) (pgx.Tx, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func lockedTurn(ctx context.Context, tx pgx.Tx) (turn, time.Time, error) {
	var t turn
	err := tx.QueryRow(ctx, "SELECT owner,endpoint,handover,head,expires,ready FROM myce_qualification.myce_coordinator_v1 WHERE id=1 FOR UPDATE").
		Scan(&t.owner, &t.endpoint, &t.number, &t.head, &t.expires, &t.ready)
	if err != nil {
		return turn{}, time.Time{}, err
	}
	var now time.Time
	err = tx.QueryRow(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&now)
	return t, now, err
}

func acquireTurn(ctx context.Context, conn *pgx.Conn, owner string, lease time.Duration) (turn, error) {
	tx, err := beginDurable(ctx, conn)
	if err != nil {
		return turn{}, err
	}
	defer tx.Rollback(ctx)
	t, now, err := lockedTurn(ctx, tx)
	if err != nil {
		return turn{}, err
	}
	if t.expires.After(now) {
		return turn{}, errTurn
	}
	err = tx.QueryRow(ctx, `UPDATE myce_qualification.myce_coordinator_v1
		SET owner=$1,endpoint=$2,handover=handover+1,expires=$3,ready=false
		WHERE id=1 RETURNING owner,endpoint,handover,head,expires,ready`, owner, "qualification", now.Add(lease)).
		Scan(&t.owner, &t.endpoint, &t.number, &t.head, &t.expires, &t.ready)
	if err != nil {
		return turn{}, err
	}
	return t, tx.Commit(ctx)
}

func renewTurn(ctx context.Context, conn *pgx.Conn, want turn, lease time.Duration) error {
	tx, err := beginDurable(ctx, conn)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	t, now, err := lockedTurn(ctx, tx)
	if err != nil {
		return err
	}
	if t.owner != want.owner || t.number != want.number || lease > 0 && !t.expires.After(now) {
		return errTurn
	}
	if _, err := tx.Exec(ctx, "UPDATE myce_qualification.myce_coordinator_v1 SET expires=$1,ready=$2 WHERE id=1", now.Add(lease), lease > 0); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// appendChange reports uncertain when COMMIT itself failed: the row may or
// may not be durable, exactly the case the queue resolves by operation ID.
func appendChange(ctx context.Context, conn *pgx.Conn, want turn, seq int64, op string, payload []byte) (uncertain bool, err error) {
	tx, err := beginDurable(ctx, conn)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	t, now, err := lockedTurn(ctx, tx)
	if err != nil {
		return false, err
	}
	if t.owner != want.owner || t.number != want.number || !t.expires.After(now) {
		return false, errTurn
	}
	if t.head != seq-1 {
		return false, errHead
	}
	if _, err := tx.Exec(ctx, "INSERT INTO myce_qualification.myce_changes_v1(sequence,handover,operation,payload) VALUES($1,$2,$3,$4)",
		seq, want.number, op, payload); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, "UPDATE myce_qualification.myce_coordinator_v1 SET head=$1 WHERE id=1", seq); err != nil {
		return false, err
	}
	err = tx.Commit(ctx)
	return err != nil, err
}

func errorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errTurn):
		return "turn-lost"
	case errors.Is(err, errHead):
		return "head-mismatch"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "error"
}

type opSample struct {
	At      time.Time     // when the operation started
	Lat     time.Duration // service time
	Resp    time.Duration // from scheduled arrival to completion (appends)
	Kind    string        // error kind, empty on success
	Payload int
}

type recorder struct {
	mu      sync.Mutex
	samples []opSample
}

func (r *recorder) add(s opSample) {
	r.mu.Lock()
	r.samples = append(r.samples, s)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []opSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]opSample(nil), r.samples...)
}

type windowStats struct {
	Start  time.Time `json:"start"`
	Count  int       `json:"appends"`
	Errors int       `json:"errors"`
	P99Ms  float64   `json:"p99_ms"`
	MaxMs  float64   `json:"max_ms"`
}

type commitResults struct {
	Append         latencyStats   `json:"append"`
	AppendResponse latencyStats   `json:"append_response"`
	Renew          latencyStats   `json:"renew"`
	RoundTrip      latencyStats   `json:"round_trip"`
	TargetRate     float64        `json:"target_rate"`
	AchievedRate   float64        `json:"achieved_rate"`
	MeasuredSec    float64        `json:"measured_seconds"`
	PayloadkBps    float64        `json:"payload_kBps"`
	WALkBps        float64        `json:"wal_kBps"`
	Errors         map[string]int `json:"errors"`
	Reacquired     int            `json:"turn_reacquired"`
	Worst          []windowStats  `json:"worst_10s_windows"`
	Mount          *mountResults  `json:"mount,omitempty"`
}

type commitOptions struct {
	duration, warmup, lease, interval, opTimeout time.Duration
	rate                                         float64
	profile                                      payloadProfile
	seed                                         uint64
	watch                                        *mountWatcher
}

func cmdCommit(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("commit", out)
	cf := addConnFlags(fs)
	o := addOutputFlags(fs)
	var opt commitOptions
	fs.DurationVar(&opt.duration, "duration", 10*time.Minute, "measured time after warm-up (max 2h)")
	fs.DurationVar(&opt.warmup, "warmup", 30*time.Second, "unmeasured warm-up")
	fs.Float64Var(&opt.rate, "rate", defaultRate, "mean change appends per second (Poisson arrivals)")
	fs.DurationVar(&opt.lease, "lease", defaultLease, "coordinator turn lease; thresholds derive from it")
	fs.DurationVar(&opt.interval, "interval", defaultInterval, "renewal interval")
	fs.DurationVar(&opt.opTimeout, "op-timeout", 30*time.Second, "give up on one operation after this long (kept long to see the whole tail)")
	profile := fs.String("profile", "queue", "payload sizes: queue or fixed:BYTES")
	fs.Uint64Var(&opt.seed, "seed", 1, "random seed for arrivals and payloads")
	watch := fs.String("watch", "", "directory on a file mount to probe while committing (isolation check)")
	watchInterval := fs.Duration("watch-interval", time.Second, "mount probe interval")
	watchTimeout := fs.Duration("watch-timeout", 2*time.Second, "a probe slower than this marks the mount unresponsive")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var err error
	if opt.profile, err = parseProfile(*profile); err != nil {
		return err
	}
	if opt.duration <= 0 || opt.duration > 2*time.Hour || opt.rate <= 0 || opt.rate > 1000 {
		return errors.New("--duration must be in (0, 2h] and --rate in (0, 1000]")
	}
	if opt.lease < 3*opt.interval || opt.interval <= 0 {
		return errors.New("the lease must span at least three renewal intervals, as in stage 2")
	}
	if *watch != "" {
		if info, err := os.Stat(*watch); err != nil || !info.IsDir() {
			return errors.New("--watch must be an existing directory")
		}
		opt.watch = &mountWatcher{probe: dirProbe(*watch), interval: *watchInterval, timeout: *watchTimeout}
	}
	t, err := cf.load()
	if err != nil {
		return err
	}
	r := newReport("commit", o)
	r.Params["duration"] = opt.duration.String()
	r.Params["warmup"] = opt.warmup.String()
	r.Params["rate"] = opt.rate
	r.Params["lease"] = opt.lease.String()
	r.Params["interval"] = opt.interval.String()
	r.Params["call_timeout"] = (opt.lease / 3).String()
	r.Params["profile"] = fmt.Sprintf("%s (mean %.0f B)", opt.profile.Name, opt.profile.mean())
	r.Params["mount_watch"] = opt.watch != nil
	info, res, err := runCommit(ctx, t, opt)
	if err != nil {
		return err
	}
	r.Server = info
	r.Results = res
	r.Checks = commitChecks(res, opt)
	r.md = func(b *strings.Builder) { commitMarkdown(b, res) }
	r.Notes = append(r.Notes,
		"Each append is BEGIN, SET LOCAL synchronous_commit, a FOR UPDATE turn read, a clock read, INSERT, UPDATE and COMMIT: seven round trips, as in stage 2.",
		"Payloads are random bytes, the worst case for TOAST compression.")
	return r.finish(out)
}

// runCommit writes through two sessions, each vetted by open and holding its
// own advisory lock, so a second commit run is refused. Appended rows stay:
// the log continues from the previous run's head, and the storage owner
// removes the qualification database afterwards.
func runCommit(ctx context.Context, t *target, opt commitOptions) (*serverInfo, *commitResults, error) {
	appendConn, err := t.open(ctx, true, commitLockKey)
	if err != nil {
		return nil, nil, err
	}
	defer func() { appendConn.Close(context.Background()) }()
	renewConn, err := t.open(ctx, true, commitRenewLockKey)
	if err != nil {
		return nil, nil, err
	}
	defer func() { renewConn.Close(context.Background()) }()
	info, err := readServerInfo(ctx, renewConn, t.class, 200)
	if err != nil {
		return nil, nil, err
	}
	// Free the turn a killed run may have left unexpired; the head stays.
	if _, err := appendConn.Exec(ctx, `UPDATE myce_qualification.myce_coordinator_v1
		SET owner='',endpoint='',expires='epoch',ready=false WHERE id=1`); err != nil {
		return nil, nil, err
	}
	owner := fmt.Sprintf("queue-qualify-%d", time.Now().UnixNano())
	cur, err := acquireTurn(ctx, appendConn, owner, opt.lease)
	if err != nil {
		return nil, nil, err
	}
	var turnMu sync.Mutex
	pool := newPayloadPool(opt.profile, opt.seed)
	arrivals := rand.New(rand.NewPCG(opt.seed+1, opt.seed+2))
	var appends, renewals recorder

	start := time.Now()
	measureFrom := start.Add(opt.warmup)
	deadline := measureFrom.Add(opt.duration)
	// runCtx ends the run at its deadline even while a reconnect is waiting
	// on a server that is gone; one operation timeout of slack lets an append
	// in flight at the deadline finish and be counted.
	runCtx, stop := context.WithDeadline(ctx, deadline.Add(opt.opTimeout))
	defer stop()
	var fatalMu sync.Mutex
	var fatal error
	failRun := func(err error) {
		fatalMu.Lock()
		if fatal == nil {
			fatal = err
		}
		fatalMu.Unlock()
		stop()
	}

	var watchDone chan []mountWindow
	if opt.watch != nil {
		watchDone = make(chan []mountWindow, 1)
		go func() { watchDone <- opt.watch.run(runCtx) }()
	}

	// reconnect replaces a connection the driver closed after a timeout. The
	// new session is vetted and re-takes its lock before it writes; a
	// refusal, such as a different server behind the same address, ends the
	// run. A busy lock is retried, since it can be this run's own dead
	// session that the server has not yet noticed.
	reconnect := func(c **pgx.Conn, lock int64) {
		if !(*c).IsClosed() {
			return
		}
		for runCtx.Err() == nil {
			call, cancel := context.WithTimeout(runCtx, opt.opTimeout)
			next, err := t.open(call, true, lock)
			cancel()
			if err == nil {
				*c = next
				return
			}
			if errors.Is(err, errRefused) {
				failRun(err)
				return
			}
			sleepCtx(runCtx, time.Second)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(opt.interval)
		defer tick.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-tick.C:
			}
			turnMu.Lock()
			want := cur
			turnMu.Unlock()
			began := time.Now()
			call, cancel := context.WithTimeout(runCtx, opt.opTimeout)
			err := renewTurn(call, renewConn, want, opt.lease)
			cancel()
			if runCtx.Err() != nil {
				return
			}
			renewals.add(opSample{At: began, Lat: time.Since(began), Kind: errorKind(err)})
			if err != nil && !errors.Is(err, errTurn) {
				// Start clean after a server or network error.
				renewConn.Close(context.Background())
			}
			reconnect(&renewConn, commitRenewLockKey)
		}
	}()

	var reacquired, payloadBytes int
	var walStart string
	walRead := false
	seq := cur.head + 1
	next := start
	for {
		next = next.Add(time.Duration(arrivals.ExpFloat64() / opt.rate * float64(time.Second)))
		if next.After(deadline) {
			break
		}
		if !sleepUntil(runCtx, next) || appendConn.IsClosed() {
			break
		}
		if !walRead && !next.Before(measureFrom) {
			walRead = true
			_ = appendConn.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text").Scan(&walStart)
		}
		payload := pool.next()
		turnMu.Lock()
		want := cur
		turnMu.Unlock()
		began := time.Now()
		call, cancel := context.WithTimeout(runCtx, opt.opTimeout)
		_, err := appendChange(call, appendConn, want, seq, fmt.Sprintf("%s-%d", owner, seq), payload)
		cancel()
		done := time.Now()
		if runCtx.Err() != nil {
			break
		}
		appends.add(opSample{At: began, Lat: done.Sub(began), Resp: done.Sub(next), Kind: errorKind(err), Payload: len(payload)})
		switch {
		case err == nil:
			seq++
			if !began.Before(measureFrom) {
				payloadBytes += len(payload)
			}
		case errors.Is(err, errTurn), errors.Is(err, errHead):
			reconnect(&appendConn, commitLockKey)
			for runCtx.Err() == nil && !appendConn.IsClosed() {
				call, cancel := context.WithTimeout(runCtx, opt.opTimeout)
				nt, err := takeOver(call, appendConn, owner, opt.lease)
				cancel()
				if err == nil {
					turnMu.Lock()
					cur = nt
					turnMu.Unlock()
					seq = nt.head + 1
					reacquired++
					break
				}
				reconnect(&appendConn, commitLockKey)
				sleepCtx(runCtx, opt.interval)
			}
		default:
			appendConn.Close(context.Background())
			reconnect(&appendConn, commitLockKey)
			if appendConn.IsClosed() {
				continue
			}
			// The sequence may or may not have committed; resync from the head.
			turnMu.Lock()
			want = cur
			turnMu.Unlock()
			var head int64
			call, cancel := context.WithTimeout(runCtx, opt.opTimeout)
			if appendConn.QueryRow(call, "SELECT head FROM myce_qualification.myce_coordinator_v1 WHERE id=1").Scan(&head) == nil {
				seq = head + 1
			}
			cancel()
		}
	}
	measured := min(time.Since(measureFrom), opt.duration).Seconds()
	stop()
	wg.Wait()
	var windows []mountWindow
	if watchDone != nil {
		windows = <-watchDone
	}
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	fatalMu.Lock()
	err = fatal
	fatalMu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	res := &commitResults{TargetRate: opt.rate, MeasuredSec: measured, Errors: map[string]int{}, Reacquired: reacquired, RoundTrip: info.RoundTrip}
	if walStart != "" {
		var bytes float64
		if appendConn.QueryRow(context.Background(), "SELECT pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), $1::pg_catalog.pg_lsn)", walStart).Scan(&bytes) == nil && measured > 0 {
			res.WALkBps = bytes / measured / 1000
		}
	}
	as, rs := measuredOnly(appends.snapshot(), measureFrom), measuredOnly(renewals.snapshot(), measureFrom)
	res.Append, res.AppendResponse = statsOf(as, false, res.Errors), statsOf(as, true, nil)
	res.Renew = statsOf(rs, false, res.Errors)
	if measured > 0 {
		res.AchievedRate = float64(res.Append.Count) / measured
		res.PayloadkBps = float64(payloadBytes) / measured / 1000
	}
	res.Worst = worstWindows(as, measureFrom, 10*time.Second, 5)
	if opt.watch != nil {
		res.Mount = correlateMount(windows, as, rs, opt.watch.probes.Load())
	}
	release, cancel := context.WithTimeout(context.Background(), opt.opTimeout)
	defer cancel()
	turnMu.Lock()
	_ = renewTurn(release, appendConn, cur, 0)
	turnMu.Unlock()
	return info, res, nil
}

// takeOver ends this run's own expired or stuck turn and acquires a new one;
// only this run writes while it holds the advisory lock.
func takeOver(ctx context.Context, conn *pgx.Conn, owner string, lease time.Duration) (turn, error) {
	if _, err := conn.Exec(ctx, "UPDATE myce_qualification.myce_coordinator_v1 SET expires='epoch',ready=false WHERE id=1 AND owner=$1", owner); err != nil {
		return turn{}, err
	}
	return acquireTurn(ctx, conn, owner, lease)
}

func measuredOnly(s []opSample, from time.Time) []opSample {
	out := s[:0:0]
	for _, x := range s {
		if !x.At.Before(from) {
			out = append(out, x)
		}
	}
	return out
}

// statsOf summarizes successful operations; failures are counted by kind.
func statsOf(s []opSample, response bool, kinds map[string]int) latencyStats {
	var v []float64
	errs := 0
	for _, x := range s {
		if x.Kind != "" {
			errs++
			if kinds != nil {
				kinds[x.Kind]++
			}
			continue
		}
		if response {
			v = append(v, ms(x.Resp))
		} else {
			v = append(v, ms(x.Lat))
		}
	}
	return summarize(v, errs)
}

func worstWindows(s []opSample, from time.Time, width time.Duration, keep int) []windowStats {
	buckets := map[int64][]opSample{}
	for _, x := range s {
		buckets[int64(x.At.Sub(from)/width)] = append(buckets[int64(x.At.Sub(from)/width)], x)
	}
	var out []windowStats
	for k, xs := range buckets {
		st := statsOf(xs, true, nil)
		out = append(out, windowStats{Start: from.Add(time.Duration(k) * width).UTC(), Count: st.Count, Errors: st.Errors, P99Ms: st.P99Ms, MaxMs: st.MaxMs})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Errors != out[j].Errors {
			return out[i].Errors > out[j].Errors
		}
		return out[i].MaxMs > out[j].MaxMs
	})
	return out[:min(keep, len(out))]
}

func commitChecks(res *commitResults, opt commitOptions) []check {
	callTimeout := opt.lease / 3
	maxLimit := ms(callTimeout / 2)
	tailLimit := ms(callTimeout / 8)
	meanLimit := 1000 / (2 * opt.rate)
	worstMax := math.Max(res.Append.MaxMs, res.Renew.MaxMs)
	worstTail := math.Max(res.AppendResponse.P99Ms, res.Renew.P99Ms)
	errs := res.Append.Errors + res.Renew.Errors
	status := func(ok bool) string {
		if ok {
			return statusPass
		}
		return statusFail
	}
	cs := []check{
		{Name: "append and renewal max", Status: status(res.Append.Count > 0 && worstMax <= maxLimit),
			Measured: fmt.Sprintf("%.1f ms", worstMax), Limit: fmt.Sprintf("<= %.0f ms (half the %s call timeout)", maxLimit, callTimeout),
			Prevents: "a commit outliving the per-call timeout (lease/3): the append becomes uncertain and closes the writer, or the renewal fails and the coordinator retires, pausing new starts"},
		{Name: "append response and renewal p99", Status: status(worstTail <= tailLimit),
			Measured: fmt.Sprintf("%.1f ms", worstTail), Limit: fmt.Sprintf("<= %.0f ms (call timeout / 8)", tailLimit),
			Prevents: "a heavy tail that predicts rarer stalls past the timeout beyond the measured window; the busy volume has hung under load once"},
		{Name: "mean append service time", Status: status(res.Append.MeanMs <= meanLimit),
			Measured: fmt.Sprintf("%.1f ms", res.Append.MeanMs), Limit: fmt.Sprintf("<= %.1f ms (writer at most 50%% busy at %.1f/s)", meanLimit, opt.rate),
			Prevents: "the single ordered writer saturating; appends are serial, so above 50% busy a 2x burst of check-ins builds a growing backlog"},
		{Name: "no failed appends or renewals", Status: status(errs == 0),
			Measured: fmt.Sprintf("%d (%v)", errs, res.Errors), Limit: "0",
			Prevents: "failed or timed-out commits, each of which closes the writer turn or retires the coordinator"},
		{Name: "WAL rate", Status: statusInfo, Measured: fmt.Sprintf("%.1f kB/s", res.WALkBps), Limit: "context: SQLite WAL was about 300 kB/s after #2093",
			Prevents: "sizing the volume and checkpoint settings from a guess"},
	}
	if opt.watch != nil {
		cs = append(cs, mountChecks(res.Mount, maxLimit)...)
	}
	return cs
}

func commitMarkdown(b *strings.Builder, res *commitResults) {
	b.WriteString("\n## Commit latency\n")
	latencyTable(b, [][2]any{
		{"append service", res.Append}, {"append response", res.AppendResponse},
		{"renewal", res.Renew}, {"SELECT 1 round trip", res.RoundTrip},
	})
	fmt.Fprintf(b, "\nAchieved %.2f appends/s against a %.2f/s target over %.0f s; payload %.1f kB/s; WAL %.1f kB/s; turn reacquired %d times.\n",
		res.AchievedRate, res.TargetRate, res.MeasuredSec, res.PayloadkBps, res.WALkBps, res.Reacquired)
	if len(res.Worst) > 0 {
		b.WriteString("\nWorst 10-second windows by errors, then maximum response:\n\n| Start | Appends | Errors | p99 ms | max ms |\n| --- | ---: | ---: | ---: | ---: |\n")
		for _, w := range res.Worst {
			fmt.Fprintf(b, "| %s | %d | %d | %.2f | %.2f |\n", w.Start.Format(time.RFC3339), w.Count, w.Errors, w.P99Ms, w.MaxMs)
		}
	}
	if m := res.Mount; m != nil {
		fmt.Fprintf(b, "\n## Mount isolation\n\n%d probes; unresponsive for %.1f s in %d windows.\n", m.Probes, m.UnresponsiveSec, len(m.Windows))
		if len(m.Windows) > 0 {
			b.WriteString("\n| Start | Seconds | Reason | Appends | Append max ms | Renew max ms | Errors |\n| --- | ---: | --- | ---: | ---: | ---: | ---: |\n")
			for _, w := range m.Windows {
				fmt.Fprintf(b, "| %s | %.1f | %s | %d | %.2f | %.2f | %d |\n", w.Start.Format(time.RFC3339), w.Seconds, w.Reason, w.Append.Count, w.Append.MaxMs, w.RenewMaxMs, w.Errors)
			}
		}
	}
}

// sleepUntil waits for t or cancellation and reports whether t arrived.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sleepCtx(ctx context.Context, d time.Duration) { sleepUntil(ctx, time.Now().Add(d)) }

// dirProbe lists a few entries of a directory on the mount. A hung SMB mount
// blocks this call in the kernel; the watcher times it out from outside.
func dirProbe(dir string) func() error {
	return func() error {
		f, err := os.Open(dir)
		if err != nil {
			return probeErr(err)
		}
		defer f.Close()
		if _, err := f.ReadDir(8); err != nil && !errors.Is(err, io.EOF) {
			return probeErr(err)
		}
		return nil
	}
}

// probeErr drops the path, which is private, and keeps the errno text.
func probeErr(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return errors.New("probe failed")
}
