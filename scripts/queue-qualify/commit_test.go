package main

import (
	"context"
	"encoding/json"
	"math"
	"math/rand/v2"
	"os"
	"testing"
	"time"
)

// The default profile must reproduce #2093's measured record payload rate
// (125.6 kB/s) at the stage 0 commit rate, or the load is a guess.
func TestQueueProfileMatchesMeasuredPayloadRate(t *testing.T) {
	p, err := parseProfile("queue")
	if err != nil {
		t.Fatal(err)
	}
	if got := defaultRate * p.mean() / 1000; math.Abs(got-125.6)/125.6 > 0.03 {
		t.Fatalf("queue profile gives %.1f kB/s", got)
	}
	if _, err := parseProfile("fixed:10"); err == nil {
		t.Fatal("tiny fixed payload accepted")
	}
	if f, err := parseProfile("fixed:4096"); err != nil || f.mean() != 4096 {
		t.Fatalf("fixed: %v %v", f, err)
	}
}

func TestRowsPayloadIsSizedStage2JSON(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, size := range []int{3900, 13800, 96000} {
		b := rowsPayload(size, rng)
		if d := math.Abs(float64(len(b)-size)) / float64(size); d > 0.1 {
			t.Errorf("size %d: got %d bytes", size, len(b))
		}
		var doc rowsDoc
		if err := json.Unmarshal(b, &doc); err != nil || doc.Version != 1 || len(doc.Rows) == 0 {
			t.Fatalf("payload shape: %v %+v", err, doc)
		}
	}
	pool := newPayloadPool(payloadProfile{Name: "x", Sizes: []int{1000, 50000}, Weights: []float64{0.5, 0.5}}, 9)
	small := 0
	for range 1000 {
		if len(pool.next()) < 5000 {
			small++
		}
	}
	if small < 400 || small > 600 {
		t.Fatalf("weights ignored: %d of 1000 small", small)
	}
}

func TestCommitChecksDeriveFromTheTurnTiming(t *testing.T) {
	opt := commitOptions{lease: 6 * time.Second, interval: time.Second, rate: 11.6}
	healthy := &commitResults{
		Append:         latencyStats{Count: 6960, MeanMs: 8, MaxMs: 300},
		AppendResponse: latencyStats{Count: 6960, P99Ms: 40},
		Renew:          latencyStats{Count: 600, P99Ms: 20, MaxMs: 120},
		Errors:         map[string]int{},
	}
	if v := verdict(commitChecks(healthy, opt)); v != statusPass {
		t.Fatalf("healthy run: %v %+v", v, commitChecks(healthy, opt))
	}
	breach := func(edit func(*commitResults), name string) {
		r := *healthy
		edit(&r)
		for _, c := range commitChecks(&r, opt) {
			if c.Name == name && c.Status == statusFail {
				return
			}
		}
		t.Errorf("%s did not fail", name)
	}
	// One renewal past half the 2 s call timeout fails the volume.
	breach(func(r *commitResults) { r.Renew.MaxMs = 1001 }, "append and renewal max")
	breach(func(r *commitResults) { r.AppendResponse.P99Ms = 251 }, "append response and renewal p99")
	breach(func(r *commitResults) { r.Append.MeanMs = 44 }, "mean append service time")
	breach(func(r *commitResults) { r.Append.Errors = 1 }, "no failed appends or renewals")
	// A longer lease scales the limits rather than hard-coding stage 2's.
	long := opt
	long.lease = 12 * time.Second
	r := *healthy
	r.Renew.MaxMs = 1500
	if v := verdict(commitChecks(&r, long)); v != statusPass {
		t.Fatalf("12 s lease should allow 1.5 s: %+v", commitChecks(&r, long))
	}
}

func TestCommitAgainstDisposablePostgres(t *testing.T) {
	pg := startPostgres(t)
	tg := pg.target(t)
	watched := t.TempDir()
	opt := commitOptions{duration: 3 * time.Second, warmup: 500 * time.Millisecond, lease: 600 * time.Millisecond,
		interval: 200 * time.Millisecond, opTimeout: 5 * time.Second, rate: 40, seed: 3,
		watch: &mountWatcher{probe: dirProbe(watched), interval: 100 * time.Millisecond, timeout: time.Second}}
	opt.profile, _ = parseProfile("queue")
	ctx := context.Background()
	info, res, err := runCommit(ctx, tg, opt)
	if err != nil {
		t.Fatal(err)
	}
	if info.SystemIdentifier == "" || res.Append.Count < 60 || res.Renew.Count < 10 || res.Append.Errors+res.Renew.Errors != 0 {
		t.Fatalf("results %+v", res)
	}
	if res.WALkBps <= 0 || res.PayloadkBps <= 0 || res.Mount == nil || res.Mount.Probes < 10 || len(res.Mount.Windows) != 0 {
		t.Fatalf("rates or mount %+v %+v", res, res.Mount)
	}
	// The run keeps its rows, never truncating, and releases the turn; a
	// second run can take over at once and continues the log.
	conn, err := tg.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var rows, head int64
	var handover int64
	var ready bool
	if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM myce_qualification.myce_changes_v1), head, handover, ready
		FROM myce_qualification.myce_coordinator_v1`).Scan(&rows, &head, &handover, &ready); err != nil {
		t.Fatal(err)
	}
	if rows < int64(res.Append.Count) || rows != head || handover != 1 || ready {
		t.Fatalf("after run: rows %d, head %d, handover %d, ready %v", rows, head, handover, ready)
	}
	opt.watch, opt.duration, opt.warmup = nil, time.Second, 0
	if _, res, err := runCommit(ctx, tg, opt); err != nil || res.Reacquired != 0 {
		t.Fatalf("second run: %v %+v", err, res)
	}
	var after int64
	conn.QueryRow(ctx, "SELECT count(*) FROM myce_qualification.myce_changes_v1").Scan(&after)
	if after <= rows {
		t.Fatalf("the second run did not add to the kept log: %d then %d", rows, after)
	}
	// A concurrent run is refused while one holds the database.
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", commitLockKey); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCommit(ctx, tg, opt); err == nil {
		t.Fatal("concurrent run accepted")
	}
	_ = os.Remove(watched)
}
