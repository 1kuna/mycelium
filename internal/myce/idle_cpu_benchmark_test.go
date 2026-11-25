//go:build !race

package myce

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Run with -benchtime=1x. One iteration is a fixed traffic window, so the
// rates remain comparable when an endpoint starts responding faster.
// MYCE_BENCH_COMMIT_UNDER_LOCK=true measures the rollback path.
func BenchmarkQueueIdleTraffic(b *testing.B) {
	source, workers := candidateCostFixture(b)
	dir := b.TempDir()
	db := filepath.Join(dir, "queue.db")
	q, err := Open(db, filepath.Join(dir, "checkpoints"), RealClock{}, WithQueueCommitUnderLock(os.Getenv("MYCE_BENCH_COMMIT_UNDER_LOCK") == "true"))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	q.admissionWaitBuild.synchronous = false
	q.checkpointProbe = func() bool { return true }
	for _, m := range workers {
		if _, err := q.Checkin(Checkin{Machine: m, period: time.Second}); err != nil {
			b.Fatal(err)
		}
	}
	admissionWaitBenchmarkSettle(source)
	now := q.clock.Now()
	err = q.update(func() error {
		source.mu.Lock()
		defer source.mu.Unlock()
		for i := range 11500 {
			j := cloneJob(source.s.Jobs[fmt.Sprintf("retained-%03d", i%160)])
			j.ID = fmt.Sprintf("history-%05d", i)
			j.Queued = now.Add(-time.Duration(i+60) * time.Second)
			j.StartedAt, j.FinishedAt = &j.Queued, &j.Queued
			j.Timing.Execution.ID = j.ID
			q.putJob(j.ID, &j)
		}
		for i := range 29 {
			j := cloneJob(source.s.Jobs[fmt.Sprintf("queued-%03d", i)])
			// The demand is real, but its required tool is absent.
			j.Spec.Needs.Tools = []string{"benchmark-missing-tool"}
			j.Queued = now
			if i >= 10 {
				j.State = "paused"
			}
			q.putJob(j.ID, &j)
		}
		for i := range 2 {
			j := cloneJob(source.s.Jobs[fmt.Sprintf("running-%d", i)])
			j.Queued, j.StartedAt = now, &now
			q.putJob(j.ID, &j)
		}
		q.s.WorkObservations = make(map[string]WorkObservation, len(source.s.WorkObservations))
		for key, observation := range source.s.WorkObservations {
			q.touchCatalog("work_observations/" + key)
			q.s.WorkObservations[key] = observation
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	budgetHost(q, 8<<30, 4)
	admissionWaitBenchmarkSettle(q)
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test", HTTP: server.Client()}
	actions := make([]idleTraffic, 0, 13)
	for i, m := range workers {
		check := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}
		if i < 2 {
			check.Attempts = []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}
		}
		actions = append(actions, idleTraffic{"checkin", time.Second, func(ctx context.Context) error {
			var reply Reply
			return client.Call(ctx, "POST", "/api/workers/checkin", check, &reply)
		}})
		var after string
		actions = append(actions, idleTraffic{"events", 0, func(ctx context.Context) error {
			var event WorkerEvent
			err := client.Call(ctx, "GET", "/api/workers/"+m.ID+"/events?session="+m.Session+"&after="+after, nil, &event)
			after = event.Cursor
			return err
		}})
	}
	for _, poll := range []struct{ name, query string }{
		{"compact", ""}, {"active", "?jobs=active"}, {"recent", "?jobs=recent&limit=40&sections=jobs"},
	} {
		actions = append(actions, idleTraffic{poll.name, 2 * time.Second, func(ctx context.Context) error {
			r, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/status"+poll.query, nil)
			if err != nil {
				return err
			}
			r.Header.Set("Authorization", "Bearer test")
			v, err := server.Client().Do(r)
			if err != nil {
				return err
			}
			defer v.Body.Close()
			if v.StatusCode != http.StatusOK {
				return fmt.Errorf("status HTTP %d", v.StatusCode)
			}
			_, err = io.Copy(io.Discard, v.Body)
			return err
		}})
	}
	for _, id := range []string{"running-0", "running-1", "queued-001", "queued-003"} {
		after := map[string]uint64{}
		actions = append(actions, idleTraffic{"wait", 0, func(ctx context.Context) error {
			reply, err := client.WaitBatchCollected(ctx, []string{id}, after, 2)
			if err == nil {
				after[id] = reply.Jobs[0].Revision
			}
			return err
		}})
	}

	// Observe actual WAL growth without automatic recycling changing the file's
	// length during the sample. The DB still uses its normal FULL commits.
	if _, err := q.db.Exec("PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		b.Fatal(err)
	}
	var persistence queuePersistenceTiming
	var commits, checkinHolds, checkinTurns []time.Duration
	pending, classes := map[string]int{}, map[string]*idleWriteRate{}
	q.observeQueueRow = func(query string, args []any) {
		if !strings.HasPrefix(query, "INSERT") {
			return
		}
		class := idleWriteClass(query, args)
		for _, arg := range args {
			switch v := arg.(type) {
			case string:
				pending[class] += len(v)
			case []byte:
				pending[class] += len(v)
			case json.RawMessage:
				pending[class] += len(v)
			case int, int64, float64:
				pending[class] += 8
			}
		}
	}
	q.observePersistence = func(p queuePersistenceTiming) {
		persistence.Bytes += p.Bytes
		persistence.Records += p.Records
		persistence.Commit += p.Commit
		if p.Commit > 0 {
			commits = append(commits, p.Commit)
			for class, size := range pending {
				if classes[class] == nil {
					classes[class] = &idleWriteRate{}
				}
				classes[class].bytes += size
				classes[class].commits++
			}
		}
		clear(pending)
	}
	q.observePersistenceHold = func(p queueHoldPhases) {
		if p.checkinBody {
			checkinHolds = append(checkinHolds, p.mutexHold())
			checkinTurns = append(checkinTurns, p.duration())
		}
	}
	stopProfile := idleCPUProfile(b)
	defer stopProfile()
	b.ResetTimer()
	cpu := benchmarkCPU()
	start := time.Now()
	queueCtx, stopQueue := context.WithCancel(context.Background())
	defer stopQueue()
	queueDone := make(chan error, 1)
	go func() { queueDone <- q.Run(queueCtx) }()
	samples := runIdleTraffic(b, actions, time.Duration(b.N)*30*time.Second)
	stopQueue()
	if err := <-queueDone; err != nil {
		b.Fatal(err)
	}
	admissionWaitBenchmarkSettle(q)
	elapsed := time.Since(start)
	used := benchmarkCPU() - cpu
	b.StopTimer()
	stopProfile()
	b.ReportMetric(float64(used)*float64(time.Minute)/float64(elapsed)/1e9, "CPU-s/min")
	wal, err := os.Stat(db + "-wal")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(max(0, wal.Size()-32))/elapsed.Seconds(), "WAL-B/s")
	b.ReportMetric(float64(persistence.Bytes)/elapsed.Seconds(), "payload-B/s")
	b.ReportMetric(float64(persistence.Records)/elapsed.Seconds(), "records/s")
	b.ReportMetric(float64(persistence.Commit)/float64(time.Millisecond)/elapsed.Seconds(), "commit-ms/s")
	b.ReportMetric(float64(len(commits))/elapsed.Seconds(), "commits/s")
	for class, rate := range classes {
		b.ReportMetric(float64(rate.bytes)/elapsed.Seconds(), class+"-B/s")
		b.ReportMetric(float64(rate.commits)/elapsed.Seconds(), class+"-commits/s")
	}
	samples["checkin-mutex"] = checkinHolds
	samples["checkin-turn"] = checkinTurns
	samples["sqlite-commit"] = commits
	for name, durations := range samples {
		if len(durations) == 0 {
			b.Errorf("no completed %s requests", name)
			continue
		}
		slices.Sort(durations)
		b.ReportMetric(float64(len(durations)), name+"-requests")
		for _, pct := range []int{50, 95} {
			index := (len(durations)*pct+99)/100 - 1
			b.ReportMetric(float64(durations[index])/float64(time.Millisecond), fmt.Sprintf("%s-p%d-ms", name, pct))
		}
	}
}

type idleWriteRate struct{ bytes, commits int }

func idleWriteClass(query string, args []any) string {
	if strings.HasPrefix(query, "INSERT INTO usage_samples") {
		return "usage"
	}
	if !strings.HasPrefix(query, "INSERT INTO queue_state") {
		return "other"
	}
	key := args[0].(string)
	if parts := strings.SplitN(key, "/", 3); len(parts) == 3 && parts[0] == "fields" {
		logical, _ := base64.RawURLEncoding.DecodeString(parts[1])
		field, _ := base64.RawURLEncoding.DecodeString(parts[2])
		if string(field) == "admission_wait" {
			return "wait"
		}
		key = string(logical)
	}
	group, _, _ := strings.Cut(key, "/")
	switch group {
	case "jobs", "submissions", "machines", "state", "health_events":
		return group
	case "work_observations", "storage_observations", "external_memory":
		return "learned"
	default:
		return "other"
	}
}

type idleTraffic struct {
	name    string
	period  time.Duration
	request func(context.Context) error
}

func runIdleTraffic(b *testing.B, actions []idleTraffic, duration time.Duration) map[string][]time.Duration {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var group sync.WaitGroup
	var mu sync.Mutex
	samples := make(map[string][]time.Duration)
	for i, action := range actions {
		group.Go(func() {
			// Stagger agents so the fixture also exercises reads between writes.
			first := time.NewTimer(time.Duration(i) * 50 * time.Millisecond)
			defer first.Stop()
			select {
			case <-ctx.Done():
				return
			case <-first.C:
			}
			var ticker *time.Ticker
			if action.period > 0 {
				ticker = time.NewTicker(action.period)
				defer ticker.Stop()
			}
			var durations []time.Duration
			defer func() {
				mu.Lock()
				samples[action.name] = append(samples[action.name], durations...)
				mu.Unlock()
			}()
			for ctx.Err() == nil {
				start := time.Now()
				if err := action.request(ctx); err == nil {
					durations = append(durations, time.Since(start))
				} else if ctx.Err() == nil {
					b.Error(action.name, err)
					return
				}
				if ticker != nil {
					select {
					case <-ctx.Done():
					case <-ticker.C:
					}
				}
			}
		})
	}
	time.Sleep(duration)
	cancel()
	group.Wait()
	return samples
}

func idleCPUProfile(b *testing.B) func() {
	b.Helper()
	path := os.Getenv("MYCE_BENCH_CPU_PROFILE")
	if path == "" {
		return func() {}
	}
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		b.Fatal(err)
	}
	return sync.OnceFunc(func() {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			b.Error(err)
		}
	})
}
