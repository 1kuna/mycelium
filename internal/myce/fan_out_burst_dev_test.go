//go:build !race

package myce

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Exercise the implementation behind myce dev, not a simulated scheduler:
// real HTTP, SQLite commits, qualification, commands and worker heartbeats.
// The test keeps the burst's functional acceptance; deterministic loss
// decisions have fake-clock tests under race.
func TestDevFanOutBurst62RealTime(t *testing.T) {
	if testing.Short() {
		t.Skip("real-time dev burst acceptance")
	}
	devFanOutBurst62(t, false)
}

// Release latency acceptance: run with -run '^$' -bench DevFanOutBurst62
// -benchtime 1x on an otherwise idle host. Wall-clock thresholds live here,
// outside fulls, because shared-host load alone exceeded them (2.8 s, #1382).
// Race instrumentation changes service cost and cannot establish this target.
func BenchmarkDevFanOutBurst62(b *testing.B) {
	for range b.N {
		r := devFanOutBurst62(b, true)
		b.ReportMetric(float64(r.p99.Milliseconds()), "p99-ack-ms")
		b.ReportMetric(float64(r.maxHeartbeatAge.Milliseconds()), "max-heartbeat-age-ms")
		if r.p99 >= 500*time.Millisecond {
			b.Errorf("p99 ack %s exceeds 500ms target", r.p99)
		}
		if r.maxHeartbeatAge >= ProductionLossAfter {
			b.Errorf("heartbeat processing stalled for %s", r.maxHeartbeatAge)
		}
		for _, m := range r.snap.Machines {
			if m.Gone || r.snap.Now.Sub(m.LastSeen) >= ProductionLossAfter {
				b.Errorf("worker heartbeat stalled: %s gone=%v age=%s", m.ID, m.Gone, r.snap.Now.Sub(m.LastSeen))
			}
		}
		for _, j := range r.snap.Jobs {
			if j.Losses != 0 {
				b.Errorf("job %s losses=%d reason=%s", j.ID, j.Losses, j.Reason)
			}
		}
	}
}

type devFanOutBurstResult struct {
	p99             time.Duration
	maxHeartbeatAge time.Duration
	snap            Snapshot
}

// devFanOutBurst62 submits 62 concurrent fan-out parents against a live
// production command and retained history. With window, the readers and the
// sentinel keep running for six seconds after the burst so heartbeat age
// covers more than one production loss interval.
func devFanOutBurst62(t testing.TB, window bool) devFanOutBurstResult {
	t.Helper()
	root := newTestStateRoot(t)
	c := DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), ExtensionRoot: filepath.Join(root, "extensions"), JobFileRoot: filepath.Join(root, "nas"), Token: "burst-test", GPU: "cpu", Workers: 2, CPUBudgetCores: 4, Interval: 100 * time.Millisecond}
	if err := os.MkdirAll(c.JobFileRoot, 0700); err != nil {
		t.Fatal(err)
	}
	e := compositionExtension(t, false)
	e.Name, e.Version, e.Kinds = "burst", "1", nil
	e.Isolation, e.Reason = "unsandboxed", "Synthetic latency fixture measures queue control traffic; native sandbox qualification is separate."
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}
	v.Execution.Main.Command = []string{"sh", "-c", "printf done"}
	v.Baselines = map[string]ExtensionBaseline{"extension:burst": v.Baselines["extension:sample"]}
	v.References = map[string]WorkReference{"extension:burst": {InputShape: "tiny", Unit: "piece", Units: 1}}
	e = freezeComposition(t, e)
	if err := InstallExtension(c.ExtensionRoot, e); err != nil {
		t.Fatal(err)
	}
	phases := make(chan submitRequestTiming, 62)
	d, err := startTestDev(t, c, func(q *Queue) {
		// The package default is synchronous for fixtures that inspect mutable
		// evidence immediately. A real dev queue publishes it asynchronously;
		// waiting for every concurrent capture here manufactures a burst barrier.
		asynchronousAdmissionWait(t, q)
		q.observeSubmit = func(p submitRequestTiming) {
			if strings.HasPrefix(p.Name, "burst-") {
				phases <- p
			}
		}
		// Match incident retained-history cardinality. These are cold fixture records;
		// new parents and all running executions below use the ordinary API.
		now := q.clock.Now()
		for i := range 3000 {
			id := fmt.Sprintf("history-%04d", i)
			j := &Job{ID: id, State: "succeeded", Attempt: 1, ReleaseConfirmed: true, FinishedAt: &now}
			q.s.Jobs[id] = j
			if q.s.Submissions == nil {
				q.s.Submissions = map[string]*submissionRecord{}
			}
			q.s.Submissions[id] = &submissionRecord{Kind: submissionBinding, JobID: id, Job: j}
		}
		for i := range 400 {
			p := q.s.Jobs[fmt.Sprintf("history-%04d", i)]
			p.Spec.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "piece"}}}
			p.FanOut = &FanOutProgress{Children: []string{fmt.Sprintf("history-%04d", i+400)}}
			q.s.Jobs[fmt.Sprintf("history-%04d", i+800)].Spec.After = &PieceDependency{Job: p.ID, Piece: "piece"}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	sentinelGate := filepath.Join(root, "finish-sentinel")
	defer func() {
		if err := os.WriteFile(sentinelGate, nil, 0600); err != nil {
			t.Error(err)
		}
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	probeWait(t, ctx, func() bool { s, err := d.Queue.Snapshot(); return err == nil && len(s.Machines) == 2 })
	cache := &EnvironmentCache{Root: filepath.Join(root, "workers", "fake-1", "environment-cache"), Limit: 1 << 30}
	if _, err := CheckExtension(ctx, c.ExtensionRoot, e, "cpu", "", "dev-host", io.Discard, cache, d.Client()); err != nil {
		t.Fatal(err)
	}
	// A live production command spans the burst and >5s afterwards, so liveness
	// cannot pass merely because the queue has no active attempts to lose.
	sentinel := Spec{Name: "heartbeat-sentinel", Project: "burst", Priority: "production", Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}, Run: RunSpec{Command: []string{"sh", "-c", `printf ready; while [ ! -f "$1" ]; do sleep .1; done`, "sentinel", sentinelGate}}}
	active, err := d.Client().Submit(ctx, sentinel)
	if err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		j, err := d.Client().GetJob(ctx, active.ID)
		return err == nil && j.State == "running" && j.StartedAt != nil
	})
	stopReads := make(chan struct{})
	readErrors := make(chan error, 1)
	var readers sync.WaitGroup
	maxHeartbeatAge := time.Duration(0)
	readers.Add(2)
	go func() {
		defer readers.Done()
		for {
			d.Queue.mu.RLock()
			now := d.Queue.clock.Now()
			for _, m := range d.Queue.s.Machines {
				maxHeartbeatAge = max(maxHeartbeatAge, now.Sub(m.LastSeen))
			}
			d.Queue.mu.RUnlock()
			select {
			case <-stopReads:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stopReads:
				return
			default:
			}
			if _, err := d.Client().inspectionSnapshot(ctx); err != nil {
				readErrors <- err
				return
			}
			select {
			case <-stopReads:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	type ack struct {
		elapsed time.Duration
		job     *Job
		err     error
	}
	results := make(chan ack, 62)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(62)
	var began time.Time
	for i := range 62 {
		go func() {
			s := Spec{Name: fmt.Sprintf("burst-%02d", i), Project: "burst", Priority: "production", SubmissionKey: fmt.Sprintf("burst-%02d", i), Extension: extensionKey(e), Workload: &Workload{InputShape: "tiny", Units: 1}, FanOut: &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "first"}, {Key: "second"}}}}
			ready.Done()
			<-start
			j, err := d.Client().Submit(ctx, s)
			results <- ack{time.Since(began), j, err}
		}()
	}
	ready.Wait()
	began = time.Now()
	close(start)
	durations := make([]time.Duration, 0, 62)
	ids := map[string]bool{active.ID: true}
	for range 62 {
		r := <-results
		durations = append(durations, r.elapsed)
		if r.err != nil {
			t.Error("submit:", r.err)
		} else {
			ids[r.job.ID] = true
			if r.job.FanOut == nil {
				t.Error("ack missing fan-out parent", r.job.ID)
			}
		}
	}
	if len(ids) != 63 {
		t.Errorf("expected 62 distinct parents and one sentinel, got %d jobs", len(ids))
	}
	slices.Sort(durations)
	// Nearest-rank p99 for 62 samples is the maximum: every ack must meet the
	// benchmark's bound, with no retries, staggering or prewarmed connections.
	p99 := durations[(99*len(durations)+99)/100-1]
	t.Logf("62 concurrent fan-out submits: p50=%s p99=max=%s", durations[len(durations)/2], p99)
	for range 62 {
		var p submitRequestTiming
		select {
		case p = <-phases:
		case <-ctx.Done():
			t.Fatal("missing submit phase observation:", ctx.Err())
		}
		total := p.Finished.Sub(p.Started)
		work := total - p.Admission - p.Decode - p.Wait - p.SQLiteCommit - p.Response - p.EvidenceWait
		t.Logf("submit_phase name=%s start_ms=%.3f end_ms=%.3f total_ms=%.3f admission_ms=%.3f decode_ms=%.3f lock_wait_ms=%.3f sqlite_commit_ms=%.3f response_ms=%.3f evidence_wait_ms=%.3f work_ms=%.3f lock_hold_ms=%.3f", p.Name, holdMS(p.Started.Sub(began)), holdMS(p.Finished.Sub(began)), holdMS(total), holdMS(p.Admission), holdMS(p.Decode), holdMS(p.Wait), holdMS(p.SQLiteCommit), holdMS(p.Response), holdMS(p.EvidenceWait), holdMS(work), holdMS(p.Hold))
	}
	if window {
		until := time.Now().Add(6 * time.Second)
		for time.Now().Before(until) {
			time.Sleep(100 * time.Millisecond)
		}
	}
	close(stopReads)
	readers.Wait()
	t.Logf("maximum sampled accepted-heartbeat age: %s", maxHeartbeatAge)
	select {
	case err := <-readErrors:
		t.Error("status reader:", err)
	default:
	}
	// A wave completes asynchronously after the burst; wait for one (the
	// functional test has no six-second window) rather than sleeping.
	waveCtx, cancelWave := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancelWave()
	probeWait(t, waveCtx, func() bool {
		s, err := d.Queue.Snapshot()
		if err != nil {
			return false
		}
		for _, j := range s.Jobs {
			if j.Batch != nil && j.State == "succeeded" {
				return true
			}
		}
		return false
	})
	snap, err := d.Queue.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	completedWaves := 0
	for _, j := range snap.Jobs {
		if j.Batch != nil && j.State == "succeeded" {
			completedWaves++
		}
		if j.State == "failed" {
			t.Errorf("job failed: %s %s", j.ID, j.Reason)
		}
	}
	if completedWaves == 0 {
		t.Error("no real fan-out wave completed")
	}
	select {
	case err := <-d.Errors():
		t.Error("dev runtime:", err)
	default:
	}
	return devFanOutBurstResult{p99, maxHeartbeatAge, snap}
}
