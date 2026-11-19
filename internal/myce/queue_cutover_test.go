package myce

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

// Real process boundary, SQLite and API; all payloads and credentials are synthetic.
func TestQueueCutoverProcess(t *testing.T) {
	if os.Getenv("MYCE_CUTOVER_HELPER") != "1" {
		t.Skip("benchmark helper")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	q, err := Open(os.Getenv("MYCE_CUTOVER_DB"), os.Getenv("MYCE_CUTOVER_ROOT"), RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	}()
	l, err := net.Listen("tcp", os.Getenv("MYCE_CUTOVER_ADDR"))
	if err != nil {
		t.Fatal(err)
	}
	s := &http.Server{Handler: API{Queue: q, Token: "synthetic"}.Handler(), ConnContext: QueueConnectionContext}
	go s.Serve(l)
	fmt.Fprintln(os.Stdout, "ready")
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := q.ShutdownServers(shutdown, s); err != nil {
		t.Fatal(err)
	}
}

// Reproduce with go test ./internal/myce -run '^$' -bench '^BenchmarkQueueCutover$' -benchtime=3x.
// Timings use wall time only for measurement; queue decisions use its Clock.
func BenchmarkQueueCutover(b *testing.B) {
	q, workers := candidateCostFixture(b)
	for i := 160; i < 12400; i++ {
		j := *q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j.ID = fmt.Sprintf("retained-%05d", i)
		q.s.Jobs[j.ID] = &j
	}
	if err := q.update(func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	var db string
	var seq int
	var name string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &db); err != nil {
		b.Fatal(err)
	}
	root := q.root
	jobs, history := len(q.s.Jobs), len(q.s.WorkObservations)
	if err := q.Close(); err != nil {
		b.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	start := func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestQueueCutoverProcess$")
		cmd.Env = append(os.Environ(), "MYCE_CUTOVER_HELPER=1", "MYCE_CUTOVER_DB="+db, "MYCE_CUTOVER_ROOT="+root, "MYCE_CUTOVER_ADDR="+addr)
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			b.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if cmd.ProcessState == nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
		})
		scanner := bufio.NewScanner(pipe)
		if !scanner.Scan() || scanner.Text() != "ready" {
			b.Fatal("queue process failed to start")
		}
		return cmd
	}
	client := Client{URL: "http://" + addr, Token: "synthetic", HTTP: &http.Client{Timeout: 30 * time.Second}, singleAttempt: true}
	var stopWall, startWall, firstWall, workersWall, gapWall time.Duration
	cmd := start()
	b.ResetTimer()
	for range b.N {
		var job Job
		if err := client.Call(context.Background(), "GET", "/api/jobs/running-0?view=wait", nil, &job); err != nil {
			b.Fatal(err)
		}
		last := time.Now()
		requested := time.Now()
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			b.Fatal(err)
		}
		if err := cmd.Wait(); err != nil {
			b.Fatal(err)
		}
		stopped := time.Now()
		cmd = start()
		ready := time.Now()
		if err := client.Call(context.Background(), "GET", "/api/jobs/running-0?view=wait", nil, &job); err != nil {
			b.Fatal(err)
		}
		first := time.Now()
		for i, m := range workers {
			err := client.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}}, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
		reconnected := time.Now()
		stopWall += stopped.Sub(requested)
		startWall += ready.Sub(stopped)
		firstWall += first.Sub(ready)
		workersWall += reconnected.Sub(first)
		gapWall += first.Sub(last)
		b.Logf("jobs=%d history=%d workers=%d stop=%s new_process_ready=%s first_request=%s reconnect=%s API_gap=%s", jobs, history, len(workers), stopped.Sub(requested), ready.Sub(stopped), first.Sub(ready), reconnected.Sub(first), first.Sub(last))
	}
	b.StopTimer()
	cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		b.Fatal(err)
	}
	// Inspect the persisted artifacts after every measured restart, not merely HTTP success.
	restored, err := Open(db, root, q.clock)
	if err != nil {
		b.Fatal(err)
	}
	defer restored.Close()
	if len(restored.s.Jobs) != jobs || len(restored.s.WorkObservations) != history {
		b.Fatal("restart lost or duplicated durable records")
	}
	for i, m := range workers {
		j := restored.s.Jobs[fmt.Sprintf("running-%d", i)]
		if j.State != "running" || j.Attempt != 1 || j.Session != m.Session || j.Losses != 0 {
			b.Fatalf("attempt changed: %+v", j)
		}
	}
	for name, d := range map[string]time.Duration{"stop-ms": stopWall, "startup-ms": startWall, "first-ms": firstWall, "workers-ms": workersWall, "gap-ms": gapWall} {
		b.ReportMetric(float64(d)/float64(time.Millisecond)/float64(b.N), name)
	}
}
