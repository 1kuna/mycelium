package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPreDeployFullSystem is a laptop acceptance scenario, not a hardware or
// exactly-once external-side-effect guarantee. Only GPU inventory/model engines
// are fake: requests, SQLite, worker spools, signals and commands are real.
func TestPreDeployFullSystem(t *testing.T) {
	clock := &manualWorkerClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	workerClocks := map[string]*manualWorkerClock{}
	for _, gpu := range []string{"cuda", "xpu", "metal"} {
		workerClocks[gpu] = &manualWorkerClock{now: clock.Now()}
	}
	// Worker retry clocks are independent of queue loss time. Releasing a retry
	// backoff must not artificially age healthy peers in the queue.
	clockStop := make(chan struct{})
	clockDone := make(chan struct{})
	go func() {
		defer close(clockDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				clock.Advance(5 * time.Millisecond)
				for _, wc := range workerClocks {
					wc.Advance(5 * time.Millisecond)
				}
			case <-clockStop:
				return
			}
		}
	}()
	t.Cleanup(func() { close(clockStop); <-clockDone })
	base := t.TempDir()
	nas := filepath.Join(base, "nas")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.Mkdir(nas, 0700))
	root := filepath.Join(base, "queue-mount")
	must(os.Symlink(nas, root))
	config := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{
		{Name: "cuda-model", Engine: "fake", Path: "fake", MemoryGB: 8, Platforms: []string{"cuda"}},
		{Name: "metal-model", Engine: "fake", Path: "fake", MemoryGB: 8, Platforms: []string{"metal"}},
	}}
	database := filepath.Join(base, "queue.db")
	open := func() *Queue {
		t.Helper()
		q, err := Open(database, root, clock)
		must(err)
		must(q.ConfigureModels(config))
		q.SetRestartGrace(2 * time.Minute)
		must(q.SetLossTimeouts(10*time.Second, 20*time.Second, 30*time.Second))
		return q
	}
	q := open()
	// Swap the actual API/SQLite queue under an exclusive lock. No request can
	// touch a closed DB, and workers keep the same endpoint across the restart.
	var apiMu sync.RWMutex
	handler := API{Queue: q, Token: "predeploy-token"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiMu.RLock()
		defer apiMu.RUnlock()
		handler.ServeHTTP(w, r)
	}))
	client := Client{URL: server.URL, Token: "predeploy-token", HTTP: &http.Client{Timeout: 5 * time.Second}}
	call := func(method, path string, in, out any) {
		t.Helper()
		must(client.Call(context.Background(), method, path, in, out))
	}
	// A terminal ledger rejects state regression during every observed phase.
	ledger := map[string]string{}
	snapshot := func() Snapshot {
		t.Helper()
		var s Snapshot
		call("GET", "/api/status", nil, &s)
		for _, j := range s.Jobs {
			if old, ok := ledger[j.ID]; ok && j.State != old {
				t.Fatalf("terminal job %s changed from %s to %s", j.ID, old, j.State)
			}
			if terminal(j.State) {
				ledger[j.ID] = j.State
			}
		}
		return s
	}
	get := func(id string) *Job {
		t.Helper()
		for _, j := range snapshot().Jobs {
			if j.ID == id {
				return j
			}
		}
		t.Fatalf("lost job %s", id)
		return nil
	}
	logs := func(id string) string {
		t.Helper()
		var ls []LogChunk
		call("GET", "/api/jobs/"+id+"/logs", nil, &ls)
		var b strings.Builder
		for _, l := range ls {
			b.WriteString(l.Text)
		}
		return b.String()
	}
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.NewTimer(8 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			if condition() {
				return
			}
			select {
			case <-deadline.C:
				s := snapshot()
				var b strings.Builder
				for _, j := range s.Jobs {
					fmt.Fprintf(&b, "%s=%s/%s/attempt%d; ", j.Spec.Name, j.State, j.Machine, j.Attempt)
				}
				for _, m := range s.Machines {
					fmt.Fprintf(&b, "%s gone=%t NASdown=%t seen=%s; ", m.ID, m.Gone, m.CheckpointUnavailable, m.LastSeen)
				}
				t.Fatalf("%s: timed out; warning=%q; %s", label, s.Warning, b.String())
			case <-ticker.C:
			}
		}
	}
	writeGate := func(gate string) { t.Helper(); must(os.WriteFile(gate, nil, 0600)) }
	var submitted []string
	submitAPI := func(s Spec) *Job {
		t.Helper()
		var j Job
		call("POST", "/api/jobs", s, &j)
		submitted = append(submitted, j.ID)
		return &j
	}
	// The command records both inherited checkpoint progress and its local mount
	// alias. Saves use temporary-file/rename; an unfinished .tmp must never resume.
	const command = `set -eu
[ -z "${MYCE_TOKEN:-}" ] || exit 90
[ ! -e "$MYCE_CHECKPOINT_DIR/unfinished.tmp" ] || exit 91
n=0
if [ -f "$MYCE_CHECKPOINT_DIR/progress" ]; then n=$(cat "$MYCE_CHECKPOINT_DIR/progress"); fi
printf 'START attempt=%s resume=%s root=%s\n' "$MYCE_ATTEMPT" "$n" "$MYCE_CHECKPOINT_DIR"
n=$((n+1))
save() { while ! (printf '%s\n' "$n" > "$MYCE_CHECKPOINT_DIR/progress.tmp" && mv "$MYCE_CHECKPOINT_DIR/progress.tmp" "$MYCE_CHECKPOINT_DIR/progress") 2>/dev/null; do sleep 0.01; done; }
save
printf 'partial' > "$MYCE_CHECKPOINT_DIR/unfinished.tmp"
trap 'save; echo PAUSED; exit 0' TERM
while [ ! -f "$FINISH" ]; do sleep 0.01; done
save
echo FINISHED
`
	makeSpec := func(name, priority string, allowed []string, gate string) Spec {
		s := spec(priority)
		s.Name = name
		s.AllowedMachines = allowed
		s.Run.Command = []string{"sh", "-c", command}
		s.Run.Env = map[string]string{"FINISH": gate}
		return s
	}
	cudaGate, xpuGate, metalGate := filepath.Join(base, "cuda-finish"), filepath.Join(base, "xpu-finish"), filepath.Join(base, "metal-finish")
	experiment := submitAPI(makeSpec("migrating-experiment", "experiment", []string{"cuda", "xpu"}, cudaGate))
	xpuJob := submitAPI(makeSpec("xpu-deadline", "deadline", []string{"xpu"}, xpuGate))
	metalJob := submitAPI(makeSpec("metal-production", "production", []string{"metal"}, metalGate))
	type liveWorker struct {
		transport    *partitionTransport
		cancel       context.CancelFunc
		done         chan error
		alias, state string
		stopped      bool
	}
	workers := map[string]*liveWorker{}
	for _, gpu := range []string{"cuda", "xpu", "metal"} {
		alias := filepath.Join(base, gpu+"-mount")
		must(os.Symlink(nas, alias))
		transport := &partitionTransport{}
		ctx, cancel := context.WithCancel(context.Background())
		lw := &liveWorker{transport: transport, cancel: cancel, done: make(chan error, 1), alias: alias, state: filepath.Join(base, gpu+"-state")}
		workers[gpu] = lw
		w := Worker{CheckpointRoot: alias, ModelConfig: config, StateDir: lw.state, Clock: workerClocks[gpu], Interval: 5 * time.Millisecond,
			Machine: machine(gpu, gpu), Client: Client{URL: server.URL, Token: "predeploy-token", HTTP: &http.Client{Transport: transport, Timeout: time.Second}}}
		go func() { lw.done <- w.Run(ctx) }()
	}
	// Cleanup also handles failed assertions while a NAS or network fault is active.
	t.Cleanup(func() {
		if _, err := os.Stat(nas); errors.Is(err, os.ErrNotExist) {
			_ = os.Rename(nas+"-offline", nas)
		}
		for _, lw := range workers {
			lw.transport.down.Store(false)
		}
		for _, id := range submitted {
			_ = client.Call(context.Background(), "POST", "/api/jobs/"+id+"/cancel", nil, nil)
		}
		for _, lw := range workers {
			if !lw.stopped {
				lw.cancel()
			}
		}
		for name, lw := range workers {
			if lw.stopped {
				continue
			}
			select {
			case err := <-lw.done:
				if err != nil {
					t.Errorf("cleanup %s: %v", name, err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("cleanup %s did not stop", name)
			}
		}
		server.Close()
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	wait("all platforms execute", func() bool {
		return strings.Contains(logs(experiment.ID), "START") && strings.Contains(logs(xpuJob.ID), "START") && strings.Contains(logs(metalJob.ID), "START")
	})
	for id, gpu := range map[string]string{experiment.ID: "cuda", xpuJob.ID: "xpu", metalJob.ID: "metal"} {
		j := get(id)
		if j.Machine != gpu || j.Attempt != 1 {
			t.Fatalf("initial placement %+v", j)
		}
		if !strings.Contains(logs(id), workers[gpu].alias) {
			t.Fatalf("%s did not use its NAS alias: %s", gpu, logs(id))
		}
	}

	// A real OpenAI HTTP request must preempt the lower-priority resumable batch,
	// load the explicit fake engine and return one complete answer.
	modelCall := func(model, priority string, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"predeploy"}]}`, model)))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer predeploy-token")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Myce-Project", "predeploy")
		req.Header.Set("X-Myce-Priority", priority)
		if priority == "deadline" {
			req.Header.Set("X-Myce-Deadline", clock.Now().Add(time.Hour).Format(time.RFC3339))
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		if response.StatusCode != 200 {
			return fmt.Errorf("model HTTP %d: %s", response.StatusCode, body)
		}
		var answer struct {
			Model   string `json:"model"`
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		if err = json.Unmarshal(body, &answer); err != nil {
			return err
		}
		if answer.Model != model || len(answer.Choices) != 1 || answer.Choices[0].Message.Content != "fake answer" || strings.Count(string(body), "fake answer") != 1 {
			return fmt.Errorf("missing/duplicate model answer: %s", body)
		}
		return nil
	}
	must(modelCall("cuda-model", "deadline", 8*time.Second))
	if !strings.Contains(logs(experiment.ID), "PAUSED") || get(experiment.ID).State != "queued" {
		t.Fatal("deadline model did not checkpoint/preempt experiment")
	}
	var slotFound bool
	for _, slot := range snapshot().LoadedModels {
		if slot.Model == "cuda-model" && slot.State == "ready" && slot.Priority == "experiment" && slot.InFlight == 0 {
			slotFound = true
		}
	}
	if !slotFound {
		t.Fatal("completed model inventory did not return to idle experiment priority")
	}
	// Submit an ordinary experiment first; resumed work must get the front of the
	// line after a production batch drains the lower-priority model reservation.
	short := func(name, priority string, allowed []string) Spec {
		s := spec(priority)
		s.Name = name
		s.AllowedMachines = allowed
		s.Run.Command = []string{"sh", "-c", "echo FINISHED"}
		return s
	}
	later := submitAPI(short("later-experiment", "experiment", []string{"cuda"}))
	production := submitAPI(short("urgent-batch", "production", []string{"cuda"}))
	wait("production then checkpoint resume", func() bool {
		return get(production.ID).State == "succeeded" && get(experiment.ID).Attempt == 2 && strings.Contains(logs(experiment.ID), "attempt=2 resume=1")
	})
	if get(later.ID).State != "queued" {
		t.Fatal("ordinary experiment jumped ahead of resumed work")
	}
	if len(snapshot().LoadedModels) != 0 {
		t.Fatal("batch reservation did not unload model")
	}

	// All transports stop together. A completed offline command/result is durable,
	// no queued successor starts, and fake-time silence never requeues the fleet.
	for _, lw := range workers {
		lw.transport.down.Store(true)
	}
	before := get(experiment.ID)
	writeGate(metalGate)
	metalNext := submitAPI(short("metal-successor", "experiment", []string{"metal"}))
	clock.Advance(10 * time.Minute)
	s := snapshot()
	if !strings.Contains(s.Warning, "network") {
		t.Fatalf("whole-network warning: %q", s.Warning)
	}
	for _, id := range []string{experiment.ID, xpuJob.ID, metalJob.ID} {
		j := get(id)
		if j.State != "running" {
			t.Fatalf("whole-network silence requeued %+v", j)
		}
	}
	wait("durable offline completion", func() bool {
		entries, _ := os.ReadDir(workers["metal"].state)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".result.json") {
				return true
			}
		}
		return false
	})
	if get(metalNext.ID).Attempt != 0 {
		t.Fatal("worker started new work while disconnected")
	}
	err := modelCall("metal-model", "experiment", 150*time.Millisecond)
	if err == nil || !(errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "HTTP")) {
		t.Fatalf("offline model must fail clearly without an answer: %v", err)
	}

	// Restart the queue while commands/spooled results are disconnected. Startup
	// grace preserves the old execution identity until workers report again.
	apiMu.Lock()
	must(q.Close())
	q = open()
	handler = API{Queue: q, Token: "predeploy-token"}.Handler()
	apiMu.Unlock()
	if j := get(experiment.ID); j.Attempt != before.Attempt || j.Session != before.Session || j.State != "running" {
		t.Fatalf("restart lost assignment: %+v", j)
	}
	for _, lw := range workers {
		lw.transport.down.Store(false)
	}
	clock.Advance(time.Second)
	wait("restart reconciliation and offline replay", func() bool { return get(metalJob.ID).State == "succeeded" && get(metalNext.ID).State == "succeeded" })
	if j := get(experiment.ID); j.Attempt != before.Attempt || j.Starts != before.Starts {
		t.Fatal("restart unnecessarily reran live command")
	}
	if strings.Count(logs(metalJob.ID), "FINISHED") != 1 {
		t.Fatal("offline completion/log replay duplicated output")
	}
	// Let all live workers report before expiring startup recovery. Scheduling
	// intentionally remains held during restart grace, even with a healthy API.
	reconnectTime := clock.Now()
	clock.Advance(time.Second)
	wait("all workers reconcile before grace expires", func() bool {
		for _, m := range snapshot().Machines {
			if m.LastSeen.Before(reconnectTime) {
				return false
			}
		}
		return true
	})
	clock.Advance(2 * time.Minute)
	graceEnd := clock.Now()
	wait("healthy inventory after restart grace", func() bool {
		for _, m := range snapshot().Machines {
			if m.LastSeen.Before(graceEnd) {
				return false
			}
		}
		return true
	})

	// Rename the shared backing directory: every mount alias becomes unavailable.
	// Running commands keep their assignment, queued work cannot start/preempt,
	// and no worker creates a replacement local mount.
	must(os.Rename(nas, nas+"-offline"))
	nasJob := submitAPI(short("nas-blocked-production", "production", []string{"cuda"}))
	clock.Advance(time.Second)
	wait("NAS status on all workers", func() bool {
		s := snapshot()
		if !strings.Contains(s.Warning, "NAS") {
			return false
		}
		for _, m := range s.Machines {
			if !m.CheckpointUnavailable {
				return false
			}
		}
		return true
	})
	if j := get(experiment.ID); j.State != "running" || j.Attempt != before.Attempt {
		t.Fatal("NAS outage stopped running command")
	}
	if get(nasJob.ID).Attempt != 0 {
		t.Fatal("job started with NAS absent")
	}
	if _, err := os.Stat(nas); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created replacement NAS: %v", err)
	}
	must(os.Rename(nas+"-offline", nas))
	clock.Advance(time.Second)
	wait("NAS recovery and priority resume", func() bool {
		return get(nasJob.ID).State == "succeeded" && get(experiment.ID).Attempt == 3 && strings.Contains(logs(experiment.ID), "attempt=3 resume=2")
	})

	// Isolate CUDA alone, keeping healthy peers. Its resumable attempt migrates to
	// XPU after experiment loss timeout. The old copy must lose on reconnection.
	writeGate(xpuGate)
	wait("free XPU", func() bool { return get(xpuJob.ID).State == "succeeded" })
	old := get(experiment.ID)
	workers["cuda"].transport.down.Store(true)
	// Advance in small increments so peers provide continuous evidence of health;
	// a single fleet-wide time jump would correctly grant a reconnect grace.
	for i := 0; i < 7; i++ {
		clock.Advance(5 * time.Second)
		now := clock.Now()
		wait("healthy peers check in", func() bool {
			seen := 0
			for _, m := range snapshot().Machines {
				if (m.ID == "xpu" || m.ID == "metal") && !m.LastSeen.Before(now) {
					seen++
				}
			}
			return seen == 2
		})
	}
	wait("isolated worker reassignment", func() bool {
		j := get(experiment.ID)
		return j.Machine == "xpu" && j.Attempt == old.Attempt+1 && strings.Contains(logs(j.ID), fmt.Sprintf("attempt=%d resume=3", j.Attempt))
	})
	migrated := get(experiment.ID)
	wait("new owner has a newer complete checkpoint", func() bool {
		oldPath := filepath.Join(nas, old.ID, fmt.Sprintf("attempt-%d", old.Attempt), "progress")
		newPath := filepath.Join(nas, old.ID, fmt.Sprintf("attempt-%d", migrated.Attempt), "progress")
		oldInfo, oldErr := os.Stat(oldPath)
		newInfo, newErr := os.Stat(newPath)
		data, err := os.ReadFile(newPath)
		return oldErr == nil && newErr == nil && err == nil && strings.TrimSpace(string(data)) == "4" && newInfo.ModTime().After(oldInfo.ModTime())
	})
	if migrated.Losses != 1 {
		t.Fatalf("single isolation counted %d losses", migrated.Losses)
	}
	workers["cuda"].transport.down.Store(false)
	// Release only CUDA's retry clock, without aging healthy queue peers.
	workerClocks["cuda"].Advance(30 * time.Second)
	clock.Advance(time.Second)
	wait("loser revoked and later experiment runs", func() bool { return get(later.ID).State == "succeeded" })
	// A fabricated old completion is sent through the API, exercising stale-result
	// rejection without depending on how quickly the loser uploads its spool.
	err = client.Call(context.Background(), "POST", "/api/workers/complete", result(old, true), nil)
	if !stale(err) {
		t.Fatalf("old execution completion accepted: %v", err)
	}
	if j := get(experiment.ID); j.Machine != "xpu" || j.Attempt != migrated.Attempt || j.State != "running" {
		t.Fatalf("returning loser changed owner: %+v", j)
	}

	// Planned leave checkpoints on XPU and resumes on CUDA through its different
	// NAS path, without consuming another infrastructure-loss retry.
	call("POST", "/api/workers/xpu/leave", map[string]bool{"now": false}, nil)
	wait("planned leave checkpoints and migrates", func() bool {
		j := get(experiment.ID)
		return j.Machine == "cuda" && j.Attempt == migrated.Attempt+1 && strings.Contains(logs(j.ID), fmt.Sprintf("attempt=%d resume=4", j.Attempt))
	})
	select {
	case err := <-workers["xpu"].done:
		must(err)
		workers["xpu"].stopped = true
	case <-time.After(5 * time.Second):
		t.Fatal("planned leave worker did not exit")
	}
	if get(experiment.ID).Losses != 1 {
		t.Fatal("planned leave consumed a loss retry")
	}
	writeGate(cudaGate)
	wait("final resumed experiment succeeds", func() bool { return get(experiment.ID).State == "succeeded" })
	// Exercise the other model priority tiers through the same real API/workers.
	must(modelCall("metal-model", "experiment", 8*time.Second))
	must(modelCall("cuda-model", "production", 8*time.Second))

	final := snapshot()
	if len(final.Jobs) != len(submitted) {
		t.Fatalf("lost/extra jobs: got %d want %d", len(final.Jobs), len(submitted))
	}
	for _, j := range final.Jobs {
		if j.State != "succeeded" || ledger[j.ID] != "succeeded" {
			t.Fatalf("unfinished/failed job %+v", j)
		}
		if n := strings.Count(logs(j.ID), "FINISHED"); n != 1 {
			t.Fatalf("job %s has %d terminal command markers", j.ID, n)
		}
	}
	// Replay terminal acknowledgements and attempt a contradictory result: neither
	// may change the terminal state or duplicate durable logs.
	winner := get(experiment.ID)
	call("POST", "/api/workers/complete", result(winner, true), nil)
	call("POST", "/api/workers/complete", result(winner, true), nil)
	err = client.Call(context.Background(), "POST", "/api/workers/complete", result(winner, false), nil)
	if err != nil && !stale(err) {
		t.Fatalf("terminal redelivery: %v", err)
	}
	if j := get(winner.ID); j.State != "succeeded" || strings.Count(logs(j.ID), "FINISHED") != 1 {
		t.Fatal("contradictory redelivery changed terminal state or duplicated output")
	}
	final = snapshot()
	for _, slot := range final.LoadedModels {
		if slot.InFlight != 0 {
			t.Fatalf("model request leaked: %+v", slot)
		}
	}
	// Every resumed attempt sees a complete predecessor checkpoint, never .tmp.
	for attempt := 2; attempt <= winner.Attempt; attempt++ {
		dir := filepath.Join(nas, winner.ID, fmt.Sprintf("attempt-%d", attempt))
		data, err := os.ReadFile(filepath.Join(dir, "progress"))
		must(err)
		if strings.TrimSpace(string(data)) != fmt.Sprint(attempt) {
			t.Fatalf("attempt %d checkpoint=%q", attempt, data)
		}
	}
	t.Logf("real API/SQLite/runner; cuda+xpu+metal; %d jobs succeeded once; experiment resumed %d times across priority, restart, network, NAS and leave faults; model calls answered once or timed out clearly", len(final.Jobs), winner.Attempt-1)
}
