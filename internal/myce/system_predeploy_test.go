package myce

import (
	"context"
	"crypto/sha256"
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
	"sync/atomic"
	"testing"
	"time"
)

// TestPreDeployFullSystem is a laptop acceptance scenario, not a hardware or
// exactly-once external-side-effect guarantee. Only GPU inventory/model engines
// are fake: requests, SQLite, worker spools, signals and commands are real.
func TestPreDeployFullSystem(t *testing.T) {
	t.Parallel()
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
	base := newTestStateRoot(t)
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
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiMu.RLock()
		defer apiMu.RUnlock()
		handler.ServeHTTP(w, r)
	}))
	client := Client{URL: server.URL, Token: "predeploy-token", HTTP: &http.Client{Timeout: testWaitTimeout}}
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
		deadline := time.NewTimer(testWaitTimeout)
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
sequence=0
if [ -f "$MYCE_CHECKPOINT_DIR/progress.json" ]; then
 n=$(sed 's/.*"value":\([0-9]*\).*/\1/' "$MYCE_CHECKPOINT_DIR/progress.json")
 sequence=$(sed 's/.*"generation":\([0-9]*\).*/\1/' "$MYCE_CHECKPOINT_DIR/progress.json")
fi
printf 'START attempt=%s resume=%s root=%s\n' "$MYCE_ATTEMPT" "$n" "$MYCE_CHECKPOINT_DIR"
n=$((n+1))
save() { generation=$((sequence+1)); while ! (printf '{"myce_checkpoint":{"generation":%s},"value":%s}\n' "$generation" "$n" > "$MYCE_CHECKPOINT_DIR/progress.tmp" && mv "$MYCE_CHECKPOINT_DIR/progress.tmp" "$MYCE_CHECKPOINT_DIR/progress.json") 2>/dev/null; do sleep 0.01; done; sequence=$generation; }
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
	// Reserve the first CUDA model only after the deadline request has made
	// room for its load. The checkpoint-ordering phase below deliberately keeps
	// the experiment queued; idle warmth alone no longer reserves that card.
	initialQueue := q
	var reserveModel sync.Once
	modelReserved := make(chan error, 1)
	for _, gpu := range []string{"cuda", "xpu", "metal"} {
		alias := filepath.Join(base, gpu+"-mount")
		must(os.Symlink(nas, alias))
		transport := &partitionTransport{}
		ctx, cancel := context.WithCancel(context.Background())
		lw := &liveWorker{transport: transport, cancel: cancel, done: make(chan error, 1), alias: alias, state: filepath.Join(base, gpu+"-state")}
		workers[gpu] = lw
		// This journey verifies real saves, cross-mount resume and NAS loss.
		w := Worker{checkpointPrepare: prepareCheckpoint, CheckpointRoot: alias, ModelConfig: config, StateDir: lw.state, Clock: workerClocks[gpu], Interval: 5 * time.Millisecond,
			modelEngine: func(model Model) Engine {
				if model.Name == "cuda-model" {
					reserveModel.Do(func() {
						modelReserved <- initialQueue.Pin(ModelPin{Project: "predeploy", Model: model.Name, Machine: "cuda"})
					})
				}
				return &FakeEngine{}
			},
			InventoryProbe: func(context.Context) ([]MachineDevice, error) {
				return []MachineDevice{{ID: "0", Platform: gpu, Identity: "fixture-" + gpu, Fingerprint: "fixture-driver", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}, nil
			},
			CPUIsolationProbe: func(context.Context) error { return nil },
			Machine:           machine(gpu, gpu, "fixture-"+gpu+"-host"), Client: Client{URL: server.URL, Token: "predeploy-token", HTTP: &http.Client{Transport: transport, Timeout: testWaitTimeout}}}
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
			case <-time.After(testWaitTimeout):
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
	must(modelCall("cuda-model", "deadline", testWaitTimeout))
	must(<-modelReserved)
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
	must(q.Unpin("predeploy", "cuda-model"))
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
	must(q.Tick())
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
		t.Fatalf("NAS outage stopped running command: state=%s attempt=%d reason=%q", j.State, j.Attempt, j.Reason)
	}
	// Submit after asynchronous probes observe the outage, not an older healthy receipt.
	nasJob := submitAPI(short("nas-blocked-production", "production", []string{"cuda"}))
	if j := get(experiment.ID); j.State != "running" || j.Attempt != before.Attempt {
		t.Fatalf("NAS-blocked job preempted running command: state=%s attempt=%d reason=%q", j.State, j.Attempt, j.Reason)
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
		oldPath := filepath.Join(nas, old.ID, fmt.Sprintf("attempt-%d", old.Attempt), "progress.json")
		newPath := filepath.Join(nas, old.ID, fmt.Sprintf("attempt-%d", migrated.Attempt), "progress.json")
		oldGeneration, oldErr := checkpointProgress(filepath.Dir(oldPath))
		newGeneration, newErr := checkpointProgress(filepath.Dir(newPath))
		data, err := os.ReadFile(newPath)
		return oldErr == nil && newErr == nil && err == nil && strings.Contains(string(data), `"value":4`) && newGeneration > oldGeneration
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
	case <-time.After(testWaitTimeout):
		t.Fatal("planned leave worker did not exit")
	}
	if get(experiment.ID).Losses != 1 {
		t.Fatal("planned leave consumed a loss retry")
	}
	writeGate(cudaGate)
	wait("final resumed experiment succeeds", func() bool { return get(experiment.ID).State == "succeeded" })
	// Exercise the other model priority tiers through the same real API/workers.
	must(modelCall("metal-model", "experiment", testWaitTimeout))
	must(modelCall("cuda-model", "production", testWaitTimeout))

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
		data, err := os.ReadFile(filepath.Join(dir, "progress.json"))
		must(err)
		if !strings.Contains(string(data), fmt.Sprintf(`"value":%d}`, attempt)) {
			t.Fatalf("attempt %d checkpoint=%q", attempt, data)
		}
	}
	t.Logf("real API/SQLite/runner; cuda+xpu+metal; %d jobs succeeded once; experiment resumed %d times across priority, restart, network, NAS and leave faults; model calls answered once or timed out clearly", len(final.Jobs), winner.Attempt-1)
}

// The engine observes the exact file handed to Load, rather than merely trusting
// cache telemetry. It is the only fake boundary besides GPU inventory.
type auditedModelEngine struct {
	FakeEngine
	root    string
	loads   *sync.Map
	invalid *sync.Map
}

func (e *auditedModelEngine) Load(ctx context.Context, m Model) error {
	if filepath.Dir(m.Path) != e.root || strings.HasSuffix(m.Path, ".partial") {
		e.invalid.Store(m.Name, "engine received a non-cache or partial path")
		return errors.New("unverified model path")
	}
	if err := verifyModel(m.Path, m); err != nil {
		e.invalid.Store(m.Name, err.Error())
		return err
	}
	old, _ := e.loads.LoadOrStore(m.Name, new(atomic.Int32))
	old.(*atomic.Int32).Add(1)
	return e.FakeEngine.Load(ctx, m)
}

// Extends the full-system scenario with controlled mid-copy faults. Every phase
// uses the actual worker loop, HTTP API, SQLite restart, files and command runner.
func TestPreDeployModelCopyFaults(t *testing.T) {
	t.Parallel()
	base := newTestStateRoot(t)
	nas, state := filepath.Join(base, "nas"), filepath.Join(base, "worker")
	cache := filepath.Join(state, "model-cache")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.Mkdir(nas, 0700))
	makeModel := func(name string, bytes int) Model {
		data := []byte(strings.Repeat(name[:1], bytes))
		must(os.WriteFile(filepath.Join(nas, name), data, 0600))
		return Model{Name: name, Path: name, Engine: "fake", Platforms: []string{"fake"}, MemoryGB: 1,
			SizeBytes: int64(bytes), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	}
	pin := makeModel("pin", 1024)
	restart := makeModel("restart", 3*1024*1024)
	network := makeModel("network", 3*1024*1024)
	urgent := makeModel("urgent", 3*1024*1024)
	bad := makeModel("bad", 1024)
	bad.SHA256 = strings.Repeat("0", 64)
	cfg := ModelConfig{Idle: time.Second, Wait: testWaitTimeout, Models: []Model{pin, restart, network, urgent, bad}}
	clock := &manualWorkerClock{now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	open := func() *Queue {
		q, err := Open(filepath.Join(base, "queue.db"), nas, clock)
		must(err)
		must(q.ConfigureModels(cfg))
		q.SetRestartGrace(time.Minute)
		return q
	}
	q := open()
	var apiMu sync.RWMutex
	handler := API{Queue: q, Token: "copy-fault-test"}.Handler()
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiMu.RLock()
		defer apiMu.RUnlock()
		handler.ServeHTTP(w, r)
	}))
	client := Client{URL: server.URL, Token: "copy-fault-test", HTTP: &http.Client{Timeout: testWaitTimeout}}
	call := func(method, path string, in, out any) {
		t.Helper()
		must(client.Call(context.Background(), method, path, in, out))
	}
	snapshot := func() Snapshot { t.Helper(); var s Snapshot; call("GET", "/api/status", nil, &s); return s }
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.NewTimer(testWaitTimeout)
		defer deadline.Stop()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			if condition() {
				return
			}
			select {
			case <-deadline.C:
				t.Fatalf("%s: timed out: %+v", label, snapshot())
			case <-tick.C:
				clock.Advance(5 * time.Millisecond)
			}
		}
	}
	var gateMu sync.Mutex
	var entered, release, cancelled chan struct{}
	chunks := 0
	arm := func() {
		gateMu.Lock()
		defer gateMu.Unlock()
		entered, release, cancelled = make(chan struct{}), make(chan struct{}), make(chan struct{})
		chunks = 0
	}
	hook := func(ctx context.Context) {
		gateMu.Lock()
		chunks++
		if entered == nil || chunks != 2 {
			gateMu.Unlock()
			return
		}
		a, b, c := entered, release, cancelled
		gateMu.Unlock()
		close(a)
		select {
		case <-b:
		case <-ctx.Done():
			close(c)
		}
	}
	waitChannel := func(label string, ch <-chan struct{}) {
		t.Helper()
		wait(label, func() bool {
			select {
			case <-ch:
				return true
			default:
				return false
			}
		})
	}
	var loads, invalid sync.Map
	transport := &partitionTransport{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := Worker{Client: Client{URL: server.URL, Token: client.Token, HTTP: &http.Client{Transport: transport, Timeout: testWaitTimeout}},
		Machine: machine("copy-worker", "fake"), CheckpointRoot: nas, ModelRoot: nas, StateDir: state,
		CacheLimit: 4 * 1024 * 1024, Clock: RealClock{}, Interval: 5 * time.Millisecond, ModelConfig: cfg,
		beforeModelCopy: hook, modelEngine: func(Model) Engine { return &auditedModelEngine{root: cache, loads: &loads, invalid: &invalid} }}
	go func() { done <- w.Run(ctx) }()
	finish := filepath.Join(base, "finish")
	t.Cleanup(func() {
		transport.down.Store(false)
		gateMu.Lock()
		if release != nil {
			select {
			case <-release:
			default:
				close(release)
			}
		}
		gateMu.Unlock()
		_ = os.WriteFile(finish, nil, 0600)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("copy worker did not stop")
		}
		server.Close()
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	call("POST", "/api/models/pin", ModelPin{Model: pin.Name, Project: "fault", Machine: w.Machine.ID}, nil)
	pinReady := func() bool {
		for _, s := range snapshot().LoadedModels {
			if s.Model == pin.Name && s.State == "ready" {
				return true
			}
		}
		return false
	}
	wait("verified pin warmup", pinReady)
	assertPin := func() {
		t.Helper()
		if !pinReady() {
			t.Fatal("pin lost its loaded engine")
		}
		count, ok := loads.Load(pin.Name)
		if !ok || count.(*atomic.Int32).Load() != 1 {
			t.Fatal("pin was unloaded/reloaded")
		}
		must(verifyModel(filepath.Join(cache, cacheKey(pin)), pin))
	}
	syncModel := func(m Model, stop bool) {
		call("POST", "/api/models/sync", SyncRequest{Model: m.Name, To: "all", Cancel: stop}, nil)
	}
	cached := func(m Model) bool {
		for _, worker := range snapshot().Machines {
			if contains(worker.CachedModels, m.Name) {
				return true
			}
		}
		return false
	}
	assertPartial := func(m Model) {
		t.Helper()
		if cached(m) {
			t.Fatal("partial reported as cached")
		}
		st, err := os.Stat(filepath.Join(cache, cacheKey(m)+".partial"))
		must(err)
		if st.Size() != 1024*1024 {
			t.Fatalf("durable prefix=%d", st.Size())
		}
		if _, err := os.Stat(filepath.Join(cache, cacheKey(m))); !os.IsNotExist(err) {
			t.Fatal("partial published", err)
		}
		if _, ok := loads.Load(m.Name); ok {
			t.Fatal("engine loaded partial copy")
		}
	}
	// Queue restart mid-copy keeps the same pin process and persistent sync.
	arm()
	syncModel(restart, false)
	waitChannel("queue restart copy gate", entered)
	assertPartial(restart)
	apiMu.Lock()
	must(q.Close())
	q = open()
	handler = API{Queue: q, Token: client.Token}.Handler()
	apiMu.Unlock()
	if len(snapshot().SyncRequests) != 1 || len(snapshot().Pins) != 1 {
		t.Fatal("restart lost sync/pin")
	}
	close(release)
	wait("copy across queue restart", func() bool { return cached(restart) })
	must(verifyModel(filepath.Join(cache, cacheKey(restart)), restart))
	assertPin()
	clock.Advance(2 * time.Minute)
	wait("pin survives idle expiry", pinReady)
	assertPin()
	// Isolated API network loss pauses the copy even while the NAS is readable.
	arm()
	syncModel(network, false)
	waitChannel("network copy gate", entered)
	assertPartial(network)
	syncModel(restart, true) // avoid recopying an explicitly evicted completed target
	transport.down.Store(true)
	waitChannel("network failure cancels background I/O", cancelled)
	assertPartial(network)
	assertPin()
	syncModel(network, true)
	transport.down.Store(false)
	wait("reconnect with paused prefix", func() bool {
		for _, m := range snapshot().Machines {
			for _, r := range m.CopyReports {
				if r.Model == network.Name && r.State == "paused" && r.Bytes == 1024*1024 {
					return true
				}
			}
		}
		return false
	})
	close(release)
	syncModel(network, false)
	wait("network copy resumes", func() bool { return cached(network) })
	must(verifyModel(filepath.Join(cache, cacheKey(network)), network))
	assertPin()
	// Production starts while an unrelated NAS syscall is still at the gate.
	arm()
	syncModel(network, true)
	syncModel(urgent, false)
	waitChannel("production copy gate", entered)
	assertPartial(urgent)
	s := spec("production")
	s.Needs.MemoryGB = 1
	s.AllowedMachines = []string{w.Machine.ID}
	s.Run.Command = []string{"sh", "-c", `echo START; while [ ! -f "$FINISH" ]; do sleep 0.01; done; echo FINISHED`}
	s.Run.Env = map[string]string{"FINISH": finish}
	var job Job
	call("POST", "/api/jobs", s, &job)
	waitChannel("production pauses copy", cancelled)
	wait("production starts once", func() bool {
		var logs []LogChunk
		call("GET", "/api/jobs/"+job.ID+"/logs", nil, &logs)
		for _, l := range logs {
			if strings.Contains(l.Text, "START") {
				return true
			}
		}
		return false
	})
	assertPartial(urgent)
	assertPin()
	must(os.WriteFile(finish, nil, 0600))
	wait("production succeeds once", func() bool {
		for _, j := range snapshot().Jobs {
			if j.ID == job.ID {
				return j.State == "succeeded" && j.Attempt == 1 && j.Starts == 1
			}
		}
		return false
	})
	close(release)
	wait("idle copy resumes after production", func() bool { return cached(urgent) })
	must(verifyModel(filepath.Join(cache, cacheKey(urgent)), urgent))
	assertPin()
	// A real call loads only the verified local publication. Bad master bytes
	// must fail before Engine.Load, rather than merely returning fake success.
	modelCall := func(m Model) int {
		request, err := http.NewRequest("POST", server.URL+"/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"test"}]}`, m.Name)))
		must(err)
		request.Header.Set("Authorization", "Bearer "+client.Token)
		request.Header.Set("X-Myce-Project", "fault")
		request.Header.Set("X-Myce-Priority", "production")
		response, err := (&http.Client{Timeout: testWaitTimeout}).Do(request)
		must(err)
		defer response.Body.Close()
		_, err = io.Copy(io.Discard, response.Body)
		must(err)
		return response.StatusCode
	}
	if code := modelCall(urgent); code != 200 {
		t.Fatal("verified model call failed", code)
	}
	if code := modelCall(bad); code < 400 {
		t.Fatal("corrupt model call succeeded", code)
	}
	if _, ok := loads.Load(bad.Name); ok {
		t.Fatal("unverified master reached Engine.Load")
	}
	invalid.Range(func(name, err any) bool { t.Errorf("invalid engine load %s: %s", name, err); return true })
	assertPin()
	var logs []LogChunk
	call("GET", "/api/jobs/"+job.ID+"/logs", nil, &logs)
	var text strings.Builder
	for _, l := range logs {
		text.WriteString(l.Text)
	}
	if strings.Count(text.String(), "START") != 1 || strings.Count(text.String(), "FINISHED") != 1 || len(snapshot().Jobs) != 1 {
		t.Fatal("lost/duplicate job", text.String())
	}
	t.Log("queue restart mid-copy, worker partition mid-copy, production during pre-copy; pin loaded once; verified local engine paths; one job completed once; bad master never loaded")
}

// Environment restart fault extends the acceptance suite with real binary
// builder process groups, HTTP, SQLite, worker cache and command execution.
func TestPreDeployEnvironmentBuildRestart(t *testing.T) {
	t.Parallel()
	base := newTestStateRoot(t)
	nas := filepath.Join(base, "nas")
	if e := os.Mkdir(nas, 0700); e != nil {
		t.Fatal(e)
	}
	q, e := Open(filepath.Join(base, "queue.db"), nas, RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	started, gate := filepath.Join(base, "building"), filepath.Join(base, "finish-build")
	build := `set -eu; echo half > "$1/interrupted-input"; touch "$2"; while [ ! -f "$3" ]; do sleep 0.01; done; printf '#!/bin/sh\necho FINISHED-environment\n' > "$1/bin/env-task"; chmod +x "$1/bin/env-task"`
	r, e := snapshotEnvironment(EnvironmentRecipe{Build: []string{"sh", "-c", build, "build", "{root}", started, gate}}, "fake")
	if e != nil {
		t.Fatal(e)
	}
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"restart-env": {"fake": r}}})
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	s := spec("experiment")
	s.Environment.Name = "restart-env"
	s.Run.Command = []string{"env-task"}
	j, e := q.Submit(s)
	if e != nil {
		t.Fatal(e)
	}
	state := filepath.Join(base, "worker")
	w := Worker{StateDir: state, Clock: RealClock{}, Interval: 5 * time.Millisecond, Client: Client{URL: server.URL, Token: "test"}, Machine: Machine{ID: "restart-worker", GPU: "fake", FreeMemoryGB: 64}}
	run := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		return cancel, done
	}
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(testWaitTimeout)
		for !condition() {
			if time.Now().After(deadline) {
				t.Fatal(label + " timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel, done := run()
	wait("builder starts", func() bool { _, e := os.Stat(started); return e == nil })
	cancel()
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	retired, err := os.ReadDir(filepath.Join(state, "environment-cache", ".Trash"))
	if err != nil || len(retired) != 0 {
		t.Fatal("cancelled build retained retired bytes", len(retired), err)
	}
	if _, err := os.Stat(filepath.Join(state, "environment-cache", r.Key, "ready.json")); !os.IsNotExist(err) {
		t.Fatal("cancelled prefix published", err)
	}

	if job(t, q, j.ID).State != "queued" {
		t.Fatal("interrupted build consumed execution")
	}
	if e = os.WriteFile(gate, nil, 0600); e != nil {
		t.Fatal(e)
	}
	cancel, done = run()
	defer func() {
		cancel()
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	wait("rebuilt job finishes", func() bool { return job(t, q, j.ID).State == "succeeded" })
	trash, err := os.ReadDir(filepath.Join(state, "environment-cache", ".Trash"))
	if err != nil || len(trash) != 0 {
		t.Fatal("worker restart retained interrupted build", len(trash), err)
	}
	if logs, e := q.Logs(j.ID, 0); e != nil || !strings.Contains(fmt.Sprint(logs), "FINISHED-environment") {
		t.Fatal(logs, e)
	}
	if j := job(t, q, j.ID); j.Attempt != 1 || j.Losses != 0 {
		t.Fatal("build restart counted as an execution loss", j)
	}
}
