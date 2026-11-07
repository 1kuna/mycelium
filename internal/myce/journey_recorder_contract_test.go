package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJourneyRecorderPassive(t *testing.T) {
	q, _ := fixture(t)
	calls := 0
	q.checkpointProbe = func() bool { calls++; return true }
	r := newJourneyRecorder(1 << 20)
	r.q = q
	q.mu.Lock()
	stop := q.clock.Now().Add(-time.Second)
	q.s.Jobs["diagnostic-only"] = &Job{ID: "diagnostic-only", State: "running", Attempt: 1, StopAt: &stop}
	before, _ := json.Marshal(q.s)
	q.mu.Unlock()
	r.capture()
	q.mu.Lock()
	after, _ := json.Marshal(q.s)
	q.mu.Unlock()
	if !bytes.Equal(before, after) || calls != 0 {
		t.Fatal("recorder mutated queue state or initiated storage probe")
	}
	q.mu.Lock()
	r.capture() // Must report omission rather than wait for a lock or repair it.
	q.mu.Unlock()
	if r.kinds["omitted_queue_locked"] != 1 {
		t.Fatal("missing lock omission")
	}
}

func TestJourneyRecorderImmutableAndBounded(t *testing.T) {
	r := newJourneyRecorder(1 << 20)
	data := map[string]any{"value": "before"}
	r.add("immutable", data)
	data["value"] = "after"
	if !bytes.Contains(r.ring[0].B[:r.ring[0].N], []byte("before")) {
		t.Fatal("retained a mutable reference")
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range len(r.ring) {
				r.add("bounded", 17)
			}
		}()
	}
	wg.Wait()
	r.add("oversized", strings.Repeat("x", journeyFactBytes*2))
	if r.count != uint64(2*len(r.ring)+2) || r.oversized != 1 {
		t.Fatal("event loss accounting", r.count, r.oversized)
	}
	for _, v := range r.ring {
		if v.N > journeyFactBytes {
			t.Fatal("unbounded retained event")
		}
	}
}

func TestJourneyRecorderObservesOriginalHTTP(t *testing.T) {
	r := newJourneyRecorder(1 << 20)
	calls := 0
	handler := r.handler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls++
		b, _ := io.ReadAll(req.Body)
		if string(b) != "original" {
			t.Fatal("changed request bytes")
		}
		req.Pattern = "POST /api/workers/checkin"
		w.WriteHeader(200)
		io.WriteString(w, `{"stop":true,"attempts":[{"id":"job","attempt":1,"session":"test","kill":true}]}`)
	}))
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, httptest.NewRequest("POST", "/api/workers/checkin", strings.NewReader("original")))
	if calls != 1 || out.Code != 200 || !strings.Contains(out.Body.String(), `"kill":true`) {
		t.Fatal("observer changed HTTP behavior")
	}
	if r.kinds["directive"] != 1 || r.kinds["directive_attempt"] != 1 {
		t.Fatal("missing original directives")
	}
}

func TestJourneyRecorderPreservesFirstTransitions(t *testing.T) {
	r := newJourneyRecorder(1 << 20)
	r.add("first_transition", map[string]any{"immutable": true})
	for range 2 * len(r.ring) {
		r.add("tail", 17)
	}
	if r.ring[0].Seq != 1 || !bytes.Contains(r.ring[0].B[:r.ring[0].N], []byte("first_transition")) {
		t.Fatal("first transition overwritten")
	}
	last := &r.ring[uint64(r.head)+(r.count-1-uint64(r.head))%uint64(r.tail)]
	if last.Seq != r.count {
		t.Fatal("latest transition lost")
	}
}
func TestJourneyRecorderResultFlagsCopied(t *testing.T) {
	r := newJourneyRecorder(1 << 20)
	generation := int64(2)
	v := Result{Success: false, Interrupted: true, FinalCheckpointGeneration: &generation}
	before, _ := json.Marshal(v)
	r.result("contract", Job{}, v, nil)
	after, _ := json.Marshal(v)
	if !bytes.Equal(before, after) {
		t.Fatal("result capture changed outcome")
	}
	generation = 3
	var f struct {
		Data struct {
			Interrupted bool  `json:"interrupted"`
			Generation  int64 `json:"final_generation"`
		}
	}
	if err := json.Unmarshal(r.ring[0].B[:r.ring[0].N], &f); err != nil || !f.Data.Interrupted || f.Data.Generation != 2 {
		t.Fatal("result flags were not copied", err)
	}
}

// Run the actual journeys in child processes. A deliberate failure in each
// recorder cleanup must retain the facts in normal go-test output. A natural
// assertion failure cannot satisfy the explicit deliberate-failure marker.
func TestJourneyRecorderFailureDumps(t *testing.T) {
	cases := []struct {
		name, run string
		facts     []string
	}{
		{"TestScarceHostYieldDev", "^TestScarceHostYieldDev$", []string{"native_facts", "native_classified", "file_finish_input", "queue_complete_input"}},
		{"TestM3DevModelEnginesPerDevice", "^TestM3DevModelEnginesPerDevice$", []string{"status_engine", "directive_engine", "runtime_route", "engine_response"}},
		{"TestDockerReleasedStageFailureDoesNotRequeue/main-nonzero", "^TestDockerReleasedStageFailureDoesNotRequeue$/^main-nonzero$", []string{"rpc_history", "rpc_command_return", "network.disconnect", "docker_custody"}},
		{"TestOutcomeRuntimeTable/docker/priority-oom", "^TestOutcomeRuntimeTable$/^docker$/^priority-oom$", []string{"rpc_history", "rpc_command_return", "network.disconnect", "docker_custody"}},
		{"TestDockerRestartMainFailures/oom", "^TestDockerRestartMainFailures$/^oom$", []string{"rpc_history", "rpc_command_return", "worker_start_result", "docker_custody"}},
		{"TestDockerReleasedStageFailureDoesNotRequeue/main-oom", "^TestDockerReleasedStageFailureDoesNotRequeue$/^main-oom$", []string{"rpc_history", "rpc_context", "rpc_command_return", "worker_start_result", "docker_custody"}},
		{"TestDockerRestartGracefulSidecarEvidence", "^TestDockerRestartGracefulSidecarEvidence$", []string{"rpc_history", "rpc_context", "rpc_command_return", "worker_start_result", "docker_custody"}},
		{"TestDockerRestartReleasedWithoutResultStaysLoss/crash", "^TestDockerRestartReleasedWithoutResultStaysLoss$/^crash$", []string{"rpc_history", "rpc_context", "rpc_command_return", "network.disconnect", "docker_custody"}},
		{"TestDockerRestartReleasedWithoutResultStaysLoss/output-sync-close", "^TestDockerRestartReleasedWithoutResultStaysLoss$/^output-sync-close$", []string{"rpc_history", "rpc_context", "rpc_command_return", "network.disconnect", "docker_custody"}},
		{"TestOutcomeRuntimeTable/docker/observation-loss", "^TestOutcomeRuntimeTable$/^docker$/^observation-loss$", []string{"rpc_history", "rpc_context", "rpc_command_return", "worker_start_result", "docker_custody"}},
		{"TestOutcomeRuntimeTable/docker/exit-143-without-signal", "^TestOutcomeRuntimeTable$/^docker$/^exit-143-without-signal$", []string{"rpc_history", "rpc_context", "rpc_command_return", "network.disconnect", "docker_custody"}},
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(exe, "-test.v", "-test.run="+tc.run)
			cmd.Env = append(os.Environ(), "MYCE_JOURNEY_FORCE_FAILURE="+tc.name)
			out, err := cmd.CombinedOutput()
			if err == nil || !bytes.Contains(out, []byte("deliberate journey recorder failure")) || !bytes.Contains(out, []byte("JOURNEY recorder test="+tc.name)) {
				t.Fatalf("missing forced failure/dump: %v\n%s", err, out)
			}
			for _, fact := range tc.facts {
				if !bytes.Contains(out, []byte(fact)) {
					t.Fatalf("missing %s\n%s", fact, out)
				}
			}
			if bytes.Contains(out, []byte("WARNING: DATA RACE")) || bytes.Contains(out, []byte("panic:")) {
				t.Fatalf("invalid failure proof\n%s", out)
			}
		})
	}
}

// Isolate record construction cost from the subprocess/HTTP journey's noise.
func BenchmarkJourneyRecorder(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprint(enabled), func(b *testing.B) {
			r := newJourneyRecorder(1 << 30)
			v := Result{Execution: Execution{ID: "fixture", Attempt: 1}, Session: "fixture", Interrupted: true}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if enabled {
					r.result("file_finish_input", Job{}, v, nil)
				}
			}
		})
	}
}

func TestJourneyRecorderRPCCause(t *testing.T) {
	r := newJourneyRecorder(1 << 20)
	// Exercise the real RPC wrapper and require an un-cancelled successful return.
	d := &dockerAttempt{journey: r, command: func(context.Context, ...string) ([]byte, error) { return nil, nil }}
	if _, err := d.callWithTimeout(0, "network", "disconnect"); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range r.ring {
		if bytes.Contains(f.B[:f.N], []byte(`"kind":"rpc_context"`)) {
			found = true
			if !bytes.Contains(f.B[:f.N], []byte(`"cause":"ok"`)) {
				t.Fatalf("recorder observed its own deferred cancel: %s", f.B[:f.N])
			}
		}
	}
	if !found {
		t.Fatal("missing RPC context")
	}
}
