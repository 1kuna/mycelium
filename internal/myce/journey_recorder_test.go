package myce

// Investigation overlay, not a product change. No queue operation, maintenance,
// clock advance, command, signal, or probe may be initiated by this recorder.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// Retain the first half and a rolling tail, as in the investigation overlay.
// Storage is 1/64 of the fixture's own memory/cache budget, including slots.
// This leaves 63/64 for the journey; overflow reports omissions without losing
// early signal/result transitions. No queue operation or probe runs here.
const journeyFactBytes = 1024

type journeyFact struct {
	Seq uint64
	N   int
	B   [journeyFactBytes]byte
}
type journeyRecorder struct {
	mu        sync.Mutex
	ring      []journeyFact
	head      int
	tail      int
	count     uint64
	oversized uint64
	kinds     map[string]int
	q         *Queue
	runtimes  []*modelRuntime
	processes []*process
	once      sync.Once
	t         *testing.T
}

var journeyTests sync.Map // *testing.T -> *journeyRecorder; serial scoped fixtures.
func journeyNew(t *testing.T, fixtureBudget int64) *journeyRecorder {
	if v, ok := journeyTests.Load(t); ok {
		return v.(*journeyRecorder)
	}
	r := newJourneyRecorder(fixtureBudget)
	r.t = t
	journeyTests.Store(t, r)
	t.Cleanup(func() {
		if !t.Failed() && os.Getenv("MYCE_JOURNEY_FORCE_FAILURE") == t.Name() {
			t.Error("deliberate journey recorder failure")
		}
		if t.Failed() {
			r.failure()
		}
		journeyTests.Delete(t)
	})
	return r
}
func journeyGet(t *testing.T) *journeyRecorder {
	v, _ := journeyTests.Load(t)
	r, _ := v.(*journeyRecorder)
	return r
}
func journeyFatal(t *testing.T, a ...any) {
	t.Helper()
	if r := journeyGet(t); r != nil {
		r.failure()
	}
	t.Fatal(a...)
}
func journeyFatalf(t *testing.T, s string, a ...any) {
	t.Helper()
	if r := journeyGet(t); r != nil {
		r.failure()
	}
	t.Fatalf(s, a...)
}
func journeyID(s string) string {
	if s == "" {
		return ""
	}
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:8])
}
func journeyTime(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixNano()
}
func journeyClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		return journeyID(err.Error())
	}
}
func (r *journeyRecorder) add(kind string, data any) {
	if r == nil {
		return
	}
	b, e := json.Marshal(struct {
		At   int64  `json:"at"`
		Kind string `json:"kind"`
		Data any    `json:"data"`
	}{RealClock{}.Now().UnixNano(), kind, data})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	r.kinds[kind]++
	if e != nil || len(b) > journeyFactBytes {
		r.oversized++
		b = []byte(`{"kind":"omitted_oversize"}`)
	}
	index := r.count - 1
	if index >= uint64(r.head) {
		index = uint64(r.head) + (index-uint64(r.head))%uint64(r.tail)
	}
	slot := &r.ring[index]
	slot.Seq = r.count
	slot.N = copy(slot.B[:], b)
}
func (r *journeyRecorder) attachProcess(p *process) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if len(r.processes) < len(r.ring) {
		r.processes = append(r.processes, p)
	} else {
		r.oversized++
	}
	r.mu.Unlock()
}
func (r *journeyRecorder) attachRuntime(rt *modelRuntime) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if len(r.runtimes) < len(r.ring) {
		r.runtimes = append(r.runtimes, rt)
	} else {
		r.oversized++
	}
	r.mu.Unlock()
}
func (r *journeyRecorder) queueLocked(q *Queue) {
	if r == nil {
		return
	}
	logical := q.clock.Now().UnixNano()
	for _, j := range q.s.Jobs {
		generation := int64(0)
		for _, v := range j.FileCheckpoints {
			generation = max(generation, v.Generation)
		}
		victim, destination, hold := "", "", ""
		authorized, final := false, false
		if y := j.ScarceHostYield; y != nil {
			victim = journeyID(y.Victim)
			destination = journeyID(y.Destination)
			hold = journeyID(y.Hold)
			authorized = y.Authorized
			final = y.FinalReceived
		}
		r.add("queue_job", map[string]any{"logical": logical, "id": journeyID(j.ID), "state": j.State, "attempt": j.Attempt, "machine": journeyID(j.Machine), "session": journeyID(j.Session), "checkpoint_generation": generation, "yield_generation": j.YieldGeneration, "victim": victim, "destination": destination, "yield_hold": hold, "authorized": authorized, "final": final, "release": j.ReleaseConfirmed, "started": journeyTime(j.StartedAt), "finished": journeyTime(j.FinishedAt), "stop_at": journeyTime(j.StopAt), "reason_hash": journeyID(j.Reason), "last_stop": journeyID(j.LastStopOutcome)})
	}
	for _, m := range q.s.Machines {
		r.add("queue_machine", map[string]any{"logical": logical, "id": journeyID(m.ID), "session": journeyID(m.Session), "gone": m.Gone, "last_seen": m.LastSeen.UnixNano(), "inventory_hash": journeyID(m.InventoryReason), "memory_error": journeyID(m.MemoryError), "host_memory_error": journeyID(m.HostMemoryError), "free_cpu": m.FreeCPUCores, "checkpoint_unavailable": m.CheckpointUnavailable})
		for _, d := range m.Devices {
			r.add("queue_device", map[string]any{"machine": journeyID(m.ID), "device": d.ID, "reason_hash": journeyID(d.Reason), "free_memory": d.FreeMemoryGB})
		}
	}
	for host, p := range q.s.HostMaintenance {
		r.add("maintenance", map[string]any{"logical": logical, "host": journeyID(host), "mode": p.Mode, "revision_time": p.UpdatedAt.UnixNano(), "owner": journeyID(p.DrainOwner), "stop_at": journeyTime(p.StopAt)})
	}
	for _, p := range q.s.PreparationPlans {
		r.add("preparation", map[string]any{"logical": logical, "machine": journeyID(p.Machine), "session": journeyID(p.Session), "kind": p.Kind, "key": journeyID(p.Key), "attempt": journeyID(p.Attempt), "started": p.Started, "started_at": journeyTime(p.StartedAt), "stop_at": journeyTime(p.StopAt)})
	}
	for _, p := range q.s.HostReleases {
		r.add("release", map[string]any{"logical": logical, "host": journeyID(p.Host), "job": journeyID(p.Job), "machine": journeyID(p.Machine), "session": journeyID(p.Session), "attempt": p.Attempt, "preparation": p.Preparation, "until": p.Until.UnixNano(), "failed": p.CleanupFailed, "cleanup_since": journeyTime(p.CleanupSince)})
	}
	for _, s := range q.s.ModelSlots {
		r.slot("queue_engine", *s)
	}
	r.add("queue_counts", map[string]any{"logical": logical, "preparations": len(q.s.PreparationPlans), "releases": len(q.s.HostReleases), "jobs": len(q.s.Jobs), "engines": len(q.s.ModelSlots)})
}
func (r *journeyRecorder) slot(kind string, s ModelSlot) {
	r.add(kind, map[string]any{"machine": journeyID(s.Machine), "session": journeyID(s.Session), "attempt": journeyID(s.Attempt), "device": s.Device, "state": s.State, "runtime_failed": s.RuntimeFailed, "stop_at": journeyTime(s.StopAt), "health_revision": s.HealthRevision})
}
func (r *journeyRecorder) runtimeLocked(rt *modelRuntime, kind string) {
	if r == nil {
		return
	}
	for _, m := range rt.models {
		r.add(kind, map[string]any{"session": journeyID(rt.session), "attempt": journeyID(m.attempt), "device": m.device, "state": m.state, "runtime_failed": m.runtimeFailed, "error_hash": journeyID(m.err), "inflight": m.inflight, "queue_flight": m.queueFlight, "closing": rt.closing, "admission_blocked": rt.admissionBlocked})
	}
}
func (r *journeyRecorder) capture() {
	r.mu.Lock()
	q := r.q
	rts := append([]*modelRuntime(nil), r.runtimes...)
	ps := append([]*process(nil), r.processes...)
	r.mu.Unlock()
	if q != nil {
		if q.mu.TryRLock() {
			r.queueLocked(q)
			q.mu.RUnlock()
		} else {
			r.add("omitted_queue_locked", nil)
		}
	}
	for _, rt := range rts {
		if rt.mu.TryLock() {
			r.runtimeLocked(rt, "failure_runtime")
			rt.mu.Unlock()
		} else {
			r.add("omitted_runtime_locked", nil)
		}
	}
	for _, p := range ps {
		if p.mu.TryLock() {
			pid := 0
			if p.cmd != nil && p.cmd.Process != nil {
				pid = p.cmd.Process.Pid
			}
			r.add("process", map[string]any{"command_hash": journeyID(strings.Join(p.job.Spec.Run.Command, "\x00")), "job": journeyID(p.job.ID), "attempt": p.job.Attempt, "pid": pid, "birth": p.birth, "stopped": p.stopped, "killed": p.killed, "stop_at": journeyTime(p.stopAt), "stop_requested": p.stopRequested.UnixNano()})
			if p.native != nil {
				for _, m := range p.native.members {
					r.add("native_member", map[string]any{"name": m.Name, "pid": m.PID, "birth": m.Birth, "phase": m.Phase, "exit_observed": m.Exit != nil})
				}
			}
			p.mu.Unlock()
		} else {
			r.add("omitted_process_locked", nil)
		}
		if d := p.docker; d != nil {
			if d.mu.TryLock() {
				s := d.state
				r.add("failure_docker", map[string]any{"identity": journeyID(s.Identity), "operation": s.Operation, "pending": s.Pending, "stopped": s.Stopped, "forced": s.Forced, "released": s.Released, "main_finished": s.MainFinished, "dispatched": journeyTime(s.DispatchedAt), "stop_at": journeyTime(s.StopAt), "network_removed": s.NetworkRemoved, "evidence_saved": s.EvidenceSaved, "error_hash": journeyID(s.Error), "failure_hash": journeyID(s.Failure), "infrastructure_stop": s.InfrastructureStop})
				for _, f := range s.Infrastructure {
					r.add("infrastructure", map[string]any{"kind": f.Kind, "at": f.At.UnixNano(), "group": f.Group, "prelaunch_refused": f.PrelaunchRefused})
				}
				d.mu.Unlock()
			} else {
				r.add("omitted_docker_locked", nil)
			}
		}
	}
}
func (r *journeyRecorder) failure() {
	if r != nil {
		r.once.Do(func() { r.capture(); r.write(true) })
	}
}
func newJourneyRecorder(fixtureBudget int64) *journeyRecorder {
	// The previous capture measured 7,332 events in the largest journey.
	// Twice that count covers a repeat without reserving the Docker fixture's
	// entire 96 MiB allowance for a roughly 200-event trace.
	slots := min(2*7332, int(fixtureBudget/64/int64(unsafe.Sizeof(journeyFact{}))))
	if slots < 2 {
		panic("journey fixture budget cannot retain facts")
	}
	return &journeyRecorder{ring: make([]journeyFact, slots), head: slots / 2, tail: slots - slots/2, kinds: map[string]int{}}
}
func (r *journeyRecorder) write(_ bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.t.Logf("JOURNEY recorder test=%s events=%d omitted_middle=%d oversized=%d head=%d tail=%d retained_bytes=%d", r.t.Name(), r.count, max(int64(0), int64(r.count)-int64(len(r.ring))), r.oversized, r.head, r.tail, len(r.ring)*int(unsafe.Sizeof(journeyFact{})))
	for i := uint64(0); i < min(r.count, uint64(r.head)); i++ {
		s := &r.ring[i]
		r.t.Logf("JOURNEY fact seq=%d %s", s.Seq, s.B[:s.N])
	}
	start := max(uint64(r.head), r.count-min(r.count, uint64(r.tail)))
	for i := start; i < r.count; i++ {
		s := &r.ring[uint64(r.head)+(i-uint64(r.head))%uint64(r.tail)]
		r.t.Logf("JOURNEY fact seq=%d %s", s.Seq, s.B[:s.N])
	}
}

// Copy bytes already supplied to/returned by the real HTTP handler. Never
// pre-read a body or make a second request. Retain no headers, URLs or payloads.
type journeyBody struct {
	io.ReadCloser
	b       bytes.Buffer
	omitted bool
}

func (b *journeyBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	left := (64 << 10) - b.b.Len()
	if n > left {
		b.omitted = true
	}
	b.b.Write(p[:min(n, left)])
	return n, e
}

type journeyWriter struct {
	http.ResponseWriter
	b       bytes.Buffer
	status  int
	omitted bool
}

func (w *journeyWriter) WriteHeader(n int) {
	if w.status == 0 {
		w.status = n
	}
	w.ResponseWriter.WriteHeader(n)
}
func (w *journeyWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, e := w.ResponseWriter.Write(p)
	left := (64 << 10) - w.b.Len()
	if n > left {
		w.omitted = true
	}
	w.b.Write(p[:min(n, left)])
	return n, e
}
func (w *journeyWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (r *journeyRecorder) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Observe only the existing control reply and exact engine route. Status
		// may be gzip encoded: the journey records its already-decoded ready read.
		if req.URL.Path != "/api/workers/checkin" && req.URL.Path != "/v1/completions" {
			next.ServeHTTP(w, req)
			return
		}
		start := RealClock{}.Now().UnixNano()
		body := &journeyBody{ReadCloser: req.Body}
		if req.Body != nil {
			req.Body = body
		}
		out := &journeyWriter{ResponseWriter: w}
		next.ServeHTTP(out, req)
		pattern := req.Pattern
		if pattern == "" {
			switch {
			case req.URL.Path == "/api/workers/checkin":
				pattern = "POST /api/workers/checkin"
			case req.URL.Path == "/api/status":
				pattern = "GET /api/status"
			case req.URL.Path == "/v1/completions":
				pattern = "POST /v1/completions"
			case bytes.HasSuffix([]byte(req.URL.Path), []byte("/maintenance")):
				pattern = req.Method + " /api/hosts/{id}/maintenance"
			default:
				pattern = req.Method + " path#" + journeyID(req.URL.Path)
			}
		}
		if pattern == "POST /v1/completions" {
			reason := "other"
			if bytes.Contains(out.b.Bytes(), []byte("model is not ready")) {
				reason = "model_not_ready"
			}
			if bytes.Contains(out.b.Bytes(), []byte("engine attempt is no longer ready")) {
				reason = "attempt_not_ready"
			}
			r.add("engine_response", map[string]any{"start": start, "status": out.status, "reason": reason, "attempt": journeyID(req.Header.Get("X-Myce-Engine-Attempt")), "session": journeyID(req.Header.Get("X-Myce-Session"))})
		}
		if pattern == "POST /api/workers/checkin" {
			var in Checkin
			if !body.omitted && json.Unmarshal(body.b.Bytes(), &in) == nil {
				for _, m := range in.ModelReports {
					r.add("checkin_engine", map[string]any{"machine": journeyID(in.Machine.ID), "session": journeyID(in.Machine.Session), "attempt": journeyID(m.Attempt), "device": m.Device, "state": m.State, "runtime_failed": m.RuntimeFailed, "error_hash": journeyID(m.Error)})
				}
			}
			var reply Reply
			if !out.omitted && json.Unmarshal(out.b.Bytes(), &reply) == nil {
				r.add("directive", map[string]any{"start": start, "status": out.status, "stop": reply.Stop, "kill": reply.Kill, "hold": reply.Hold, "leave": reply.Leave, "admission_blocked": reply.AdmissionBlocked, "maintenance": reply.HostMaintenance, "preparation_stop_at": journeyTime(reply.PreparationStopAt), "attempt_count": len(reply.Attempts)})
				for _, s := range reply.ModelPlan {
					r.slot("directive_engine", s)
				}
				for _, a := range reply.Attempts {
					r.add("directive_attempt", map[string]any{"job": journeyID(a.ID), "attempt": a.Attempt, "session": journeyID(a.Session), "stop": a.Stop, "kill": a.Kill, "hold": a.Hold, "user_cancel": a.UserCancel, "grace_seconds": a.GraceSeconds})
				}
			}
		}

		r.add("http", map[string]any{"start": start, "pattern": pattern, "status": out.status, "request_omitted": body.omitted, "response_omitted": out.omitted})
	})
}
func journeyStartDev(t *testing.T, c DevConfig, configure ...func(*Queue)) (*Dev, error) {
	r := journeyNew(t, c.EnvironmentCacheLimit)
	c.journey = r
	return startDev(c, func(q *Queue) {
		if len(configure) > 0 && configure[0] != nil {
			configure[0](q)
		}
		r.q = q
		q.journey = r
	})
}
func (r *journeyRecorder) rpc(kind, operation string, p *readinessProgress, err error) {
	if r == nil {
		return
	}
	p.mu.Lock()
	effective := p.budget
	if effective > 0 {
		effective = max(effective, scaleReadinessDuration(p.gap, 2), scaleReadinessDuration(p.activityPeriod, 2))
	}
	data := map[string]any{"operation": operation, "budget_ns": int64(p.budget), "effective_budget_ns": int64(effective), "suspended": p.suspended, "gap_ns": int64(p.gap), "revision": p.revision, "last": p.last.UnixNano(), "started": p.started.UnixNano(), "real_last": p.realLast.UnixNano(), "stage": p.stage, "completed": p.completed, "class": journeyClass(err)}
	p.mu.Unlock()
	r.add(kind, data)
}

// Only copied native/runtime facts: no observation probe or mutation.
func (r *journeyRecorder) nativeLocked(g *nativeGroup, phase string) {
	if r == nil {
		return
	}
	p := g.p
	r.add("native_control", map[string]any{"phase": phase, "job": journeyID(p.job.ID), "attempt": p.job.Attempt, "priority": p.priorityStop != nil, "priority_provenance": journeyPriority(p.priorityStop), "stopped": p.stopped, "killed": p.killed, "stop_at": journeyTime(p.stopAt), "stop_requested": p.stopRequested.UnixNano()})
	r.nativeFacts(phase, "group", p.job.ID, g.p.outcome)
	for _, m := range g.members {
		r.add("native_identity", map[string]any{"phase": phase, "job": journeyID(p.job.ID), "member": m.Name, "pid": m.PID, "birth": m.Birth, "state": m.Phase})
		r.nativeFacts(phase, m.Name, p.job.ID, m.memberOutcome)
	}
}
func (r *journeyRecorder) nativeFacts(phase, member, job string, f memberOutcome) {
	if r == nil {
		return
	}
	data := map[string]any{"phase": phase, "member": member, "job": journeyID(job), "exit_observed": f.Exit != nil, "observation_lost": f.ObservationLost, "infrastructure_count": len(f.Infrastructure)}
	if f.Exit != nil {
		data["exit_status"] = f.Exit.Status
		data["exit_at"] = f.Exit.ObservedAt.UnixNano()
	}
	r.add("native_facts", data)
	for _, s := range f.Signals {
		r.add("native_signal", map[string]any{"phase": phase, "member": member, "job": journeyID(job), "signal": s.Signal, "at": s.At.UnixNano(), "shutdown": s.Shutdown, "before_exit": s.BeforeExit})
	}
}
func (r *journeyRecorder) result(phase string, j Job, v Result, err error) {
	if r == nil {
		return
	}
	operation, class := "", ""
	if v.ExecutionCause != nil {
		operation = v.ExecutionCause.Operation
		class = v.ExecutionCause.Class
	}
	r.add("result_flags", map[string]any{"phase": phase, "job": journeyID(v.ID), "attempt": v.Attempt, "session": journeyID(v.Session), "success": v.Success, "interrupted": v.Interrupted, "priority": v.PriorityStop != nil, "priority_provenance": journeyPriority(v.PriorityStop), "forced": v.Forced, "release_only": v.ReleaseOnly, "release_pending": v.ReleasePending, "polite": v.PoliteStop, "operator": v.OperatorStop, "oom": v.OutOfMemory, "final_generation": v.FinalCheckpointGeneration, "error_hash": journeyID(v.Error), "cause_operation": operation, "cause_class": class, "call_error": journeyClass(err)})
}

func (r *journeyRecorder) startResult(p *process, j Job, err error) {
	if p != nil {
		r.attachProcess(p)
	}
	r.add("worker_start_result", map[string]any{"job": journeyID(j.ID), "attempt": j.Attempt, "class": journeyClass(err)})
}
func (r *journeyRecorder) rpcStart(op string, p *readinessProgress, h readinessHistory, timeout time.Duration) {
	r.rpc("rpc_start", op, p, nil)
	r.add("rpc_history", map[string]any{"operation": op, "mean_ns": int64(h.Load.Mean), "deviation_ns": int64(h.Load.Deviation), "gap_ns": int64(h.Gap), "explicit_budget_ns": int64(timeout)})
}
func (r *journeyRecorder) rpcReturn(d *dockerAttempt, op string, p *readinessProgress, err, cause, parent error) {
	r.rpc("rpc_return", op, p, err)
	r.add("rpc_context", map[string]any{"operation": op, "cause": journeyClass(cause), "parent": journeyClass(parent)})
}
func (r *journeyRecorder) commandReturn(op string, c *exec.Cmd, err, cause error, stdout, stderr int) {
	data := map[string]any{"operation": op, "class": journeyClass(err), "cause": journeyClass(cause), "stdout_bytes": stdout, "stderr_bytes": stderr}
	if s := c.ProcessState; s != nil {
		data["pid"] = s.Pid()
		data["exit_code"] = s.ExitCode()
		data["exited"] = s.Exited()
		if w, ok := s.Sys().(syscall.WaitStatus); ok {
			data["signaled"] = w.Signaled()
			data["signal"] = int(w.Signal())
		}
	}
	r.add("rpc_command_return", data)
}

func (r *journeyRecorder) dockerLocked(d *dockerAttempt, kind string) {
	s := d.state
	r.add(kind, map[string]any{"identity": journeyID(s.Identity), "operation": s.Operation, "pending": s.Pending, "stopped": s.Stopped, "forced": s.Forced, "released": s.Released, "main_finished": s.MainFinished, "dispatched": journeyTime(s.DispatchedAt), "stop_at": journeyTime(s.StopAt), "network_removed": s.NetworkRemoved, "error_hash": journeyID(s.Error), "failure_hash": journeyID(s.Failure)})
}

func journeyPriority(p *PriorityPreemption) any {
	if p == nil {
		return nil
	}
	return map[string]any{"cause": p.Cause, "demand": journeyID(p.Demand), "victim": journeyID(p.Victim)}
}
