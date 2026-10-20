package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func attemptWire(t *testing.T, client Client, c Checkin) Reply {
	t.Helper()
	var reply Reply
	if err := client.Call(context.Background(), "POST", "/api/workers/checkin", c, &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestAttemptProtocolLegacyWorkerNewQueue(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	m := machine("legacy", "fake")
	submit(t, q, spec("experiment"))
	reply := attemptWire(t, client, Checkin{Machine: m})
	if reply.Job == nil || reply.AttemptProtocol != "" || len(reply.Attempts) != 0 {
		t.Fatalf("legacy assignment changed: %+v", reply)
	}
	j := reply.Job
	clock.Advance(time.Second)
	c := Checkin{Machine: m, Running: &Execution{ID: j.ID, Attempt: j.Attempt, Progress: 7}, Started: true}
	if reply = attemptWire(t, client, c); reply.Stop || job(t, q, j.ID).Progress != 7 {
		t.Fatal("legacy heartbeat/progress changed")
	}
	submit(t, q, spec("production"))
	reply = attemptWire(t, client, c)
	if !reply.Stop || reply.Kill || reply.GraceSeconds != 10 || reply.PriorityStop == nil || reply.PriorityStop.Cause != "priority_preemption" {
		t.Fatalf("legacy P3 stop changed: %+v", reply)
	}
	finishInterrupted(t, q, j)
	if got := job(t, q, j.ID); got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("planned stop charged retries: %+v", got)
	}
}

func TestAttemptProtocolKeyedWorkerNewQueue(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	w := Worker{Clock: clock, Machine: machine("keyed", "fake"), Client: Client{URL: server.URL, Token: "test"}}
	submit(t, q, spec("experiment"))
	c := Checkin{Machine: w.Machine}
	reply, err := w.checkin(context.Background(), c)
	if err != nil || reply.Job == nil || reply.Job.Session != c.Machine.Session {
		t.Fatalf("keyed assignment: %+v %v", reply, err)
	}
	j := reply.Job
	submit(t, q, spec("experiment"))
	clock.Advance(time.Second)
	c.Running = &Execution{ID: j.ID, Attempt: j.Attempt, Progress: 9}
	c.Started = true
	reply, err = w.checkin(context.Background(), c)
	if err != nil || reply.Stop || reply.Job != nil || job(t, q, j.ID).Progress != 9 || job(t, q, j.ID).StartedAt == nil {
		t.Fatalf("keyed heartbeat admitted a neighbor or lost progress: %+v %v", reply, err)
	}
	// An idle report re-delivers the same assignment, never a second attempt.
	idle := Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: c.Machine}
	wire := attemptWire(t, w.Client, idle)
	if wire.Job != nil || wire.Stop || len(wire.Attempts) != 1 || wire.Attempts[0].Job.ID != j.ID || wire.Attempts[0].Session != j.Session {
		t.Fatalf("wire lost assignment identity: %+v", wire)
	}
	submit(t, q, spec("production"))
	reply, err = w.checkin(context.Background(), c)
	if err != nil || !reply.Stop || reply.PriorityStop == nil || reply.GraceSeconds != 10 {
		t.Fatalf("keyed P3 stop: %+v %v", reply, err)
	}
	// Omission is not an exact release acknowledgement, even after grace.
	clock.Advance(11 * time.Second)
	wire = attemptWire(t, w.Client, idle)
	if len(wire.Attempts) != 1 || !wire.Attempts[0].Kill || wire.Attempts[0].ID != j.ID || wire.Attempts[0].Attempt != j.Attempt || job(t, q, j.ID).ReleaseConfirmed {
		t.Fatalf("omitted attempt discharged custody: %+v", wire)
	}
	wrong := result(j, false)
	wrong.ReleaseOnly, wrong.Attempt = true, j.Attempt+1
	if err := q.Complete(wrong); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.ReleaseConfirmed || got.State != "stopping" {
		t.Fatal("wrong release key discharged the stopping attempt")
	}
	finishInterrupted(t, q, j)
	if got := job(t, q, j.ID); got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("keyed planned stop charged retries: %+v", got)
	}
}

func TestAttemptProtocolOldQueueRefused(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest, http.StatusUpgradeRequired} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				rw.WriteHeader(status)
				// A older unkeyed queue either rejects the new fields or returns an unkeyed reply.
				_ = json.NewEncoder(rw).Encode(Reply{Stop: true, Kill: true})
			}))
			defer server.Close()
			w := Worker{Machine: machine("new", "fake"), Client: Client{URL: server.URL, Token: "test"}}
			reply, err := w.checkin(context.Background(), Checkin{Machine: w.Machine})
			var he *HTTPError
			if !errors.As(err, &he) || he.Status != 426 || he.Code != "worker_protocol_invalid" || reply.Stop || reply.Job != nil {
				t.Fatalf("old queue not clearly refused: %+v %v", reply, err)
			}
		})
	}
}

func TestAttemptProtocolOldQueueWorkerRetainsCustody(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("saved", "fake")
	j := check(t, q, m, nil).Job
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(rw).Encode(Reply{}) // no keyed revision echo
	}))
	defer server.Close()
	w := Worker{Machine: m, Clock: clock, StateDir: t.TempDir(), Client: Client{URL: server.URL, Token: "test"}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	r := result(j, true)
	store.state.Session, store.state.Job, store.state.Result = j.Session, j, &r
	if err = store.save(); err != nil {
		t.Fatal(err)
	}
	store.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = w.Run(ctx)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 426 || he.Code != "worker_protocol_invalid" {
		t.Fatalf("worker did not exit clearly on old queue: %v", err)
	}
	store, err = w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if store.state.Session != j.Session || store.state.Result == nil || !store.matchesResult(*store.state.Result, m.ID) {
		t.Fatal("old queue refusal discarded buffered custody")
	}
}

func TestAttemptProtocolInvalidReports(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	for _, edit := range []func(*Checkin){
		func(c *Checkin) { c.AttemptProtocol = "unknown" },
		func(c *Checkin) { c.AttemptProtocol = "" },
		func(c *Checkin) { c.Attempts[0].Session = "" },
		func(c *Checkin) { c.Attempts[0].Session = "other" },
		func(c *Checkin) { c.Attempts[0].ID = "" },
		func(c *Checkin) { c.Attempts[0].Attempt = 0 },
		func(c *Checkin) { c.Attempts = append(c.Attempts, c.Attempts[0]) },
		func(c *Checkin) { c.Running = &c.Attempts[0].Execution },
		func(c *Checkin) { c.Started = true },
	} {
		m := machine("invalid", "fake")
		c := Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m, Attempts: []AttemptReport{{Execution: Execution{ID: "job", Attempt: 1}, Session: m.Session}}}
		edit(&c)
		var reply Reply
		err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/workers/checkin", c, &reply)
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != 426 || he.Code != "worker_protocol_invalid" {
			t.Fatalf("invalid keyed report accepted: %+v %v", c, err)
		}
	}
}

func TestAttemptProtocolStaleResultByKey(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	m := machine("keyed", "fake")
	submit(t, q, spec("experiment"))
	c := Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m}
	old := attemptWire(t, client, c).Attempts[0].Job
	if err := q.Leave(m.ID, true); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, old)
	clock.Advance(time.Second)
	m.Session = "replacement-session"
	c.Machine, c.Fresh, c.PreviousSession = m, true, old.Session
	current := attemptWire(t, client, c).Attempts[0].Job
	if current.Attempt <= old.Attempt {
		t.Fatal("attempt not superseded")
	}
	for _, edit := range []func(*Result){
		func(r *Result) { *r = result(old, true) },
		func(r *Result) { r.ID = "other-job" },
		func(r *Result) { r.Attempt = old.Attempt },
		func(r *Result) { r.Session = old.Session },
		func(r *Result) { r.Machine = "other-machine" },
	} {
		r := result(current, true)
		edit(&r)
		if err := client.Call(context.Background(), "POST", "/api/workers/complete", r, nil); !stale(err) {
			t.Fatalf("stale key accepted: %+v %v", r.Execution, err)
		}
	}
	finish(t, q, current, true)
	requireState(t, q, current.ID, "succeeded")
}

func TestAttemptProtocolWrongStopIgnored(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	key := AttemptReport{Execution: Execution{ID: "job", Attempt: 2}, Session: "session"}
	c := Checkin{Machine: Machine{ID: "worker", Session: key.Session}, Attempts: []AttemptReport{key}}
	for _, edit := range []func(*AttemptDirective){
		func(a *AttemptDirective) { a.ID = "other-job" },
		func(a *AttemptDirective) { a.Attempt-- },
		func(a *AttemptDirective) { a.Session = "old-session" },
	} {
		a := AttemptDirective{Execution: key.Execution, Session: key.Session, Stop: true, Kill: true, Hold: true}
		edit(&a)
		reply, err := workerAttemptReply(c, Reply{AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptDirective{a}})
		if err != nil || reply.Stop || reply.Kill || reply.Hold {
			t.Fatalf("wrong stop applied: %+v %v", reply, err)
		}
		// The independent event path uses the same full key before process control.
		// An unused PID makes any mistaken delivery observable in p.killed
		// without signalling a real process. macOS/Linux PID limits are smaller.
		p := &process{job: Job{ID: key.ID, Attempt: key.Attempt, Session: key.Session}, cmd: &exec.Cmd{Process: &os.Process{Pid: 1 << 30}}}
		control := processControl{clock: clock, current: p, pending: WorkerEvent{Execution: a.Execution, Session: a.Session, deadline: clock.Now(), Kill: true}}
		control.apply()
		if p.stopped || p.killed || p.stopAt != nil {
			t.Fatal("wrong event affected current process")
		}
	}
	a := AttemptDirective{Execution: key.Execution, Session: key.Session, Stop: true, GraceSeconds: 10}
	reply, err := workerAttemptReply(c, Reply{AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptDirective{a}})
	if err != nil || !reply.Stop || reply.GraceSeconds != 10 {
		t.Fatal("correct stop lost")
	}
}

func TestAttemptProtocolStopEventKey(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	m := machine("events", "fake")
	submit(t, q, spec("experiment"))
	j := attemptWire(t, client, Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m}).Attempts[0].Job
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	var event WorkerEvent
	if err := client.Call(context.Background(), "GET", "/api/workers/"+m.ID+"/events?session="+m.Session, nil, &event); err != nil {
		t.Fatal(err)
	}
	if event.ID != j.ID || event.Attempt != j.Attempt || event.Session != j.Session || event.StopAt == nil || !event.StopAt.Equal(clock.Now().Add(StopGrace)) {
		t.Fatalf("stop event lost key or deadline: %+v", event)
	}
}

func TestAttemptProtocolRestartCustody(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	q.checkpointProbe = func() bool { return true }
	s := spec("experiment")
	s.Resumable = false
	submit(t, q, s)
	m := machine("restart", "fake")
	c := Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m}
	reply, err := q.Checkin(c)
	if err != nil || len(reply.Attempts) != 1 || reply.Attempts[0].Job == nil {
		t.Fatalf("assignment: %+v %v", reply, err)
	}
	j := reply.Attempts[0].Job
	w := Worker{Machine: m, StateDir: t.TempDir()}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	r := result(j, true)
	store.state.Session, store.state.Job, store.state.Result = m.Session, j, &r
	if err = store.save(); err != nil {
		t.Fatal(err)
	}
	store.close()
	q.Close()
	clock.Advance(time.Minute)
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.checkpointProbe = func() bool { return true }
	store, err = w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if !store.matchesResult(*store.state.Result, m.ID) || store.state.Job.Attempt != j.Attempt || store.state.Session != j.Session {
		t.Fatal("spool lost execution key")
	}
	c.Attempts = []AttemptReport{{Execution: store.state.Result.Execution, Session: store.state.Session}}
	reply, err = q.Checkin(c)
	if err != nil || len(reply.Attempts) != 1 || reply.Attempts[0].Stop || reply.Attempts[0].Session != j.Session || reply.Attempts[0].Attempt != j.Attempt {
		t.Fatalf("queue restart lost execution key: %+v %v", reply, err)
	}
	if err = q.Complete(*store.state.Result); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "succeeded" || !got.ReleaseConfirmed {
		t.Fatal("buffered keyed result did not settle exact custody")
	}
	// Verify no fresh-session rewrite was hidden in the saved spool.
	data, err := os.ReadFile(filepath.Join(w.StateDir, "state.json"))
	var saved workerState
	if err != nil || json.Unmarshal(data, &saved) != nil || saved.Session != j.Session {
		t.Fatal("recovery changed session before acknowledgement")
	}
}

func TestAttemptProtocolExactNativeReleaseAck(t *testing.T) {
	for _, cancelJob := range []bool{false, true} {
		t.Run(map[bool]string{false: "priority", true: "cancel"}[cancelJob], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("release", "fake")
			submit(t, q, spec("experiment"))
			c := Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m}
			reply, err := q.Checkin(c)
			if err != nil {
				t.Fatal(err)
			}
			j := reply.Attempts[0].Job
			if cancelJob {
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				submit(t, q, spec("production"))
			}
			c.Attempts = []AttemptReport{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: m.Session}}
			clock.Advance(StopGrace)
			reply, err = q.Checkin(c)
			if err != nil || len(reply.Attempts) != 1 || !reply.Attempts[0].Kill {
				t.Fatalf("missing kill-time stop: %+v %v", reply, err)
			}
			r := result(j, false)
			r.ReleaseOnly = true
			if err = q.Complete(r); err != nil {
				t.Fatal(err)
			}
			want := "queued"
			if cancelJob {
				want = "cancelled"
			}
			got := job(t, q, j.ID)
			if got.State != want || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("exact native acknowledgement left custody stopping: state=%s want=%s", got.State, want)
			}
		})
	}
}
