package myce

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	proofToken       = "disposable-shared-queue-test"
	proofMemberLease = 2400 * time.Millisecond
)

func proofClock() *devClock {
	return &devClock{base: &fakeClock{now: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)}}
}

// Runs the same member entry point as the opt-in CLI, with fixture timing and
// an injectable peer link. Separate OS processes allow SIGTERM, STOP and KILL.
func TestSharedQueueProofProcess(t *testing.T) {
	if os.Getenv("MYCE_SHARED_PROOF_HELPER") != "1" {
		t.Skip("subprocess entry point")
	}
	if os.Getenv("MYCE_SHARED_PROOF_SINGLE") == "1" {
		serveProofSingle(t)
		return
	}
	path := os.Getenv("MYCE_SHARED_PROOF_DATABASE")
	endpoint := os.Getenv("MYCE_SHARED_PROOF_LISTEN")
	l, err := net.Listen("tcp", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	dir := os.Getenv("MYCE_SHARED_PROOF_FILES")
	fmt.Println(l.Addr().String())
	ctx, cancel := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer cancel()
	err = ServeSharedQueueProof(ctx, l, SharedQueueProofConfig{
		Database: path, Postgres: os.Getenv("MYCE_SHARED_PROOF_POSTGRES"),
		FileStore: filepath.Join(dir, "files"), Checkpoints: filepath.Join(dir, "checkpoints"), Token: proofToken, Clock: proofClock(),
		interval: 100 * time.Millisecond, lease: proofMemberLease,
		peerTransport: proofRoundTrip(func(r *http.Request) (*http.Response, error) {
			if _, err := os.Stat(path + ".isolate-peer"); err == nil {
				return nil, errors.New("proof peer link partition")
			}
			return http.DefaultTransport.RoundTrip(r)
		}),
		verifyBundles: func(ctx context.Context, store *FileStore, refs map[FileRef]bool) error {
			// A hung mount ignores cancellation, so the hold does too.
			hold := filepath.Join(dir, filepath.Base(filepath.Dir(path)))
			if _, err := os.Stat(hold + ".hang-store"); err == nil {
				if err := os.WriteFile(hold+".store-blocked", nil, 0600); err != nil {
					return err
				}
				for {
					if _, err := os.Stat(hold + ".hang-store"); errors.Is(err, os.ErrNotExist) {
						break
					}
					time.Sleep(25 * time.Millisecond)
				}
			}
			return verifyFrozenBundleFiles(ctx, store, refs)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

type proofRoundTrip func(*http.Request) (*http.Response, error)

func (f proofRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type proofProcess struct {
	cmd      *exec.Cmd
	done     chan error
	log      string
	db, addr string
	client   Client
	stopped  bool
}

func startProofMember(t *testing.T, dir, name, addr, dsn string) *proofProcess {
	t.Helper()
	return startProofProcess(t, "TestSharedQueueProofProcess", dir, name, addr, "MYCE_SHARED_PROOF_POSTGRES="+dsn)
}

// startProofProcess runs a queue entry point test as its own OS process. All
// processes in one fixture share the file store and checkpoint root in dir.
func startProofProcess(t *testing.T, entry, dir, name, addr string, env ...string) *proofProcess {
	t.Helper()
	p := &proofProcess{db: filepath.Join(dir, name, "queue.db"), log: filepath.Join(dir, name+".log"), done: make(chan error, 1)}
	p.cmd = exec.Command(os.Args[0], "-test.run=^"+entry+"$", "-test.v=false")
	p.cmd.Env = append(append(os.Environ(),
		"MYCE_SHARED_PROOF_HELPER=1",
		"MYCE_SHARED_PROOF_DATABASE="+p.db,
		"MYCE_SHARED_PROOF_LISTEN="+addr,
		"MYCE_SHARED_PROOF_FILES="+dir), env...)
	if err := os.MkdirAll(filepath.Join(dir, "checkpoints"), 0700); err != nil {
		t.Fatal(err)
	}
	log, err := os.OpenFile(p.log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = log
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		log.Close()
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			data, _ := os.ReadFile(p.log)
			t.Logf("%s member log: %s", name, data)
		}
		if !p.stopped {
			p.stop(t, syscall.SIGKILL)
		}
	})
	address := make(chan string, 1)
	go func() {
		defer log.Close()
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			address <- scanner.Text()
		} else {
			address <- ""
		}
		for scanner.Scan() {
			fmt.Fprintln(log, scanner.Text())
		}
		p.done <- p.cmd.Wait()
	}()
	select {
	case p.addr = <-address:
		if _, _, err := net.SplitHostPort(p.addr); err != nil {
			p.fail(t, "member did not report a listener")
		}
	case <-time.After(10 * time.Second):
		p.fail(t, "timed out waiting for member listener")
	}
	p.client = Client{URL: "http://" + p.addr, Token: proofToken, SharedQueueProof: true,
		HTTP: &http.Client{Timeout: 2 * time.Second}, singleAttempt: true}
	return p
}

func (p *proofProcess) fail(t *testing.T, msg string) {
	t.Helper()
	log, _ := os.ReadFile(p.log)
	t.Fatalf("%s\n%s", msg, log)
}

func (p *proofProcess) stop(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if p.stopped {
		t.Fatal("proof member already stopped")
	}
	_ = p.cmd.Process.Signal(sig)
	select {
	case err := <-p.done:
		p.stopped = true
		if sig == syscall.SIGTERM && err != nil {
			p.fail(t, fmt.Sprintf("graceful stop: %v", err))
		}
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		p.stopped = true
		p.fail(t, "member did not exit after signal")
	}
}

func proofEventually(t *testing.T, why string, f func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for !f() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", why)
		case <-tick.C:
		}
	}
}

func proofLeader(t *testing.T, a *sharedQueueAuthority, p *proofProcess, after int64) sharedQueueTurn {
	t.Helper()
	var turn sharedQueueTurn
	defer func() {
		if t.Failed() {
			data, _ := os.ReadFile(p.log)
			t.Logf("last turn: %+v\nmember log: %s", turn, data)
		}
	}()
	proofEventually(t, "routable coordinator "+filepath.Base(filepath.Dir(p.db)), func() bool {
		var err error
		turn, err = a.current(t.Context())
		return err == nil && turn.ready && turn.number > after && turn.endpoint == p.client.URL
	})
	proofEventually(t, "coordinator accepts API traffic", func() bool {
		return p.client.Call(t.Context(), http.MethodGet, "/api/ping", nil, nil) == nil
	})
	return turn
}

func proofSpec(key string) Spec {
	s := spec("experiment")
	s.SubmissionKey, s.Resumable = key, false
	s.Needs = ResourceNeeds{Platform: Platform{Required: []string{"cpu"}}, CPUCores: 1, HostMemoryGB: 0.25}
	return s
}

func proofSubmit(t *testing.T, c Client, s Spec) *Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	job, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func proofUnavailable(t *testing.T, err error) {
	t.Helper()
	var reply *HTTPError
	if !errors.As(err, &reply) || reply.Status != http.StatusServiceUnavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
}

func proofReplica(t *testing.T, p *proofProcess, id string) (int64, *Job) {
	t.Helper()
	// Add the explicit wire name for last-known to avoid treating an ordinary
	// coordinator status response as valid replica evidence.
	var result struct {
		LastKnown bool  `json:"last_known"`
		Sequence  int64 `json:"sequence"`
		Job       *Job  `json:"job"`
	}
	if err := p.client.Call(t.Context(), http.MethodGet, sharedReplicaPath+"?job="+id, nil, &result); err != nil {
		p.fail(t, fmt.Sprintf("replica status: %v", err))
	}
	if !result.LastKnown {
		t.Fatal("replica response lacks last-known marker")
	}
	return result.Sequence, result.Job
}

func startProofWorker(t *testing.T, c Client, dir string) {
	t.Helper()
	run := &workerTestRun{done: make(chan struct{}), state: filepath.Join(dir, "worker")}
	ctx, cancel := context.WithCancel(context.Background())
	w := Worker{
		Clock: RealClock{}, serviceStop: ctx, StateDir: run.state, Client: c, Interval: 100 * time.Millisecond,
		// Advertise the same sampled-memory admission capability as a CPU dev worker.
		sharedCPUDev: true, devEnvironmentSizing: true, CPUIsolationProbe: func(context.Context) error { return nil },
		Machine: machine("proof-worker", "cpu", "proof-worker-host"),
	}
	if workerDevProcessMemory != nil {
		w.processMemory = workerDevProcessMemory()
	}
	go func() { run.err = w.Run(ctx); close(run.done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.done:
			if run.err != nil {
				t.Error(run.err)
			}
		case <-time.After(10 * time.Second):
			t.Error("proof worker did not stop")
		}
	})
}

func proofBlockingJob(t *testing.T, c Client, dir string) (*Job, func(), func()) {
	t.Helper()
	starts, release, finished := filepath.Join(dir, "starts"), filepath.Join(dir, "release"), filepath.Join(dir, "finished")
	unblock := func() {
		if err := os.WriteFile(release, nil, 0600); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(unblock)
	s := proofSpec("running")
	source := filepath.Join(dir, "seed")
	if err := os.WriteFile(source, []byte("proof"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := c.UploadFile(ctx, source, ref); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{"seed": ref}
	s.Outputs = map[string]string{"result.txt": "results"}
	s.Run.Command = []string{"/bin/sh", "-c", `printf 'start\n' >> "$1"; while [ ! -f "$2" ]; do sleep 0.05; done; cat seed > result.txt || exit; : > "$3"`, "proof", starts, release, finished}
	job := proofSubmit(t, c, s)
	job = proofJobState(t, c, job.ID, func(j *Job) bool { return j.State == "running" && j.StartedAt != nil })
	proofEventually(t, "CPU process starts", func() bool { _, err := os.Stat(starts); return err == nil })
	return job, unblock, func() {
		t.Helper()
		proofEventually(t, "CPU process finishes", func() bool { _, err := os.Stat(finished); return err == nil })
		if data, err := os.ReadFile(starts); err != nil || string(data) != "start\n" {
			t.Fatalf("CPU process starts=%q, %v", data, err)
		}
	}
}

func proofJobState(t *testing.T, c Client, id string, f func(*Job) bool) *Job {
	t.Helper()
	var j *Job
	var last string
	defer func() {
		if t.Failed() && last != "" {
			t.Logf("latest job state: %s", last)
		}
	}()
	proofEventually(t, "job state for "+id, func() bool {
		var err error
		j, err = c.GetJob(t.Context(), id)
		if err != nil {
			last = err.Error()
		} else {
			last = j.State + " " + j.Reason
		}
		return err == nil && f(j)
	})
	return j
}

func assertProofAttempt(t *testing.T, before, after *Job) {
	t.Helper()
	if after.Attempt != before.Attempt || after.Session != before.Session || after.Machine != before.Machine ||
		after.StartedAt == nil || !after.StartedAt.Equal(*before.StartedAt) || len(after.Copies) != 1 {
		t.Fatalf("attempt changed during handover: before %d/%s, after %d/%s, copies %d",
			before.Attempt, before.Session, after.Attempt, after.Session, len(after.Copies))
	}
}

func TestSharedQueueMemberRunningHandover(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "myce")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "../../cmd/myce")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	pythonPath, err := filepath.Abs("../../clients/python/src")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		sig  syscall.Signal
	}{{"planned", syscall.SIGTERM}, {"crash", syscall.SIGKILL}} {
		t.Run(tc.name, func(t *testing.T) {
			pg := startProofPostgres(t)
			a := pg.authority(t)
			dir := t.TempDir()
			old := startProofMember(t, dir, "old", "127.0.0.1:0", pg.dsn)
			first := proofLeader(t, a, old, 0)
			next := startProofMember(t, dir, "next", "127.0.0.1:0", pg.dsn)
			unauthenticated := next.client
			unauthenticated.Token = ""
			for _, path := range []string{"/api/ping", sharedReplicaPath} {
				var refusal *HTTPError
				err := unauthenticated.Call(t.Context(), http.MethodGet, path, nil, nil)
				if !errors.As(err, &refusal) || refusal.Status != http.StatusUnauthorized {
					t.Fatalf("unauthenticated access to %s = %v", path, err)
				}
			}
			startProofWorker(t, next.client, dir)
			addresses := old.client.URL + "," + next.client.URL
			client, err := NewMultiEndpointClient(addresses, proofToken)
			if err != nil {
				t.Fatal(err)
			}
			before, unblock, finished := proofBlockingJob(t, client, dir)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			ready := filepath.Join(dir, "client-ready")
			download := filepath.Join(dir, "python-download")
			python := exec.CommandContext(ctx, "python3", "-c", `
from pathlib import Path
from sys import argv
from mycelium import Client
job, ready, destination = argv[1:]
client = Client(timeout=12)
assert client.get(job)["state"] == "running"
Path(ready).touch()
assert client.wait(job)["state"] == "succeeded"
client.download_result(job, "result.txt", destination)
assert Path(destination).read_bytes() == b"proof"
`, before.ID, ready, download)
			clientEnv := append(os.Environ(), "MYCE_QUEUE="+addresses, "MYCE_TOKEN="+proofToken)
			python.Env = append(clientEnv, "PYTHONPATH="+pythonPath, "PYTHONDONTWRITEBYTECODE=1")
			var pythonLog bytes.Buffer
			python.Stdout, python.Stderr = &pythonLog, &pythonLog
			if err := python.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				if !waited {
					_ = python.Process.Kill()
					_ = python.Wait()
				}
			}()
			proofEventually(t, "Python client reads from the original member", func() bool {
				_, err := os.Stat(ready)
				return err == nil
			})
			proofEventually(t, "follower replays running job", func() bool {
				_, j := proofReplica(t, next, before.ID)
				return j != nil && j.State == "running" && j.StartedAt != nil
			})
			old.stop(t, tc.sig)
			second := proofLeader(t, a, next, first.number)
			if second.number != first.number+1 {
				t.Fatalf("handover number = %d, want %d", second.number, first.number+1)
			}
			active := proofJobState(t, next.client, before.ID, func(j *Job) bool { return j.State == "running" })
			assertProofAttempt(t, before, active)
			cli := func(args ...string) []byte {
				t.Helper()
				cmd := exec.CommandContext(ctx, binary, args...)
				cmd.Env = clientEnv
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("CLI %s: %v\n%s", args[0], err, out)
				}
				return out
			}
			if out := cli("status", "--json", before.ID); !bytes.Contains(out, []byte(before.ID)) {
				t.Fatalf("CLI status did not show the running job: %s", out)
			}
			current := resultIdentity(active)
			current.Success = true
			// A request carrying the retired turn must never reach completion,
			// even when all three execution identity fields still match.
			proofUnavailable(t, next.client.callOnceShared(ctx, http.MethodPost, "/api/workers/complete",
				current, nil, strconv.FormatInt(first.number, 10)))
			for _, bad := range []Result{
				{Execution: Execution{ID: active.ID, Attempt: active.Attempt + 1}, Machine: active.Machine, Session: active.Session},
				{Execution: Execution{ID: active.ID, Attempt: active.Attempt}, Machine: active.Machine, Session: "wrong-session"},
			} {
				var refusal *HTTPError
				err := next.client.Call(t.Context(), http.MethodPost, "/api/workers/started", bad, nil)
				if !errors.As(err, &refusal) || refusal.Status != http.StatusConflict {
					t.Fatalf("start under the wrong attempt identity = %v", err)
				}
				bad.Success = true
				err = next.client.Call(ctx, http.MethodPost, "/api/workers/complete", bad, nil)
				if !errors.As(err, &refusal) || refusal.Code != "request_stale" {
					t.Fatalf("result under the wrong attempt identity = %v", err)
				}
			}
			unblock()
			finished()
			after := proofJobState(t, next.client, before.ID, proofSucceeded)
			assertProofAttempt(t, before, after)
			if after.Losses != before.Losses || !after.Copies[0].ReleaseConfirmed {
				t.Fatalf("handover changed loss accounting or lost release: losses=%d, released=%t",
					after.Losses, after.Copies[0].ReleaseConfirmed)
			}
			waitErr := python.Wait()
			waited = true
			if waitErr != nil {
				t.Fatalf("Python client failed after handover: %v\n%s", waitErr, pythonLog.String())
			}
			if out := cli("wait", "--timeout", "5s", before.ID); !strings.Contains(string(out), "succeeded") {
				t.Fatalf("CLI wait did not report success: %s", out)
			}
			dest := filepath.Join(dir, "download")
			if err := client.DownloadResult(ctx, after.ID, "result.txt", dest); err != nil {
				t.Fatal(err)
			}
			if b, err := os.ReadFile(dest); err != nil || string(b) != "proof" {
				t.Fatalf("verified result = %q, %v", b, err)
			}
		})
	}
}

func TestSharedQueueMemberDatabaseOutage(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	leader := startProofMember(t, dir, "leader", "127.0.0.1:0", pg.dsn)
	proofLeader(t, a, leader, 0)
	local := startProofMember(t, dir, "local", "127.0.0.1:0", pg.dsn)
	startProofWorker(t, local.client, dir)
	before, unblock, finished := proofBlockingJob(t, local.client, dir)
	proofEventually(t, "follower has last-known running state", func() bool {
		_, j := proofReplica(t, local, before.ID)
		return j != nil && j.StartedAt != nil
	})
	pg.stop()
	err := local.client.Call(t.Context(), http.MethodPost, "/api/jobs", proofSpec("during-outage"), nil)
	proofUnavailable(t, err)
	err = local.client.Call(t.Context(), http.MethodPost, "/api/workers/started", result(before, true), nil)
	proofUnavailable(t, err)
	err = local.client.Call(t.Context(), http.MethodPost, "/api/workers/complete", result(before, true), nil)
	proofUnavailable(t, err)
	_, err = local.client.GetJob(t.Context(), before.ID)
	proofUnavailable(t, err)
	rev, status := proofReplica(t, local, before.ID)
	if rev == 0 || status == nil || status.State != "running" {
		t.Fatalf("last-known result: sequence=%d job=%+v", rev, status)
	}
	unblock()
	finished()
	// The process can exit locally while no authoritative result can commit.
	_, err = local.client.GetJob(t.Context(), before.ID)
	proofUnavailable(t, err)
	_, status = proofReplica(t, local, before.ID)
	if status == nil || status.State != "running" {
		t.Fatal("an unacknowledged result changed the committed copy")
	}
	pg.start()
	after := proofJobState(t, local.client, before.ID, proofSucceeded)
	assertProofAttempt(t, before, after)
}

func proofSucceeded(j *Job) bool { return j.State == "succeeded" }

func TestSharedQueueMemberFollowerLossPreservesAuthority(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	leader := startProofMember(t, dir, "leader", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, leader, 0)
	follower := startProofMember(t, dir, "follower", "127.0.0.1:0", pg.dsn)
	failed, peer := machine("lost-worker", "cpu"), machine("peer-worker", "cpu")
	checkin := func(c Client, m Machine) Reply {
		t.Helper()
		var reply Reply
		err := c.Call(t.Context(), http.MethodPost, "/api/workers/checkin",
			Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}, &reply)
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	checkin(leader.client, peer)
	spec := proofSpec("lost-attempt")
	spec.AllowedMachines = []string{failed.ID}
	job := proofSubmit(t, leader.client, spec)
	var assigned *Job
	proofEventually(t, "follower's worker receives an assignment", func() bool {
		assigned = assignedJob(checkin(follower.client, failed))
		return assigned != nil
	})
	if assigned.ID != job.ID {
		t.Fatalf("unexpected assignment: %s", assigned.ID)
	}
	if err := follower.client.Call(t.Context(), http.MethodPost, "/api/workers/started", result(assigned, true), nil); err != nil {
		t.Fatal(err)
	}
	follower.stop(t, syscall.SIGKILL)
	var after *Job
	for elapsed := time.Second; elapsed <= 2*LossAfter; elapsed += time.Second {
		if err := leader.client.Call(t.Context(), http.MethodPost, "/api/dev/clock/advance", map[string]int{"seconds": 1}, nil); err != nil {
			t.Fatal(err)
		}
		checkin(leader.client, peer)
		var err error
		after, err = leader.client.GetJob(t.Context(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if after.Losses != 0 {
			break
		}
	}
	if after == nil || after.State != "queued" || after.Losses != 1 || after.Attempt != assigned.Attempt {
		t.Fatalf("follower loss did not requeue the exact attempt once: %+v", after)
	}
	turn, err := a.current(t.Context())
	if err != nil || !turn.ready || !sameSharedTurn(first, turn) {
		t.Fatalf("follower loss changed the coordinator: %+v, %v", turn, err)
	}
}

func TestSharedQueueMemberSingleSurvivor(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	one := startProofMember(t, dir, "one", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, one, 0)
	two := startProofMember(t, dir, "two", "127.0.0.1:0", pg.dsn)
	three := startProofMember(t, dir, "three", "127.0.0.1:0", pg.dsn)
	seed := proofSubmit(t, one.client, proofSpec("seed"))
	proofEventually(t, "third member seeds SQLite", func() bool {
		_, j := proofReplica(t, three, seed.ID)
		return j != nil
	})
	three.stop(t, syscall.SIGKILL)
	// Removing a follower changes neither the authority nor write availability.
	later := proofSubmit(t, two.client, proofSpec("while-follower-offline"))
	now, err := a.current(t.Context())
	if err != nil || !sameSharedTurn(first, now) || now.head <= first.head {
		t.Fatalf("follower failure changed live authority: %+v, %v", now, err)
	}
	// Force both failures before observing either exit. The returning copy
	// missed a committed submit and must catch up with no other queue process.
	if err := one.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	two.stop(t, syscall.SIGKILL)
	one.stop(t, syscall.SIGKILL)
	survivor := startProofMember(t, dir, "three", three.addr, pg.dsn)
	proofLeader(t, a, survivor, first.number)
	j, err := survivor.client.GetJob(t.Context(), later.ID)
	if err != nil || j.State != "queued" {
		t.Fatalf("handover lost an accepted submit: %v, %v", j, err)
	}
	startProofWorker(t, survivor.client, dir)
	for _, job := range []*Job{seed, later, proofSubmit(t, survivor.client, proofSpec("solo"))} {
		finished := proofJobState(t, survivor.client, job.ID, proofSucceeded)
		if finished.Attempt != 1 {
			t.Fatalf("job needed %d attempts on the sole survivor", finished.Attempt)
		}
	}
}

func TestSharedQueueMemberSuspendedCoordinator(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	old := startProofMember(t, dir, "old", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, old, 0)
	next := startProofMember(t, dir, "next", "127.0.0.1:0", pg.dsn)
	// Buffer the old coordinator's successful control reply as if delayed in
	// transit. The worker then verifies the reply after the handover.
	captured, deliver := make(chan bool, 1), make(chan struct{})
	client := next.client
	client.HTTP = &http.Client{Timeout: 12 * time.Second, Transport: proofRoundTrip(func(r *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(r)
		if err != nil || r.URL.Path != "/api/workers/checkin" {
			return response, err
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(body))
		captured <- err == nil && response.StatusCode == http.StatusOK && response.Header.Get(sharedTurnHeader) == strconv.FormatInt(first.number, 10)
		select {
		case <-deliver:
			return response, err
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	})}
	defer close(deliver)
	done := make(chan error, 1)
	go func() {
		var reply Reply
		fake := machine("delayed-worker", "cpu", "delayed-host")
		done <- client.Call(t.Context(), http.MethodPost, "/api/workers/checkin", Checkin{Machine: fake, AttemptProtocol: keyedAttemptProtocol, period: time.Second}, &reply)
	}()
	select {
	case ok := <-captured:
		if !ok {
			t.Fatal("old coordinator did not commit a control reply")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker control reply did not arrive")
	}
	if err := old.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	proofLeader(t, a, next, first.number)
	job := proofSubmit(t, next.client, proofSpec("new-authority"))
	if err := old.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	deliver <- struct{}{}
	proofUnavailable(t, <-done)
	proofUnavailable(t, old.client.Call(t.Context(), http.MethodGet,
		sharedTurnPath+"?handover="+strconv.FormatInt(first.number, 10), nil, nil))
	got := proofSubmit(t, old.client, proofSpec("new-authority"))
	if got.ID != job.ID {
		t.Fatalf("keyed replay after old member returns returned %q, want %q", got.ID, job.ID)
	}
}

type proofPostgresLink struct {
	listener net.Listener
	socket   string
	mu       sync.Mutex
	blocked  bool
	clients  map[net.Conn]bool
	done     chan struct{}
	children sync.WaitGroup
}

func startProofPostgresLink(t *testing.T, pg *proofPostgres) (*proofPostgresLink, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &proofPostgresLink{listener: listener, socket: filepath.Join(pg.root, ".s.PGSQL."+pg.port), clients: make(map[net.Conn]bool), done: make(chan struct{})}
	go l.run()
	t.Cleanup(func() {
		l.cut(true)
		_ = l.listener.Close()
		<-l.done
		l.children.Wait()
	})
	port := listener.Addr().(*net.TCPAddr).Port
	return l, "host=127.0.0.1 dbname=postgres user=proof sslmode=disable port=" + strconv.Itoa(port)
}

func (l *proofPostgresLink) cut(block bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.blocked = block
	if block {
		for c := range l.clients {
			_ = c.Close()
		}
	}
}

func (l *proofPostgresLink) run() {
	defer close(l.done)
	for {
		front, err := l.listener.Accept()
		if err != nil {
			return
		}
		l.mu.Lock()
		if l.blocked {
			l.mu.Unlock()
			_ = front.Close()
			continue
		}
		l.clients[front] = true
		l.mu.Unlock()
		l.children.Go(func() {
			defer func() {
				_ = front.Close()
				l.mu.Lock()
				delete(l.clients, front)
				l.mu.Unlock()
			}()
			back, err := net.Dial("unix", l.socket)
			if err != nil {
				return
			}
			defer back.Close()
			writes := make(chan struct{})
			go func() {
				_, _ = io.Copy(back, front)
				_ = back.Close()
				close(writes)
			}()
			_, _ = io.Copy(front, back)
			_ = front.Close()
			<-writes
		})
	}
}

func TestSharedQueueMemberNetworkPartitions(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	link, throughLink := startProofPostgresLink(t, pg)
	old := startProofMember(t, dir, "old", "127.0.0.1:0", throughLink)
	first := proofLeader(t, a, old, 0)
	next := startProofMember(t, dir, "next", "127.0.0.1:0", pg.dsn)
	seed := proofSubmit(t, old.client, proofSpec("initial"))
	proofEventually(t, "follower has seed", func() bool {
		_, j := proofReplica(t, next, seed.ID)
		return j != nil
	})
	// Both sides reach PostgreSQL, but this member cannot reach the owner.
	marker := next.db + ".isolate-peer"
	if err := os.WriteFile(marker, nil, 0600); err != nil {
		t.Fatal(err)
	}
	err := next.client.Call(t.Context(), http.MethodPost, "/api/jobs", proofSpec("peer-isolated"), nil)
	proofUnavailable(t, err)
	proofSubmit(t, old.client, proofSpec("old-still-live"))
	still, err := a.current(t.Context())
	if err != nil || !sameSharedTurn(first, still) {
		t.Fatalf("peer partition changed the coordinator: %+v, %v", still, err)
	}
	// Now the owner loses its own PostgreSQL connection. The peer can take
	// over even while its link to the old owner is still partitioned.
	link.cut(true)
	proofLeader(t, a, next, first.number)
	err = old.client.Call(t.Context(), http.MethodPost, "/api/jobs", proofSpec("authority-isolated"), nil)
	proofUnavailable(t, err)
	proofSubmit(t, next.client, proofSpec("next-still-live"))
	link.cut(false)
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	proofEventually(t, "former owner can route after partition heals", func() bool {
		return old.client.Call(t.Context(), http.MethodGet, "/api/ping", nil, nil) == nil
	})
}
