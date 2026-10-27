package myce

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Reproduce #149 at the real daemon boundary: a running production attempt
// outlives timed-out inventory reads, retains custody, and completes normally.
func TestDockerObservationTimeoutRetriesHealthyAttempt(t *testing.T) {
	for _, operation := range []string{"ps", "inspect"} {
		t.Run(operation, func(t *testing.T) {
			w, store, clock, root := dockerFixture(t)
			var armed atomic.Bool
			var failures atomic.Int32
			timedOut := make(chan struct{}, 2)
			w.dockerCommand = func(ctx context.Context, args ...string) ([]byte, error) {
				if armed.Load() && args[0] == operation && failures.Add(1) <= 2 {
					<-ctx.Done()
					timedOut <- struct{}{}
					return nil, ctx.Err()
				}
				cmd := exec.CommandContext(ctx, filepath.Join(root, "docker"), args...)
				cmd.Env = dockerClientEnvironment()
				return cmd.Output()
			}
			j := dockerJob()
			j.Spec.Priority = "production"
			q, _ := fixture(t)
			q.clock = clock
			submit(t, q, j.Spec)
			assigned := check(t, q, w.Machine, nil).Job
			if assigned == nil {
				t.Fatal("production fixture was not assigned")
			}
			j = *assigned
			j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
			p, err := startDockerFixture(w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(p.docker.close)
			await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
			armed.Store(true)
			select {
			case <-timedOut:
			case <-time.After(5 * time.Second):
				t.Fatal("observation did not time out")
			}
			retryStatus := func() string {
				p.docker.mu.Lock()
				defer p.docker.mu.Unlock()
				return p.docker.state.ObservationRetry
			}
			await(t, func() bool { return strings.Contains(retryStatus(), "observation retry") })
			clock.advance(3 * time.Minute)
			select {
			case <-timedOut:
			case <-time.After(5 * time.Second):
				t.Fatal("observation was not retried")
			}
			await(t, func() bool { return strings.Contains(retryStatus(), "unavailable 3m") })
			p.docker.mu.Lock()
			state := p.docker.state
			p.docker.mu.Unlock()
			if state.Stopped || state.Forced || state.ObservationLost || state.InfrastructureStop || state.Released || state.Identity != store.state.Identity || state.Container != "id1" {
				t.Fatalf("timeout changed exact custody or stop authority: %+v", state)
			}
			select {
			case f := <-p.done:
				t.Fatalf("timeout finished a healthy attempt: %+v", f)
			default:
			}
			await(t, func() bool { return retryStatus() == "" && p.docker.status() == "running" })
			if err := os.Remove(filepath.Join(root, "running")); err != nil {
				t.Fatal(err)
			}
			select {
			case f := <-p.done:
				if f.err != nil || !f.result.Success || f.result.Interrupted || f.result.ReleasePending {
					t.Fatalf("recovered observation lost completion: %+v", f)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("healthy attempt did not complete")
			}
			calls, err := os.ReadFile(filepath.Join(root, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			creates, starts := 0, 0
			for _, line := range strings.Split(string(calls), "\n") {
				argv := strings.Fields(line)
				if len(argv) == 0 {
					continue
				}
				switch argv[0] {
				case "kill", "stop":
					t.Fatalf("timeout signaled payload: %s", line)
				case "create":
					creates++
				case "start":
					starts++
				}
			}
			if creates != 1 || starts != 1 {
				t.Fatalf("timeout relaunched payload: creates=%d starts=%d", creates, starts)
			}
		})
	}
}

// A heartbeat sent during a slow startup read can remain at the queue after
// that read recovers. It must not turn observation health into stop authority.
func TestDockerObservationRetryHeartbeatAllowsOwnedLaunch(t *testing.T) {
	for _, protocol := range []string{"legacy", "machine"} {
		t.Run(protocol, func(t *testing.T) {
			q, clock := fixture(t)
			m := dockerMachine("worker", "fake")
			if protocol == "machine" {
				m = m3Machine()
				m.Capabilities = []string{"docker"}
			}
			submit(t, q, dockerJob().Spec)
			var j *Job
			if protocol == "machine" {
				j = assignedJob(machineCheck(t, q, m))
			} else {
				j = check(t, q, m, nil).Job
			}
			if j == nil {
				t.Fatal("missing assignment")
			}
			launch := HostLaunch{Machine: j.Machine, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt)}
			d := &dockerAttempt{clock: clock, timeout: time.Second, state: dockerExecution{Operation: "create"}, admit: func() error { return q.AuthorizeLaunch(launch) }}
			reads, starts := 0, 0
			d.command = func(_ context.Context, args ...string) ([]byte, error) {
				if args[0] == "inspect" {
					reads++
					if reads == 1 {
						return nil, context.DeadlineExceeded
					}
					m.DockerCleanup = d.status()
					c := Checkin{Machine: m}
					execution := Execution{ID: j.ID, Attempt: j.Attempt}
					if protocol == "machine" {
						c.AttemptProtocol = keyedAttemptProtocol
						c.Attempts = []AttemptReport{{Execution: execution, Session: j.Session}}
					} else {
						c.Running = &execution
					}
					if _, err := q.Checkin(c); err != nil {
						t.Fatal(err)
					}
				} else if args[0] == "start" {
					starts++
				}
				return nil, nil
			}
			if _, err := d.call("inspect", "owned"); err != nil {
				t.Fatal(err)
			}
			// No newer heartbeat clears the queue's retry-era observation.
			if _, err := d.operation("start", "start", "owned"); err != nil {
				t.Fatalf("recovered observation refused owned launch: %v", err)
			}
			current := job(t, q, j.ID)
			if reads != 2 || starts != 1 || d.state.Stopped || current.Attempt != 1 || current.Losses != 0 {
				t.Fatal("observation changed launch authority", current, d.state)
			}
		})
	}
}
