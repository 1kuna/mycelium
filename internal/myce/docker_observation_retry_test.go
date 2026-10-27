package myce

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
			await(t, func() bool { return strings.Contains(p.docker.status(), "observation retry") })
			clock.advance(3 * time.Minute)
			select {
			case <-timedOut:
			case <-time.After(5 * time.Second):
				t.Fatal("observation was not retried")
			}
			await(t, func() bool { return strings.Contains(p.docker.status(), "unavailable 3m") })
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
			await(t, func() bool { return p.docker.status() == "running" })
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
			if strings.Contains(string(calls), "kill ") || strings.Contains(string(calls), "stop ") || strings.Count(string(calls), "start id1\n") != 1 || strings.Count(string(calls), "create --restart=no ") != 1 {
				t.Fatalf("timeout signaled or relaunched payload: %s", calls)
			}
		})
	}
}
