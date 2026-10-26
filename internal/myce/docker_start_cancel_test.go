package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise the real launch/cleanup path against the fake Docker CLI. The
// observation budget stays two seconds while runtime startup takes longer.
func TestDockerSlowStartHasIndependentBudget(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	binary := filepath.Join(root, "docker")
	w.dockerCommand = func(ctx context.Context, args ...string) ([]byte, error) {
		if args[0] == "start" {
			select {
			case <-time.After(2200 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = dockerClientEnvironment()
		return cmd.Output()
	}
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
	if err := os.Remove(filepath.Join(root, "running")); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-p.done:
		if !f.result.Success || f.result.ReleasePending {
			t.Fatalf("slow successful start lost: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow start did not finish")
	}
}

func TestDockerCancelledStartRestartAfterRemoval(t *testing.T) {
	w, s, clock, root := dockerFixture(t)
	j := dockerJob()
	j.Spec.Logs = "none"
	s.state.Job, s.state.Identity = &j, "cancelled-start"
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"created": "id1", "label": s.state.Identity} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	d := &dockerAttempt{dir: s.dir, binary: filepath.Join(root, "docker"), env: dockerClientEnvironment(), clock: clock, timeout: 2 * time.Second,
		state: dockerExecution{Identity: s.state.Identity, Endpoint: w.DockerEndpoint, Container: "id1", PendingContainer: "id1", Operation: "start", Pending: true, Stopped: true, Forced: true, LogsNone: true}}
	d.command = func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, d.binary, args...)
		cmd.Env = dockerClientEnvironment()
		out, err := cmd.Output()
		if args[0] == "inspect" && err == nil {
			var objects []dockerInspection
			if err := json.Unmarshal(out, &objects); err != nil {
				return nil, err
			}
			for i := range objects {
				objects[i].State.Status = "created"
				objects[i].State.ExitCode = 128
			}
			return json.Marshal(objects)
		}
		if args[0] == "rm" && err == nil {
			return nil, context.Canceled // Removal happened, receipt was lost.
		}
		return out, err
	}
	if err := d.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if done, err := d.reconcile(); done || !errors.Is(err, context.Canceled) {
		t.Fatal("lost removal receipt not reproduced", done, err)
	}
	p, err := w.restoreDocker(s)
	if err != nil || p == nil {
		t.Fatal("restore", err)
	}
	t.Cleanup(p.docker.close)
	select {
	case f := <-p.done:
		if f.result.Success || f.result.ReleasePending || !f.result.Interrupted {
			t.Fatalf("restart lost exact cleanup/outcome: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restart after removal retained fence")
	}
}

func TestDockerLateStartTerminalOOMRetainsOutcome(t *testing.T) {
	_, clock := fixture(t)
	at := clock.Now()
	var cs []dockerInspection
	if err := json.Unmarshal([]byte(`[{"Id":"exact","Config":{"Labels":{"myce.attempt":"owned"}},"HostConfig":{"RestartPolicy":{"Name":"no"},"NetworkMode":"none"},"State":{"Running":false,"Status":"exited","ExitCode":137,"OOMKilled":true}}]`), &cs); err != nil {
		t.Fatal(err)
	}
	cs[0].State.StartedAt = at.Add(time.Second)
	d := &dockerAttempt{clock: clock, timeout: time.Second, state: dockerExecution{Identity: "owned", Container: "exact", PendingContainer: "exact", Operation: "start", Pending: true, Stopped: true, DispatchedAt: &at,
		Containers: map[string]dockerMember{"exact": {Name: "main", ExitCode: new(int), StderrTail: "provisional"}}}}
	d.command = func(_ context.Context, args ...string) ([]byte, error) {
		switch args[0] {
		case "ps":
			return []byte("exact"), nil
		case "inspect":
			return json.Marshal(cs)
		case "network":
			return nil, nil
		default:
			t.Fatalf("terminal started object must be retained, got %v", args)
			return nil, nil
		}
	}
	if done, err := d.reconcile(); !done || err != nil {
		t.Fatal("terminal start not settled", done, err)
	}
	r := dockerOutcome(d.state, Result{}, context.Canceled)
	if r.Success || r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != "oom" || !d.state.OOMKilled || !d.state.MainFinished {
		t.Fatalf("late OOM evidence lost: %+v %+v", r, d.state)
	}
	if d.state.Containers["exact"].StderrTail == "provisional" {
		t.Fatal("provisional evidence survived positive start")
	}
}

func TestDockerCancelledStartSettlesExactContainer(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(map[bool]string{false: "created", true: "already-removed"}[removed], func(t *testing.T) {
			w, s, _, root := dockerFixture(t)
			binary := filepath.Join(root, "docker")
			w.dockerCommand = func(ctx context.Context, args ...string) ([]byte, error) {
				if args[0] == "start" {
					// Docker cancels the runtime before it produces a StartedAt.
					if removed {
						if err := os.Remove(filepath.Join(root, "created")); err != nil {
							return nil, err
						}
					}
					return nil, context.Canceled
				}
				cmd := exec.CommandContext(ctx, binary, args...)
				cmd.Env = dockerClientEnvironment()
				return cmd.Output()
			}
			j := dockerJob()
			j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
			p, err := startDockerFixture(w, s, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(p.docker.close)
			select {
			case f := <-p.done:
				if f.result.Success || f.result.ReleasePending || !f.result.Interrupted {
					t.Fatalf("cancelled start did not release as interruption: %+v", f)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled start retained permanent fence")
			}
			data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
			if err != nil || !strings.Contains(string(data), `"released":true`) || strings.Contains(string(data), `"pending":true`) {
				t.Fatalf("release not durable: %s %v", data, err)
			}
			if _, err := os.Stat(filepath.Join(root, "created")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed start container remains", err)
			}
		})
	}
}
