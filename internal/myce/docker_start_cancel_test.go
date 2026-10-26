package myce

import (
	"context"
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
		return exec.CommandContext(ctx, binary, args...).Output()
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
				return exec.CommandContext(ctx, binary, args...).Output()
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
