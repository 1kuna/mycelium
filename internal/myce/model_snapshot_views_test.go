//go:build linux || darwin

package myce

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Each payload stays alive at a pipe barrier until both have read the model.
// They hold the real cache acquisitions simultaneously: no per-job staging.
func modelSnapshotViews(t *testing.T, mode string, wrap func(*testing.T, Worker, *exec.Cmd, string, string, map[string]string)) {
	t.Helper()
	if mode == "plain" && os.Geteuid() == 0 {
		t.Skip("root bypasses ordinary file modes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, modelRoot := t.TempDir(), t.TempDir()
	repo := hubRepo(t, modelRoot, "org/tiny", tinyRevision, map[string]string{
		"model.safetensors": tinyWeights, "config.json": tinyConfig, "custom.sh": "#!/bin/sh\nexit 0\n",
	})
	custom, err := filepath.EvalSymlinks(filepath.Join(repo, "snapshots", tinyRevision, "custom.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(custom, 0500); err != nil {
		t.Fatal(err)
	}
	w := snapshotWorker(t, modelRoot, &fakeClock{now: time.Unix(1000, 0)})
	j := snapshotJob("org/tiny")
	type acquisition struct {
		paths    map[string]string
		releases []func()
		err      error
	}
	acquired := make(chan acquisition, 2)
	for i := 0; i < 2; i++ {
		go func() {
			paths, releases, err := w.acquireModelSnapshots(ctx, j)
			acquired <- acquisition{paths, releases, err}
		}()
	}
	var paths [2]string
	for i := range paths {
		a := <-acquired
		for _, release := range a.releases {
			t.Cleanup(release)
		}
		if a.err != nil {
			t.Fatal(a.err)
		}
		paths[i] = a.paths["MYCE_MODEL_BASE"]
	}
	if paths[0] != paths[1] {
		t.Fatal("concurrent attempts got separate cached copies")
	}
	before, err := os.Stat(filepath.Join(paths[0], "model.safetensors"))
	if err != nil {
		t.Fatal(err)
	}
	// Ordinary writes must fail even in the unsandboxed default. Confined jobs
	// must also fail after chmod, which Landlock deliberately does not mediate.
	script := `set -eu
 test "$(cat "$MYCE_MODEL_BASE/model.safetensors")" = 'synthetic weights'
 if (printf poisoned > "$MYCE_MODEL_BASE/model.safetensors") 2>/dev/null; then exit 81; fi
 if (printf poisoned >> "$MYCE_MODEL_BASE/config.json") 2>/dev/null; then exit 82; fi
 if (touch "$MYCE_MODEL_BASE/new") 2>/dev/null; then exit 83; fi
 if (rm "$MYCE_MODEL_BASE/config.json") 2>/dev/null; then exit 84; fi
 printf 'ready\n'
 read gate
 `
	if mode != "plain" {
		script += `
 /bin/sh "$MYCE_MODEL_BASE/custom.sh"
 if ("$MYCE_MODEL_BASE/custom.sh") 2>/dev/null; then exit 87; fi
 `
	}
	if mode != "plain" {
		script += `
 chmod u+w "$MYCE_MODEL_BASE/model.safetensors" 2>/dev/null || :
 chmod u+w "$MYCE_MODEL_BASE" 2>/dev/null || :
 if (printf poisoned > "$MYCE_MODEL_BASE/model.safetensors") 2>/dev/null; then exit 85; fi
 if (touch "$MYCE_MODEL_BASE/new") 2>/dev/null; then exit 86; fi
 `
	}
	var commands []*exec.Cmd
	var pipes []io.WriteCloser
	var errorsOut [2]bytes.Buffer
	for i := range paths {
		work := t.TempDir()
		env := map[string]string{"PATH": "/usr/bin:/bin", "MYCE_MODEL_BASE": paths[i]}
		cmd := exec.CommandContext(ctx, "/bin/sh", "-ec", script)
		cmd.Dir = work
		cmd.Env = environmentList(env)
		local := *w
		local.nativeInputPaths(j, map[string]string{"MYCE_MODEL_BASE": paths[i]})
		if wrap != nil {
			wrap(t, local, cmd, root, work, env)
		}
		input, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		output, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = &errorsOut[i]
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		})
		commands = append(commands, cmd)
		pipes = append(pipes, input)
		if line, err := bufio.NewReader(output).ReadString('\n'); err != nil || line != "ready\n" {
			_ = cmd.Wait()
			t.Fatalf("%s payload not ready: %q %v: %s", mode, line, err, &errorsOut[i])
		}
	}
	for _, pipe := range pipes {
		if _, err := fmt.Fprintln(pipe, "go"); err != nil {
			t.Fatal(err)
		}
		_ = pipe.Close()
	}
	for i, cmd := range commands {
		if err := cmd.Wait(); err != nil {
			t.Errorf("%s payload: %v: %s", mode, err, &errorsOut[i])
		}
	}
	for name, want := range map[string]string{"model.safetensors": tinyWeights, "config.json": tinyConfig} {
		data, err := os.ReadFile(filepath.Join(paths[0], name))
		if err != nil || string(data) != want {
			t.Errorf("shared bytes changed: %s %v", name, err)
		}
	}
	after, err := os.Stat(filepath.Join(paths[1], "model.safetensors"))
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("cached file replaced", err)
	}
}

func TestModelSnapshotPlainReadOnlyViews(t *testing.T) { modelSnapshotViews(t, "plain", nil) }
