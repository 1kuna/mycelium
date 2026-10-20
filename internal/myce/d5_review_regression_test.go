package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestD5ReviewIndependent137BeforeStopObservation(t *testing.T) {
	w, store, clock, root := dockerFixture(t)
	q, _ := fixture(t)
	q.clock = clock
	w.CheckpointRoot = q.root
	m := w.Machine
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.FreeHostMemoryGB = true, 32, 32
	j := dockerGroupJob(t)
	j.Spec.Priority = "production"
	submitted := submit(t, q, j.Spec)
	running := check(t, q, m, nil).Job
	if running == nil {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(running, false)); err != nil {
		t.Fatal(err)
	}
	p, err := startDockerFixture(w, store, *running)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	p.docker.close()
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte("main-killed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "running"), filepath.Join(root, "natural-exit")); err != nil {
		t.Fatal(err)
	}
	p.stop(clock.Now(), StopGrace, false)
	restartTick(t, p.docker)
	clock.advance(StopGrace)
	for i := 0; i < 4 && !p.docker.state.Released; i++ {
		restartTick(t, p.docker)
	}
	if !p.docker.state.Released {
		t.Fatal("custody not released")
	}
	calls, err := os.ReadFile(filepath.Join(root, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(calls), "kill --signal=KILL id1") || strings.Contains(string(calls), "kill --signal=TERM id1") {
		t.Fatal("fixture delivered a main stop signal")
	}
	t.Log("main was terminal before stop intent; zero main stop signals delivered")
	r := dockerOutcome(p.docker.state, result(running, false), nil)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, submitted.ID)
	t.Logf("independent exit before stop: exit=%d mainFinished=%v infrastructureStop=%v interrupted=%v state=%s retries=%d", p.docker.state.ExitCode, p.docker.state.MainFinished, p.docker.state.InfrastructureStop, r.Interrupted, got.State, got.InfrastructureRetries)
	if got.State != "failed" || got.InfrastructureRetries != 0 || r.Interrupted {
		t.Fatalf("independent exit must fail without retry: state=%s retries=%d error=%s", got.State, got.InfrastructureRetries, r.Error)
	}
}

func TestD5ReviewNativeIndependent143BeforeSignal(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "native-group"}[group], func(t *testing.T) {
			dir := t.TempDir()
			_, _, p := d5NativeStart(t, group, nil, []string{"sh", "-c", `touch "$1"; while [ ! -f "$2" ]; do sleep .01; done; exit 143`, "main", dir + "/ready", dir + "/finish"})
			nativeFile(t, dir, "ready")
			// Record intent only. No signal is sent; payload independently exits 143.
			p.mu.Lock()
			p.stopped = true
			p.mu.Unlock()
			nativeTouch(t, dir, "finish")
			r := d5Finish(t, p)
			t.Logf("independent exit 143: success=%v interrupted=%v error=%s", r.Success, r.Interrupted, r.Error)
			if r.Success || r.Interrupted {
				t.Fatal("independent payload exit became interruption", r)
			}
		})
	}
}

func TestDockerIndependentHelper137BeforeStopObservation(t *testing.T) {
	_, s, d, c, root := restartResultFixture(t)
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte("sidecar-killed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "running-sidecar-sortformer"), filepath.Join(root, "natural-sidecar-exit")); err != nil {
		t.Fatal(err)
	}
	d.stop(c.Now(), StopGrace, false)
	restartTick(t, d)
	c.advance(StopGrace)
	for i := 0; i < 5 && !d.state.Released; i++ {
		restartTick(t, d)
	}
	if !d.state.Released {
		t.Fatal("custody not released")
	}
	state := restartRead(t, s)
	helper := state.Containers["sidecar-sortformer"]
	if len(helper.Signals) != 0 || helper.Exit == nil || helper.Exit.Status != 137 {
		t.Fatal("independent helper evidence lost", helper)
	}
	r := dockerOutcome(state, Result{}, nil)
	if r.Success || r.Interrupted {
		t.Fatal("helper exit masked by later main delivery", r)
	}
}
