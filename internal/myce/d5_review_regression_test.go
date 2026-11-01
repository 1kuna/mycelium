package myce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestD5ReviewNativeIndependent143BeforeSignal(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "native-group"}[group], func(t *testing.T) {
			_, _, p := d5NativeStart(t, group, nil, []string{"sh", "-c", `touch "$1"; while [ ! -f "$2" ]; do sleep .01; done; exit 143`, "main", "{work}/ready", "{work}/finish"})
			dir := p.job.Spec.Run.Workdir
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
