package myce

import (
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
