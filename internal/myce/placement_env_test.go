package myce

import (
	"testing"
)

func TestWorkerPlacementEnvironmentEveryAttempt(t *testing.T) {
	// cuda shares xpu's probed-device path and metal shares fake's (#1086).
	for _, platform := range []string{"fake", "xpu"} {
		t.Run(platform, func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = platform
			w.Machine.Device = "0"
			if platform == "cuda" || platform == "xpu" {
				w.nativeGPUDeviceProbe = syntheticNativeGPUDeviceProbe(MachineDevice{ID: "0", Platform: platform, Selector: "0"})
			}
			store, e := w.openStore()
			if e != nil {
				t.Fatal(e)
			}
			defer store.close()
			want := platform
			if want == "fake" {
				want = "cpu-fake"
			}
			for attempt := 1; attempt <= 2; attempt++ {
				w.Machine.ID = "worker-" + platform
				j := recoveryJob(t, []string{"sh", "-c", `test "$MYCE_PLATFORM" = "$EXPECTED_PLATFORM" && test "$MYCE_DEVICES" = 0 && test "$MYCE_MACHINE" = "$EXPECTED_MACHINE" && test -z "$MYCE_TOKEN"`})
				j.Attempt = attempt
				j.Spec.Run.Env = map[string]string{"MYCE_PLATFORM": "wrong", "MYCE_MACHINE": "wrong", "MYCE_DEVICES": "wrong", "EXPECTED_PLATFORM": want, "EXPECTED_MACHINE": w.Machine.ID}
				w = workerWithPreparedCheckpoint(t, w, store, j)
				p, e := startAfterFixtureInputs(t, w, store, j)
				if e != nil {
					t.Fatal(e)
				}
				done := <-p.done
				if done.err != nil || !done.result.Success {
					t.Fatalf("placement env attempt %d: success=%t result error=%q save error=%v", attempt, done.result.Success, done.result.Error, done.err)
				}
			}
		})
	}
}
