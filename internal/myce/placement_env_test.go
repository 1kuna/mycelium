package myce

import (
	"strings"
	"testing"
)

func TestWorkerPlacementEnvironmentEveryAttempt(t *testing.T) {
	for _, platform := range []string{"fake", "cuda", "xpu", "metal"} {
		t.Run(platform, func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = platform
			w.Machine.Device = "0"
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
				p, e := w.start(store, j)
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
func TestModelPlacementEnvironment(t *testing.T) {
	m := Model{DeviceEnv: map[string]string{"MYCE_MACHINE": "wrong"}}
	m = withPlacementEnvironment(m, Machine{ID: "b70-1", GPU: "xpu", Device: "1"})
	_, _, _, env, e := serverCommand(Model{Name: "env", Engine: "server", Path: "unused", StartCommand: []string{"echo"}, HealthURL: "http://127.0.0.1:{port}", DeviceEnv: m.DeviceEnv}, 7332)
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"MYCE_PLATFORM=xpu", "MYCE_DEVICES=1", "MYCE_MACHINE=b70-1"} {
		if !strings.Contains(strings.Join(env, "\n"), want) {
			t.Fatalf("missing %s", want)
		}
	}
}
