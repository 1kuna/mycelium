package myce

import (
	"os"
	"path/filepath"
	"testing"
)

// A fast first CLI invocation must not make an outcome/recovery fixture depend
// on subsequent process scheduling. Reuse the healthy-helper latency from
// TestDockerHealthyHelperStartupBudget, but introduce it after the first create
// has taught the adapter its local initialization envelope.
func TestDockerRestartFixtureSurvivesCLIStartupVariation(t *testing.T) {
	binary := dockerCLITestBinary(t)
	wrapper := filepath.Join(t.TempDir(), "docker-cli")
	script := "#!/bin/sh\nfor arg do\n" +
		"  if [ \"$arg\" = create ] && [ -f \"$MYCE_TEST_DOCKER_ROOT/object-sidecar-sortformer.json\" ]; then sleep 0.3; fi\n" +
		"done\nexec " + quoteShell(binary) + " \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GATE_TEST_DOCKER_BINARY", wrapper)
	w, store, d, clock, root := restartResultFixture(t)
	if d.state.Container == "" || !d.state.Containers[d.state.Container].Started || d.state.Pending {
		t.Fatalf("fixture did not freeze a durably started main: %+v", d.state)
	}
	restartMainExit(t, d, root)
	clock.advance(StopGrace)
	if r := restartSavedResult(t, w, store); !r.Success || r.Interrupted {
		t.Fatalf("CLI startup variation changed recovered completion: %+v", r)
	}
}
