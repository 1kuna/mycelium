package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDockerTestMainNoHelperTempLeak(t *testing.T) {
	if os.Getenv("MYCE_TEMP_LEAK_HELPER") == "1" {
		os.Exit(0)
	}
	tmp := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestDockerTestMainNoHelperTempLeak$")
	child.Env = append(os.Environ(), "TMPDIR="+tmp, "MYCE_TEMP_LEAK_HELPER=1")
	if b, err := child.CombinedOutput(); err != nil {
		t.Fatalf("helper: %v %s", err, b)
	}
	dirs, err := filepath.Glob(filepath.Join(tmp, "myce-docker-tests-*"))
	if err != nil || len(dirs) != 0 {
		t.Fatalf("helper leaked %d fake Docker dirs: %v", len(dirs), err)
	}
}
