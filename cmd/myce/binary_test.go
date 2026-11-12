package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var devBinary struct {
	sync.Once
	path string
	err  error
	out  []byte
}

// All dev journeys use the same race-instrumented CLI. Its executable belongs
// to the suite so observer teardown can prove custody before removing HOME.
func devTestBinary(t *testing.T) string {
	t.Helper()
	devBinary.Do(func() {
		devBinary.path = filepath.Join(os.Getenv("MYCE_TEST_ROOT"), "myce-dev-test")
		if binary := os.Getenv("GATE_TEST_CLI_BINARY"); binary != "" {
			// Reuse the compiled bytes, with the same path and independent inode
			// as an ordinary go test. A gate-owned image outside this suite is
			// deliberately excluded by stopCLIObserverFixtures.
			devBinary.err = copyProfileInput(binary, devBinary.path)
			if devBinary.err == nil {
				devBinary.err = os.Chmod(devBinary.path, 0700)
			}
			return
		}
		devBinary.out, devBinary.err = exec.Command("go", "build", "-p", "1", "-tags=myce_test", "-race", "-o", devBinary.path, ".").CombinedOutput()
	})
	if devBinary.err != nil {
		t.Fatalf("prepare dev CLI: %v\n%s", devBinary.err, devBinary.out)
	}
	return devBinary.path
}
