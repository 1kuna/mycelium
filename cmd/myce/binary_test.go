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

// All dev journeys use the same race-instrumented CLI. Its lifetime is the
// package fixture's, rather than the first test's temporary directory.
func devTestBinary(t *testing.T) string {
	t.Helper()
	devBinary.Do(func() {
		devBinary.path = filepath.Join(os.Getenv("MYCE_TEST_DOCKER_BIN"), "myce-dev-test")
		devBinary.out, devBinary.err = exec.Command("go", "build", "-tags=myce_test", "-race", "-o", devBinary.path, ".").CombinedOutput()
	})
	if devBinary.err != nil {
		t.Fatalf("build dev CLI: %v\n%s", devBinary.err, devBinary.out)
	}
	return devBinary.path
}
