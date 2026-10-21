package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCLITestMainNoHelperTempLeak(t *testing.T) {
	tmp := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestQueueBindStopIsSuccessful$")
	child.Env = append(os.Environ(), "TMPDIR="+tmp)
	if b, err := child.CombinedOutput(); err != nil {
		t.Fatalf("isolated suite: %v %s", err, b)
	}
	dirs, err := filepath.Glob(filepath.Join(tmp, "myce-cli-docker-tests-*"))
	if err != nil || len(dirs) != 0 {
		t.Fatalf("isolated suite leaked %d CLI shim roots: %v", len(dirs), err)
	}
}
