package testenv

import (
	"os/exec"
	"testing"
)

func TestCheckProcessesRejectsRunningChild(t *testing.T) {
	root := t.TempDir()
	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	})
	if err := CheckProcesses(root); err == nil {
		t.Fatal("harness accepted a running test child")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if err := CheckProcesses(root); err != nil {
		t.Fatal("joined child still reported", err)
	}
}
