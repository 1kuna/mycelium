package myce

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestProcessBirthGoneIsNotExist(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if _, err := processBirth(cmd.Process.Pid); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("gone process: %v", err)
	}
}
