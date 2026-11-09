package myce

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestRereviewHealthyNASPerOperationBudget(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probe: func(path string) error {
		time.Sleep(120 * time.Millisecond)
		return probeNASDirectory(path)
	}}
	start := time.Now()
	got, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
	if err != nil || !got.RequireMountedNAS {
		t.Fatalf("healthy NAS refused after %s: %v", time.Since(start), err)
	}
	if time.Since(start) < 350*time.Millisecond {
		t.Fatal("fixture did not exceed old total budget")
	}
}

func exitNine(t *testing.T) *os.ProcessState {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", "exit 9")
	_ = cmd.Run()
	return cmd.ProcessState
}
