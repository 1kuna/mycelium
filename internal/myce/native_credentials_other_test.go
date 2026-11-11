//go:build !darwin && !linux

package myce

import (
	"os/exec"
	"testing"
)

func TestNativeCredentialsUnsupportedPlatformLeavesCommand(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "true")
	before := append([]string(nil), cmd.Args...)
	if err := (Worker{}).withholdNativeCredentials(cmd, "", "", nil); err != nil || len(cmd.Args) != len(before) {
		t.Fatalf("args %q err %v", cmd.Args, err)
	}
	if err := protectServiceCredentials(); err != nil {
		t.Fatal(err)
	}
}
