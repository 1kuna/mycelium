package myce

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
)

func TestFleetRuntimeLinuxPrivateExecutable(t *testing.T) {
	const child = "MYCE_TEST_PRIVATE_INSTALL_IMAGE"
	if os.Getenv(child) == "1" {
		if err := protectServiceCredentials(); err != nil {
			t.Fatal(err)
		}
		fmt.Println("ready")
		_, _ = io.Copy(io.Discard, os.Stdin)
		return
	}
	if os.Geteuid() == 0 {
		t.Skip("installer runs under an ordinary test account")
	}
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	slot, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer slot.Close()
	ctx, cancel := context.WithTimeout(t.Context(), testWaitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "-test.run=^TestFleetRuntimeLinuxPrivateExecutable$")
	cmd.Env = []string{child + "=1"}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stdin.Close()
		if err := cmd.Wait(); err != nil {
			t.Error(err)
		}
	}()
	ready, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || ready != "ready\n" {
		t.Fatalf("protected service fixture did not report readiness: %q %v", ready, err)
	}
	if err := verifyInstallServiceImage(ctx, cmd.Process.Pid, slot, nil); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("protected service mapping should require signed slot evidence: %v", err)
	}
}
