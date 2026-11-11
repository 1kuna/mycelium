package myce

import (
	"os"
	"os/exec"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

// #1309: an EnvironmentFile service's token is in its initial environment;
// a same-user job must not read it through /proc.
func TestServiceCredentialsHiddenFromProcEnviron(t *testing.T) {
	if err := protectServiceCredentials(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := unix.Prctl(unix.PR_SET_DUMPABLE, 1, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}()
	out, err := exec.Command("cat", "/proc/"+strconv.Itoa(os.Getpid())+"/environ").CombinedOutput()
	if err == nil {
		t.Fatalf("job read the service environment (%d bytes)", len(out))
	}
}
