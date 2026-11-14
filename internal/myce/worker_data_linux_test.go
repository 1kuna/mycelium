package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func requireEphemeralTestBoundary(t *testing.T) {
	t.Helper()
	if err := ephemeralSupport(); err != nil {
		t.Skip("real ephemeral execution requires a qualified Linux boundary:", err)
	}
}

func TestEphemeralCoreRoutingAndActualHelperLimits(t *testing.T) {
	for _, pattern := range []string{"", "|/usr/lib/systemd/systemd-coredump", "  |handler"} {
		if ephemeralCorePattern([]byte(pattern), nil) == nil {
			t.Fatal("unsupported core route accepted", pattern)
		}
	}
	if ephemeralCorePattern(nil, os.ErrPermission) == nil {
		t.Fatal("unknown core route accepted")
	}
	if ephemeralCorePattern([]byte("core.%p\n"), nil) != nil {
		t.Fatal("regular core route rejected")
	}
	if err := ephemeralCoreRoute(); err != nil {
		t.Skip("this host has unsupported core routing:", err)
	}
	args, err := nativePolicyArgs(nativeLinuxPolicy{Ephemeral: true, Read: []string{"/usr", "/bin", "/lib", "/lib64"}}, []string{"/bin/sh", "-c", `printf '%s %s' "$(ulimit -c)" "$(ulimit -H -c)"`})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("core helper timed out")
		}
		t.Fatal(err, string(output))
	}
	if strings.TrimSpace(string(output)) != "0 0" {
		t.Fatal("helper did not suppress soft and hard core limits", string(output))
	}
}
