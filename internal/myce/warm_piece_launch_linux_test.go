package myce

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestWarmPieceLinuxNativeExecKeepsOnlyMainBootstrap(t *testing.T) {
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi < 6 {
		t.Skipf("Landlock ABI 6 required: abi=%d errno=%v", abi, errno)
	}
	for _, tc := range []struct {
		member, payload string
	}{
		{"main", `printf 'ready' >&5; if ( : >&6 ) 2>/dev/null; then exit 1; fi`},
		{"sidecar", `if ( : >&5 ) 2>/dev/null || ( : >&6 ) 2>/dev/null; then exit 1; fi`},
	} {
		t.Run(tc.member, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			conn, child, err := newWarmPieceChannel()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			defer child.Close()
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `read gate || exit 125; exec "$@" < /dev/null`,
				"myce-member-"+tc.member, "/bin/sh", "-ec", tc.payload)
			spool := t.TempDir()
			cmd.Dir = filepath.Join(spool, "work")
			if err := os.Mkdir(cmd.Dir, 0700); err != nil {
				t.Fatal(err)
			}
			cmd.Stdin = strings.NewReader("start\n")
			// Put descriptors in both test slots so the exec allowlist, in
			// addition to launch wiring, has to exclude sidecars and FD 6.
			cmd.ExtraFiles = []*os.File{nil, nil, child, child}
			policy, err := (Worker{}).nativeLinuxPolicy(cmd, spool, cmd.Dir, jobBaseEnvironment(), nil)
			if err != nil {
				t.Fatal(err)
			}
			wrapper, err := nativePolicyFileArgs(policy, cmd.Args[4:], cmd.Dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := nativeWrapPayload(cmd, wrapper); err != nil {
				t.Fatal(err)
			}
			cmd.Env = environmentList(jobBaseEnvironment())
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("native exec failed: %v\n%s", err, out)
			}
			if tc.member == "main" {
				if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				var got [5]byte
				if _, err := io.ReadFull(conn, got[:]); err != nil || string(got[:]) != "ready" {
					t.Fatalf("main bootstrap exchange returned %q: %v", got, err)
				}
			}
		})
	}
}
