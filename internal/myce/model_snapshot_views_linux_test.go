package myce

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

func TestModelSnapshotLinuxReadOnlyViews(t *testing.T) {
	for _, mode := range []string{"landlock", "namespace"} {
		t.Run(mode, func(t *testing.T) {
			abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
			if errno != 0 || abi < 6 {
				t.Skip("Landlock ABI 6 unavailable")
			}
			support := nativeLinuxDetection()
			if mode == "namespace" && support.mode != "namespace" {
				t.Skip("namespace confinement unavailable: " + support.reason)
			}
			modelSnapshotViews(t, mode, func(t *testing.T, w Worker, cmd *exec.Cmd, spool, work string, env map[string]string) {
				policy, err := w.nativeLinuxPolicy(cmd, spool, work, env, nil)
				if err != nil {
					t.Fatal(err)
				}
				args, err := nativePolicyArgs(policy, cmd.Args)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "namespace" {
					args = append(append(append([]string{support.bwrap}, nativeBwrapPolicyArgs(policy)...), "--"), args...)
				}
				cmd.Path = args[0]
				cmd.Args = args
				cmd.Env = environmentList(jobBaseEnvironment())
			})
		})
	}
}
