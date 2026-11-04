package myce

import (
	"os/exec"
	"testing"
)

func TestModelSnapshotSeatbeltReadOnlyViews(t *testing.T) {
	modelSnapshotViews(t, "seatbelt", func(t *testing.T, w Worker, cmd *exec.Cmd, spool, work string, env map[string]string) {
		if err := w.isolateNativeCommand(cmd, spool, work, env, nil, false); err != nil {
			t.Fatal(err)
		}
	})
}
