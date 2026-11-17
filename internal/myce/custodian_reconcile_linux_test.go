package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCustodianStartupScopeAbsence(t *testing.T) {
	valid := "LoadState=not-found\nActiveState=inactive\nSubState=dead\nJob=\nControlGroup=\n"
	if err := absentCustodianScope(valid); err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{"", strings.Replace(valid, "not-found", "loaded", 1), strings.Replace(valid, "Job=", "Job=123", 1), strings.Replace(valid, "ControlGroup=", "ControlGroup=/scope", 1), strings.Replace(valid, "SubState=dead\n", "", 1), valid + "Job=\n"} {
		if err := absentCustodianScope(output); err == nil {
			t.Fatal("uncertain manager state accepted", output)
		}
	}
}

func TestCustodianStartupFence(t *testing.T) {
	bin := t.TempDir()
	manager := filepath.Join(bin, "systemctl")
	if err := os.WriteFile(manager, []byte("#!/bin/sh\nprintf 'LoadState=not-found\\nActiveState=inactive\\nSubState=dead\\nJob=\\nControlGroup=\\n'\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "lsof"), []byte("#!/bin/sh\nprintf 'p1\\nn/unrelated\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	dir, id := t.TempDir(), ID()
	if err := fenceCustodianStartup(t.Context(), dir, id); err != nil {
		t.Fatal(err)
	}
	// A helper can be alive before acquiring its owner lock. This must hold even
	// when the manager no longer knows its unit and no snapshot exists.
	cmd := exec.Command("/bin/sh", "-c", "read line", id)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { stdin.Close(); cmd.Wait() }()
	if err := fenceCustodianStartup(t.Context(), dir, id); err == nil {
		t.Fatal("live pre-entry helper released")
	}
	if err := os.WriteFile(manager, []byte("#!/bin/sh\necho inspector-failed >&2\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := fenceCustodianStartup(t.Context(), dir, ID()); err == nil {
		t.Fatal("inspection error accepted")
	}
}
