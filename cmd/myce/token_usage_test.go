package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandUsageDoesNotExposeSecrets(t *testing.T) {
	if os.Getenv("MYCE_USAGE_CHILD") == "1" {
		os.Args = append([]string{"myce"}, os.Args[3:]...)
		main()
		os.Exit(0)
	}

	const token = "test-secret-123"
	const invitationToken = "test-invitation-secret-456"
	t.Setenv("MYCE_TOKEN", token)
	t.Setenv("MYCE_QUEUE", "http://127.0.0.1:1")
	invitation := filepath.Join(t.TempDir(), "joining.json")
	if err := os.WriteFile(invitation, []byte(`{"version":1,"fingerprint":"`+strings.Repeat("0", 64)+`","token":"`+invitationToken+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_JOINING_FILE", invitation)
	for _, tc := range []struct{ command, option string }{{"queue", "--help"}, {"wait", "--unknown-flag"}} {
		command, option := tc.command, tc.option
		t.Run(command+"/"+option, func(t *testing.T) {
			args := append(strings.Fields(command), option)
			stdout, stderr, err := captureUsageOutput(t, args)
			waitHelp := command == "wait" && option != "--unknown-flag"
			retired := command == "disable" || command == "enable"
			if err == nil && !waitHelp {
				t.Fatal("expected help or flag error")
			}
			if option != "--unknown-flag" && !waitHelp && !retired && !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("expected ErrHelp, got %T", err)
			}
			for _, output := range []string{stdout, stderr} {
				if strings.Contains(output, token) || strings.Contains(output, invitationToken) {
					t.Fatal("credential exposed in command output")
				}
			}
			if retired {
				if !strings.Contains(stdout+stderr, "retired host control; use myce host drain|disable|resume HOST") {
					t.Fatal("missing retired-command diagnostic")
				}
			} else if !strings.Contains(stdout+stderr, "Usage of ") && !strings.Contains(stdout+stderr, "Usage: myce wait ") {
				t.Fatal("command did not render flag usage")
			}
		})
	}

	home := t.TempDir()
	runCLI := func(arg string) (string, int) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestCommandUsageDoesNotExposeSecrets$", "--", arg)
		cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"), "MYCE_USAGE_CHILD=1"}
		out, err := cmd.CombinedOutput()
		if err == nil {
			return string(out), 0
		}
		if e, ok := err.(*exec.ExitError); ok {
			return string(out), e.ExitCode()
		}
		t.Fatal(err)
		return "", -1
	}
	tc := struct {
		arg  string
		code int
	}{"--help", 0}
	out, code := runCLI(tc.arg)
	if code != tc.code || !strings.Contains(out, "usage: myce ") || strings.Contains(out, "MYCE_TOKEN") {
		t.Errorf("%s: exit %d, output %q", tc.arg, code, out)
	}
}

// Temp files avoid blocking on a pipe when a command prints a large FlagSet.
func captureUsageOutput(t *testing.T, args []string) (string, string, error) {
	t.Helper()
	var files [2]*os.File
	for i := range files {
		f, err := os.CreateTemp(t.TempDir(), "output")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		files[i] = f
	}
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = files[0], files[1]
	defer func() { os.Stdout, os.Stderr = stdout, stderr }()
	err := run(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	var output [2]string
	for i, f := range files {
		data, readErr := os.ReadFile(f.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		output[i] = string(data)
	}
	return output[0], output[1], err
}
