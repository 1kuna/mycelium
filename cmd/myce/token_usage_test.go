package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandUsageDoesNotExposeSecrets(t *testing.T) {
	const token = "test-secret-123"
	const invitationToken = "test-invitation-secret-456"
	t.Setenv("MYCE_TOKEN", token)
	t.Setenv("MYCE_QUEUE", "http://127.0.0.1:1")
	invitation := filepath.Join(t.TempDir(), "joining.json")
	if err := os.WriteFile(invitation, []byte(`{"version":1,"fingerprint":"`+strings.Repeat("0", 64)+`","token":"`+invitationToken+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_JOINING_FILE", invitation)
	// Include every command and nested action, including commands sharing a FlagSet.
	commands := []string{
		"queue", "join", "leave", "submit", "usage", "status", "logs", "cancel", "dev",
		"disable", "enable", "resolve-docker", "detect", "service", "wait", "measure",
		"invite", "benchmark", "sim", "install worker", "install queue", "uninstall", "doctor",
		"file put", "file get", "model fetch", "model approve-local", "model sync", "model pin", "model unpin",
		"extension add", "extension list", "extension setup", "extension check",
		"host drain", "host disable", "host resume", "host status", "host budget",
	}
	for _, command := range commands {
		for _, option := range []string{"-h", "--help", "--unknown-flag"} {
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
