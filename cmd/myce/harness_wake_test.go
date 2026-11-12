package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHarnessWakeDetection(t *testing.T) {
	cli := filepath.Join(t.TempDir(), "t3")
	if err := os.WriteFile(cli, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		env  []string
		want string
	}{
		{"harness shell", []string{"T3CODE_THREAD_ID=thread-1", "T3CODE_CLI_PATH=" + cli}, "myce:wake/t3/thread-1"},
		{"no harness", []string{"HOME=/h"}, ""},
		{"relative CLI", []string{"T3CODE_THREAD_ID=thread-1", "T3CODE_CLI_PATH=t3"}, ""},
		{"flag-like target", []string{"T3CODE_THREAD_ID=--wait", "T3CODE_CLI_PATH=" + cli}, ""},
	} {
		got := detectHarnessWake(tc.env)
		if got != tc.want {
			t.Fatalf("%s: got %q want %q", tc.name, got, tc.want)
		}
		if got != "" {
			if err := checkHarnessWake(got, tc.env); err != nil {
				t.Fatalf("%s: check: %v", tc.name, err)
			}
			if err := checkHarnessWake(got, []string{"HOME=/h"}); err == nil {
				t.Fatalf("%s: binding without its saved channel passed check", tc.name)
			}
		}
	}
}

// MY-52/#1504: a default submit from an agent's shell wakes that agent once
// when its job finishes, with no notifier setup; a shell with no wake channel
// is told to hold one background `myce wait`. Laptop-only: `myce dev`.
func TestDevSubmitWakesSubmitterOnce(t *testing.T) {
	binary := devTestBinary(t)
	source, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	for _, name := range []string{".mycelium/profiles/quickstart.yaml", "examples/quickstart-input/hello.txt"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(filepath.Dir(filepath.Join(project, name)), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(project, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The harness's CLI stands in for `t3 thread send THREAD -`.
	wakes := filepath.Join(project, "wakes")
	cli := filepath.Join(project, "harness-cli")
	if err := os.WriteFile(cli, []byte("#!/bin/sh\nprintf 'argv:%s %s %s %s\\n' \"$1\" \"$2\" \"$3\" \"$4\" >> "+shellArg(wakes)+"\ncat >> "+shellArg(wakes)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("T3CODE_THREAD_ID", "thread-under-test")
	t.Setenv("T3CODE_CLI_PATH", cli)
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	script := `set -eu
cd "$1"
profile=.mycelium/profiles/quickstart.yaml
"$2" run --profile "$profile" --key wake-fixture > receipt.json
until grep -rqs '"delivery_status": *"delivered"' .mycelium/receipts; do sleep 0.1; done
"$2" run --profile "$profile" --key wake-fixture > replay.json
env -u T3CODE_THREAD_ID "$2" run --profile "$profile" --key unbound-fixture > unbound.json 2> unbound.err`
	var output bytes.Buffer
	if err := devCommandSession(ctx, []string{"sh", "-c", script, "fixture", project, binary}, nil, &output, &output); err != nil {
		t.Fatalf("dev session: %v\n%s", err, output.String())
	}
	var receipt, replay, unbound profileReceipt
	for path, r := range map[string]*profileReceipt{"receipt.json": &receipt, "replay.json": &replay, "unbound.json": &unbound} {
		if err := readProfileJSON(filepath.Join(project, path), r); err != nil {
			t.Fatal(path, err)
		}
	}
	if receipt.Delivery != "armed" {
		t.Fatalf("default submit did not arm a wake: %+v", receipt)
	}
	data, err := os.ReadFile(wakes)
	if err != nil {
		t.Fatal(err)
	}
	wake := string(data)
	if strings.Count(wake, "argv:") != 1 || strings.Count(wake, `"delivery_id":`) != 1 {
		t.Fatalf("want exactly one wake:\n%s", wake)
	}
	for _, want := range []string{"argv:thread send thread-under-test -", receipt.JobID, `"status":"succeeded"`, "--wait --out DIR"} {
		if !strings.Contains(wake, want) {
			t.Fatalf("wake lacks %q:\n%s", want, wake)
		}
	}
	if replay.JobID != receipt.JobID || replay.DeliveryStatus != "delivered" {
		t.Fatalf("same-key replay changed the job or its delivery: %+v", replay)
	}
	if unbound.Delivery != "not_configured" {
		t.Fatalf("shell without a wake channel armed one: %+v", unbound)
	}
	hint, err := os.ReadFile(filepath.Join(project, "unbound.err"))
	if err != nil || !strings.Contains(string(hint), "run `myce wait "+unbound.JobID+"` once as a background task") {
		t.Fatalf("no single-wait guidance: %v\n%s", err, hint)
	}
}
