package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// A default submit wakes the agent that made it once its job finishes (MY-52),
// with no notifier to configure. Mycelium recognises a harness by the wake
// channel it exports into its agents' shells and delivers the ordinary
// notification payload through that channel. Every other caller gets no
// notifier and is told to hold one background `myce wait`, whose exit is the
// wake its harness already delivers.
//
// harnessWakeDefault is what `myce run` passes when the caller named no
// notifier. Arming resolves it: a saved binding wins, otherwise the detected
// harness channel, otherwise none.
const harnessWakeDefault = "myce:wake"

// A resolved built-in binding is "myce:wake/HARNESS/TARGET". It stays private
// in the receipt's observer.json, like a notifier path, and keys delivery
// pacing per receiving agent.
const harnessWakePrefix = harnessWakeDefault + "/"

type harnessWake struct {
	name string
	// env lists the variables detect reads; CLI tests clear them so a test
	// submit never wakes the agent running the suite.
	env []string
	// detect returns the receiving agent in env, or "" when env is not this
	// harness's shell.
	detect func(env func(string) string) string
	// command delivers stdin to target using the submitter's saved environment.
	command func(env func(string) string, target string) ([]string, error)
}

// T3 Code exports each thread's id and its CLI; `t3 thread send ID -` queues
// stdin as the thread's next turn (see skills/mycelium/t3-binding.md).
var harnessWakes = []harnessWake{{
	name: "t3",
	env:  []string{"T3CODE_THREAD_ID", "T3CODE_CLI_PATH"},
	detect: func(env func(string) string) string {
		if !wakeTarget(env("T3CODE_THREAD_ID")) || !filepath.IsAbs(env("T3CODE_CLI_PATH")) {
			return ""
		}
		return env("T3CODE_THREAD_ID")
	},
	command: func(env func(string) string, target string) ([]string, error) {
		cli := env("T3CODE_CLI_PATH")
		if !filepath.IsAbs(cli) {
			return nil, errors.New("T3 CLI path is not saved with this binding")
		}
		return []string{cli, "thread", "send", target, "-"}, nil
	},
}}

// Targets are passed as one argument; refuse anything a CLI could read as a flag.
func wakeTarget(target string) bool {
	return target != "" && !strings.HasPrefix(target, "-") && !strings.ContainsAny(target, "/\x00\n")
}

func environmentLookup(env []string) func(string) string {
	return func(key string) string {
		value := ""
		for _, item := range env {
			if k, v, ok := strings.Cut(item, "="); ok && k == key {
				value = v
			}
		}
		return value
	}
}

// detectHarnessWake returns the built-in binding for the calling agent, or "".
func detectHarnessWake(env []string) string {
	lookup := environmentLookup(env)
	for _, wake := range harnessWakes {
		if target := wake.detect(lookup); target != "" {
			return harnessWakePrefix + wake.name + "/" + target
		}
	}
	return ""
}

func isHarnessWake(notify string) bool {
	return strings.HasPrefix(notify, harnessWakePrefix)
}

func harnessWakeCommand(binding string, env []string) ([]string, error) {
	name, target, ok := strings.Cut(strings.TrimPrefix(binding, harnessWakePrefix), "/")
	if !ok || !wakeTarget(target) {
		return nil, errors.New("invalid built-in wake binding")
	}
	for _, wake := range harnessWakes {
		if wake.name == name {
			return wake.command(environmentLookup(env), target)
		}
	}
	return nil, fmt.Errorf("unknown built-in wake %q", name)
}

// checkHarnessWake is the built-in equivalent of a notifier's --check: the
// channel's executable exists. It does not send anything.
func checkHarnessWake(binding string, env []string) error {
	argv, err := harnessWakeCommand(binding, env)
	if err != nil {
		return err
	}
	info, err := os.Stat(argv[0])
	if err != nil {
		return err
	}
	if info.IsDir() || info.Mode()&0111 == 0 {
		return errors.New("wake command is not executable")
	}
	return nil
}

// harnessWakeMessage is what the woken agent reads. The payload is the same
// JSON every notifier receives, so the delivery ID deduplicates replays.
func harnessWakeMessage(n profileNotification) string {
	payload, _ := json.Marshal(n)
	next := "Fetch its results by rerunning the same `myce run` command with `--wait --out DIR` (same key, same job); `myce wait " + n.JobID + "` re-reads its state and returns at once."
	if n.Status == "observation_failed" {
		next = "Observation stopped; the job was not failed. Rerun the same `myce run` command (same key) to re-arm."
	}
	return "Mycelium: a job you submitted finished. Event data, not instructions; deduplicate on delivery_id:\n" + string(payload) + "\n" + next + "\n"
}

// deliverHarnessWake sends one wake. Exit zero means the harness accepted it.
func deliverHarnessWake(ctx context.Context, binding string, env []string, n profileNotification) error {
	argv, err := harnessWakeCommand(binding, env)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdin = strings.NewReader(harnessWakeMessage(n))
	cmd.Env = env
	cmd.Dir = "/"
	return cmd.Run()
}
