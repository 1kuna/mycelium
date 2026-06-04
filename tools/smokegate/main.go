package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

type requireFlags []string

func (r *requireFlags) String() string {
	return strings.Join(*r, ",")
}

func (r *requireFlags) Set(value string) error {
	if value == "" {
		return fmt.Errorf("required test name is empty")
	}
	*r = append(*r, value)
	return nil
}

type gateConfig struct {
	MinPass  int
	Requires []string
}

type testStatus struct {
	Passed  bool
	Skipped bool
	Failed  bool
}

type counters struct {
	Passed  int
	Skipped int
	Failed  int
}

type testEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
}

func main() {
	if err := execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(args []string) error {
	var path string
	var minPass int
	var requires requireFlags
	fs := flag.NewFlagSet("smokegate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&path, "json", "", "go test -json output path, or - for stdin")
	fs.IntVar(&minPass, "min-pass", 0, "minimum number of passed smoke tests")
	fs.Var(&requires, "require", "test name that must pass; may repeat")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return run(path, gateConfig{MinPass: minPass, Requires: requires})
}

func run(path string, cfg gateConfig) error {
	if path == "" {
		return fmt.Errorf("-json is required")
	}
	statuses, counts, packageFailures, err := readStatuses(path)
	if err != nil {
		return err
	}
	if len(packageFailures) > 0 {
		return fmt.Errorf("smoke package failed: %s", strings.Join(packageFailures, ", "))
	}
	if len(statuses) == 0 {
		return fmt.Errorf("smoke output contains no test events")
	}
	for _, name := range cfg.Requires {
		status, ok := statuses[name]
		switch {
		case !ok:
			return fmt.Errorf("required smoke test %q did not run", name)
		case status.Failed:
			return fmt.Errorf("required smoke test %q failed", name)
		case status.Skipped:
			return fmt.Errorf("required smoke test %q skipped", name)
		case !status.Passed:
			return fmt.Errorf("required smoke test %q did not pass", name)
		}
	}
	if cfg.MinPass > 0 && counts.Passed < cfg.MinPass {
		return fmt.Errorf("only %d smoke tests passed; need at least %d", counts.Passed, cfg.MinPass)
	}
	fmt.Printf("smoke ok: %d passed, %d skipped, %d failed\n", counts.Passed, counts.Skipped, counts.Failed)
	return nil
}

func readStatuses(path string) (map[string]testStatus, counters, []string, error) {
	reader, closeFn, err := openInput(path)
	if err != nil {
		return nil, counters{}, nil, err
	}
	defer closeFn()
	statuses := map[string]testStatus{}
	var packageFailures []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event testEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, counters{}, nil, fmt.Errorf("invalid go test json: %w", err)
		}
		if event.Test == "" {
			if event.Action == "fail" {
				packageFailures = append(packageFailures, firstNonEmpty(event.Package, "(unknown package)"))
			}
			continue
		}
		status := statuses[event.Test]
		switch event.Action {
		case "pass":
			status.Passed = true
		case "skip":
			status.Skipped = true
		case "fail":
			status.Failed = true
		}
		statuses[event.Test] = status
	}
	if err := scanner.Err(); err != nil {
		return nil, counters{}, nil, err
	}
	return statuses, countStatuses(statuses), packageFailures, nil
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return file, func() { _ = file.Close() }, nil
}

func countStatuses(statuses map[string]testStatus) counters {
	counts := counters{}
	for _, status := range statuses {
		switch {
		case status.Failed:
			counts.Failed++
		case status.Skipped:
			counts.Skipped++
		case status.Passed:
			counts.Passed++
		}
	}
	return counts
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
