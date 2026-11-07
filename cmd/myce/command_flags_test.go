package main

import (
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestCommandFlagsHelp(t *testing.T) {
	for _, tc := range []struct {
		command, own, unrelated string
	}{
		{"queue", "listen db discovery", "queue name now workers since"},
		{"join", "name daily-use heartbeat native-read-root preemption-stop-grace production-stop-grace", "listen db now workers since"},
		{"dev", "listen workers fake-gpus", "queue name now daily-use since"},
		{"cancel", "project key config", "listen name workers since now"},
		{"leave", "name now", "listen workers since project"},
		{"submit", "files-from config", "listen name now workers since"},
		{"usage", "since by", "listen name now workers full"},
		{"status", "config full json history", "listen name now workers since"},
		{"logs", "config follow", "listen name now workers since"},
		{"ledger", "config after high-water limit", "listen name now workers since"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			_, help, err := captureUsageOutput(t, []string{tc.command, "--help"})
			if !errors.Is(err, flag.ErrHelp) || !strings.Contains(help, "Usage of myce "+tc.command) {
				t.Fatalf("help: %v\n%s", err, help)
			}
			for _, name := range strings.Fields(tc.own + " token joining-file") {
				if !strings.Contains(help, "  -"+name+" ") && !strings.Contains(help, "  -"+name+"\n") {
					t.Errorf("missing own flag -%s: %s", name, help)
				}
			}
			for _, name := range strings.Fields(tc.unrelated) {
				if strings.Contains(help, "  -"+name+" ") || strings.Contains(help, "  -"+name+"\n") {
					t.Errorf("unrelated flag -%s: %s", name, help)
				}
			}
		})
	}
}

func TestCommandFlagsLegacyCompatibility(t *testing.T) {
	catalog := flag.NewFlagSet("catalog", flag.ContinueOnError)
	token := catalog.String("token", "", "fixture authority")
	catalog.String("queue", "", "fixture address")
	catalog.String("joining-file", "", "fixture invitation")
	catalog.String("config", "", "fixture config")
	catalog.String("project", "", "fixture project")
	catalog.String("key", "", "fixture key")
	workers := catalog.Int("workers", 2, "legacy unrelated flag")
	var inputs []string
	catalog.Func("input", "legacy repeated flag", func(value string) error {
		inputs = append(inputs, value)
		return nil
	})
	own := commandFlagSet("cancel", catalog)
	parsed, err := parseCommandFlags(own, catalog, []string{"--token", "fixture", "--workers", "3", "--input", "a", "--input", "b", "job"})
	if err != nil || *token != "fixture" || *workers != 3 || strings.Join(inputs, ",") != "a,b" || parsed.Arg(0) != "job" {
		t.Fatalf("legacy parsing changed: %v, workers=%d inputs=%v args=%v", err, *workers, inputs, parsed.Args())
	}
	visited := map[string]bool{}
	parsed.Visit(func(f *flag.Flag) { visited[f.Name] = true })
	if !visited["token"] || !visited["workers"] || !visited["input"] {
		t.Fatalf("explicit flag tracking lost: %v", visited)
	}
}

func TestCommandFlagsSetupHelp(t *testing.T) {
	for _, command := range []string{"doctor", "uninstall", "install"} {
		_, help, err := captureUsageOutput(t, []string{command, "--help"})
		if !errors.Is(err, flag.ErrHelp) || !strings.Contains(help, "Usage of ") {
			t.Fatalf("%s help: %v %s", command, err, help)
		}
		if strings.Contains(help, "  -heartbeat ") || strings.Contains(help, "  -model-cache ") {
			t.Fatalf("%s includes unrelated worker flags: %s", command, help)
		}
		if command != "install" && (!strings.Contains(help, "  -config ") || !strings.Contains(help, "  -root ")) {
			t.Fatalf("%s omits installation selector: %s", command, help)
		}
		if command == "uninstall" && !strings.Contains(help, "  -dry-run\n") {
			t.Fatalf("uninstall omits preview: %s", help)
		}
	}
}
