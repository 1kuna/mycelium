package myce

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func queueOverrideCommand(c ServiceConfig, args []string) string {
	words := append([]string{c.Binary}, args...)
	var quoted []string
	for _, word := range words {
		quoted = append(quoted, unitQuote(word))
	}
	return ":" + strings.Join(quoted, " ")
}

func TestInstallQueueExecStartOverride(t *testing.T) {
	for _, tc := range []string{"no override", "equivalent", "hidden catalog and timing", "direct discovery", "discovery mismatch", "defaults and durations", "wrapper saved arguments", "wrapper explicit arguments", "unreset command", "empty reset", "multiple commands", "malformed", "environment expansion", "specifier", "unknown flag", "different binary"} {
		t.Run(tc, func(t *testing.T) {
			f := newInstallMatrixFixture(t, "linux", "queue", 3)
			c := f.c
			c.ExperimentLossTimeout = time.Minute
			c.DiscoveryListen = "127.0.0.1:7443"
			args := c.Args()
			var want []string
			switch tc {
			case "hidden catalog and timing":
				c.ModelCatalog = "/fixture/catalog.yaml"
				c.ProductionLossTimeout = 5 * time.Second
				c.DeadlineLossTimeout = 20 * time.Second
				args = append(args, "--production-loss-timeout", "60s", "--deadline-loss-timeout", "60s", "--experiment-loss-timeout", "60s")
				want = []string{"--deadline-loss-timeout", "--model-config", "--production-loss-timeout"}
			case "direct discovery", "discovery mismatch":
				var direct []string
				for i := 0; i < len(args); i++ {
					if args[i] == "--service-config" {
						i++
						continue
					}
					direct = append(direct, args[i])
				}
				args = direct
				bind := c.DiscoveryListen
				if tc == "discovery mismatch" {
					bind = "127.0.0.1:7444"
					want = []string{"--discovery-listen"}
				}
				args = append(args, "--discovery-listen", bind)
			case "defaults and durations":
				var compact []string
				for i := 0; i < len(args); i++ {
					if args[i] == "--file-retention" || args[i] == "--file-store-production-reserve" {
						i++
						continue
					}
					compact = append(compact, args[i])
				}
				args = append(compact, "--experiment-loss-timeout=60s")
			case "wrapper saved arguments":
				args = []string{"service", "--config", c.Config}
			case "wrapper explicit arguments":
				args = append([]string{"service", "--config", c.Config, "--"}, args...)
			case "unknown flag":
				args = append(args, "--debug-listen", "127.0.0.1:0")
				want = []string{"--debug-listen"}
			}
			start := queueOverrideCommand(f.c, args)
			body := "[Service]\nExecStart=\nExecStart=" + start + "\n"
			switch tc {
			case "no override":
				body = "[Service]\nCPUQuota=45%\n# ExecStart=/ignored\n[Unit]\nDescription=fixture\n"
			case "unreset command":
				body = "[Service]\nExecStart=" + start + "\n"
				want = []string{"ExecStart"}
			case "empty reset":
				body = "[Service]\nExecStart=\n"
				want = []string{"ExecStart"}
			case "multiple commands":
				body += "ExecStart=" + start + "\n"
				want = []string{"ExecStart"}
			case "malformed":
				body = "[Service]\nExecStart=\nExecStart=\"unterminated\n"
				want = []string{"ExecStart"}
			case "environment expansion":
				body = "[Service]\nExecStart=\nExecStart=" + strings.TrimPrefix(start, ":") + " --model-config $CATALOG\n"
				want = []string{"ExecStart"}
			case "specifier":
				body += "ExecStart=\nExecStart=" + start + " --model-config %h/catalog\n"
				want = []string{"ExecStart"}
			case "different binary":
				body = strings.Replace(body, unitQuote(c.Binary), unitQuote("/fixture/other"), 1)
				want = []string{"executable"}
			}
			path := filepath.Join(t.TempDir(), "override.conf")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			activated := false
			command := func(ctx context.Context, name string, argv ...string) ([]byte, error) {
				call := strings.Join(argv, " ")
				if strings.Contains(call, "--property=DropInPaths") {
					return []byte(path), nil
				}
				if len(argv) > 1 {
					switch argv[1] {
					case "enable", "restart", "stop", "daemon-reload":
						activated = true
					}
				}
				return f.manager.run(ctx, name, argv...)
			}
			err := Install(context.Background(), c, f.source, "synthetic-fixture", command)
			if len(want) == 0 {
				if err != nil {
					t.Fatal(err)
				}
				if !activated {
					t.Fatal("equivalent/no override installation did not activate")
				}
			} else {
				var refusal *InstallExecStartOverrideError
				if !errors.Is(err, ErrQueueExecStartOverride) || !errors.As(err, &refusal) {
					t.Fatalf("expected typed refusal: %v", err)
				}
				if !reflect.DeepEqual(refusal.Flags, want) {
					t.Fatalf("flags: got %v want %v", refusal.Flags, want)
				}
				if strings.Contains(err.Error(), "/fixture/") || strings.Contains(err.Error(), c.Config) {
					t.Fatal("refusal exposed argument values")
				}
				if activated {
					t.Fatal("refusal changed manager state")
				}
				f.assertRestored(t)
			}
			got, err := os.ReadFile(path)
			if err != nil || string(got) != body {
				t.Fatal("operator override changed")
			}
		})
	}
}

func TestInstallQueueSaveSettingsOnly(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepare and retire", true: "publication rollback"}[fail], func(t *testing.T) {
			f := newInstallMatrixFixture(t, "linux", "queue", 3)
			c := f.c
			values := ServiceConfig{}
			flags := flag.NewFlagSet("install", flag.ContinueOnError)
			RegisterInstallFlags(flags, &values)
			if err := flags.Parse([]string{"--save-settings-only", "--file-store-bytes=214748364800", "--production-loss-timeout=60s"}); err != nil {
				t.Fatal(err)
			}
			var err error
			c, err = OverlayInstallFlags(flags, c, values)
			if err != nil {
				t.Fatal(err)
			}
			override := filepath.Join(t.TempDir(), "override.conf")
			body := "[Service]\nExecStart=\nExecStart=" + queueOverrideCommand(f.c, f.c.Args()) + "\n"
			if err := os.WriteFile(override, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			mutating := false
			retired := false
			command := func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if strings.Contains(strings.Join(args, " "), "--property=DropInPaths") {
					if retired {
						return nil, nil
					}
					return []byte(override), nil
				}
				if len(args) > 1 {
					switch args[1] {
					case "daemon-reload", "enable", "restart", "stop", "disable":
						mutating = true
					}
				}
				return f.manager.run(ctx, name, args...)
			}
			files := installFiles{}
			if fail {
				files.before = func(op, path string) error {
					if op == "rename" && path == c.Service {
						return errors.New("fixture publication failure")
					}
					return nil
				}
			}
			err = installWithFiles(context.Background(), c, "/fixture/unread-binary", "", command, files)
			if mutating {
				t.Fatal("settings-only changed manager state")
			}
			if fail {
				if err == nil {
					t.Fatal("expected failure")
				}
				f.assertRestored(t)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			saved, err := ReadServiceConfig(c.Config)
			if err != nil || saved.SaveSettingsOnly || saved.FileStoreBytes != c.FileStoreBytes || saved.ProductionLossTimeout != time.Minute {
				t.Fatalf("saved settings/ephemeral mode incorrect: %v", err)
			}
			for _, path := range []string{c.Binary, c.Credentials} {
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, f.files[path]) {
					t.Fatal("settings-only replaced binary or credentials")
				}
			}
			got, err := os.ReadFile(c.Service)
			if err != nil || string(got) != c.ServiceText() {
				t.Fatal("generated unit not prepared")
			}
			got, err = os.ReadFile(override)
			if err != nil || string(got) != body {
				t.Fatal("override not preserved")
			}
			if err := Install(context.Background(), saved, f.source, "", command); !errors.Is(err, ErrQueueExecStartOverride) {
				t.Fatalf("normal install must still refuse until retirement: %v", err)
			}
			if mutating {
				t.Fatal("refusing normal install activated saved settings")
			}
			retired = true // Operator removes only the override, then performs the normal install.
			if err := Install(context.Background(), saved, f.source, "", command); err != nil {
				t.Fatal(err)
			}
			if !mutating {
				t.Fatal("normal install after retirement did not activate")
			}
		})
	}
}
