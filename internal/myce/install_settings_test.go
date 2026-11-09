package myce

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func installTestField(c *ServiceConfig, path string) reflect.Value {
	v := reflect.ValueOf(c).Elem()
	for _, part := range strings.Split(path, ".") {
		v = v.FieldByName(part)
	}
	return v
}
func installTestFlagValue(s InstallSetting, zero bool) string {
	values := ServiceConfig{}
	f := flag.NewFlagSet("fixture", flag.ContinueOnError)
	s.bind(f, &values)
	v := f.Lookup(s.Flag).Value.(flag.Getter).Get()
	switch v.(type) {
	case []string:
		if zero {
			return ""
		}
		return "/fixture/changed"
	case bool:
		if zero {
			return "false"
		}
		return "true"
	case int:
		if zero {
			return "0"
		}
		return "7"
	case int64:
		if zero {
			return "0"
		}
		return "34567"
	case time.Duration:
		if zero {
			return "0s"
		}
		return "19s"
	case float64:
		if zero {
			return "0"
		}
		return "7"
	case string:
		if zero {
			return ""
		}
		if s.normalize != nil {
			return "/fixture/changed"
		}
		switch s.Flag {
		case "discovery-listen":
			return "127.0.0.1:7443"
		case "queue":
			return "https://changed.invalid"
		case "gpu":
			return "cuda"
		case "memory-mode":
			return "auto"
		case "cpu-quota":
			return "31%"
		case "memory-max":
			return "800M"
		case "tasks-max":
			return "77"
		default:
			return "changed-fixture"
		}
	default:
		panic("missing automatic flag test type")
	}
}

func TestInstallSettingsOverlayMatrix(t *testing.T) {
	// #95: explicitly clearing the credential reference must fail before upgrade writes.
	for _, withToken := range []bool{false, true} {
		name := "environment-file/explicit-empty-without-token"
		if withToken {
			name = "environment-file/explicit-empty-with-token"
		}
		t.Run(name, func(t *testing.T) {
			base, source := preserveWorkerFixture(t)
			if err := Install(context.Background(), base, source, "fixture", fakeInstallManager); err != nil {
				t.Fatal(err)
			}
			// The saved reference may point to an absent external file. It is never read.
			base.EnvironmentFile = filepath.Join(base.NAS, "absent-private-reference")
			before, err := json.Marshal(base)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(base.Config, before, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(base.Credentials); err != nil {
				t.Fatal(err)
			}
			serviceBefore, err := os.ReadFile(base.Service)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("MYCE_TOKEN", "")
			if withToken {
				t.Setenv("MYCE_TOKEN", "synthetic-empty-environment-test")
			}
			setting := installEnvironmentFileSetting()
			kind := "empty-zero-false"

			values := ServiceConfig{}
			f := flag.NewFlagSet("overlay", flag.ContinueOnError)
			f.SetOutput(io.Discard)
			RegisterInstallFlags(f, &values)
			raw := installTestFlagValue(setting, kind == "empty-zero-false")
			args := []string{"--" + setting.Flag + "=" + raw}
			if kind == "omitted" {
				args = nil
				if err := f.Set(setting.Flag, raw); err != nil {
					t.Fatal(err)
				}
				// Set counts as explicit in flag.FlagSet too. Re-register a new set
				// with those ambient defaults to distinguish defaults from Visit.
				f = flag.NewFlagSet("omitted", flag.ContinueOnError)
				RegisterInstallFlags(f, &values)
			}
			if err := f.Parse(args); err != nil {
				t.Fatal(err)
			}
			got, overlayErr := OverlayInstallFlags(f, base, values)
			want := base
			if kind != "omitted" {
				field := installTestField(&values, setting.Field)
				installTestField(&want, setting.Field).Set(field)
				if setting.Flag == "file-cache-bytes" {
					want.FileCacheBytesExplicit = want.FileCacheBytes > 0
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("overlay changed omitted/unrelated fields or ignored explicit input (%s)", kind)
			}
			validationErr := want.ValidateInstall()
			// Empty saved references are valid, but explicitly clearing
			// this flag must fail before credential provisioning.
			invalidExplicitEmpty := setting.Flag == "environment-file" && kind == "empty-zero-false"
			if (overlayErr != nil) != (validationErr != nil || invalidExplicitEmpty) {
				t.Fatalf("validation did not run after overlay: overlay=%v validation=%v", overlayErr, validationErr)
			}
			if overlayErr != nil && !strings.HasPrefix(overlayErr.Error(), "usage:") {
				t.Fatal("invalid explicit value must be a usage error")
			}

			after, err := os.ReadFile(base.Config)
			if err != nil || string(after) != string(before) {
				t.Fatal("rejected install changed saved config")
			}
			serviceAfter, err := os.ReadFile(base.Service)
			if err != nil || string(serviceAfter) != string(serviceBefore) {
				t.Fatal("rejected install changed service definition")
			}
			if _, err := os.Lstat(base.Credentials); !os.IsNotExist(err) {
				t.Fatal("rejected install created credentials")
			}
		})
	}
}
