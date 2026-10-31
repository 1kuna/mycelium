package myce

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestGPUTelemetryHelperInstallConfig(t *testing.T) {
	c, err := PlanInstall("worker", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux", Platform: "fake", MemoryMode: "fixed", MemoryGB: 1})
	if err != nil {
		t.Fatal(err)
	}
	overlay := func(base ServiceConfig, args ...string) (ServiceConfig, error) {
		values := ServiceConfig{}
		f := flag.NewFlagSet("install", flag.ContinueOnError)
		RegisterInstallFlags(f, &values)
		if err := f.Parse(args); err != nil {
			return ServiceConfig{}, err
		}
		return OverlayInstallFlags(f, base, values)
	}
	path := "/usr/local/libexec/myce/gpu telemetry;literal"
	c, err = overlay(c, "--gpu-telemetry-helper", path)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(c)
	config := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(config, b, 0600); err != nil {
		t.Fatal(err)
	}
	saved, err := ReadServiceConfig(config)
	if err != nil || saved.GPUTelemetryHelper != path {
		t.Fatalf("helper did not survive saved configuration: %v", err)
	}
	args := saved.Args()
	i := slices.Index(args, "--gpu-telemetry-helper")
	if i < 0 || i+1 >= len(args) || args[i+1] != path {
		t.Fatal("worker service omitted, split or changed helper executable")
	}
	unchanged, err := overlay(saved)
	if err != nil || unchanged.GPUTelemetryHelper != path {
		t.Fatal("upgrade discarded omitted helper setting")
	}
	cleared, err := overlay(saved, "--gpu-telemetry-helper=")
	if err != nil || cleared.GPUTelemetryHelper != "" || slices.Contains(cleared.Args(), "--gpu-telemetry-helper") {
		t.Fatal("operator could not disable helper")
	}
	for _, invalid := range []string{"relative", "/fixture/reader\nother", "/fixture/reader\x00other"} {
		if _, err := overlay(saved, "--gpu-telemetry-helper", invalid); err == nil {
			t.Fatalf("accepted invalid helper path %q", invalid)
		}
	}
}

func TestInstallQueueFileStoreDefaults(t *testing.T) {
	for _, platform := range []string{"linux", "darwin"} {
		t.Run(platform, func(t *testing.T) {
			c, err := PlanInstall("queue", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: platform})
			if err != nil {
				t.Fatal(err)
			}
			// Older saved configs lack both fields; upgrades retain runtime defaults.
			b, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			var legacy map[string]json.RawMessage
			if err := json.Unmarshal(b, &legacy); err != nil {
				t.Fatal(err)
			}
			delete(legacy, "file_store_bytes")
			delete(legacy, "file_store_production_reserve")
			b, err = json.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(t.TempDir(), "queue.json")
			if err := os.WriteFile(config, b, 0600); err != nil {
				t.Fatal(err)
			}
			saved, err := ReadServiceConfig(config)
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range []ServiceConfig{c, saved} {
				if got.FileStoreBytes != 100<<30 || got.FileStoreProductionReserve != 0 {
					t.Fatal("fresh/legacy install changed file-store defaults")
				}
			}
			values := ServiceConfig{}
			f := flag.NewFlagSet("install", flag.ContinueOnError)
			RegisterInstallFlags(f, &values)
			if err := f.Parse([]string{"--file-store-bytes=12", "--file-store-production-reserve=6"}); err != nil {
				t.Fatal(err)
			}
			configured, err := OverlayInstallFlags(f, c, values)
			if err != nil {
				t.Fatal(err)
			}
			// launchd reads Args from the saved config; systemd also embeds them.
			args := configured.Args()
			for _, pair := range [][2]string{{"--file-store-bytes", "12"}, {"--file-store-production-reserve", "6"}} {
				i := slices.Index(args, pair[0])
				if i < 0 || i+1 >= len(args) || args[i+1] != pair[1] {
					t.Errorf("service args omitted %v", pair)
				}
				if platform == "linux" && !strings.Contains(configured.ServiceText(), fmt.Sprintf("%q %q", pair[0], pair[1])) {
					t.Errorf("generated unit omitted %v", pair)
				}
			}
			for _, limits := range [][2]int64{{0, 0}, {-1, 0}, {12, -1}, {12, 13}} {
				invalid := configured
				invalid.FileStoreBytes, invalid.FileStoreProductionReserve = limits[0], limits[1]
				if invalid.ValidateInstall() == nil {
					t.Errorf("accepted invalid file-store capacity/reserve %v", limits)
				}
			}
		})
	}
}

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

func TestInstallSettingsTableComplete(t *testing.T) {
	fields := map[string]bool{}
	names := map[string]bool{}
	for _, s := range InstallSettings() {
		if !installTestField(&ServiceConfig{}, s.Field).IsValid() {
			t.Fatalf("table names missing field: %s", s.Field)
		}
		fields[s.Field] = true
		if s.Flag != "" {
			if names[s.Flag] || s.bind == nil || s.copy == nil || s.validate == nil {
				t.Fatalf("incomplete or duplicate installer setting: %s", s.Flag)
			}
			names[s.Flag] = true
		}
	}
	var check func(reflect.Type, string)
	check = func(typ reflect.Type, prefix string) {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name := prefix + f.Name
			if f.Type == reflect.TypeOf(Detection{}) {
				check(f.Type, name+".")
			} else if !fields[name] {
				t.Errorf("new ServiceConfig field needs a table policy: %s", name)
			}
		}
	}
	check(reflect.TypeOf(ServiceConfig{}), "")
}

func TestInstallSettingsOverlayMatrix(t *testing.T) {
	cases := 0
	for _, platform := range []string{"linux", "darwin"} {
		for _, role := range []string{"queue", "worker"} {
			base, err := PlanInstall(role, "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: platform, Arch: "arm64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 3})
			if err != nil {
				t.Fatal(err)
			}
			base.HostID = "fixture-host"
			// Omitted optional paths and nil pointers are values too, never defaults.
			base.EnvironmentRoot = ""
			base.EnvironmentCatalog = "/fixture/catalog/catalog.yaml"
			for _, setting := range InstallSettings() {
				if setting.Flag == "" {
					continue
				}
				for _, kind := range []string{"omitted", "changed", "empty-zero-false"} {
					cases++
					t.Run(platform+"/"+role+"/"+setting.Flag+"/"+kind, func(t *testing.T) {
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
					})
				}
			}
		}
	}
	t.Logf("settings matrix cases: %d", cases)
}
func TestInstallSettingsAliasesAndRelativePaths(t *testing.T) {
	c, _ := PlanInstall("queue", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux"})
	f := flag.NewFlagSet("aliases", flag.ContinueOnError)
	values := ServiceConfig{}
	RegisterInstallFlags(f, &values)
	if err := f.Parse([]string{"--checkpoint-root=/fixture/one", "--checkpoints=/fixture/two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := OverlayInstallFlags(f, c, values); err == nil || !strings.Contains(err.Error(), "same setting") {
		t.Fatal("ambiguous aliases accepted")
	}
	for _, s := range InstallSettings() {
		if s.Flag == "" || s.normalize == nil {
			continue
		}
		t.Run(s.Flag, func(t *testing.T) {
			f := flag.NewFlagSet("relative", flag.ContinueOnError)
			values := ServiceConfig{}
			RegisterInstallFlags(f, &values)
			if err := f.Parse([]string{fmt.Sprintf("--%s=relative", s.Flag)}); err != nil {
				t.Fatal(err)
			}
			got, err := OverlayInstallFlags(f, c, values)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := filepath.Abs("relative")
			if installTestField(&got, s.Field).String() != want {
				t.Fatal("relative flag path not normalized")
			}
		})
	}
}
