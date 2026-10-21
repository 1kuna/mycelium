package myce

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"html"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func completeLaunchdProbe(t *testing.T, path string, c ServiceConfig) error {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	text := string(b)
	for _, want := range []string{"<key>RunAtLoad</key><true/>", "<string>detect</string>", "<string>--config</string>", html.EscapeString(c.Path)} {
		if !strings.Contains(text, want) {
			t.Fatalf("probe does not use saved service environment: missing %s", want)
		}
	}
	_, rest, ok := strings.Cut(text, "<key>StandardOutPath</key><string>")
	if !ok {
		t.Fatal("probe has no output path")
	}
	out, _, _ := strings.Cut(rest, "</string>")
	data, _ := json.Marshal(HardwarePreflight{Detection: c.Detection, Devices: c.MachineDevices})
	return os.WriteFile(html.UnescapeString(out), data, 0600)
}
func TestOnboardingLinuxLingeringActionAndRefusal(t *testing.T) {
	for _, mode := range []string{"enabled", "enable", "denied", "unverified", "unknown", "manager-missing"} {
		t.Run(mode, func(t *testing.T) {
			c := ServiceConfig{Detection: Detection{OS: "linux"}}
			calls := []string{}
			enabled := mode == "enabled"
			report, err := EnsureOnboardingServiceSession(context.Background(), c, func(_ context.Context, n string, args ...string) ([]byte, error) {
				calls = append(calls, n+" "+strings.Join(args, " "))
				if n == "loginctl" && args[0] == "enable-linger" {
					if mode == "denied" {
						return nil, errors.New("private-value")
					}
					if mode != "unverified" {
						enabled = true
					}
					return nil, nil
				}
				if n == "loginctl" {
					if mode == "unknown" {
						return nil, nil
					}
					if enabled {
						return []byte("yes"), nil
					}
					return []byte("no"), nil
				}
				if n == "systemctl" && mode == "manager-missing" {
					return nil, errors.New("private-value")
				}
				return nil, nil
			})
			code := map[string]string{"denied": "lingering_permission_required", "unverified": "lingering_unavailable", "unknown": "lingering_unavailable", "manager-missing": "service_session_unavailable"}[mode]
			if code != "" {
				requirePreflightCode(t, err, code)
				if strings.Contains(err.Error(), "private-value") {
					t.Fatal("command output leaked")
				}
				return
			}
			if err != nil || report.Lingering == "" {
				t.Fatal(report, err)
			}
			action := "loginctl enable-linger " + strconv.Itoa(os.Getuid())
			if containsString(calls, action) != (mode == "enable") {
				t.Fatal(calls)
			}
			for _, call := range calls {
				if strings.Contains(call, "sudo") {
					t.Fatal("privilege escalation")
				}
			}
		})
	}
}
func TestOnboardingLaunchdGUIEnvironmentAndLimitations(t *testing.T) {
	c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "darwin", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
	if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	domain := "gui/" + strconv.Itoa(os.Getuid())
	bootstraps, bootouts := 0, 0
	command := func(_ context.Context, n string, args ...string) ([]byte, error) {
		if n != "launchctl" {
			t.Fatalf("non-launchd command %s", n)
		}
		switch args[0] {
		case "print":
			if args[1] != domain {
				t.Fatal(args)
			}
		case "bootstrap":
			bootstraps++
			if args[1] != domain {
				t.Fatal("not gui domain", args)
			}
			return nil, completeLaunchdProbe(t, args[2], c)
		case "bootout":
			bootouts++
		}
		return nil, nil
	}
	report, err := EnsureOnboardingServiceSession(context.Background(), c, command)
	if err != nil || !strings.Contains(report.Limitation, "GUI login") || !strings.Contains(report.MemoryEnforcement, "admission-only") {
		t.Fatal(report, err)
	}
	if err = ValidateWorkerServiceEnvironment(context.Background(), c, RealClock{}, command); err != nil {
		t.Fatal(err)
	}
	if bootstraps != 1 || bootouts != 1 {
		t.Fatal(bootstraps, bootouts)
	}
	entries, _ := os.ReadDir(filepath.Dir(c.Config))
	if len(entries) != 0 {
		t.Fatal("probe artifacts leaked", entries)
	}
	c.RequireMemoryCeilings = true
	_, err = EnsureOnboardingServiceSession(context.Background(), c, command)
	requirePreflightCode(t, err, "memory_ceilings_unsupported")
	c.RequireMemoryCeilings = false
	_, err = EnsureOnboardingServiceSession(context.Background(), c, func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("private-value") })
	requirePreflightCode(t, err, "gui_session_required")
}
func TestOnboardingParticipationPersistenceAndGeneratedServices(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		t.Run(osname, func(t *testing.T) {
			c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: osname, Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
			c.DailyUse, c.Laptop = true, true
			c.CPUReserveCores = 0
			c.CPUBudgetCores = 0
			c.HostMemoryBudgetGB = 0
			c.Detection.ReserveGB = 0
			f := flag.NewFlagSet("install", flag.ContinueOnError)
			values := ServiceConfig{}
			RegisterInstallFlags(f, &values)
			f.Parse(nil)
			got, err := OverlayInstallFlags(f, c, values)
			if err != nil || !reflect.DeepEqual(got, c) {
				t.Fatal("rerun changed values", err)
			}
			if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(got)
			os.WriteFile(c.Config, data, 0600)
			loaded, err := ReadServiceConfig(c.Config)
			if err != nil || !reflect.DeepEqual(loaded, c) {
				t.Fatal("saved config lost policy", err)
			}
			unit := loaded.ServiceText()
			for _, want := range []string{"service", "--config"} {
				if !strings.Contains(unit, want) {
					t.Fatal(unit)
				}
			}
			if osname == "linux" && !strings.Contains(unit, "--daily-use=true") {
				t.Fatal("unit omitted policy")
			}
			// macOS service reads these same Args from config; no hand-edited plist.
			args := strings.Join(loaded.Args(), " ")
			for _, want := range []string{"--daily-use=true", "--laptop=true", "--desktop=false", "--cpu-budget-cores 0", "--host-memory-budget-gb 0"} {
				if !strings.Contains(args, want) {
					t.Fatal(args)
				}
			}
			f = flag.NewFlagSet("overlay", flag.ContinueOnError)
			values = ServiceConfig{}
			RegisterInstallFlags(f, &values)
			f.Parse([]string{"--daily-use=false", "--laptop=false", "--desktop=false"})
			got, err = OverlayInstallFlags(f, loaded, values)
			if err != nil || got.DailyUse || got.Laptop || got.Desktop || got.CPUReserveCores != 0 {
				t.Fatal(got, err)
			}
			f = flag.NewFlagSet("invalid", flag.ContinueOnError)
			RegisterInstallFlags(f, &values)
			f.Parse([]string{"--desktop", "--laptop"})
			if _, err = OverlayInstallFlags(f, got, values); err == nil {
				t.Fatal("conflicting chassis policy accepted")
			}
		})
	}
}
func TestOnboardingServiceRejectsMissingComponentAndSanitizesOutput(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux", Platform: "xpu"}, MachineDevices: []DeviceInstall{{"0", Detection{Platform: "xpu"}}, {"1", Detection{Platform: "xpu"}}}}
	for _, mode := range []string{"missing-component", "probe-failure"} {
		err := ValidateWorkerServiceEnvironment(context.Background(), c, RealClock{}, func(context.Context, string, ...string) ([]byte, error) {
			if mode == "probe-failure" {
				return []byte("private-value"), errors.New("private-value")
			}
			return []byte(`{"platform":"xpu","devices":[{"selector":"0","detection":{"platform":"xpu"}}]}`), nil
		})
		code := "service_environment_invalid"
		if mode == "missing-component" {
			code = "service_inventory_incomplete"
		}
		requirePreflightCode(t, err, code)
		if strings.Contains(err.Error(), "private-value") {
			t.Fatal("probe leaked credentials")
		}
	}
}

// Backend availability is checked only when a recipe needs it. Retained tool
// PATH entries on upgrades must not make an offline CPU service unusable.
func TestOnboardingServicePreservesToolPATHWithoutEagerProbes(t *testing.T) {
	t.Setenv("PATH", os.Getenv("PATH"))
	c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "linux", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
	c.Path = filepath.Join(c.WorkerHome, ".local", "share", "mycelium", "tools", "uv-0.8.22-linux-arm64") + ":/usr/bin"
	p := MachineProbe{OS: "linux", Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("idle service invoked backend tool")
		return nil, nil
	}}
	got, err := DetectServiceHardware(context.Background(), c, p, GPUAccessProbe{})
	if err != nil || got.Platform != "cpu" || os.Getenv("PATH") != c.Path {
		t.Fatal(got, err)
	}
}
