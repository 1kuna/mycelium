package myce

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestOnboardingCPUServiceNeedsNoGPU(t *testing.T) {
	t.Setenv("PATH", os.Getenv("PATH"))
	for _, osname := range []string{"linux", "darwin"} {
		c := ServiceConfig{Path: os.Getenv("PATH"), Detection: Detection{OS: osname, Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"}}
		p := MachineProbe{OS: osname, Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) {
			t.Fatal("CPU service probed a GPU")
			return nil, exec.ErrNotFound
		}}
		access := GPUAccessProbe{Glob: func(string) ([]string, error) { t.Fatal("CPU service checked GPU nodes"); return nil, nil }}
		got, err := DetectService(context.Background(), c, p, access)
		if err != nil || got.Platform != "cpu" {
			t.Fatalf("CPU service: %+v %v", got, err)
		}
	}
}

func TestOnboardingDriverFailureNeverMeansNoGPU(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		p := MachineProbe{OS: osname, Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("driver failed") }}
		_, err := p.Detect(context.Background())
		if err == nil || errors.Is(err, ErrNoGPU) {
			t.Fatalf("driver failure classified as GPU absence: %v", err)
		}
	}
}

func TestOnboardingParticipationInstallFlags(t *testing.T) {
	c, _ := PlanInstall("worker", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "darwin", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
	values := ServiceConfig{}
	f := flag.NewFlagSet("install", flag.ContinueOnError)
	RegisterInstallFlags(f, &values)
	if err := f.Parse([]string{"--daily-use", "--laptop", "--cpu-budget-cores=0", "--reserve-cpu-cores=0"}); err != nil {
		t.Fatal(err)
	}
	got, err := OverlayInstallFlags(f, c, values)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(got.Args(), " ")
	for _, want := range []string{"--daily-use=true", "--laptop=true", "--desktop=false", "--cpu-budget-cores 0", "--reserve-cpu-cores 0"} {
		if !strings.Contains(args, want) {
			t.Fatalf("missing %s in %s", want, args)
		}
	}
}

func TestOnboardingLegacyMountFlagCannotCreateLocalNAS(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		for _, allow := range []bool{false, true} {
			home := t.TempDir()
			c, _ := PlanInstall("worker", home, "", home+"/absent-nas", "http://queue.invalid", "hypha", Detection{OS: osname, Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
			c.RequireMountedNAS = false
			c.AllowDeviceless = allow
			err := prepareInstallNAS(context.Background(), c, func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
			if err == nil {
				t.Fatalf("%s created local storage for an absent NAS", osname)
			}
			if _, err = os.Stat(c.NAS); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s created missing NAS root", osname)
			}
		}
	}
}

func TestOnboardingAbsentExplicitSelectorsNeverSelectCPU(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		p := onboardingProbe("cpu")
		p.OS = osname
		c := ServiceConfig{Device: "1,0"}
		_, err := PreflightHardware(context.Background(), c, p)
		requirePreflightCode(t, err, "device_selector_unmatched")
		if c.Device != "1,0" || c.Detection.Platform != "" {
			t.Fatal("failed preflight changed explicit allowance")
		}
	}
}
