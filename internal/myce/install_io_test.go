package myce

import (
	"flag"
	"slices"
	"strings"
	"testing"
)

func TestInstallQueueIOPacing(t *testing.T) {
	c, err := PlanInstall("worker", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux", Platform: "fake", MemoryMode: "fixed", MemoryGB: 1})
	if err != nil {
		t.Fatal(err)
	}
	if c.PaceQueueIO || slices.Contains(c.Args(), "--pace-queue-io") {
		t.Fatal("unqualified pacing became an install default")
	}
	var values ServiceConfig
	f := flag.NewFlagSet("install", flag.ContinueOnError)
	RegisterInstallFlags(f, &values)
	if err := f.Parse([]string{"--pace-queue-io"}); err != nil {
		t.Fatal(err)
	}
	c, err = OverlayInstallFlags(f, c, values)
	if err != nil || !slices.Contains(c.Args(), "--pace-queue-io") || !strings.Contains(c.ServiceText(), `"--pace-queue-io"`) {
		t.Fatal("installer did not carry opt-in policy to the worker", err)
	}
	for _, invalid := range []ServiceConfig{func() ServiceConfig { v := c; v.Role = "queue"; return v }(), func() ServiceConfig { v := c; v.Detection.OS = "darwin"; return v }()} {
		if invalid.ValidateInstall() == nil {
			t.Fatal("accepted unsupported pacing service")
		}
	}
}

func TestInstallIOWeight(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		t.Run(role, func(t *testing.T) {
			c, err := PlanInstall(role, "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux", Platform: "fake", MemoryMode: "fixed", MemoryGB: 1})
			if err != nil {
				t.Fatal(err)
			}
			overlay := func(base ServiceConfig, args ...string) (ServiceConfig, error) {
				var values ServiceConfig
				f := flag.NewFlagSet("install", flag.ContinueOnError)
				RegisterInstallFlags(f, &values)
				if err := f.Parse(args); err != nil {
					return ServiceConfig{}, err
				}
				return OverlayInstallFlags(f, base, values)
			}
			if strings.Contains(c.ServiceText(), "IOWeight=") {
				t.Fatal("default install changed I/O policy")
			}
			weighted, err := overlay(c, "--io-weight=10")
			if err != nil || !strings.Contains(weighted.ServiceText(), "IOAccounting=yes\nIOWeight=10\n") {
				t.Fatal("explicit service-subtree weight missing", err)
			}
			preserved, err := overlay(weighted)
			if err != nil || preserved.IOWeight != 10 {
				t.Fatal("upgrade lost explicit policy", err)
			}
			cleared, err := overlay(preserved, "--io-weight=0")
			if err != nil || strings.Contains(cleared.ServiceText(), "IOWeight=") {
				t.Fatal("could not restore system default", err)
			}
			for _, value := range []string{"-1", "10001"} {
				if _, err := overlay(c, "--io-weight="+value); err == nil {
					t.Errorf("accepted invalid weight %s", value)
				}
			}
			c.Detection.OS = "darwin"
			if _, err := overlay(c, "--io-weight=10"); err == nil {
				t.Error("accepted unenforced Darwin setting")
			}
		})
	}
}
