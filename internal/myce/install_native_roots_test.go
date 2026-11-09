package myce

import (
	"flag"
	"io"
	"testing"
)

func TestNativeRootFlagsRejectInvalidPaths(t *testing.T) {
	for _, root := range []string{"relative", "~/models", "/models/~", "/models/*", "/models/?", "/models/[ab]", "/models\nother", "/models\rother", "/models\x00other"} {
		t.Run(root, func(t *testing.T) {
			for _, name := range []string{"native-read-root", "native-exec-root"} {
				values := ServiceConfig{}
				f := flag.NewFlagSet("install", flag.ContinueOnError)
				f.SetOutput(io.Discard)
				RegisterInstallFlags(f, &values)
				if err := f.Parse([]string{"--" + name, root}); err == nil {
					t.Errorf("--%s accepted %q", name, root)
				}
			}
		})
	}
}

func TestNativeRootSavedEntriesValidated(t *testing.T) {
	c, err := PlanInstall("worker", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux", Platform: "fake", MemoryMode: "fixed", MemoryGB: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []*[]string{&c.NativeReadRoots, &c.NativeExecuteRoots} {
		for _, root := range []string{"", "relative", "/fixture/*", "/fixture/~", "/fixture\nother"} {
			*field = []string{root}
			if err := c.ValidateInstall(); err == nil {
				t.Errorf("saved config accepted native root %q", root)
			}
		}
		*field = nil
	}
}
