package myce

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNativeRootInstallConfig(t *testing.T) {
	c, err := PlanInstall("worker", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux", Platform: "fake", MemoryMode: "fixed", MemoryGB: 1})
	if err != nil {
		t.Fatal(err)
	}
	c.NativeReadRoots = []string{"/fixture/old-models"}
	c.NativeExecuteRoots = []string{"/fixture/old-runtime"}
	overlay := func(base ServiceConfig, args ...string) ServiceConfig {
		t.Helper()
		values := ServiceConfig{}
		f := flag.NewFlagSet("install", flag.ContinueOnError)
		RegisterInstallFlags(f, &values)
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		got, err := OverlayInstallFlags(f, base, values)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	read := []string{"/fixture/models,version-2", "/fixture/other models"}
	execute := []string{"/fixture/runtime,version-2", "/fixture/other runtime"}
	c = overlay(c, "--native-read-root", read[0], "--native-read-root", read[1], "--native-exec-root", execute[0], "--native-exec-root", execute[1])
	if !reflect.DeepEqual(c.NativeReadRoots, read) || !reflect.DeepEqual(c.NativeExecuteRoots, execute) {
		t.Fatalf("repeated paths were split or old grants retained: %v %v", c.NativeReadRoots, c.NativeExecuteRoots)
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	saved, err := ReadServiceConfig(path)
	if err != nil || !reflect.DeepEqual(saved, c) {
		t.Fatalf("saved grants changed: %v", err)
	}
	if got := overlay(saved); !reflect.DeepEqual(got, saved) {
		t.Fatal("omitted flags changed saved configuration")
	}
	argv := map[string][]string{}
	args := saved.Args()
	for i, arg := range args {
		if arg == "--native-read-root" || arg == "--native-exec-root" {
			if i+1 == len(args) {
				t.Fatal("native root flag missing path")
			}
			argv[arg] = append(argv[arg], args[i+1])
		}
	}
	if !reflect.DeepEqual(argv["--native-read-root"], read) || !reflect.DeepEqual(argv["--native-exec-root"], execute) {
		t.Fatalf("service arguments lost grants: %v", argv)
	}
	cleared := overlay(saved, "--native-read-root=")
	if len(cleared.NativeReadRoots) != 0 || !reflect.DeepEqual(cleared.NativeExecuteRoots, execute) {
		t.Fatal("clearing read grants changed execution grants")
	}
	cleared = overlay(saved, "--native-exec-root=")
	if len(cleared.NativeExecuteRoots) != 0 || !reflect.DeepEqual(cleared.NativeReadRoots, read) {
		t.Fatal("clearing execution grants changed read grants")
	}
	cleared = overlay(saved, "--native-read-root=/fixture/discarded", "--native-read-root=", "--native-read-root=/fixture/replacement")
	if !reflect.DeepEqual(cleared.NativeReadRoots, []string{"/fixture/replacement"}) {
		t.Fatal("empty flag did not clear preceding occurrences")
	}
}

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
