package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Representative profiles (testdata/adapter-convert/*/profile.yaml) convert to
// the minimal adapter form in adapter.yaml, keep the legacy reader's execution
// contract apart from dropped non-production targets, and resolve against the
// real dev API. Production fixtures use OS/ARCH placeholders for this host.
func TestAdapterConvertFixturesResolveOnDev(t *testing.T) {
	fixtures, err := filepath.Abs(filepath.Join("testdata", "adapter-convert"))
	if err != nil {
		t.Fatal(err)
	}
	names, err := os.ReadDir(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(base, "queue.db"), Checkpoints: filepath.Join(base, "checkpoints"), Token: "convert-fixture", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	host := strings.NewReplacer("os: OS", "os: "+runtime.GOOS, "arch: ARCH", "arch: "+runtime.GOARCH)
	for _, dir := range names {
		name := dir.Name()
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(base, name)
			if err := os.CopyFS(root, os.DirFS(filepath.Join(fixtures, name))); err != nil {
				t.Fatal(err)
			}
			profile, err := os.ReadFile(filepath.Join(root, "profile.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			conversionWrite(t, filepath.Join(root, "profile.yaml"), host.Replace(string(profile)))
			t.Chdir(root)
			var out, report bytes.Buffer
			err = adapterCommand(context.Background(), []string{"convert", "--profile", "profile.yaml"}, &out, &report)
			golden := filepath.Join(fixtures, name, "adapter.yaml")
			if name == "unconvertible" {
				if err == nil || out.Len() != 0 || strings.Count(report.String(), "Conversion refused:") != 1 || !strings.Contains(report.String(), "managed Docker was retired") {
					t.Fatalf("want one clear refusal and no output: %v\n%s", err, report.String())
				}
				return
			}
			if err != nil {
				t.Fatalf("%v\n%s", err, report.String())
			}
			// Golden files keep the OS/ARCH placeholders.
			generic := strings.NewReplacer("os: "+runtime.GOOS+"\n", "os: OS\n", "arch: "+runtime.GOARCH+"\n", "arch: ARCH\n").Replace(out.String())
			if name != "production" {
				generic = out.String()
			}
			if *updateAdapterFixtures {
				conversionWrite(t, golden, generic)
			}
			want, err := os.ReadFile(golden)
			if err != nil || string(want) != generic {
				t.Fatalf("output differs from %s: %v\n%s", golden, err, out.String())
			}
			for _, field := range []string{"schema:", "default:", "name:", "class: experiment"} {
				if strings.Contains(out.String(), field) {
					t.Errorf("default-valued %q not omitted", field)
				}
			}
			legacy, _, _, err := readProfile("profile.yaml")
			if err != nil {
				t.Fatal(err)
			}
			a, err := decodeAdapter(out.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			converted, _, err := adapterProfile(a, "")
			if err != nil {
				t.Fatal(err)
			}
			if entryProduction(legacy.Spec) != (len(converted.Targets) > 0) {
				t.Fatalf("targets: production must keep them, others drop them: %+v", converted.Targets)
			}
			if !entryProduction(legacy.Spec) {
				legacy.Targets = nil
			}
			// The adapter reader defaults a missing source to git HEAD.
			if reflect.ValueOf(legacy.Source).IsZero() {
				legacy.Source.Git = "HEAD"
			}
			if !reflect.DeepEqual(legacy.Spec, converted.Spec) || !reflect.DeepEqual(legacy.Check, converted.Check) || !reflect.DeepEqual(legacy.Source, converted.Source) || legacy.MemoryClass != converted.MemoryClass {
				t.Fatalf("conversion changed the execution contract:\nlegacy=%+v\nconverted=%+v", legacy, converted)
			}
			conversionWrite(t, "adapter.yaml", out.String())
			// The dev worker's first check-in is asynchronous.
			var resolved bytes.Buffer
			for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
				resolved.Reset()
				err = specCommand([]string{"check", "--resolve", "--queue", d.URL, "--token", "convert-fixture", "adapter.yaml"}, &resolved, io.Discard)
				if err == nil || time.Now().After(deadline) {
					break
				}
			}
			if err != nil {
				t.Fatalf("resolve on dev: %v\n%s", err, resolved.String())
			}
		})
	}
}
