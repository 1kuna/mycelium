package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestAdapterConvertQueueReadableInlineLock(t *testing.T) {
	for _, backend := range []string{"uv", "pixi"} {
		t.Run(backend, func(t *testing.T) {
			root := t.TempDir()
			t.Chdir(root)
			conversionWrite(t, "requirements.txt", "example-package==1.0.0\n")
			profile := `name: example-test
project: example
class: test
source: {git: HEAD}
run: {command: [python, test.py, --suite, tooling]}
needs: {cpu_cores: 2, memory_gb: 1, platform: cpu}
targets: [{os: linux, arch: amd64, platform: cpu}]
environment: {lock_file: requirements.txt, python: '3.13', platforms: [cpu]}
resumable: true
check: [test, -s, marker.txt]
`
			if backend == "pixi" {
				conversionWrite(t, "pixi.lock", "version: 6\nenvironments: {}\n")
				conversionWrite(t, "pixi.toml", "[workspace]\nname='example'\n")
				profile = strings.Replace(profile, "lock_file: requirements.txt, python: '3.13'", "backend: pixi, lock_file: pixi.lock", 1)
			}
			conversionWrite(t, "old.yaml", profile)
			var out bytes.Buffer
			if err := adapterCommand(context.Background(), []string{"convert", "--profile", "old.yaml"}, &out, io.Discard); err != nil {
				t.Fatal(err)
			}
			conversionWrite(t, "adapter.yaml", out.String())
			a, err := decodeAdapter(out.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			converted, _, err := adapterProfile(a, "example-test")
			if err != nil {
				t.Fatal(err)
			}
			legacy, _, _, err := readProfile("old.yaml")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(legacy.Spec, converted.Spec) || !reflect.DeepEqual(legacy.Check, converted.Check) || !reflect.DeepEqual(legacy.Source, converted.Source) || legacy.MemoryClass != converted.MemoryClass {
				t.Fatalf("conversion changed execution/recovery contract: legacy=%+v converted=%+v", legacy, converted)
			}
			q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), filepath.Join(root, "checkpoints"), myce.RealClock{})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			oldJob, err := q.Submit(legacy.Spec)
			if err != nil {
				t.Fatal(err)
			}
			newJob, err := q.Submit(converted.Spec)
			if err != nil {
				t.Fatal(err)
			}
			expectedFile, expectedBytes := "requirements.txt", "example-package==1.0.0\n"
			if backend == "pixi" {
				expectedFile, expectedBytes = "pixi.lock", "version: 6\nenvironments: {}\n"
			}
			if string(newJob.EnvironmentRecipes["cpu"].Files[expectedFile]) != expectedBytes {
				t.Fatal("inline recipe bytes changed")
			}
			if len(oldJob.EnvironmentRecipes) != 1 || !reflect.DeepEqual(oldJob.EnvironmentRecipes, newJob.EnvironmentRecipes) || newJob.EnvironmentRecipes["cpu"].Backend != legacy.Environment.Backend {
				t.Fatal("inline recipe bytes or identity changed")
			}
			// Both readers feed the same queue validation. A missing queue-side lock
			// must still be diagnosed there, rather than uploaded by the adapter.
			for _, missing := range []bool{false, true} {
				if missing {
					if err := os.Remove(legacy.Environment.LockFile); err != nil {
						t.Fatal(err)
					}
				}
				oldReport, err := q.Resolve(myce.ResolveRequest{Spec: legacy.Spec})
				if err != nil {
					t.Fatal(err)
				}
				newReport, err := q.Resolve(myce.ResolveRequest{Spec: converted.Spec})
				if err != nil {
					t.Fatal(err)
				}
				newReport.ObservedAt = oldReport.ObservedAt
				if oldReport.Portability != nil && newReport.Portability != nil {
					// Separate read-only requests have separate observation times.
					newReport.Portability.ObservedAt = oldReport.Portability.ObservedAt
				}
				if !reflect.DeepEqual(oldReport, newReport) {
					t.Fatalf("resolve changed: old=%+v new=%+v", oldReport, newReport)
				}
				if missing && (len(newReport.Findings) == 0 || newReport.Findings[0].Field != "environment.lock_file" || newReport.Findings[0].Side != "queue") {
					t.Fatalf("missing queue lock not diagnosed: %+v", newReport.Findings)
				}
			}
		})
	}
}
