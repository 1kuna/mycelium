package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var updateAdapterFixtures = flag.Bool("update-adapter-fixtures", false, "regenerate the converter's own dogfood and sanitized starter fixtures")

func conversionWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}

const conversionProfile = `name: example-test
project: example
class: test
source: {git: HEAD, paths: [scripts/], allow_dirty: true}
run:
  command: [python, scripts/test.py, 'literal $HOME; space']
  env: {THREADS: '2', VALUE: 'quote " remains'}
needs: {cpu_cores: 2, memory: auto, platform: {preferred: [cpu]}}
targets: [{os: linux, arch: amd64, platform: cpu}]
allowed_machines: [test-host]
memory_class: per_item
environment: existing-catalog
inputs: {item.json: data/original.json}
outputs: {result.json: original-prefix}
diagnostics: {reports: [{path: test.xml, format: pytest-junit}]}
resumable: true
check: [python, scripts/test.py, --check]
`

func TestAdapterConvertProfilePreservesFieldsAndApply(t *testing.T) {
	t.Chdir(t.TempDir())
	conversionWrite(t, "old.yaml", conversionProfile)
	args := []string{"convert", "--profile", "old.yaml", "--out", "new.yaml"}
	var out, report bytes.Buffer
	if err := adapterCommand(context.Background(), args, &out, &report); err != nil {
		t.Fatalf("%v\n%s", err, report.String())
	}
	if _, err := os.Stat("new.yaml"); !os.IsNotExist(err) {
		t.Fatal("planning wrote output")
	}
	a, err := decodeAdapter(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "example-test")
	if err != nil {
		t.Fatal(err)
	}
	var old executionProfile
	if err := yaml.Unmarshal([]byte(conversionProfile), &old); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.Run.Command, old.Run.Command) {
		t.Fatalf("profile semantics changed:\nold=%+v\nnew=%+v", old, p)
	}
	if !strings.Contains(report.String(), "check -> checks.result") || !strings.Contains(report.String(), "no files written") {
		t.Fatal(report.String())
	}
	want := out.String()
	out.Reset()
	report.Reset()
	if err := adapterCommand(context.Background(), append(args, "--apply"), &out, &report); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("new.yaml")
	if err != nil || string(got) != want || out.String() != want {
		t.Fatalf("apply differs from reviewed output: %v", err)
	}
	if err := adapterCommand(context.Background(), append(args, "--apply"), &out, &report); err == nil {
		t.Fatal("overwrote existing output")
	}
	if got, _ := os.ReadFile("new.yaml"); string(got) != want {
		t.Fatal("refusal changed existing output")
	}
	if err := adapterCommand(context.Background(), []string{"convert", "--profile", "old.yaml", "--out", "old.yaml", "--apply"}, &out, &report); err == nil {
		t.Fatal("overwrote source")
	}
	if got, _ := os.ReadFile("old.yaml"); string(got) != conversionProfile {
		t.Fatal("source changed")
	}

	{
		root, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
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
		conversionWrite(t, "inline-profile.yaml", profile)
		var out bytes.Buffer
		if err := adapterCommand(context.Background(), []string{"convert", "--profile", "inline-profile.yaml"}, &out, io.Discard); err != nil {
			t.Fatal(err)
		}
		conversionWrite(t, "inline-adapter.yaml", out.String())
		a, err := decodeAdapter(out.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		converted, _, err := adapterProfile(a, "example-test")
		if err != nil {
			t.Fatal(err)
		}
		q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), filepath.Join(root, "checkpoints"), myce.RealClock{})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		newJob, err := q.Submit(converted.Spec)
		if err != nil {
			t.Fatal(err)
		}
		if len(newJob.EnvironmentRecipes) != 1 || string(newJob.EnvironmentRecipes["cpu"].Files["requirements.txt"]) != "example-package==1.0.0\n" {
			t.Fatal("inline recipe bytes or identity changed")
		}
		if err := os.Remove("requirements.txt"); err != nil {
			t.Fatal(err)
		}
		newReport, err := q.Resolve(myce.ResolveRequest{Spec: converted.Spec})
		if err != nil {
			t.Fatal(err)
		}
		if len(newReport.Findings) == 0 || newReport.Findings[0].Field != "environment.lock_file" || newReport.Findings[0].Side != "queue" {
			t.Fatalf("missing queue lock not diagnosed: %+v", newReport.Findings)
		}
	}

}

const conversionLegacy = `name: legacy-stage
version: '1'
purpose: Legacy fixture
platforms: [fake, cuda]
kinds: [work]
default_class: batch
resumable: true
memory_gb: 1
host_memory_gb: 2
run: [python, '{extension}/app.py']
setup: [python, '{extension}/setup.py']
check: [python, '{extension}/check.py']
platform_baselines:
  fake:
    kind:work: {input: input.json, output: fake.json, measure: exact, native: reference, variation: exact bytes}
    extension:legacy-stage: {input: input.json, output: fake.json, measure: exact, native: direct reference, variation: exact bytes}
  cuda:
    kind:work: {input: input.json, output: cuda.json, measure: exact, native: reference, variation: exact bytes}
    extension:legacy-stage: {input: input.json, output: cuda.json, measure: exact, native: direct reference, variation: exact bytes}
outputs: [result.json]
`
