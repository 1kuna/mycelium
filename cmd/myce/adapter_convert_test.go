package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
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
	if !reflect.DeepEqual(p.Spec, old.Spec) || !reflect.DeepEqual(p.Source, old.Source) || !reflect.DeepEqual(p.Check, old.Check) || p.MemoryClass != old.MemoryClass {
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
}

func TestAdapterConvertNativeStarterAndDogfood(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	var out, report bytes.Buffer
	args := []string{"convert", "--extension", "examples/extensions/http-stage", "--project", "adapter-example"}
	if err := adapterCommand(context.Background(), args, &out, &report); err != nil {
		t.Fatalf("%v\n%s", err, report.String())
	}
	a, err := decodeAdapter(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "http-stage")
	if err != nil {
		t.Fatal(err)
	}
	var original myce.Extension
	raw, err := readConversionYAML("examples/extensions/http-stage/extension.yaml", &original)
	if err != nil {
		t.Fatal(err)
	}
	want := original.Variants[0]
	// Lowering allocates an empty sweep map even when no sweep was authored.
	if want.Sweeps == nil {
		want.Sweeps = map[string]myce.ReferenceSweep{}
	}
	got := p.Native.Variants[0]
	if p.Name != original.Name || p.Native.Name != original.Name || p.Class != original.DefaultClass || p.Source.Directory != "examples/extensions/http-stage" || p.Native.Version != original.Version || p.Outputs["result.json"] != "results" {
		t.Fatalf("native identity/source/output changed: %+v", p)
	}
	if !reflect.DeepEqual(got, want) || len(a.Entries["http-stage"].Checks.Result) != 0 {
		t.Fatalf("native recipe/check changed:\nold=%+v\nnew=%+v", want, got)
	}
	var converted map[string]any
	if err := yaml.Unmarshal(out.Bytes(), &converted); err != nil {
		t.Fatal(err)
	}
	newVariant := converted["entries"].(map[string]any)["http-stage"].(map[string]any)["variants"].([]any)[0].(map[string]any)
	oldVariant := raw["variants"].([]any)[0].(map[string]any)
	if !reflect.DeepEqual(oldVariant["environments"], newVariant["environments"]) {
		t.Fatal("authored environment normalized or replaced")
	}
	if !strings.Contains(report.String(), "fresh native qualification required") || !strings.Contains(report.String(), "no reports migrated") {
		t.Fatal(report.String())
	}
	if *updateAdapterFixtures {
		conversionWrite(t, "examples/adapters/http-stage.yaml", out.String())
	}
	fixture, err := os.ReadFile("examples/adapters/http-stage.yaml")
	if err != nil || !bytes.Equal(out.Bytes(), fixture) {
		t.Fatalf("starter fixture differs from conversion: %v", err)
	}
	out.Reset()
	report.Reset()
	args = []string{"convert", "--profile", ".mycelium/profiles/gate-full.yaml", "--profile", ".mycelium/profiles/go-test.yaml"}
	if err := adapterCommand(context.Background(), args, &out, &report); err != nil {
		t.Fatalf("%v\n%s", err, report.String())
	}
	if *updateAdapterFixtures {
		conversionWrite(t, ".mycelium/adapter.yaml", out.String())
	}
	dogfood, err := os.ReadFile(".mycelium/adapter.yaml")
	if err != nil || !bytes.Equal(out.Bytes(), dogfood) {
		t.Fatalf("dogfood differs from explicit conversion: %v", err)
	}
	wantBytes := out.String()
	out.Reset()
	args = []string{"convert", "--profile", ".mycelium/profiles/go-test.yaml", "--profile", ".mycelium/profiles/gate-full.yaml"}
	if err := adapterCommand(context.Background(), args, &out, &report); err != nil || out.String() != wantBytes {
		t.Fatalf("source order changed output: %v", err)
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
  cuda:
    kind:work: {input: input.json, output: cuda.json, measure: exact, native: reference, variation: exact bytes}
outputs: [result.json]
`

func TestAdapterConvertLegacyRequiresTargetsAndRetainsBaselines(t *testing.T) {
	t.Chdir(t.TempDir())
	conversionWrite(t, "native/extension.yaml", conversionLegacy)
	for _, name := range []string{"app.py", "setup.py", "check.py", "input.json", "fake.json", "cuda.json"} {
		conversionWrite(t, "native/"+name, "[]\n")
	}
	args := []string{"convert", "--extension", "native", "--project", "example"}
	var out, report bytes.Buffer
	if err := adapterCommand(context.Background(), args, &out, &report); err == nil || out.Len() != 0 || !strings.Contains(report.String(), "OS/ARCH/fake") || !strings.Contains(report.String(), "OS/ARCH/cuda") {
		t.Fatalf("legacy targets guessed: %v\n%s", err, report.String())
	}
	report.Reset()
	args = append(args, "--target", "linux/amd64/cuda", "--target", "darwin/arm64/fake")
	if err := adapterCommand(context.Background(), args, &out, &report); err != nil {
		t.Fatalf("%v\n%s", err, report.String())
	}
	a, err := decodeAdapter(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "legacy-stage")
	if err != nil {
		t.Fatal(err)
	}
	if !p.Resumable || p.Native.Name != "legacy-stage" || len(p.Native.Variants) != 2 {
		t.Fatalf("legacy identity changed: %+v", p)
	}
	for _, v := range p.Native.Variants {
		if v.Needs.MemoryGB != 1 || v.Needs.HostMemoryGB != 2 || len(v.Setup) != 2 || len(v.Check) != 2 || len(v.Execution.Main.Command) != 2 || v.Baselines["kind:work"].Output != v.Target.Platform+".json" {
			t.Fatalf("legacy recipe changed: %+v", v)
		}
	}
}

func TestAdapterConvertAggregatesMissingConflictsAndUnsupportedFields(t *testing.T) {
	t.Chdir(t.TempDir())
	conversionWrite(t, "one.yaml", conversionProfile)
	conversionWrite(t, "two.yaml", strings.ReplaceAll(conversionProfile, "project: example", "project: different"))
	conversionWrite(t, "bad.yaml", strings.ReplaceAll(conversionProfile, "memory_class: per_item", "memory_class: invented"))
	conversionWrite(t, "native/extension.yaml", conversionLegacy+"benchmark: [python, benchmark.py]\n")
	args := []string{"convert", "--profile", "one.yaml", "--profile", "two.yaml", "--profile", "bad.yaml", "--profile", "missing.yaml", "--extension", "native", "--out", "new.yaml", "--apply"}
	var out, report bytes.Buffer
	if err := adapterCommand(context.Background(), args, &out, &report); err == nil {
		t.Fatal("invalid conversion accepted")
	}
	for _, text := range []string{"conflicting project", "conflicting entry name", "missing.yaml", "memory_class", "benchmark has no adapter equivalent", "app.py", "setup.py", "check.py", "input.json", "fake.json", "cuda.json", "OS/ARCH/fake", "OS/ARCH/cuda"} {
		if !strings.Contains(report.String(), text) {
			t.Errorf("missing diagnostic %q in %s", text, report.String())
		}
	}
	if out.Len() != 0 {
		t.Fatal("partial adapter emitted")
	}
	if _, err := os.Stat("new.yaml"); !os.IsNotExist(err) {
		t.Fatal("invalid conversion wrote output")
	}
}

func TestAdapterConvertNativeKeepsReferencesSweepsAndAssets(t *testing.T) {
	t.Chdir(t.TempDir())
	const source = `name: composed
version: '3'
purpose: Native recipe field preservation
outputs: [result.json]
resumable: true
assets:
  model.bin: {sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, size_bytes: 4}
variants:
  - target: {os: linux, arch: amd64, platform: fake}
    needs: {memory: auto}
    execution:
      main: {command: [python, '{extension}/app.py'], env: {MODE: literal, input: literal, output: literal, lock_file: literal}, memory_gb: 0.5}
      sidecars:
        - {name: helper, command: [python, '{extension}/service.py'], memory_gb: 0.1, port: 0, readiness: {path: /health, timeout: 5s}}
    setup: [python, '{extension}/setup.py']
    check: [python, '{extension}/check.py']
    baselines:
      extension:composed: {input: input.json, output: output.json, measure: numeric, absolute: 0.1, relative: 0.01, native: reference, variation: measured}
    references:
      extension:composed: {input_shape: one, unit: item, units: 1}
    sweeps:
      extension:composed: {command: [python, '{extension}/check.py'], run: [python, '{extension}/sweep.py'], counts: [1, 2], max_concurrency: 2}
`
	conversionWrite(t, "native/extension.yaml", source)
	for _, name := range []string{"app.py", "service.py", "setup.py", "check.py", "sweep.py", "input.json", "output.json"} {
		conversionWrite(t, "native/"+name, "[1]\n")
	}
	var out, report bytes.Buffer
	if err := adapterCommand(context.Background(), []string{"convert", "--extension", "native", "--project", "example"}, &out, &report); err != nil {
		t.Fatalf("%v\n%s", err, report.String())
	}
	a, err := decodeAdapter(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "composed")
	if err != nil {
		t.Fatal(err)
	}
	var old myce.Extension
	if err := yaml.Unmarshal([]byte(source), &old); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.Native.Variants, old.Variants) || !reflect.DeepEqual(p.Native.Assets, old.Assets) || !p.Native.Resumable {
		t.Fatalf("native fields changed: %+v", p.Native)
	}
}
