package main

import "testing"

func TestAdapterQuality(t *testing.T) {
	a, err := decodeAdapter([]byte(`schema: 1
project: fixture
default: work
entries:
  work:
    quality_tolerance: {score: 0.25}
    variants:
      - target: {os: linux, arch: amd64, platform: cpu}
        needs: {cpu_cores: 1, memory_gb: 1}
        execution: {main: {command: [echo, result]}}
        checks: {qualification: [echo, check], result: [true]}
        baselines:
          extension:work: {input: input, output: reference, measure: exact, native: fixture, variation: exact}
        quality:
          extension:work:
            score: {reference: corpus-v1-reference-v2, value: 10.125, reference_value: 10, margin_of_error: 0.125}
`))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	if p.QualityTolerance["score"] != 0.25 || p.Native == nil {
		t.Fatal("quality tolerance lost")
	}
	m := p.Native.Variants[0].Quality["extension:work"]["score"]
	if m.Value == nil || *m.Value != 10.125 || m.MarginOfError == nil || *m.MarginOfError != 0.125 {
		t.Fatal("quality evidence lost", m)
	}
}
