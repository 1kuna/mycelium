package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunMeasureRequiresExplicitTargetAndBudget(t *testing.T) {
	var out, errout bytes.Buffer
	e := runMeasure(context.Background(), nil, &out, &errout)
	if e == nil || !strings.Contains(e.Error(), "--worker") {
		t.Fatalf("expected explicit target validation: %v", e)
	}
}
func TestRunMeasureRejectsUnknownOutputBeforeReadingCatalog(t *testing.T) {
	var out, errout bytes.Buffer
	e := runMeasure(context.Background(), []string{"--model-config", "missing", "--models", "one", "--worker", "local", "--device", "0", "--platform", "cuda", "--memory-gb", "10", "--output", "csv"}, &out, &errout)
	if e == nil || !strings.Contains(e.Error(), "--output") {
		t.Fatalf("expected output validation: %v", e)
	}
}

func TestRunMeasureRejectsInvalidConcurrencyAndTimeout(t *testing.T) {
	for _, args := range [][]string{{"--warmup-parallel", "0"}, {"--load-timeout", "0s"}, {"--warmup-parallel", "-1"}, {"--load-timeout", "-1s"}} {
		var out, errout bytes.Buffer
		if err := runMeasure(context.Background(), args, &out, &errout); err == nil || !strings.Contains(err.Error(), "must be positive") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}
