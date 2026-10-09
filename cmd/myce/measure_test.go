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
