package vllmargs

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestNormalizeCanonicalizesGPUUtilization(t *testing.T) {
	got, err := Normalize([]string{"--model", "m", "--gpu-memory-utilization=0.7", "--gpu-memory-utilization", "0.8", "--port", "8000"})
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	want := []string{"--model", "m", "--port", "8000", "--gpu-memory-utilization", "0.8"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %+v", got)
	}
}

func TestNormalizeWithoutUtilizationLeavesArgs(t *testing.T) {
	got, err := Normalize([]string{"--model", "m"})
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"--model", "m"}) {
		t.Fatalf("args = %+v", got)
	}
}

func TestNormalizeRejectsMissingOrInvalidUtilization(t *testing.T) {
	if _, err := Normalize([]string{"--gpu-memory-utilization"}); err == nil || !strings.Contains(err.Error(), "missing value") {
		t.Fatalf("missing err = %v", err)
	}
	if _, err := Normalize([]string{"--gpu-memory-utilization", "2"}); err == nil || !strings.Contains(err.Error(), "value must be") {
		t.Fatalf("range err = %v", err)
	}
	if _, err := Normalize([]string{"--gpu-memory-utilization=bad"}); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("parse err = %v", err)
	}
}

func TestGPUMemoryUtilization(t *testing.T) {
	value, ok, err := GPUMemoryUtilization([]string{"--x", "1", "--gpu-memory-utilization=0.625"})
	if err != nil || !ok || value != 0.625 {
		t.Fatalf("util value=%f ok=%t err=%v", value, ok, err)
	}
	value, ok, err = GPUMemoryUtilization([]string{"--x", "1"})
	if err != nil || ok || value != 0 {
		t.Fatalf("missing util value=%f ok=%t err=%v", value, ok, err)
	}
	if _, _, err := GPUMemoryUtilization([]string{"--gpu-memory-utilization", "0"}); err == nil {
		t.Fatal("expected invalid utilization error")
	}
}
