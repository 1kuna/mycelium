package vllm

import "testing"

func TestNewAdapterNamesVLLM(t *testing.T) {
	adapter := NewAdapter("vllm")
	if adapter.Name() != "vllm" {
		t.Fatalf("name = %s", adapter.Name())
	}
}
