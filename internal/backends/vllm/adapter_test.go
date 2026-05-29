package vllm

import "testing"

func TestNewAdapterNamesVLLM(t *testing.T) {
	adapter := NewAdapter("vllm")
	if adapter.Name() != "vllm" {
		t.Fatalf("name = %s", adapter.Name())
	}
	configured := NewAdapterWithConfig(Config{BinaryPath: "vllm"})
	if configured.Name() != "vllm" {
		t.Fatalf("configured name = %s", configured.Name())
	}
}
