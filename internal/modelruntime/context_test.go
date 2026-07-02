package modelruntime

import (
	"testing"

	"mycelium/internal/domain"
)

func TestEffectiveContextLengthHonorsVLLMMaxModelLen(t *testing.T) {
	preset := domain.Preset{
		Backend:       domain.BackendVLLM,
		ContextLength: 49152,
		LaunchArgs:    []string{"--max-model-len", "8192"},
	}
	if got := EffectiveContextLength(preset); got != 8192 {
		t.Fatalf("EffectiveContextLength = %d, want 8192", got)
	}

	preset.LaunchArgs = []string{"--max-model-len=4096"}
	if got := EffectiveContextLength(preset); got != 4096 {
		t.Fatalf("EffectiveContextLength flag=value = %d, want 4096", got)
	}

	preset.Backend = domain.BackendLlamaCpp
	if got := EffectiveContextLength(preset); got != 49152 {
		t.Fatalf("non-vllm EffectiveContextLength = %d, want 49152", got)
	}
}

func TestEffectiveContextLengthIgnoresInvalidOrLargerVLLMMaxModelLen(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
	}{
		{name: "missing value", args: []string{"--max-model-len"}},
		{name: "invalid value", args: []string{"--max-model-len", "nope"}},
		{name: "invalid flag equals", args: []string{"--max-model-len=nope"}},
		{name: "larger than catalog", args: []string{"--max-model-len", "65536"}},
		{name: "absent", args: []string{"--served-model-name", "model-a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preset := domain.Preset{
				Backend:       domain.BackendVLLM,
				ContextLength: 49152,
				LaunchArgs:    tt.args,
			}
			if got := EffectiveContextLength(preset); got != 49152 {
				t.Fatalf("EffectiveContextLength = %d, want 49152", got)
			}
		})
	}

	preset := domain.Preset{
		Backend:    domain.BackendVLLM,
		LaunchArgs: []string{"--max-model-len", "8192"},
	}
	if got := EffectiveContextLength(preset); got != 8192 {
		t.Fatalf("zero catalog EffectiveContextLength = %d, want 8192", got)
	}
}
