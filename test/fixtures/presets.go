package fixtures

import "mycelium/internal/domain"

func MakePreset(opts ...func(*domain.Preset)) domain.Preset {
	p := domain.Preset{
		ID:            "preset_test",
		ModelRef:      "qwen2.5-9b-instruct",
		Backend:       domain.BackendLlamaCpp,
		ContextLength: 8000,
		Quant:         "Q4_K_M",
		Capabilities:  []domain.Capability{domain.CapabilityChat},
		LaunchProfile: "llamacpp-cuda",
		EstWeightsMB:  5600,
		KVPerTokenMB:  0.18,
	}
	for _, opt := range opts {
		opt(&p)
	}
	return p
}

func WithPresetID(id string) func(*domain.Preset) {
	return func(p *domain.Preset) { p.ID = id }
}

func WithModelRef(model string) func(*domain.Preset) {
	return func(p *domain.Preset) { p.ModelRef = model }
}

func WithAliases(aliases ...string) func(*domain.Preset) {
	return func(p *domain.Preset) { p.Aliases = append([]string(nil), aliases...) }
}

func WithWeights(mb int) func(*domain.Preset) {
	return func(p *domain.Preset) { p.EstWeightsMB = mb }
}

func WithKVPerToken(mb float64) func(*domain.Preset) {
	return func(p *domain.Preset) { p.KVPerTokenMB = mb }
}

func WithContextLength(n int) func(*domain.Preset) {
	return func(p *domain.Preset) { p.ContextLength = n }
}

func WithLaunchProfile(profile string) func(*domain.Preset) {
	return func(p *domain.Preset) { p.LaunchProfile = profile }
}

func WithLaunchArgs(args ...string) func(*domain.Preset) {
	return func(p *domain.Preset) { p.LaunchArgs = append([]string(nil), args...) }
}

func WithPresetNode(id string) func(*domain.Preset) {
	return func(p *domain.Preset) { p.NodeID = id }
}

func MakeClaim(weightsMB, kvMB int) domain.Claim {
	return domain.Claim{WeightsMB: weightsMB, KVReservedMB: kvMB}
}
