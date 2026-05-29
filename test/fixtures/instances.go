package fixtures

import "mycelium/internal/domain"

func MakeInstance(opts ...func(*domain.ModelInstance)) domain.ModelInstance {
	inst := domain.ModelInstance{
		ID:             "inst_test",
		PresetID:       "preset_test",
		NodeID:         "node_test",
		AcceleratorSet: []int{0},
		Claim:          MakeClaim(5600, 1476),
		State:          domain.InstReady,
		Addr:           "127.0.0.1:60000",
		Priority:       domain.PriorityNormal,
	}
	for _, opt := range opts {
		opt(&inst)
	}
	return inst
}

func WithInstanceID(id string) func(*domain.ModelInstance) {
	return func(i *domain.ModelInstance) { i.ID = id }
}

func OnNode(id string) func(*domain.ModelInstance) {
	return func(i *domain.ModelInstance) { i.NodeID = id }
}

func WithInstancePreset(id string) func(*domain.ModelInstance) {
	return func(i *domain.ModelInstance) { i.PresetID = id }
}

func WithClaim(c domain.Claim) func(*domain.ModelInstance) {
	return func(i *domain.ModelInstance) { i.Claim = c }
}

func WithInstancePriority(p domain.Priority) func(*domain.ModelInstance) {
	return func(i *domain.ModelInstance) { i.Priority = p }
}

func Loading(i *domain.ModelInstance) {
	i.State = domain.InstLoading
	i.Loading = true
}
