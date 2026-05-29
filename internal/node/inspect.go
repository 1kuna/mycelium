package node

import (
	"context"

	"mycelium/internal/domain"
)

type ModelInspector interface {
	InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error)
}

type StaticInspector struct {
	Metadata domain.ModelMetadata
	Err      error
}

func (s StaticInspector) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	if s.Err != nil {
		return domain.ModelMetadata{}, s.Err
	}
	return s.Metadata, nil
}
