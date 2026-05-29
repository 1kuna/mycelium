package node

import (
	"context"

	"mycelium/internal/domain"
)

type ModelInspector interface {
	InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error)
}

type MetadataParser interface {
	Parse(ctx context.Context, modelRef string) (domain.ModelMetadata, error)
}

type ParserInspector struct {
	Parser MetadataParser
}

func (p ParserInspector) InspectModel(ctx context.Context, preset domain.Preset) (domain.ModelMetadata, error) {
	if p.Parser == nil {
		return domain.ModelMetadata{}, domain.ErrUnsupported
	}
	return p.Parser.Parse(ctx, preset.ModelRef)
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
