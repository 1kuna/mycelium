package estimate

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type MetadataParser interface {
	Parse(ctx context.Context, modelRef string) (domain.ModelMetadata, error)
}

type GGUFEstimator struct {
	parser MetadataParser
	agents map[string]ports.NodeAgent
}

func NewGGUF(parser MetadataParser, agents map[string]ports.NodeAgent) *GGUFEstimator {
	return &GGUFEstimator{parser: parser, agents: agents}
}

func (e *GGUFEstimator) Estimate(ctx context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error) {
	if err := ctx.Err(); err != nil {
		return domain.Claim{}, err
	}
	if contextLen <= 0 {
		return domain.Claim{}, fmt.Errorf("context length must be positive: %d", contextLen)
	}
	if concurrency <= 0 {
		return domain.Claim{}, fmt.Errorf("concurrency must be positive: %d", concurrency)
	}

	metadata, err := e.metadata(ctx, p)
	if err != nil {
		return domain.Claim{}, err
	}
	if metadata.WeightsMB <= 0 {
		return domain.Claim{}, fmt.Errorf("metadata for %q has invalid weights: %dMB", p.ID, metadata.WeightsMB)
	}
	if metadata.KVPerTokenMB < 0 {
		return domain.Claim{}, fmt.Errorf("metadata for %q has invalid kv_per_token: %f", p.ID, metadata.KVPerTokenMB)
	}
	kv := int(math.Ceil(float64(contextLen*concurrency) * metadata.KVPerTokenMB))
	return domain.Claim{WeightsMB: metadata.WeightsMB, KVReservedMB: kv}, nil
}

func (e *GGUFEstimator) metadata(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error) {
	if isLocalFile(p.ModelRef) {
		if e.parser == nil {
			return domain.ModelMetadata{}, fmt.Errorf("gguf parser is not configured for local model %q", p.ModelRef)
		}
		return e.parser.Parse(ctx, p.ModelRef)
	}
	if p.NodeID != "" {
		agent, ok := e.agents[p.NodeID]
		if !ok {
			return domain.ModelMetadata{}, fmt.Errorf("no node agent for model %q on node %q", p.ModelRef, p.NodeID)
		}
		return agent.InspectModel(ctx, p)
	}
	return domain.ModelMetadata{}, fmt.Errorf("model %q is not local and has no owning node", p.ModelRef)
}

func isLocalFile(path string) bool {
	if path == "" || strings.Contains(path, "://") {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

type CommandParser struct {
	Binary string
	Args   []string
}

func NewCommandParser(binary string, args []string) *CommandParser {
	if len(args) == 0 {
		args = []string{"--json", "{model}"}
	}
	return &CommandParser{Binary: binary, Args: args}
}

func (p *CommandParser) Parse(ctx context.Context, modelRef string) (domain.ModelMetadata, error) {
	if p.Binary == "" {
		return domain.ModelMetadata{}, fmt.Errorf("gguf parser binary is not configured")
	}
	cmd := exec.CommandContext(ctx, p.Binary, renderParserArgs(p.Args, modelRef)...)
	out, err := cmd.Output()
	if err != nil {
		return domain.ModelMetadata{}, err
	}
	var metadata domain.ModelMetadata
	if err := json.Unmarshal(out, &metadata); err != nil {
		return domain.ModelMetadata{}, fmt.Errorf("parse gguf metadata: %w", err)
	}
	if metadata.ModelRef == "" {
		metadata.ModelRef = modelRef
	}
	if metadata.Format == "" {
		metadata.Format = "gguf"
	}
	return metadata, nil
}

func renderParserArgs(args []string, modelRef string) []string {
	replacer := strings.NewReplacer("{model}", modelRef)
	out := make([]string, len(args))
	for i, arg := range args {
		out[i] = replacer.Replace(arg)
	}
	return out
}

var _ ports.ResourceEstimator = (*GGUFEstimator)(nil)
var _ MetadataParser = (*CommandParser)(nil)
