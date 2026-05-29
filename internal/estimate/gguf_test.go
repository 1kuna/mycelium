package estimate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestGGUFEstimatorConformanceWithNodeInspection(t *testing.T) {
	node := fixtures.MakeNode()
	agent := mocks.NewNodeAgent(node)
	contract.RunResourceEstimatorConformance(t, "gguf-node",
		func() ports.ResourceEstimator {
			return NewGGUF(nil, map[string]ports.NodeAgent{node.ID: agent})
		},
		fixtures.MakePreset(fixtures.WithPresetNode(node.ID)))
}

func TestGGUFEstimatorUsesNodeSideInspectionForRemoteModel(t *testing.T) {
	node := fixtures.MakeNode()
	agent := mocks.NewNodeAgent(node)
	agent.Metadata = domain.ModelMetadata{ModelRef: "remote.gguf", Format: "gguf", WeightsMB: 100, KVPerTokenMB: 0.5, ContextLength: 4096}
	estimator := NewGGUF(nil, map[string]ports.NodeAgent{node.ID: agent})

	claim, err := estimator.Estimate(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("remote.gguf"), fixtures.WithPresetNode(node.ID)), 1000, 2)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	if claim != (domain.Claim{WeightsMB: 100, KVReservedMB: 1000}) {
		t.Fatalf("claim = %+v", claim)
	}
	if len(agent.Calls) != 1 || agent.Calls[0] != "inspect:preset_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestGGUFEstimatorUsesParserForLocalFile(t *testing.T) {
	model := filepath.Join(t.TempDir(), "model.gguf")
	if err := os.WriteFile(model, []byte("not really gguf"), 0o600); err != nil {
		t.Fatalf("write model: %v", err)
	}
	parser := &staticParser{metadata: domain.ModelMetadata{WeightsMB: 50, KVPerTokenMB: 0.25}}
	estimator := NewGGUF(parser, nil)

	claim, err := estimator.Estimate(context.Background(), fixtures.MakePreset(fixtures.WithModelRef(model)), 1000, 1)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	if claim != (domain.Claim{WeightsMB: 50, KVReservedMB: 250}) {
		t.Fatalf("claim = %+v", claim)
	}
	if parser.modelRef != model {
		t.Fatalf("parser model = %s", parser.modelRef)
	}
}

func TestGGUFEstimatorFailsLoudOnMissingInputs(t *testing.T) {
	estimator := NewGGUF(nil, nil)
	_, err := estimator.Estimate(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("remote.gguf")), 1000, 1)
	if err == nil || !strings.Contains(err.Error(), "no owning node") {
		t.Fatalf("missing node err = %v", err)
	}
	_, err = estimator.Estimate(context.Background(), fixtures.MakePreset(fixtures.WithPresetNode("missing")), 1000, 1)
	if err == nil || !strings.Contains(err.Error(), "no node agent") {
		t.Fatalf("missing agent err = %v", err)
	}
	_, err = estimator.Estimate(context.Background(), fixtures.MakePreset(fixtures.WithPresetNode("missing")), 0, 1)
	if err == nil || !strings.Contains(err.Error(), "context length") {
		t.Fatalf("bad context err = %v", err)
	}
	_, err = estimator.Estimate(context.Background(), fixtures.MakePreset(fixtures.WithPresetNode("missing")), 1, 0)
	if err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("bad concurrency err = %v", err)
	}
}

func TestGGUFEstimatorFailsOnBadMetadataAndContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewGGUF(&staticParser{}, nil).Estimate(ctx, fixtures.MakePreset(), 1000, 1)
	if err == nil {
		t.Fatal("expected context error")
	}

	model := filepath.Join(t.TempDir(), "model.gguf")
	if err := os.WriteFile(model, []byte("not really gguf"), 0o600); err != nil {
		t.Fatalf("write model: %v", err)
	}
	_, err = NewGGUF(nil, nil).Estimate(context.Background(), fixtures.MakePreset(fixtures.WithModelRef(model)), 1000, 1)
	if err == nil || !strings.Contains(err.Error(), "parser") {
		t.Fatalf("missing parser err = %v", err)
	}
	_, err = NewGGUF(&staticParser{metadata: domain.ModelMetadata{WeightsMB: 0, KVPerTokenMB: 0.1}}, nil).Estimate(context.Background(), fixtures.MakePreset(fixtures.WithModelRef(model)), 1000, 1)
	if err == nil || !strings.Contains(err.Error(), "invalid weights") {
		t.Fatalf("weights err = %v", err)
	}
	_, err = NewGGUF(&staticParser{metadata: domain.ModelMetadata{WeightsMB: 1, KVPerTokenMB: -1}}, nil).Estimate(context.Background(), fixtures.MakePreset(fixtures.WithModelRef(model)), 1000, 1)
	if err == nil || !strings.Contains(err.Error(), "invalid kv") {
		t.Fatalf("kv err = %v", err)
	}
}

func TestCommandParserParsesJSONMetadata(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fixture is unix-only")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "parser.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '{\"weights_mb\":12,\"kv_per_token_mb\":0.5,\"context_length\":2048}'\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	model := filepath.Join(dir, "model.gguf")
	parser := NewCommandParser(script, []string{"{model}"})
	metadata, err := parser.Parse(context.Background(), model)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if metadata.ModelRef != model || metadata.Format != "gguf" || metadata.WeightsMB != 12 {
		t.Fatalf("metadata = %+v", metadata)
	}
	if got := renderParserArgs([]string{"--model={model}"}, "abc"); got[0] != "--model=abc" {
		t.Fatalf("render args = %+v", got)
	}
}

func TestCommandParserFailsOnMissingBinaryAndBadJSON(t *testing.T) {
	_, err := NewCommandParser("", nil).Parse(context.Background(), "model.gguf")
	if err == nil || !strings.Contains(err.Error(), "binary") {
		t.Fatalf("missing binary err = %v", err)
	}
	parser := NewCommandParser("printf", []string{"not-json"})
	_, err = parser.Parse(context.Background(), "model.gguf")
	if err == nil || !strings.Contains(err.Error(), "parse gguf metadata") {
		t.Fatalf("bad json err = %v", err)
	}
	parser = NewCommandParser("/missing/parser", []string{"{model}"})
	_, err = parser.Parse(context.Background(), "model.gguf")
	if err == nil {
		t.Fatal("expected command error")
	}
}

type staticParser struct {
	metadata domain.ModelMetadata
	err      error
	modelRef string
}

func (p *staticParser) Parse(_ context.Context, modelRef string) (domain.ModelMetadata, error) {
	p.modelRef = modelRef
	if p.err != nil {
		return domain.ModelMetadata{}, p.err
	}
	return p.metadata, nil
}

func TestStaticParserError(t *testing.T) {
	wantErr := errors.New("parse")
	model := filepath.Join(t.TempDir(), "model.gguf")
	if err := os.WriteFile(model, []byte("not really gguf"), 0o600); err != nil {
		t.Fatalf("write model: %v", err)
	}
	_, err := NewGGUF(&staticParser{err: wantErr}, nil).Estimate(context.Background(), fixtures.MakePreset(fixtures.WithModelRef(model)), 1000, 1)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v", err)
	}
}
