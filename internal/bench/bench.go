package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"mycelium/internal/ports"
)

type Client interface {
	Complete(ctx context.Context, model, prompt string) (Completion, error)
}

type Completion struct {
	Text          string  `json:"text"`
	TokensPerSec  float64 `json:"tokens_per_sec"`
	TTFTms        int     `json:"ttft_ms"`
	ContextTokens int     `json:"context_tokens"`
}

type Request struct {
	Prompt    string
	Models    []string
	OutputDir string
}

type Result struct {
	Model         string  `json:"model"`
	OutputPath    string  `json:"output_path"`
	Bytes         int     `json:"bytes"`
	TokensPerSec  float64 `json:"tokens_per_sec"`
	TTFTms        int     `json:"ttft_ms"`
	ContextTokens int     `json:"context_tokens"`
	DurationMS    int     `json:"duration_ms"`
	UserPick      *bool   `json:"user_pick,omitempty"`
	Notes         string  `json:"notes,omitempty"`
	Error         string  `json:"error,omitempty"`
}

type Runner struct {
	Client Client
	Clock  ports.Clock
}

func (r Runner) Run(ctx context.Context, req Request) ([]Result, error) {
	if r.Client == nil {
		return nil, fmt.Errorf("bench client is not configured")
	}
	if r.Clock == nil {
		return nil, fmt.Errorf("bench clock is not configured")
	}
	if req.Prompt == "" {
		return nil, fmt.Errorf("prompt is required")
	}
	if len(req.Models) == 0 {
		return nil, fmt.Errorf("at least one model is required")
	}
	if req.OutputDir == "" {
		return nil, fmt.Errorf("output dir is required")
	}
	if err := os.MkdirAll(req.OutputDir, 0755); err != nil {
		return nil, err
	}
	results := make([]Result, 0, len(req.Models))
	used := map[string]int{}
	for _, model := range req.Models {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		start := r.Clock.Now()
		completion, err := r.Client.Complete(ctx, model, req.Prompt)
		result := Result{Model: model, DurationMS: elapsedMS(r.Clock.Now().Sub(start))}
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}
		name := uniqueName(safeName(model)+".txt", used)
		path := filepath.Join(req.OutputDir, name)
		if err := os.WriteFile(path, []byte(completion.Text), 0644); err != nil {
			return results, err
		}
		result.OutputPath = path
		result.Bytes = len(completion.Text)
		result.TokensPerSec = completion.TokensPerSec
		result.TTFTms = completion.TTFTms
		result.ContextTokens = completion.ContextTokens
		results = append(results, result)
	}
	if err := writeMetrics(req.OutputDir, results); err != nil {
		return results, err
	}
	return results, nil
}

func writeMetrics(outputDir string, results []Result) error {
	path := filepath.Join(outputDir, "metrics.json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func elapsedMS(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d / time.Millisecond)
}

var unsafeName = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

func safeName(model string) string {
	name := strings.Trim(unsafeName.ReplaceAllString(model, "-"), "-")
	if name == "" {
		return "model"
	}
	return name
}

func uniqueName(name string, used map[string]int) string {
	count := used[name]
	used[name] = count + 1
	if count == 0 {
		return name
	}
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	return fmt.Sprintf("%s-%d%s", base, count+1, ext)
}
