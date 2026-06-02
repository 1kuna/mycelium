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

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/internal/trace"
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
	Model         string       `json:"model"`
	OutputPath    string       `json:"output_path"`
	Bytes         int          `json:"bytes"`
	TokensPerSec  float64      `json:"tokens_per_sec"`
	TTFTms        int          `json:"ttft_ms"`
	ContextTokens int          `json:"context_tokens"`
	DurationMS    int          `json:"duration_ms"`
	UserPick      *bool        `json:"user_pick,omitempty"`
	Notes         string       `json:"notes,omitempty"`
	Error         string       `json:"error,omitempty"`
	Trace         []trace.Step `json:"trace,omitempty"`
}

type Runner struct {
	Client Client
	Clock  ports.Clock
	Store  JobStore
}

type JobStore interface {
	SaveJob(ctx context.Context, job domain.Job) error
}

func (r Runner) Run(ctx context.Context, req Request) ([]Result, error) {
	return r.run(ctx, req, domain.Job{})
}

func (r Runner) RunJob(ctx context.Context, parent domain.Job) ([]Result, error) {
	if parent.ID == "" {
		return nil, fmt.Errorf("benchmark parent job id is required")
	}
	if parent.Benchmark == nil {
		return nil, fmt.Errorf("benchmark spec is required")
	}
	if r.Store == nil {
		return nil, fmt.Errorf("benchmark job store is not configured")
	}
	if parent.TaskType == "" {
		parent.TaskType = "benchmark"
	}
	parent.Status = domain.JobRunning
	if err := r.Store.SaveJob(ctx, parent); err != nil {
		return nil, err
	}
	req := Request{
		Prompt:    parent.Benchmark.Prompt,
		Models:    append([]string(nil), parent.Benchmark.Models...),
		OutputDir: parent.Benchmark.OutputDir,
	}
	results, err := r.run(ctx, req, parent)
	if err != nil {
		parent.Status = domain.JobFailed
		parent.Error = err.Error()
		_ = r.Store.SaveJob(ctx, parent)
		return results, err
	}
	parent.Status = domain.JobDone
	parent.Error = ""
	if err := r.Store.SaveJob(ctx, parent); err != nil {
		return results, err
	}
	return results, nil
}

func (r Runner) run(ctx context.Context, req Request, parent domain.Job) ([]Result, error) {
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
	usedChildIDs := map[string]int{}
	for _, model := range req.Models {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		child, hasChild := r.childJob(parent, model, usedChildIDs)
		if hasChild {
			child.Status = domain.JobRunning
			if err := r.Store.SaveJob(ctx, child); err != nil {
				return results, err
			}
		}
		tr := trace.New(r.Clock.Now)
		start := r.Clock.Now()
		var completion Completion
		err := tr.Do("benchmark/complete", map[string]any{"model": model}, func() error {
			var err error
			completion, err = r.Client.Complete(ctx, model, req.Prompt)
			return err
		})
		result := Result{Model: model, DurationMS: elapsedMS(r.Clock.Now().Sub(start)), Trace: append([]trace.Step(nil), tr.Steps...)}
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			if hasChild {
				child.Status = domain.JobFailed
				child.Error = err.Error()
				if err := r.Store.SaveJob(ctx, child); err != nil {
					return results, err
				}
			}
			continue
		}
		name := uniqueName(safeName(model)+".txt", used)
		path := filepath.Join(req.OutputDir, name)
		if err := tr.Do("benchmark/write_output", map[string]any{"model": model, "path": path}, func() error {
			return os.WriteFile(path, []byte(completion.Text), 0644)
		}); err != nil {
			if hasChild {
				child.Status = domain.JobFailed
				child.Error = err.Error()
				_ = r.Store.SaveJob(ctx, child)
			}
			return results, err
		}
		result.Trace = append([]trace.Step(nil), tr.Steps...)
		result.OutputPath = path
		result.Bytes = len(completion.Text)
		result.TokensPerSec = completion.TokensPerSec
		result.TTFTms = completion.TTFTms
		result.ContextTokens = completion.ContextTokens
		results = append(results, result)
		if hasChild {
			child.Status = domain.JobDone
			child.Progress = []domain.JobProgress{{Stage: "output", Message: path, At: r.Clock.Now()}}
			if err := r.Store.SaveJob(ctx, child); err != nil {
				return results, err
			}
		}
	}
	if err := writeMetrics(req.OutputDir, results); err != nil {
		return results, err
	}
	return results, nil
}

func (r Runner) childJob(parent domain.Job, model string, used map[string]int) (domain.Job, bool) {
	if parent.ID == "" {
		return domain.Job{}, false
	}
	return domain.Job{
		ID:        parent.ID + "-" + uniqueName(safeName(model), used),
		TaskType:  "benchmark_child",
		Model:     model,
		Project:   parent.Project,
		Priority:  domain.PriorityBackground,
		SpeedPref: parent.SpeedPref,
		ParentID:  parent.ID,
	}, true
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
