package controlcli

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"mycelium/internal/bench"
	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/optimizer"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/pkg/api"
)

func Run(ctx context.Context, args []string) error {
	return RunWithClient(ctx, args, http.DefaultClient)
}

func RunWithClient(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce <add-model|models|nodes|projects|jobs|recommendations|benchmark>")
	}
	switch args[0] {
	case "add-model":
		return runAddModel(ctx, args[1:])
	case "models":
		return runModels(ctx, args[1:])
	case "nodes":
		return runNodes(ctx, args[1:])
	case "projects":
		return runProjects(ctx, args[1:])
	case "jobs":
		return runJobs(ctx, args[1:])
	case "recommendations":
		return runRecommendations(ctx, args[1:])
	case "benchmark":
		return runBenchmark(ctx, args[1:], client)
	default:
		return fmt.Errorf("unknown myce command %q", args[0])
	}
}

func runAddModel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("add-model", flag.ContinueOnError)
	store := fs.String("store", defaultCatalogStore(), "catalog store directory")
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "preset id")
	model := fs.String("model", "", "logical model name")
	contextLen := fs.Int("context", 2048, "preset context length")
	quant := fs.String("quant", "unknown", "preset quantization label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: myce add-model [flags] <source>")
	}
	req := catalog.InstallRequest{
		Source:        fs.Arg(0),
		ID:            *id,
		Model:         *model,
		ContextLength: *contextLen,
		Quant:         *quant,
		Backend:       domain.BackendLlamaCpp,
	}
	control, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer control.Close()
	job := domain.Job{
		ID:       catalog.InstallJobID(req),
		TaskType: "catalog_install",
		Model:    req.Source,
		PresetID: req.ID,
		Status:   domain.JobQueued,
	}
	if err := control.SaveJob(ctx, job); err != nil {
		return err
	}
	fmt.Printf("job\t%s\tstarted\n", job.ID)
	result, err := catalog.NewInstaller(*store).InstallWithProgress(ctx, req, func(event catalog.ProgressEvent, state catalog.InstallState) error {
		job.Status = installStageStatus(event.Stage)
		job.PresetID = state.PresetID
		job.Progress = append(job.Progress, domain.JobProgress{Stage: event.Stage, Message: event.Message, At: event.At})
		if err := control.SaveJob(ctx, job); err != nil {
			return err
		}
		fmt.Printf("job\t%s\t%s\t%s\n", job.ID, event.Stage, event.Message)
		return nil
	})
	if err != nil {
		job.Status = domain.JobFailed
		job.Error = err.Error()
		_ = control.SaveJob(ctx, job)
		return err
	}
	if err := control.SavePreset(ctx, result.Preset); err != nil {
		job.Status = domain.JobFailed
		job.Error = err.Error()
		_ = control.SaveJob(ctx, job)
		return err
	}
	job.Status = domain.JobDone
	job.PresetID = result.Preset.ID
	if err := control.SaveJob(ctx, job); err != nil {
		return err
	}
	fmt.Printf("preset\t%s\t%s\n", result.Preset.ID, result.Preset.ModelRef)
	return nil
}

func runModels(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return fmt.Errorf("usage: myce models list [--db path]")
	}
	store, err := openCLIStore(args[1:])
	if err != nil {
		return err
	}
	defer store.Close()
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return err
	}
	for _, preset := range presets {
		fmt.Printf("%s\t%s\t%s\t%d\n", preset.ID, preset.ModelRef, preset.Backend, preset.ContextLength)
	}
	return nil
}

func runNodes(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return fmt.Errorf("usage: myce nodes list [--db path]")
	}
	store, err := openCLIStore(args[1:])
	if err != nil {
		return err
	}
	defer store.Close()
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		fmt.Printf("%s\t%s\t%s\t%s\n", node.ID, node.Name, node.Address, node.Status)
	}
	return nil
}

func runProjects(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "set" {
		return fmt.Errorf("usage: myce projects set --id id [--db path]")
	}
	fs := flag.NewFlagSet("projects set", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "project id")
	defaultModel := fs.String("default-model", "", "default model or preset id")
	priority := fs.String("priority", string(domain.PriorityInteractive), "priority")
	speed := fs.String("speed-pref", string(domain.SpeedThroughput), "speed preference")
	contextCap := fs.Int("context-cap", 0, "context cap")
	expectedConcurrency := fs.Int("expected-concurrency", 1, "expected concurrent requests for resource estimates")
	latencyTarget := fs.Int("latency-target-ms", 0, "latency target in milliseconds")
	preemption := fs.String("preemption", string(domain.PreemptSoft), "preemption mode")
	autoApply := fs.Bool("auto-apply", false, "enable optimizer auto-apply")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	project := domain.Project{
		ID:                  *id,
		DefaultModel:        *defaultModel,
		Priority:            domain.Priority(*priority),
		SpeedPref:           domain.SpeedPref(*speed),
		ContextCap:          *contextCap,
		ExpectedConcurrency: *expectedConcurrency,
		LatencyTargetMS:     *latencyTarget,
		Preemption:          domain.Preemption(*preemption),
		AutoApply:           *autoApply,
	}
	if err := store.SaveProject(ctx, project); err != nil {
		return err
	}
	fmt.Printf("project\t%s\n", project.ID)
	return nil
}

func runJobs(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return fmt.Errorf("usage: myce jobs list [--db path]")
	}
	store, err := openCLIStore(args[1:])
	if err != nil {
		return err
	}
	defer store.Close()
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\n", job.ID, job.TaskType, job.Project, job.Model, job.Status, jobProgressSummary(job))
	}
	return nil
}

func runBenchmark(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 || args[0] != "run" {
		return fmt.Errorf("usage: myce benchmark run --url gateway --prompt prompt --model id [--model id] --out dir [--db path]")
	}
	fs := flag.NewFlagSet("benchmark run", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	url := fs.String("url", "", "Mycelium gateway URL")
	prompt := fs.String("prompt", "", "benchmark prompt")
	out := fs.String("out", "", "output directory")
	id := fs.String("id", "", "parent benchmark job id")
	project := fs.String("project", "", "project id")
	var models repeatedString
	fs.Var(&models, "model", "model or preset id; may be repeated")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *url == "" {
		return fmt.Errorf("--url is required")
	}
	if *prompt == "" {
		return fmt.Errorf("--prompt is required")
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	if len(models) == 0 {
		return fmt.Errorf("--model is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	jobID := *id
	if jobID == "" {
		jobID = fmt.Sprintf("benchmark-%d", clock.System{}.Now().UnixNano())
	}
	parent := domain.Job{
		ID:       jobID,
		TaskType: "benchmark",
		Project:  *project,
		Priority: domain.PriorityBackground,
		Status:   domain.JobQueued,
		Benchmark: &domain.BenchmarkSpec{
			Prompt:    *prompt,
			Models:    append([]string(nil), models...),
			OutputDir: *out,
		},
	}
	runner := bench.Runner{
		Client: benchmarkGatewayClient{BaseURL: *url, Client: client},
		Clock:  clock.System{},
		Store:  store,
	}
	fmt.Printf("benchmark\t%s\tstarted\n", parent.ID)
	results, err := runner.RunJob(ctx, parent)
	for _, result := range results {
		if result.Error != "" {
			fmt.Printf("benchmark-result\t%s\terror\t%s\n", result.Model, result.Error)
			continue
		}
		fmt.Printf("benchmark-result\t%s\t%s\n", result.Model, result.OutputPath)
	}
	if err != nil {
		return err
	}
	fmt.Printf("benchmark\t%s\tdone\t%s\n", parent.ID, *out)
	return nil
}

type repeatedString []string

func (r *repeatedString) String() string {
	return strings.Join(*r, ",")
}

func (r *repeatedString) Set(value string) error {
	if value == "" {
		return fmt.Errorf("model must not be empty")
	}
	*r = append(*r, value)
	return nil
}

type benchmarkGatewayClient struct {
	BaseURL string
	Client  *http.Client
}

func (c benchmarkGatewayClient) Complete(ctx context.Context, model, prompt string) (bench.Completion, error) {
	body, err := json.Marshal(api.OpenAIChatRequest{
		Model: model,
		Messages: []api.OpenAIMessage{{
			Role:    "user",
			Content: prompt,
		}},
	})
	if err != nil {
		return bench.Completion{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return bench.Completion{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return bench.Completion{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return bench.Completion{}, err
	}
	if resp.StatusCode >= 400 {
		return bench.Completion{}, fmt.Errorf("gateway returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var chat api.OpenAIChatResponse
	if err := json.Unmarshal(data, &chat); err != nil {
		return bench.Completion{}, err
	}
	if len(chat.Choices) == 0 {
		return bench.Completion{}, fmt.Errorf("gateway response had no choices")
	}
	return bench.Completion{
		Text:          chat.Choices[0].Message.Content,
		ContextTokens: chat.Usage.TotalTokens,
	}, nil
}

func installStageStatus(_ string) domain.JobStatus {
	return domain.JobRunning
}

func jobProgressSummary(job domain.Job) string {
	if job.Error != "" {
		return job.Error
	}
	if len(job.Progress) == 0 {
		return "-"
	}
	last := job.Progress[len(job.Progress)-1]
	if last.Message == "" {
		return last.Stage
	}
	return last.Stage + ":" + last.Message
}

func runRecommendations(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce recommendations <generate|list|apply|calibrate-speed>")
	}
	switch args[0] {
	case "generate":
		return runRecommendationsGenerate(ctx, args[1:])
	case "list":
		return runRecommendationsList(ctx, args[1:])
	case "apply":
		return runRecommendationsApply(ctx, args[1:])
	case "calibrate-speed":
		return runRecommendationsCalibrateSpeed(ctx, args[1:])
	default:
		return fmt.Errorf("unknown recommendations command %q", args[0])
	}
}

func runRecommendationsGenerate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations generate", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	projectID := fs.String("project", "", "project id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *projectID == "" {
		return fmt.Errorf("--project is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	project, err := store.Project(ctx, *projectID)
	if err != nil {
		return err
	}
	service := optimizer.RecommendationService{Store: store, Clock: clock.System{}}
	records, err := service.EvaluateProject(ctx, project)
	if err != nil {
		return err
	}
	for _, rec := range records {
		fmt.Printf("%s\t%s\t%s\t%s\t%t\n", rec.ID, rec.ProjectID, rec.Type, recommendationTarget(rec), rec.Applied)
	}
	return nil
}

func runRecommendationsList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations list", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	project := fs.String("project", "", "project id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	recs, err := store.ListRecommendations(ctx, *project)
	if err != nil {
		return err
	}
	for _, rec := range recs {
		fmt.Printf("%s\t%s\t%s\t%s\t%t\n", rec.ID, rec.ProjectID, rec.Type, recommendationTarget(rec), rec.Applied)
	}
	return nil
}

func runRecommendationsApply(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations apply", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "recommendation id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	rec, err := store.Recommendation(ctx, *id)
	if err != nil {
		return err
	}
	if rec.Rejected {
		return fmt.Errorf("recommendation %q was rejected: %s", rec.ID, rec.RejectReason)
	}
	switch rec.Type {
	case optimizer.RecommendationContextCap:
		project, err := store.Project(ctx, rec.ProjectID)
		if err != nil {
			return err
		}
		if rec.PresetID == "" {
			return fmt.Errorf("recommendation %q has no preset to apply", rec.ID)
		}
		preset, err := store.Preset(ctx, rec.PresetID)
		if err != nil {
			return err
		}
		forced := project
		forced.AutoApply = true
		applied := optimizer.ApplyRecommendation(forced, preset, optimizer.Recommendation{
			Type:           rec.Type,
			ProjectID:      rec.ProjectID,
			CurrentCap:     rec.CurrentValue,
			RecommendedCap: rec.RecommendedValue,
			Rationale:      rec.Rationale,
		})
		if !applied.Applied {
			return fmt.Errorf("recommendation %q was not applied: %s", rec.ID, applied.Log.Result)
		}
		applied.Project.AutoApply = project.AutoApply
		if err := store.SaveProject(ctx, applied.Project); err != nil {
			return err
		}
		if err := store.SavePreset(ctx, applied.Preset); err != nil {
			return err
		}
	case optimizer.RecommendationEngineParameter:
		if rec.RecommendedPresetID == "" {
			return fmt.Errorf("engine recommendation %q has no preset to apply", rec.ID)
		}
		project, err := store.Project(ctx, rec.ProjectID)
		if err != nil {
			return err
		}
		project.DefaultModel = rec.RecommendedPresetID
		if err := store.SaveProject(ctx, project); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown recommendation type %q", rec.Type)
	}
	if err := store.MarkRecommendationApplied(ctx, *id, clock.System{}.Now().UTC()); err != nil {
		return err
	}
	fmt.Printf("recommendation\t%s\tapplied\n", *id)
	return nil
}

func recommendationTarget(rec domain.RecommendationRecord) string {
	if rec.RecommendedPresetID != "" {
		return rec.RecommendedPresetID
	}
	if rec.RecommendedValue != 0 {
		return fmt.Sprint(rec.RecommendedValue)
	}
	return "-"
}

func runRecommendationsCalibrateSpeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations calibrate-speed", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	nodes, err := optimizer.CalibrateSpeedClasses(ctx, store, clock.System{})
	if err != nil {
		return err
	}
	for _, node := range nodes {
		fmt.Printf("%s\t%.2f\t%s\n", node.ID, node.SpeedClass.TokensPerSecRef, node.SpeedClass.Source)
	}
	return nil
}

func openCLIStore(args []string) (*storesqlite.Store, error) {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return storesqlite.Open(*dbPath)
}

func defaultCatalogStore() string {
	return filepath.Join(defaultMyceliumHome(), "catalog")
}

func defaultControlStorePath() string {
	return filepath.Join(defaultMyceliumHome(), "mycelium.db")
}

func defaultMyceliumHome() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".mycelium"
	}
	return filepath.Join(home, ".mycelium")
}
