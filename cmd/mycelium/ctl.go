package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
	storesqlite "mycelium/internal/store/sqlite"
)

func runControl(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce <add-model|models|nodes|projects|jobs|recommendations>")
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
	result, err := catalog.NewInstaller(*store).Install(ctx, catalog.InstallRequest{
		Source:        fs.Arg(0),
		ID:            *id,
		Model:         *model,
		ContextLength: *contextLen,
		Quant:         *quant,
		Backend:       domain.BackendLlamaCpp,
	})
	if err != nil {
		return err
	}
	control, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer control.Close()
	if err := control.SavePreset(ctx, result.Preset); err != nil {
		return err
	}
	for _, event := range result.Progress {
		fmt.Printf("%s\t%s\n", event.Stage, event.Message)
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
	priority := fs.String("priority", string(domain.PriorityInteractive), "priority")
	speed := fs.String("speed-pref", string(domain.SpeedThroughput), "speed preference")
	contextCap := fs.Int("context-cap", 0, "context cap")
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
		ID:         *id,
		Priority:   domain.Priority(*priority),
		SpeedPref:  domain.SpeedPref(*speed),
		ContextCap: *contextCap,
		Preemption: domain.Preemption(*preemption),
		AutoApply:  *autoApply,
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
		fmt.Printf("%s\t%s\t%s\t%s\n", job.ID, job.Project, job.Model, job.Status)
	}
	return nil
}

func runRecommendations(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce recommendations <list|apply>")
	}
	switch args[0] {
	case "list":
		return runRecommendationsList(ctx, args[1:])
	case "apply":
		return runRecommendationsApply(ctx, args[1:])
	default:
		return fmt.Errorf("unknown recommendations command %q", args[0])
	}
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
		fmt.Printf("%s\t%s\t%s\t%d\t%t\n", rec.ID, rec.ProjectID, rec.Type, rec.RecommendedValue, rec.Applied)
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
	if err := store.MarkRecommendationApplied(ctx, *id, time.Now().UTC()); err != nil {
		return err
	}
	fmt.Printf("recommendation\t%s\tapplied\n", *id)
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
