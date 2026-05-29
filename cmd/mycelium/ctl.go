package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
)

func runControl(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce <add-model>")
	}
	switch args[0] {
	case "add-model":
		return runAddModel(ctx, args[1:])
	default:
		return fmt.Errorf("unknown myce command %q", args[0])
	}
}

func runAddModel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("add-model", flag.ContinueOnError)
	store := fs.String("store", defaultCatalogStore(), "catalog store directory")
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
	for _, event := range result.Progress {
		fmt.Printf("%s\t%s\n", event.Stage, event.Message)
	}
	fmt.Printf("preset\t%s\t%s\n", result.Preset.ID, result.Preset.ModelRef)
	return nil
}

func defaultCatalogStore() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".mycelium/catalog"
	}
	return filepath.Join(home, ".mycelium", "catalog")
}
