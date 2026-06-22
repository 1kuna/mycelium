package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"mycelium/internal/modelcompat"
)

type repeatedString []string

func (v *repeatedString) String() string {
	data, _ := json.Marshal([]string(*v))
	return string(data)
}

func (v *repeatedString) Set(value string) error {
	*v = append(*v, value)
	return nil
}

func runModels(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("models subcommand is required: compat")
	}
	switch args[0] {
	case "compat":
		return runModelsCompat(ctx, args[1:])
	default:
		return fmt.Errorf("unknown models subcommand %q", args[0])
	}
}

func runModelsCompat(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("models compat", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	dbPath := fs.String("db", "", "control store path")
	model := fs.String("model", "", "logical model name")
	jsonOut := fs.Bool("json", false, "write JSON")
	var artifacts repeatedString
	var formats repeatedString
	fs.Var(&artifacts, "artifact", "model artifact path or Hugging Face ref; repeatable")
	fs.Var(&formats, "format", "artifact format; repeatable, paired with --artifact")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(artifacts) == 0 {
		return fmt.Errorf("models compat requires at least one --artifact")
	}
	if len(artifacts) != len(formats) {
		return fmt.Errorf("models compat requires one --format for each --artifact")
	}
	store, err := openEngineStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	plans, err := store.ListBootstrapPlans(ctx)
	if err != nil {
		return err
	}
	profiles, err := store.ListEngineProfiles(ctx)
	if err != nil {
		return err
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return err
	}
	specs := make([]modelcompat.Artifact, 0, len(artifacts))
	for i := range artifacts {
		specs = append(specs, modelcompat.Artifact{Ref: artifacts[i], Format: formats[i]})
	}
	report, err := modelcompat.Advise(modelcompat.Request{
		Model:         *model,
		Artifacts:     specs,
		Plans:         plans,
		Profiles:      profiles,
		LegacyPresets: presets,
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	printModelCompatibilityReport(report)
	return nil
}

func printModelCompatibilityReport(report modelcompat.Report) {
	fmt.Printf("model-compat\t%s\tartifacts=%d\trows=%d\n", report.Model, len(report.Artifacts), len(report.Rows))
	for _, artifact := range report.Artifacts {
		fmt.Printf("artifact\t%s\t%s\t%s\n", artifact.Ref, artifact.Format, artifact.Scope)
	}
	for _, row := range report.Rows {
		fmt.Printf("compat\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.HostID,
			row.HostPlatform,
			row.ArtifactRef,
			row.ArtifactFormat,
			row.Backend,
			row.EngineFamily,
			row.Source,
			row.Status,
			row.EngineProfileID,
			row.NeededFormat,
			row.Multimodal,
			row.Reason)
	}
}
