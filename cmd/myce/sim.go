package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"mycelium/internal/myce"
)

func simCommand(args []string, out io.Writer) error {
	f := flag.NewFlagSet("sim", flag.ContinueOnError)
	hostConfig := f.String("host-config", "", "physical-host production hold policy YAML (overrides fleet hosts)")
	fleet := f.String("fleet", "examples/sim/fleet.yaml", "fleet YAML")
	workload := f.String("workload", "examples/sim/workload.yaml", "workload YAML")
	days := f.Float64("days", 7, "simulated days (maximum 90)")
	seed := f.Int64("seed", 1, "deterministic seed")
	output := f.String("json", "sim-results.json", "JSON output path")
	pipelineScenario := f.String("pipeline-scenario", "", "per-piece qualification: homogeneous, heterogeneous-tail, separate-stage, shared-stage, mixed-production")
	whatif := f.String("what-if", "", "comparison: split-batch, declared-batch or per-piece")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("sim accepts flags only")
	}
	if *whatif != "" && *whatif != "split-batch" && *whatif != "declared-batch" && *whatif != "per-piece" {
		return errors.New("what-if must be split-batch, declared-batch or per-piece")
	}
	if *pipelineScenario != "" {
		fleet, work, opts, err := myce.PipelineScenario(*pipelineScenario)
		if err != nil {
			return err
		}
		opts.Seed = *seed
		return runPipelineComparison(out, *output, fleet, work, opts, *pipelineScenario == "homogeneous")
	}
	var fleetConfig myce.SimFleet
	var workConfig myce.SimWorkload
	if e := myce.ReadSimYAML(*fleet, &fleetConfig); e != nil {
		return e
	}
	if *hostConfig != "" {
		hosts, e := myce.ReadHostPolicies(*hostConfig)
		if e != nil {
			return e
		}
		fleetConfig.Hosts = hosts
	}
	if e := myce.ReadSimYAML(*workload, &workConfig); e != nil {
		return e
	}
	if *whatif != "" {
		marked := false
		for _, stream := range workConfig.Streams {
			marked = marked || stream.Batch > 1
		}
		if !marked {
			return errors.New("split-batch requires a stream with batch_files > 1")
		}
	}
	opts := myce.SimOptions{Days: *days, Seed: *seed}
	if *whatif == "per-piece" {
		return runPipelineComparison(out, *output, fleetConfig, workConfig, opts, false)
	}

	baseline, e := myce.Simulate(fleetConfig, workConfig, opts)
	if e != nil {
		return e
	}
	myce.PrintSim(out, baseline)
	var result any = baseline
	gateFailed := false
	if *whatif == "declared-batch" {
		opts.Split = true
		naive, err := myce.Simulate(fleetConfig, workConfig, opts)
		if err != nil {
			return err
		}
		opts.Split = false
		opts.Declared = true
		declared, err := myce.Simulate(fleetConfig, workConfig, opts)
		if err != nil {
			return err
		}
		fmt.Fprintln(out, "\nNaive split:")
		myce.PrintSim(out, naive)
		fmt.Fprintln(out, "\nDeclared fan-out (real queue planner):")
		myce.PrintSim(out, declared)
		paired, gain, worstRegression := 0, 0.0, 0.0
		keys := make([]string, 0, len(baseline.Batches))
		for key := range baseline.Batches {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			one, many := baseline.Batches[key], declared.Batches[key]
			if one.FinishedSeconds != nil && many.FinishedSeconds != nil && one.SubmittedSeconds == many.SubmittedSeconds {
				paired++
				difference := *one.FinishedSeconds - *many.FinishedSeconds
				gain += difference
				worstRegression = max(worstRegression, -difference)
			}
		}
		if paired > 0 {
			gain /= float64(paired)
		}
		delta := declared.Production.P95 - baseline.Production.P95
		passed := paired > 0 && paired == len(baseline.Batches) && paired == len(declared.Batches) && baseline.BatchUnfinished == 0 && declared.BatchUnfinished == 0 && declared.Failed == baseline.Failed && gain > 0 && delta == 0 && declared.Production.Started == baseline.Production.Started && declared.Production.Unstarted == baseline.Production.Unstarted && declared.BatchMeanSeconds < naive.BatchMeanSeconds && declared.Production.P95 == naive.Production.P95 && declared.Failed == naive.Failed
		gateFailed = !passed
		fmt.Fprintf(out, "\nDeclared fan-out gate: %t; batch makespan mean gain %.1f s; production p95 delta %+.2f s.\n", passed, gain, delta)
		result = struct {
			Baseline        myce.SimReport `json:"baseline"`
			Naive           myce.SimReport `json:"naive_split"`
			Declared        myce.SimReport `json:"declared"`
			GatePassed      bool           `json:"gate_passed"`
			Gain            float64        `json:"batch_seconds_sooner"`
			ProductionDelta float64        `json:"production_p95_delta_seconds"`
			Paired          int            `json:"paired_completed_batches"`
			WorstRegression float64        `json:"worst_batch_regression_seconds"`
		}{baseline, naive, declared, passed, gain, delta, paired, worstRegression}
	}
	if *whatif == "split-batch" {
		opts.Split = true
		split, e := myce.Simulate(fleetConfig, workConfig, opts)
		if e != nil {
			return e
		}
		fmt.Fprintln(out, "\nSplit into per-file jobs:")
		myce.PrintSim(out, split)
		keys := []string{}
		for k := range baseline.Batches {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		paired := 0
		sooner := 0.0
		for _, k := range keys {
			one := baseline.Batches[k]
			many := split.Batches[k]
			if one.FinishedSeconds != nil && many.FinishedSeconds != nil {
				paired++
				sooner += *one.FinishedSeconds - *many.FinishedSeconds
			}
		}
		if paired > 0 {
			sooner /= float64(paired)
		}
		fmt.Fprintf(out, "\n%d paired completed batches finish %.1f s sooner on average; production p95 changes %+.2f s.\n", paired, sooner, split.Production.P95-baseline.Production.P95)
		result = struct {
			Baseline                  myce.SimReport `json:"baseline"`
			Split                     myce.SimReport `json:"split"`
			Paired                    int            `json:"paired_completed_batches"`
			BatchSecondsSooner        float64        `json:"batch_seconds_sooner"`
			ProductionP95DeltaSeconds float64        `json:"production_p95_delta_seconds"`
		}{baseline, split, paired, sooner, split.Production.P95 - baseline.Production.P95}
	}
	data, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(*output, append(data, '\n'), 0600); e != nil {
		return e
	}
	if gateFailed {
		return errors.New("declared fan-out simulator gate failed; see comparison JSON")
	}
	return nil
}

func runPipelineComparison(out io.Writer, output string, fleet myce.SimFleet, work myce.SimWorkload, opts myce.SimOptions, control bool) error {
	comparison, err := myce.ComparePieceDependencies(fleet, work, opts)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(comparison, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(output, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Fprintf(out, "Per-piece gate: %t; coverage %t; earlier start %t; mean gain %.3fs; production p95 delta %+.3fs; worst regression %.3fs\n", comparison.GatePassed, comparison.Coverage, comparison.Overlap, comparison.MeanGain, comparison.ProductionDelta, comparison.WorstRegression)
	if control {
		a, b := comparison.Baseline, comparison.PerPiece
		if comparison.Coverage && comparison.MeanGain == 0 && comparison.ProductionDelta == 0 && a.Failed == 0 && b.Failed == 0 && a.Unfinished == 0 && b.Unfinished == 0 {
			return nil
		}
		return errors.New("homogeneous no-overlap control failed")
	}
	if !comparison.GatePassed {
		return errors.New("per-piece comparison gate failed")
	}
	return nil
}
