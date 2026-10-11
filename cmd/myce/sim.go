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
	fleet := f.String("fleet", "examples/sim/fleet.yaml", "fleet YAML")
	workload := f.String("workload", "examples/sim/workload.yaml", "workload YAML")
	days := f.Float64("days", 7, "simulated days (maximum 90)")
	seed := f.Int64("seed", 1, "deterministic seed")
	output := f.String("json", "sim-results.json", "JSON output path")
	whatif := f.String("what-if", "", "comparison: split-batch")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("sim accepts flags only")
	}
	if *whatif != "" && *whatif != "split-batch" {
		return errors.New("what-if must be split-batch")
	}
	var fleetConfig myce.SimFleet
	var workConfig myce.SimWorkload
	if e := myce.ReadSimYAML(*fleet, &fleetConfig); e != nil {
		return e
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
	baseline, e := myce.Simulate(fleetConfig, workConfig, opts)
	if e != nil {
		return e
	}
	myce.PrintSim(out, baseline)
	var result any = baseline
	if *whatif != "" {
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
	return os.WriteFile(*output, append(data, '\n'), 0600)
}
