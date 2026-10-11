package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// runMeasure runs only on the selected worker; it never dispatches remote work.
func runMeasure(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	f := flag.NewFlagSet("measure", flag.ContinueOnError)
	f.SetOutput(stderr)
	config := f.String("model-config", "", "model catalog")
	envConfig := f.String("environment-config", "", "environment catalog")
	models := f.String("models", "", "comma-separated catalog names, in loading order")
	worker := f.String("worker", "", "local worker identity (same as join --name)")
	device := f.String("device", "", "one CUDA ID or native XPU index")
	platform := f.String("platform", "", "cuda or xpu")
	budget := f.Float64("memory-gb", 0, "explicit absolute device usage budget in GiB")
	state := f.String("state-dir", "", "same spool directory used by join")
	root := f.String("model-root", "", "local NAS model root")
	cache := f.String("model-cache", "", "local model cache (default worker spool/model-cache)")
	envCache := f.String("environment-cache", "", "local environment cache (default worker spool/environment-cache)")
	prefix := f.String("report-prefix", "", "write PREFIX.json and PREFIX.txt (default unique myce-measure ID)")
	output := f.String("output", "plain", "plain or json report to stdout")
	warmup := f.String("warmup", "", "JSON file mapping model name to method/path/body/headers")
	parallel := f.Int("warmup-parallel", 1, "maximum concurrent warm-ups (greater than one warms after all models load)")
	loadTimeout := f.Duration("load-timeout", 5*time.Minute, "per-model measurement load timeout")
	force := f.Bool("force", false, "bypass production refusal during approved maintenance; never bypass spool lock")
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue URL (must match join)")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *parallel <= 0 || *loadTimeout <= 0 {
		return errors.New("--warmup-parallel and --load-timeout must be positive")
	}
	if f.NArg() != 0 || *config == "" || *models == "" || *worker == "" || *device == "" || *platform == "" || *budget <= 0 {
		return errors.New("measure requires --model-config --models --worker --device --platform --memory-gb; run locally with join stopped and its exact --queue/--state-dir")
	}
	if *output != "plain" && *output != "json" {
		return errors.New("--output must be plain or json")
	}
	catalog, e := myce.ReadModels(*config)
	if e != nil {
		return e
	}
	ec, e := myce.ReadEnvironments(*envConfig)
	if e != nil {
		return e
	}
	var selected []myce.Model
	seen := map[string]bool{}
	for _, name := range strings.Split(*models, ",") {
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			return errors.New("--models requires distinct nonempty names")
		}
		seen[name] = true
		found := false
		for _, m := range catalog.Models {
			if m.Name == name {
				selected = append(selected, m)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown model %q", name)
		}
	}
	warms := map[string]myce.MeasureWarmup{}
	if *warmup != "" {
		file, e := os.Open(*warmup)
		if e != nil {
			return e
		}
		d := json.NewDecoder(file)
		d.DisallowUnknownFields()
		e = d.Decode(&warms)
		if e == nil {
			var extra any
			if d.Decode(&extra) != io.EOF {
				e = errors.New("warmup file requires one JSON object")
			}
		}
		file.Close()
		if e != nil {
			return e
		}
		for name := range warms {
			if !seen[name] {
				return fmt.Errorf("warmup names unselected model %q", name)
			}
		}
	}
	w := myce.Worker{StateDir: *state, Client: myce.Client{URL: *queue, Token: env("MYCE_TOKEN", "")}, Machine: myce.Machine{ID: *worker, GPU: *platform}, Clock: myce.RealClock{}}
	// Cache defaults are resolved from join's store by Prepare below; opening it
	// separately here would make a race between lock acquisition and preparation.
	var mc *myce.ModelCache
	var environments *myce.EnvironmentCache
	if *prefix == "" {
		*prefix = "myce-measure-" + myce.ID()
	}
	jf, err := os.OpenFile(*prefix+".json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer jf.Close()
	tf, err := os.OpenFile(*prefix+".txt", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer tf.Close()
	report, e := myce.Measure(ctx, myce.MeasureOptions{WarmupParallel: *parallel, LoadTimeout: *loadTimeout, Worker: w, Models: selected, Platform: *platform, Device: *device, BudgetGB: *budget, Force: *force, Warmup: warms,
		Prepare: func(c context.Context, m myce.Model, dir string) (myce.Model, func(), error) {
			if mc == nil {
				// Match join's cache defaults without creating another worker identity.
				cr := *cache
				if cr == "" {
					cr = filepath.Join(dir, "model-cache")
				}
				er := *envCache
				if er == "" {
					er = filepath.Join(dir, "environment-cache")
				}
				mc = &myce.ModelCache{Root: cr, NASRoot: *root, Limit: 100 * 1024 * 1024 * 1024, Clock: w.Clock, Protected: func(string) bool { return true }}
				if err := mc.Initialize(); err != nil {
					return m, nil, err
				}
				environments = &myce.EnvironmentCache{Root: er, Limit: 20 * 1024 * 1024 * 1024, Clock: w.Clock}
			}
			if m.Engine != "fake" {
				path, err := mc.Prepare(c, m)
				if err != nil {
					return m, nil, err
				}
				m.Path = path
			}
			return myce.PrepareModelEnvironment(c, m, *platform, ec, environments)
		}})
	if e != nil {
		report.Error = e.Error()
	}
	var jsonData, plain strings.Builder
	encoder := json.NewEncoder(&jsonData)
	encoder.SetIndent("", "  ")
	e = errors.Join(e, encoder.Encode(report))
	fmt.Fprintf(&plain, "Worker %s device %s/%s; budget %.3f GiB; fits: %t\nBaseline used: device %.3f GiB, host %.3f GiB\nCombined absolute peak: device %.3f GiB, host %.3f GiB\nCombined peak delta from baseline: device %.3f GiB, host %.3f GiB\n", report.Worker, report.Platform, report.Device, report.BudgetGB, report.Fits, report.Baseline.DeviceGB, report.Baseline.HostGB, report.Peak.DeviceGB, report.Peak.HostGB, report.Delta.DeviceGB, report.Delta.HostGB)
	for _, m := range report.Models {
		fmt.Fprintf(&plain, "%s: load %.3fs; warm-up %.3fs; incremental peak delta device %.3f GiB, host %.3f GiB; absolute peak device %.3f GiB, host %.3f GiB\n", m.Model, m.LoadSeconds, m.WarmupSeconds, m.Delta.DeviceGB, m.Delta.HostGB, m.Peak.DeviceGB, m.Peak.HostGB)
	}
	fmt.Fprintf(&plain, "Warm-up phase %.3fs; parallel limit %d; per-model load timeout %.3fs. Concurrent warm-up peaks are joint measurements, not per-model attribution.\n", report.WarmupSeconds, report.WarmupParallel, report.LoadTimeoutSeconds)
	fmt.Fprintf(&plain, "Peaks sampled nominally every %d ms; native probes can be slower and brief spikes may be missed. Incremental measurements depend on load order. Device/host usage includes other processes. Load time excludes cache/environment preparation.\n", report.SampleIntervalMS)
	if e != nil {
		fmt.Fprintf(&plain, "Measurement failed: %v\n", e)
	}
	_, je := io.WriteString(jf, jsonData.String())
	_, te := io.WriteString(tf, plain.String())
	e = errors.Join(e, je, te, jf.Sync(), tf.Sync())
	if *output == "json" {
		_, err = io.WriteString(stdout, jsonData.String())
	} else {
		_, err = io.WriteString(stdout, plain.String())
	}
	fmt.Fprintf(stderr, "Reports: %s.json and %s.txt\n", *prefix, *prefix)
	return errors.Join(e, err)
}
