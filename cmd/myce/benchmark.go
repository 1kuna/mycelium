package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"mycelium/internal/myce"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func benchmarkCommand(args []string) error {
	f := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	state := f.String("state-dir", "", "own local state/scratch folder (required)")
	hostDir := f.String("host-state-dir", "", "shared host state directory")
	root := f.String("extension-root", "", "prepared local extension library/cache")
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "file-store queue")
	gpu := f.String("gpu", "", "accelerator platform (required)")
	device := f.String("device", "", "physical resource selector")
	daily := f.Bool("daily-use", false, "wait for owner idle and safe power/heat; cancel on return")
	laptop := f.Bool("laptop", false, "require external power")
	desktop := f.Bool("desktop", false, "declare a desktop; allow GPU-only thermal coverage on external power")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *desktop && *laptop {
		return errors.New("--desktop and --laptop are mutually exclusive")
	}
	if *state == "" || *gpu == "" || f.NArg() != 0 {
		return errors.New("benchmark requires --state-dir and --gpu")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *hostDir == "" {
		var err error
		*hostDir, err = myce.HostStateDirectory()
		if err != nil {
			return err
		}
	}
	if err := myce.PrepareStateRoots(*state, *hostDir); err != nil {
		return err
	}
	m := myce.Machine{GPU: *gpu, Device: *device}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	profile := myce.LocalProfileProbe().Detect(probe, m, *state, myce.RealClock{}.Now())
	cancel()
	err := myce.RunOnboardingBenchmark(ctx, myce.BenchmarkConfig{HostStateDir: *hostDir, Clock: myce.RealClock{}, Machine: m, StateDir: *state, ExtensionRoot: *root, Client: myce.Client{URL: *queue, Token: env("MYCE_TOKEN", "")}, DailyUse: *daily, Laptop: *laptop, Desktop: *desktop}, &profile)
	if err != nil {
		return err
	}
	if err = json.NewEncoder(os.Stdout).Encode(profile); err != nil {
		return err
	}
	if profile.Benchmark.State != "complete" {
		return errors.New("benchmark incomplete; inspect unavailable resources; retry on next join")
	}
	return nil
}
