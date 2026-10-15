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
	root := f.String("extension-root", "", "prepared local extension library/cache")
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "file-store queue")
	gpu := f.String("gpu", "", "accelerator platform (required)")
	device := f.String("device", "", "physical resource selector")
	daily := f.Bool("daily-use", false, "wait for owner idle and safe power/heat; cancel on return")
	laptop := f.Bool("laptop", false, "require external power")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *state == "" || *gpu == "" || f.NArg() != 0 {
		return errors.New("benchmark requires --state-dir and --gpu")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := os.MkdirAll(*state, 0700); err != nil {
		return err
	}
	m := myce.Machine{GPU: *gpu, Device: *device}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	profile := myce.LocalProfileProbe().Detect(probe, m, *state, myce.RealClock{}.Now())
	cancel()
	err := myce.RunOnboardingBenchmark(ctx, myce.BenchmarkConfig{Clock: myce.RealClock{}, Machine: m, StateDir: *state, ExtensionRoot: *root, Client: myce.Client{URL: *queue, Token: env("MYCE_TOKEN", "")}, DailyUse: *daily, Laptop: *laptop}, &profile)
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
