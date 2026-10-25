package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"mycelium/internal/myce"
	"time"
)

func reconcileDockerRetentionCommand(args []string, out io.Writer) error {
	f := flag.NewFlagSet("reconcile-docker-retention", flag.ContinueOnError)
	config := f.String("config", "", "successor worker config with completed conversion journal")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *config == "" {
		return errors.New("reconcile-docker-retention requires --config for a completed worker conversion")
	}
	report, err := myce.ReconcileRetiredDocker(context.Background(), *config, myce.RealClock{})
	if e := json.NewEncoder(out).Encode(report); e != nil {
		return errors.Join(err, e)
	}
	return err
}

// Reconciliation takes the worker spool's exclusive lock and releases only this attempt.
func resolveDockerCommand(args []string) error {
	f := flag.NewFlagSet("resolve-docker", flag.ContinueOnError)
	dir := f.String("state-dir", "", "offline worker spool")
	identity := f.String("attempt-id", "", "exact attempt identity from worker custody")
	timeout := f.Duration("timeout", 10*time.Second, "exact-custody reconciliation deadline")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *timeout <= 0 || *dir == "" || *identity == "" {
		return errors.New("resolve-docker requires exact worker state, attempt identity and a positive timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return myce.ReconcileDockerAttempt(ctx, *dir, *identity)
}
