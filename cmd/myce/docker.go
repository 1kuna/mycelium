package main

import (
	"context"
	"errors"
	"flag"
	"mycelium/internal/myce"
	"time"
)

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
