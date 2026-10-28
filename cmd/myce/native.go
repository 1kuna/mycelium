package main

import (
	"context"
	"errors"
	"flag"
	"mycelium/internal/myce"
	"time"
)

func resolveNativeCommand(args []string) error {
	f := flag.NewFlagSet("resolve-native", flag.ContinueOnError)
	dir := f.String("state-dir", "", "offline worker spool")
	identity := f.String("attempt-id", "", "exact attempt identity from worker custody")
	timeout := f.Duration("timeout", 30*time.Second, "inspection budget; expiry retains custody")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *dir == "" || *identity == "" || *timeout <= 0 {
		return errors.New("resolve-native requires exact worker state, attempt identity and a positive timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return myce.ReconcileNativeAttempt(ctx, *dir, *identity)
}
