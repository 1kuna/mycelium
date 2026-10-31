package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"mycelium/internal/myce"
)

func workerEnvCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 2 || args[0] != "env" || args[1] != "prune" {
		return errors.New("usage: myce worker env prune [--config FILE] [--environment-cache DIR] [--apply]")
	}
	f := flag.NewFlagSet("worker env prune", flag.ContinueOnError)
	config := f.String("config", "", "installed worker config")
	root := f.String("environment-cache", "", "prepared environment cache (default: installed worker cache)")
	address := f.String("queue", os.Getenv("MYCE_QUEUE"), "queue address (default: installed authority)")
	joining := f.String("joining-file", os.Getenv("MYCE_JOINING_FILE"), "private pinned joining authority")
	apply := f.Bool("apply", false, "remove unlocked, unreferenced environments unused for at least 24h")
	if err := f.Parse(args[2:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("worker env prune accepts no positional arguments")
	}
	if *root == "" {
		worker, ok := installedExtensionWorker(*config)
		if !ok || worker.EnvironmentCache == "" {
			return errors.New("worker env prune requires an installed worker config or --environment-cache")
		}
		*root = worker.EnvironmentCache
	}
	// Typing the wrong cache path must not create a new empty cache.
	if st, err := os.Stat(*root); err != nil {
		return err
	} else if !st.IsDir() {
		return errors.New("environment cache must be a directory")
	}
	client, err := runClient(*config, *joining, *address, os.Getenv("MYCE_TOKEN"))
	if err != nil {
		return err
	}
	if client.HTTP != nil {
		defer client.HTTP.CloseIdleConnections()
	}
	cache := &myce.EnvironmentCache{Root: *root}
	report, err := cache.Prune(ctx, *apply, func(ctx context.Context, key string) (bool, error) {
		var state struct {
			Referenced bool `json:"referenced"`
		}
		err := client.Call(ctx, "GET", "/api/environments/"+key+"/reference", nil, &state)
		return state.Referenced, err
	})
	fmt.Fprintln(out, "KEY AGE SIZE_BYTES REFERENCED LOCKED RECENT REMOVED")
	for _, v := range report.Entries {
		fmt.Fprintf(out, "%s %s %d %t %t %t %t\n", v.Key, v.Age.Round(time.Second), v.Bytes, v.Referenced, v.Locked, v.Recent, v.Removed)
	}
	fmt.Fprintf(out, "apply=%t reclaimed_bytes=%d\n", report.Apply, report.ReclaimedBytes)
	return err
}
