package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"mycelium/internal/myce"
)

func migrateCommand(args []string, probe myce.MachineProbe) error {
	if len(args) == 0 || args[0] != "worker-unit" {
		return errors.New("usage: myce migrate worker-unit --config INSTALLED_CONFIG")
	}
	f := flag.NewFlagSet("migrate worker-unit", flag.ContinueOnError)
	path := f.String("config", "", "installed worker configuration; stages placement for the next normal fleet upgrade")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if *path == "" || f.NArg() != 0 {
		return errors.New("migrate worker-unit requires --config")
	}
	c, err := myce.ReadServiceConfig(*path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	status, err := myce.StageWorkerUnitMigration(ctx, c, myce.BoundSetupCommand(probe.Command))
	if status != nil {
		err = errors.Join(err, json.NewEncoder(os.Stdout).Encode(status))
	}
	return err
}
