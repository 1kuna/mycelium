package main

import (
	"context"
	"errors"
	"flag"
	"mycelium/internal/myce"
	"os"
	"time"
)

func hostReconcileCommand(args []string, probe myce.MachineProbe) error {
	f := flag.NewFlagSet("host reconcile", flag.ContinueOnError)
	local := f.Bool("local", false, "reconcile this host")
	dir := f.String("host-state-dir", "", "shared host state")
	timeout := f.Duration("timeout", 30*time.Second, "overall reconciliation bound")
	if err := f.Parse(args); err != nil {
		return err
	}
	if !*local || f.NArg() != 0 || *timeout <= 0 {
		return errors.New("usage: myce host reconcile --local [--host-state-dir DIR] [--timeout 30s]")
	}
	if *dir == "" {
		var err error
		*dir, err = myce.HostStateDirectory()
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	intent, err := myce.ReadHostIntent(*dir)
	if err != nil {
		return err
	}
	var cleanupErr error
	if intent.Desired == "closed" {
		h := myce.HostControl{Dir: *dir, Docker: true, Timeout: *timeout, CloseExpected: &intent.HostRevision}
		h.ResolveServices = func() (map[string]myce.HostService, error) {
			services := map[string]myce.HostService{}
			if probe.OS != "linux" {
				return services, nil
			}
			units, err := myce.WorkerUnits(*dir, "")
			if err != nil {
				return nil, err
			}
			for _, unit := range units {
				services[unit] = &myce.SystemdHostService{Unit: unit, Command: probe.Command}
			}
			return services, nil
		}
		_, cleanupErr = h.Disable(ctx, intent.Reason, 0)
	}
	workers, err := myce.HostWorkers(*dir)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	return errors.Join(cleanupErr, myce.ReconcileHostLocal(ctx, *dir, myce.RealClock{}, myce.HostQueueClient(workers, os.Getenv("MYCE_TOKEN"))))
}
