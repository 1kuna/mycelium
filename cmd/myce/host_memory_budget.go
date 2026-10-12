package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"mycelium/internal/myce"
)

func hostMemoryBudgetCommand(args []string) error {
	if len(args) == 0 || (args[0] != "set" && args[0] != "show") {
		return errors.New("usage: myce host-memory-budget set GiB|show [--host-state-dir DIR] [--config PATH]")
	}
	action := args[0]
	args = args[1:]
	value := 0.0
	if action == "set" {
		if len(args) == 0 {
			return errors.New("set requires positive GiB")
		}
		var err error
		value, err = strconv.ParseFloat(args[0], 64)
		if err != nil {
			return errors.New("set requires numeric GiB")
		}
		args = args[1:]
	}
	f := flag.NewFlagSet("host-memory-budget", flag.ContinueOnError)
	dir := f.String("host-state-dir", "", "shared host control directory")
	config := f.String("config", "", "installed worker config for directory/default budget")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected host-memory-budget arguments")
	}
	fallback := 0.0
	source := "install default (uncapped)"
	if *config != "" {
		c, err := myce.ReadServiceConfig(*config)
		if err != nil {
			return err
		}
		if c.Role != "worker" {
			return errors.New("budget config must be a worker")
		}
		if *dir == "" {
			*dir = c.HostStateDir
		}
		fallback = c.HostMemoryBudgetGB
		source = *config
	}
	if *dir == "" {
		var err error
		*dir, err = myce.HostStateDirectory()
		if err != nil {
			return err
		}
	}
	if action == "set" {
		budget, err := myce.SetHostMemoryBudget(*dir, value)
		if err != nil {
			return err
		}
		fmt.Printf("Host memory budget: %g GiB; generation %d; %s\n", budget.GiB, budget.Generation, filepath.Join(*dir, "host-memory-budget.json"))
		return nil
	}
	budget, where, err := myce.EffectiveHostMemoryBudget(*dir, fallback)
	if err != nil {
		return err
	}
	if budget.Generation == 0 && *config == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		paths, err := filepath.Glob(filepath.Join(home, ".local", "share", "mycelium", "worker*", "config.json"))
		if err != nil {
			return err
		}
		for _, path := range paths {
			c, err := myce.ReadServiceConfig(path)
			if err != nil {
				return err
			}
			if c.Role == "worker" && c.HostStateDir == *dir && (source == "install default (uncapped)" || c.HostMemoryBudgetGB > 0 && (fallback == 0 || c.HostMemoryBudgetGB < fallback)) {
				fallback = c.HostMemoryBudgetGB
				source = path
			}
		}
		budget.GiB = fallback
	}
	if budget.Generation == 0 {
		where = source
	}
	fmt.Printf("Host memory budget: %g GiB; generation %d; source: %s\n", budget.GiB, budget.Generation, where)
	return nil
}
