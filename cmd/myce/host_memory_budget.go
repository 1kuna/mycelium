package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"

	"mycelium/internal/myce"
)

func hostMemoryBudgetCommand(args []string) error {
	if len(args) == 0 || (args[0] != "configure" && args[0] != "show" && args[0] != "import") {
		return errors.New("usage: myce host-memory-budget configure GiB|import|show [--host-state-dir DIR] [--config PATH]; changes require CLOSED")
	}
	action := args[0]
	args = args[1:]
	value := 0.0
	if action == "configure" {
		if len(args) == 0 {
			return errors.New("configure requires positive GiB")
		}
		var err error
		value, err = strconv.ParseFloat(args[0], 64)
		if err != nil {
			return errors.New("configure requires numeric GiB")
		}
		args = args[1:]
	}
	f := flag.NewFlagSet("host-memory-budget", flag.ContinueOnError)
	dir := f.String("host-state-dir", "", "shared host control directory")
	config := f.String("config", "", "installed worker config for directory and UNQUALIFIED migration input")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected host-memory-budget arguments")
	}
	fallback := 0.0
	if *config != "" {
		c, err := myce.ReadServiceConfig(*config)
		if err != nil {
			return err
		}
		if c.Role != "worker" {
			return errors.New("allowance config must be a worker")
		}
		if *dir == "" {
			*dir = c.HostStateDir
		}
		fallback = c.HostMemoryBudgetGB
	}
	if *dir == "" {
		var err error
		*dir, err = myce.HostStateDirectory()
		if err != nil {
			return err
		}
	}
	var budget myce.HostMemoryBudget
	var err error
	source := "host-state.json"
	switch action {
	case "configure":
		budget, err = myce.SetHostMemoryBudget(*dir, value)
	case "import":
		budget, err = myce.ImportHostMemoryBudget(*dir)
	case "show":
		budget, source, err = myce.EffectiveHostMemoryBudget(*dir, fallback)
	}
	if err != nil {
		return err
	}
	fmt.Printf("Host memory allowance: %d bytes (%g GiB); %s; source: %s\n", budget.Bytes, budget.GiB, budget.Qualification, source)
	return nil
}
