package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"mycelium/internal/myce"
	"os"
)

func hostConvertCommand(args []string, out io.Writer) error {
	return hostConvertCommandWithProbe(args, myce.LocalMachineProbe(), out)
}
func hostConvertCommandWithProbe(args []string, probe myce.MachineProbe, out io.Writer) error {
	f := flag.NewFlagSet("host convert", flag.ContinueOnError)
	local := f.Bool("local", false, "convert the stopped installation in place, as its worker user")
	config := f.String("config", "", "an existing config selecting the installation root")
	dry := f.Bool("dry-run", false, "read-only inventory; no files or services changed")
	if e := f.Parse(args); e != nil {
		return e
	}
	if !*local || f.NArg() != 0 {
		return errors.New("usage: myce host convert --local [--config FILE] [--dry-run]")
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return e
	}
	ctx, cancel := myce.SetupTimeout(context.Background())
	defer cancel()
	r, e := myce.ConvertLocalInstallation(ctx, myce.LocalConversionOptions{Home: home, Config: *config, Probe: probe, Clock: myce.RealClock{}, DryRun: *dry})
	if we := json.NewEncoder(out).Encode(r); we != nil {
		return we
	}
	return e
}
