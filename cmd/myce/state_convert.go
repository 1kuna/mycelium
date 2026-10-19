package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"

	"mycelium/internal/myce"
)

func stateConvertCommand(args []string, out io.Writer) error {
	f := flag.NewFlagSet("state convert", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	source := f.String("source", "", "stopped local source bundle (never modified)")
	target := f.String("target", "", "new same-filesystem target directory")
	host := f.String("host-id", "", "physical host identity")
	dry := f.Bool("dry-run", false, "inventory, hash and validate without publication")
	apply := f.Bool("apply", false, "publish a validated CLOSED target")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *dry == *apply {
		return errors.New("usage: myce state convert --source DIR --target DIR --host-id ID --dry-run|--apply")
	}
	m, err := myce.ConvertPersistedState(myce.PersistedConversionOptions{Source: *source, Target: *target, Host: *host, Clock: myce.RealClock{}, Apply: *apply})
	if e := json.NewEncoder(out).Encode(m); e != nil {
		return e
	}
	return err
}
