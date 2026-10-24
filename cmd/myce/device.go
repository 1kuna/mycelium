package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"

	"mycelium/internal/myce"
)

func deviceCommand(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "clear-quarantine" {
		return errors.New("usage: myce device clear-quarantine --state-dir DIR --device ID")
	}
	f := flag.NewFlagSet("device clear-quarantine", flag.ContinueOnError)
	root := f.String("state-dir", "", "local worker state directory")
	id := f.String("device", "", "numeric device ID from worker inventory")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("clear-quarantine takes no positional arguments")
	}
	if err := myce.RequestDeviceQuarantineClear(*root, *id); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]any{"device": *id, "clear_requested": true})
}
