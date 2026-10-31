package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"

	"mycelium/internal/myce"
)

func curvePreservationCommand(args []string, out io.Writer) error {
	f := flag.NewFlagSet("release verify-curves", flag.ContinueOnError)
	beforePath := f.String("before", "", "before inspection snapshot (server now is cutover boundary)")
	afterPath := f.String("after", "", "after inspection snapshot")
	ledgerPath := f.String("ledgers", "", "JSON array of job ledger pages containing accepted learning")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *beforePath == "" || *afterPath == "" || *ledgerPath == "" {
		return errors.New("verify-curves requires --before, --after and --ledgers")
	}
	read := func(path string, value any) error {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, value)
	}
	var before, after myce.Snapshot
	var pages []myce.JobLedgerPage
	if err := read(*beforePath, &before); err != nil {
		return err
	}
	if err := read(*afterPath, &after); err != nil {
		return err
	}
	if err := read(*ledgerPath, &pages); err != nil {
		return err
	}
	receipt, err := myce.VerifyCurvePreservation(before, after, pages)
	if encodeErr := json.NewEncoder(out).Encode(receipt); encodeErr != nil {
		return encodeErr
	}
	return err
}
