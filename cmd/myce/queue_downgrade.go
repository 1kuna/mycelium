package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"mycelium/internal/myce"
)

func queueDowngradeCommand(args []string, out io.Writer) error {
	f := flag.NewFlagSet("queue downgrade", flag.ContinueOnError)
	db := f.String("db", "", "stopped queue database (required)")
	to := f.String("to", "v2", "adjacent release persistence layout (v2)")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *db == "" || *to != "v2" || f.NArg() != 0 {
		return errors.New("usage: myce queue downgrade --db DATABASE --to v2 (stop the queue first)")
	}
	if err := myce.DowngradeQueue(*db); err != nil {
		return err
	}
	_, err := fmt.Fprintln(out, "Queue verified in normalized-v2 format; ready for the previous release.")
	return err
}
