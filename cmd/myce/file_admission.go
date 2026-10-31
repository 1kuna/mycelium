package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"mycelium/internal/myce"
	"net/http"
)

func fileAdmissionCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: myce file admission status|observe|activate|rollback [client flags]")
	}
	action := args[0]
	if action != "status" && action != "observe" && action != "activate" && action != "rollback" {
		return errors.New("admission requires status, observe, activate or rollback")
	}
	f := flag.NewFlagSet("file admission "+action, flag.ContinueOnError)
	authority := installedClientFlags(f)
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("admission takes no positional arguments or byte budgets")
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	method, path := http.MethodPost, "/api/files/admission/"+action
	if action == "status" {
		method, path = http.MethodGet, "/api/files/admission"
	}
	var report myce.StorageMigrationStatus
	if err := c.Call(ctx, method, path, nil, &report); err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}
