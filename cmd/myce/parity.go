package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"
	"time"

	"mycelium/internal/myce"
)

const parityUsage = "usage: myce parity list --project P | add --project P --work WORK --host HOST --evidence JOB | remove --project P --work WORK --host HOST"

// parityCommand edits the project's own production parity host list with the
// same authority the project uses to submit (#1341, MY-44, MY-19).
func parityCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "list" && args[0] != "add" && args[0] != "remove" {
		return &exitError{2, errors.New(parityUsage)}
	}
	f := flag.NewFlagSet("parity "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	project := f.String("project", "", "project that owns the production work")
	work := f.String("work", "", "exact work identity: kind:<name>, program:<type> or extension:<name>")
	host := f.String("host", "", "physical host ID from myce status")
	evidence := f.String("evidence", "", "succeeded check or experiment job that compared outputs on HOST")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || *project == "" || args[0] != "list" && (*work == "" || *host == "") || args[0] != "add" && *evidence != "" {
		return &exitError{2, errors.New(parityUsage)}
	}
	client, err := authority.client()
	if err != nil {
		return err
	}
	if client.HTTP != nil {
		defer client.HTTP.CloseIdleConnections()
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var record myce.ProjectParityHosts
	request := myce.ParityHostRequest{Project: *project, Work: *work, Host: *host, Evidence: *evidence}
	switch args[0] {
	case "list":
		err = client.Call(ctx, "GET", "/api/parity-hosts?project="+url.QueryEscape(*project), nil, &record)
	case "add":
		err = client.Call(ctx, "POST", "/api/parity-hosts", request, &record)
	default:
		err = client.Call(ctx, "POST", "/api/parity-hosts/remove", request, &record)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(record)
}
