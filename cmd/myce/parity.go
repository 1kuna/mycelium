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

const parityUsage = "usage: myce parity list --project P | add --project P --work WORK [--kind KIND] --evidence JOB | remove --project P --work WORK --kind KIND\nKIND names a kind of machine, never a host: an OS (linux, mac), an architecture (x86, arm) and/or a platform (cuda, xpu, metal, cpu), for example cuda, mac or linux/amd64/cuda"

// parityCommand edits the project's own production approvals, which name
// kinds of machine (MY-51), with the same authority the project uses to
// submit (#1341, MY-44, MY-19).
func parityCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "list" && args[0] != "add" && args[0] != "remove" {
		return &exitError{2, errors.New(parityUsage)}
	}
	f := flag.NewFlagSet("parity "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	project := f.String("project", "", "project that owns the production work")
	work := f.String("work", "", "exact work identity: kind:<name>, program:<type> or extension:<name>")
	kind := f.String("kind", "", "kind of machine: OS, architecture and/or platform, e.g. cuda, mac or linux/amd64/cuda; add defaults to the kind the evidence ran as")
	// Previous-release scripts (MY-42): the queue converts a host to its kind
	// and warns. Removed next release.
	host := f.String("host", "", "deprecated: production approval names a machine kind; use --kind")
	evidence := f.String("evidence", "", "succeeded check or experiment job that compared outputs on a machine of that kind")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	bad := f.NArg() != 0 || *project == ""
	switch args[0] {
	case "add":
		bad = bad || *work == ""
	case "remove":
		bad = bad || *work == "" || *kind == "" && *host == "" || *evidence != ""
	default:
		bad = bad || *kind != "" || *host != "" || *evidence != ""
	}
	if bad {
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
	var record myce.ProjectParity
	request := myce.ParityRequest{Project: *project, Work: *work, Kind: *kind, Host: *host, Evidence: *evidence}
	switch args[0] {
	case "list":
		err = client.Call(ctx, "GET", "/api/parity?project="+url.QueryEscape(*project), nil, &record)
	case "add":
		err = client.Call(ctx, "POST", "/api/parity", request, &record)
	default:
		err = client.Call(ctx, "POST", "/api/parity/remove", request, &record)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(record)
}
