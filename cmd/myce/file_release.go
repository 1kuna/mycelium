package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"

	"mycelium/internal/myce"
)

func fileReleaseCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: myce file release plan --project PROJECT JOB... | apply --accept-expiry PLAN.json")
	}
	f := flag.NewFlagSet("file release "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	project := f.String("project", "", "project attribution (not an authorization boundary)")
	accept := f.Bool("accept-expiry", false, "accept irreversible expiry after all downstream use and required preservation are complete")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "plan":
		if *project == "" || f.NArg() == 0 || *accept {
			return errors.New("plan requires --project and explicit job IDs")
		}
	case "apply":
		if f.NArg() != 1 || !*accept || *project != "" {
			return errors.New("apply requires --accept-expiry and one reviewed plan file")
		}
	default:
		return errors.New("release requires plan or apply")
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if args[0] == "plan" {
		var plan myce.FileReleasePlan
		err = c.Call(ctx, "POST", "/api/files/release/plan", myce.FileReleaseSelection{Project: *project, Jobs: f.Args()}, &plan)
		if err != nil {
			return err
		}
		return enc.Encode(plan)
	}
	file, err := os.Open(f.Arg(0))
	if err != nil {
		return err
	}
	defer file.Close()
	var plan myce.FileReleasePlan
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&plan); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected one release plan")
	}
	var result myce.FileReleaseResult
	err = c.Call(ctx, "POST", "/api/files/release/apply", myce.FileReleaseApply{Selection: plan.Selection, PlanID: plan.ID, AcceptExpiry: *accept}, &result)
	if err != nil {
		return err
	}
	return enc.Encode(result)
}
