package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"mycelium/internal/myce"
	"os"
	"strconv"
	"strings"
)

func modelCommand(args []string) error {
	if len(args) > 0 && args[0] == "fetch" {
		return &myce.HTTPError{Code: "model_fetch_retired", Message: "download the model with its upstream client and stage it under the configured model root (docs/services.md)"}
	}
	if len(args) == 0 {
		return errors.New("usage: myce model approve-local|sync|pin|unpin [flags]")
	}
	f := flag.NewFlagSet("model "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	to := f.String("to", "", "sync to machine, platform, or all")
	cancel := f.Bool("cancel", false, "cancel pre-copy request; keep resumable partial bytes")
	project := f.String("project", "", "project owning pin")
	machine := f.String("machine", "", "pin to machine")
	platform := f.String("platform", "", "pin to one machine on platform")
	start := f.String("start", "", "window start HH:MM")
	end := f.String("end", "", "window end HH:MM")
	timezone := f.String("timezone", "UTC", "IANA time zone")
	weekdays := f.String("weekdays", "", "comma-separated 0=Sunday through 6=Saturday")
	approvalRoot := f.String("model-approval-root", "", "worker-local approval directory")
	localPath := f.String("path", "", "exact local model folder to approve")
	approvedRoot := f.String("approved-root", "", "root bounding model folder and symlinked blobs")
	flags := args[1:]
	if len(flags) > 0 && !strings.HasPrefix(flags[0], "-") {
		flags = append(append([]string{}, flags[1:]...), flags[0])
	}
	if e := f.Parse(flags); e != nil {
		return e
	}
	if f.NArg() != 1 {
		return errors.New("supply one model name")
	}
	if args[0] == "approve-local" {
		m, err := myce.ApproveLocalModel(context.Background(), *approvalRoot, f.Arg(0), *localPath, *approvedRoot)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			LocalSource string           `json:"local_source"`
			Files       []myce.ModelFile `json:"files"`
			SizeBytes   int64            `json:"size_bytes"`
			SHA256      string           `json:"sha256"`
		}{m.LocalSource, m.Files, m.SizeBytes, m.SHA256})
	}
	client, err := authority.ownerClient()
	if err != nil {
		return err
	}
	if client.HTTP != nil {
		defer client.HTTP.CloseIdleConnections()
	}
	if args[0] == "sync" {
		return client.Call(context.Background(), "POST", "/api/models/sync", myce.SyncRequest{Model: f.Arg(0), To: *to, Cancel: *cancel}, nil)
	}
	p := myce.ModelPin{Project: *project, Model: f.Arg(0), Machine: *machine, Platform: *platform, Start: *start, End: *end, Timezone: *timezone}
	if *weekdays != "" {
		for _, d := range strings.Split(*weekdays, ",") {
			n, e := strconv.Atoi(d)
			if e != nil {
				return fmt.Errorf("weekday: %w", e)
			}
			p.Weekdays = append(p.Weekdays, n)
		}
	}
	if args[0] != "pin" && args[0] != "unpin" {
		return errors.New("unknown model command")
	}
	return client.Call(context.Background(), "POST", "/api/models/"+args[0], p, nil)
}
