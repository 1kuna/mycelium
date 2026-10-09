package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"mycelium/internal/myce"
	"path/filepath"
	"strconv"
	"strings"
)

func modelCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: myce model fetch|sync|pin|unpin [flags]")
	}
	f := flag.NewFlagSet("model "+args[0], flag.ContinueOnError)
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue URL")
	token := f.String("token", env("MYCE_TOKEN", ""), "fleet token (prefer MYCE_TOKEN)")
	to := f.String("to", "", "sync to machine, platform, or all")
	cancel := f.Bool("cancel", false, "cancel pre-copy request; keep resumable partial bytes")
	project := f.String("project", "", "project owning pin")
	machine := f.String("machine", "", "pin to machine")
	platform := f.String("platform", "", "pin to one machine on platform")
	start := f.String("start", "", "window start HH:MM")
	end := f.String("end", "", "window end HH:MM")
	timezone := f.String("timezone", "UTC", "IANA time zone")
	weekdays := f.String("weekdays", "", "comma-separated 0=Sunday through 6=Saturday")
	url := f.String("url", "", "Hugging Face HTTPS file URL")
	root := f.String("model-root", "", "NAS model folder")
	size := f.Int64("size-bytes", 0, "expected size")
	sum := f.String("sha256", "", "expected SHA-256")
	flags := args[1:]
	if len(flags) > 0 && !strings.HasPrefix(flags[0], "-") {
		flags = append(append([]string{}, flags[1:]...), flags[0])
	}
	if e := f.Parse(flags); e != nil {
		return e
	}
	if f.NArg() != 1 {
		return errors.New("supply one model name (or NAS-relative destination for fetch)")
	}
	if args[0] == "fetch" {
		p := filepath.Clean(f.Arg(0))
		if *root == "" || filepath.IsAbs(p) || p == ".." || strings.HasPrefix(p, ".."+string(filepath.Separator)) {
			return errors.New("fetch needs NAS --model-root and a relative destination")
		}
		return myce.FetchModel(context.Background(), *url, filepath.Join(*root, p), *size, *sum)
	}
	if *token == "" {
		return errors.New("set MYCE_TOKEN")
	}
	if args[0] == "sync" {
		return (myce.Client{URL: *queue, Token: *token}).Call(context.Background(), "POST", "/api/models/sync", myce.SyncRequest{Model: f.Arg(0), To: *to, Cancel: *cancel}, nil)
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
	return (myce.Client{URL: *queue, Token: *token}).Call(context.Background(), "POST", "/api/models/"+args[0], p, nil)
}
