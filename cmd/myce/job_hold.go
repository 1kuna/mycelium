package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"
	"strconv"
	"time"

	"mycelium/internal/myce"
)

func jobHoldCommand(operation string, args []string, out io.Writer) error {
	id := ""
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		id, args = args[0], args[1:]
	}
	f := flag.NewFlagSet(operation, flag.ContinueOnError)
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "queue address")
	token := f.String("token", env("MYCE_TOKEN", ""), "queue owner token (prefer MYCE_TOKEN)")
	f.Lookup("token").DefValue = ""
	revision := f.String("if-revision", "", "required current job hold revision (0 before first hold)")
	reason := f.String("reason", "", "reason for the hold")
	until := f.String("until", "", "optional absolute expiry (RFC3339); otherwise explicit resume required")
	if err := f.Parse(args); err != nil {
		return err
	}
	if id == "" && f.NArg() == 1 {
		id = f.Arg(0)
	} else if f.NArg() != 0 {
		return errors.New("expected one job ID")
	}
	expected, err := strconv.ParseUint(*revision, 10, 64)
	if err != nil || id == "" {
		return errors.New("usage: myce pause|resume JOB --if-revision N [--reason TEXT] [--until TIME]")
	}
	if *token == "" {
		return errors.New("set MYCE_TOKEN")
	}
	request := myce.JobHoldRequest{ExpectedRevision: &expected, Reason: *reason}
	if *until != "" {
		expiry, err := time.Parse(time.RFC3339, *until)
		if err != nil {
			return err
		}
		request.Until = &expiry
	}
	if operation == "pause" && *reason == "" {
		return errors.New("pause requires --reason")
	}
	if operation == "resume" && (*reason != "" || *until != "") {
		return errors.New("resume accepts no reason or until")
	}
	endpoint := "resume"
	if operation == "pause" {
		endpoint = "hold"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var hold myce.JobHold
	client := myce.Client{URL: *queue, Token: *token}
	if err := client.Call(ctx, "POST", "/api/jobs/"+url.PathEscape(id)+"/"+endpoint, request, &hold); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(hold)
}
