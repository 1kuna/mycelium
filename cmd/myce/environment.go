package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"

	"mycelium/internal/myce"
)

func environmentCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "list" && args[0] != "get" && args[0] != "retry" {
		return errors.New("usage: myce env list | get [options] NAME | retry [options] WORKER HASH OS ARCH PLATFORM")
	}
	f := flag.NewFlagSet("env "+args[0], flag.ContinueOnError)
	config := f.String("config", "", "installed config")
	joining := f.String("joining-file", env("MYCE_JOINING_FILE", ""), "private joining file")
	queue := f.String("queue", env("MYCE_QUEUE", ""), "queue address")
	token := f.String("token", env("MYCE_TOKEN", ""), "queue token (prefer installed authority)")
	f.Lookup("token").DefValue = ""
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if args[0] == "list" && f.NArg() != 0 || args[0] == "get" && f.NArg() != 1 || args[0] == "retry" && f.NArg() != 5 {
		return errors.New("usage: myce env list | get [options] NAME | retry [options] WORKER HASH OS ARCH PLATFORM")
	}
	client, err := runClient(*config, *joining, *queue, *token)
	if err != nil {
		return err
	}
	if args[0] == "retry" {
		request := struct {
			Worker string               `json:"worker"`
			Target myce.ExecutionTarget `json:"target"`
		}{f.Arg(0), myce.ExecutionTarget{OS: f.Arg(2), Arch: f.Arg(3), Platform: f.Arg(4)}}
		var reply map[string]bool
		if err := client.Call(ctx, "POST", "/api/environments/"+url.PathEscape(f.Arg(1))+"/retry", request, &reply); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(reply)
	}
	if args[0] == "get" {
		var entry myce.EnvironmentEntry
		if err := client.Call(ctx, "GET", "/api/environments/"+url.PathEscape(f.Arg(0)), nil, &entry); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(entry)
	}
	var entries []myce.EnvironmentEntry
	if err := client.Call(ctx, "GET", "/api/environments", nil, &entries); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(entries)
}
