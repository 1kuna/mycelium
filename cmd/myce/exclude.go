package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"
	"os/user"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func excludeCommand(ctx context.Context, args []string, out io.Writer) error {
	usage := errors.New("usage: myce exclude add SELECTOR [--reason TEXT] [--by OPERATOR] | remove SELECTOR [--by OPERATOR] | list [--config PATH]")
	if len(args) == 0 || args[0] != "add" && args[0] != "remove" && args[0] != "list" {
		return usage
	}
	f := flag.NewFlagSet("exclude "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	reason := f.String("reason", "", "operator reason for withholding capacity")
	by := f.String("by", "", "operator attribution (defaults to current OS username; not a separate authentication identity)")
	var flags, positional []string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			key := strings.TrimLeft(arg, "-")
			if (key == "reason" || key == "by" || key == "config" || key == "joining-file" || key == "queue" || key == "token") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		} else {
			positional = append(positional, arg)
		}
	}
	if err := f.Parse(flags); err != nil {
		return err
	}
	if f.NArg() != 0 || args[0] == "list" && len(positional) != 0 || args[0] != "list" && len(positional) != 1 {
		return usage
	}
	var invalid bool
	f.Visit(func(v *flag.Flag) {
		invalid = invalid || v.Name == "reason" && args[0] != "add" || v.Name == "by" && args[0] == "list"
	})
	if invalid {
		return usage
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	if args[0] == "list" {
		var records []myce.ComponentExclusion
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := c.Call(readCtx, "GET", "/api/exclusions", nil, &records); err != nil {
			return err
		}
		active := []myce.ComponentExclusion{}
		for _, p := range records {
			if p.Active {
				active = append(active, p)
			}
		}
		return json.NewEncoder(out).Encode(active)
	}
	if *by == "" {
		u, err := user.Current()
		if err != nil {
			return err
		}
		*by = u.Username
	}
	var p myce.ComponentExclusion
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = c.Call(readCtx, "GET", "/api/exclusions/resolve?selector="+url.QueryEscape(positional[0]), nil, &p)
	cancel()
	if err != nil {
		return err
	}
	r := myce.ComponentExclusionRequest{Action: args[0], Selector: p.Selector, Reason: *reason, SetBy: *by, IfRevision: &p.Revision}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Call(writeCtx, "POST", "/api/exclusions", r, &p); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(p)
}
