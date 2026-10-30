package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func reserveCommand(ctx context.Context, args []string, out io.Writer) error {
	usage := errors.New("usage: myce reserve add SELECTOR --job ID --owner OWNER --reason TEXT [--id ID] [--max-lifetime 2h] | list | release ID --if-revision N --reason TEXT")
	if len(args) == 0 || args[0] != "add" && args[0] != "list" && args[0] != "release" {
		return usage
	}
	f := flag.NewFlagSet("reserve "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	id := f.String("id", "", "stable reservation identity (generated if omitted)")
	job := f.String("job", "", "existing validation job/check ID")
	owner := f.String("owner", "", "operator attribution, not a separate authentication identity")
	reason := f.String("reason", "", "reason for reserving or conditionally releasing")
	lifetime := f.Duration("max-lifetime", myce.DefaultReservationLifetime, "lost-binding backstop; at most 24h; live jobs and pending cleanup retain the fence")
	revision := f.Uint64("if-revision", 0, "release only this observed reservation revision")
	var flags, positional []string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			if !strings.Contains(arg, "=") && i+1 < len(args) {
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
	invalid := false
	f.Visit(func(v *flag.Flag) {
		switch v.Name {
		case "id", "job", "owner", "max-lifetime":
			invalid = invalid || args[0] != "add"
		case "if-revision":
			invalid = invalid || args[0] != "release"
		case "reason":
			invalid = invalid || args[0] == "list"
		}
	})
	if invalid || args[0] == "add" && (*job == "" || *owner == "" || *reason == "") || args[0] == "release" && (*revision == 0 || *reason == "") {
		return usage
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if args[0] == "list" {
		var records []myce.Reservation
		if err := c.Call(ctx, "GET", "/api/reservations", nil, &records); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(records)
	}
	var record myce.Reservation
	if args[0] == "add" {
		if *id == "" {
			*id = myce.ID()
		}
		request := myce.ReservationRequest{ReservationOptions: myce.ReservationOptions{ID: *id, Selector: positional[0], Owner: *owner, Reason: *reason, MaxLifetime: lifetime.String()}, Job: *job}
		err = c.Call(ctx, "POST", "/api/reservations", request, &record)
	} else {
		err = c.Call(ctx, "POST", "/api/reservations/"+url.PathEscape(positional[0])+"/release", map[string]any{"if_revision": *revision, "reason": *reason}, &record)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(record)
}
