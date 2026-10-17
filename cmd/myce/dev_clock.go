package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func devClockCommand(args []string, out io.Writer) error {
	if len(args) < 2 || args[0] != "advance" {
		return errors.New("usage: myce dev clock advance <duration> [--queue URL] [--token TOKEN]")
	}
	f := flag.NewFlagSet("dev clock advance", flag.ContinueOnError)
	queue := f.String("queue", env("MYCE_QUEUE", "http://127.0.0.1:7331"), "dev queue address")
	token := f.String("token", env("MYCE_TOKEN", "dev-token"), "dev queue token")
	f.Lookup("token").DefValue = ""
	flags := args[1:]
	if len(flags) > 0 && !strings.HasPrefix(flags[0], "-") {
		flags = append(append([]string{}, flags[1:]...), flags[0])
	}
	if err := f.Parse(flags); err != nil {
		return err
	}
	if f.NArg() != 1 {
		return errors.New("dev clock advance requires one duration")
	}
	// Reuse the existing bounded day/Go-duration parser (up to 90d).
	delta, err := usageDuration(f.Arg(0))
	if err != nil || delta < time.Nanosecond {
		return errors.New("advance must be positive, at least 1ns and at most 90d")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var reply struct {
		Now time.Time `json:"now"`
	}
	client := myce.Client{URL: *queue, Token: *token}
	if err := client.Call(ctx, "POST", "/api/dev/clock/advance", struct {
		Seconds float64 `json:"seconds"`
	}{delta.Seconds()}, &reply); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(reply)
}
