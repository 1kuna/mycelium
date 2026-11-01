package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func pauseCommand(ctx context.Context, action string, args []string, out io.Writer) error {
	f := flag.NewFlagSet(action, flag.ContinueOnError)
	config := f.String("config", "", "installed config")
	queue := f.String("queue", os.Getenv("MYCE_QUEUE"), "queue address")
	token := f.String("token", "", "queue token (prefer MYCE_TOKEN)")
	joining := f.String("joining-file", os.Getenv("MYCE_JOINING_FILE"), "joining file")
	var reason, until string
	if action == "pause" {
		f.StringVar(&reason, "reason", "", "required pause reason")
		f.StringVar(&until, "until", "", "optional absolute RFC3339 resume time")
	}
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		args = append(append([]string{}, args[1:]...), args[0])
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 1 {
		return fmt.Errorf("usage: myce %s JOB [flags]", action)
	}
	request := myce.PauseJobRequest{Reason: reason}
	if action == "pause" {
		if strings.TrimSpace(reason) == "" {
			return errors.New("pause requires --reason TEXT")
		}
		if until != "" {
			t, err := time.Parse(time.RFC3339, until)
			if err != nil {
				return fmt.Errorf("--until must be an absolute RFC3339 time: %w", err)
			}
			request.Until = &t
		}
	}
	if *token == "" {
		*token = os.Getenv("MYCE_TOKEN")
	}
	client, err := runClient(*config, *joining, *queue, *token)
	if err != nil {
		return err
	}
	var j myce.Job
	if err = client.Call(ctx, "GET", "/api/jobs/"+f.Arg(0), nil, &j); err != nil {
		return err
	}
	var body any = request
	if action == "resume" {
		body = myce.ResumeJobRequest{IfRevision: j.PauseRevision}
	}
	id := j.ID
	j = myce.Job{}
	if err = client.Call(ctx, "POST", "/api/jobs/"+id+"/"+action, body, &j); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(j)
}
