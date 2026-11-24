package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"math"
	"os"
	"strings"

	"mycelium/internal/myce"
)

func jobCommand(ctx context.Context, args []string, out io.Writer) error {
	const usage = "usage: myce job resize --project NAME --host-memory-gb GIB --by REQUESTER --reason TEXT JOB_ID..."
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		_, err := io.WriteString(out, usage+"\n")
		return err
	}
	if len(args) == 0 || args[0] != "resize" {
		return errors.New(usage)
	}
	f := flag.NewFlagSet("job resize", flag.ContinueOnError)
	config := f.String("config", "", "installed config")
	queue := f.String("queue", os.Getenv("MYCE_QUEUE"), "queue address")
	joining := f.String("joining-file", os.Getenv("MYCE_JOINING_FILE"), "joining file")
	token := f.String("token", os.Getenv("MYCE_TOKEN"), "queue token (prefer installed authority)")
	project := f.String("project", "", "owning project")
	gb := f.Float64("host-memory-gb", 0, "host memory charge and hard limit in GiB")
	by := f.String("by", "", "operator-asserted requester label")
	reason := f.String("reason", "", "reason for the resize")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() == 0 || *gb <= 0 || math.IsNaN(*gb) || math.IsInf(*gb, 0) ||
		strings.TrimSpace(*project) == "" || strings.TrimSpace(*reason) == "" || strings.TrimSpace(*by) == "" {
		return errors.New(usage)
	}
	client, err := runClient(*config, *joining, *queue, *token)
	if err != nil {
		return err
	}
	request := myce.ResizeJobsRequest{Jobs: f.Args(), Project: *project, HostMemoryGB: *gb, RequestedBy: *by, Reason: *reason}
	var receipt myce.ResizeJobsReceipt
	if err := client.Call(ctx, "POST", "/api/jobs/resize", request, &receipt); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(receipt)
}
