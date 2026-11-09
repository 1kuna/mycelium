package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func policyCommand(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: myce policy get [--yaml] | set policy.yaml --if-revision N")
	}
	// Accept the documented filename-before-flags form as well as Go flag order.
	rest := args[1:]
	filename := ""
	if args[0] == "set" && len(rest) > 0 && len(rest[0]) > 0 && rest[0][0] != '-' {
		filename = rest[0]
		rest = rest[1:]
	}
	f := flag.NewFlagSet("policy "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	asYAML := f.Bool("yaml", false, "export policy YAML (revision is printed to stderr)")
	revision := f.String("if-revision", "", "required current policy revision")
	if err := f.Parse(rest); err != nil {
		return err
	}
	var client myce.Client
	var err error
	if args[0] == "get" {
		client, err = authority.client()
	} else {
		client, err = authority.ownerClient()
	}
	if err != nil {
		return err
	}
	if client.HTTP != nil {
		defer client.HTTP.CloseIdleConnections()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if args[0] != "get" {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, 15*time.Second)
		defer stop()
	}
	var record myce.PriorityPolicyRecord
	switch args[0] {
	case "get":
		if f.NArg() != 0 || *revision != "" {
			return errors.New("policy get accepts no file or revision")
		}
		if err := client.Call(ctx, "GET", "/api/policy", nil, &record); err != nil {
			return err
		}
	case "set":
		if filename == "" && f.NArg() == 1 {
			filename = f.Arg(0)
		} else if f.NArg() != 0 {
			return errors.New("policy set requires one YAML file")
		}
		expected, err := strconv.ParseUint(*revision, 10, 64)
		if err != nil || filename == "" {
			return errors.New("policy set requires a file and --if-revision N")
		}
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		policy, err := myce.ReadPriorityPolicy(file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		request := struct {
			ExpectedRevision uint64              `json:"expected_revision"`
			Policy           myce.PriorityPolicy `json:"policy"`
		}{expected, policy}
		// The file is authoritative for file-store limits; an older queue ignores this.
		if err := client.Call(ctx, "PUT", "/api/policy?file_store_limits=1", request, &record); err != nil {
			return err
		}
	default:
		return errors.New("policy command must be get or set")
	}
	if *asYAML {
		fmt.Fprintf(os.Stderr, "policy revision %d\n", record.Revision)
		return yaml.NewEncoder(out).Encode(record.Policy)
	}
	return json.NewEncoder(out).Encode(record)
}
