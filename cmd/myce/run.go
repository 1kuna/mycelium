package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func runCommand(ctx context.Context, args []string, out, errOut io.Writer) (resultErr error) {
	f := flag.NewFlagSet("run", flag.ContinueOnError)
	f.SetOutput(errOut)
	address := f.String("queue", os.Getenv("MYCE_QUEUE"), "queue address (default: installed config)")
	token := f.String("token", "", "fleet token (prefer installed config or MYCE_TOKEN)")
	config := f.String("config", "", "installed queue/worker config (default: installed worker, then queue)")
	joining := f.String("joining-file", os.Getenv("MYCE_JOINING_FILE"), "private pinned joining authority")
	gpu := f.String("gpu", "", "required accelerator: cuda, xpu or metal (default: CPU only)")
	cpu := f.Int("cpu", 1, "logical CPU cores to reserve")
	mem := f.String("mem", "1G", "host RAM for CPU jobs; accelerator RAM with --gpu (G/GiB or M/MiB)")
	hostMem := f.String("host-mem", "1G", "additional host RAM for GPU jobs")
	project := f.String("project", "default", "project")
	class := f.String("class", "", "job class (default: experiment)")
	priority := f.String("priority", "", "legacy priority: production, deadline or experiment")
	deadline := f.String("deadline", "", "RFC3339 deadline (required for deadline priority/class)")
	name := f.String("name", "", "job name (default: command basename)")
	detach := f.Bool("detach", false, "print job ID and return without waiting")
	profile := f.String("profile", "", "project execution profile name or YAML path (returns a JSON receipt)")
	key := f.String("key", "", "profile submission key (default: content-derived; reuse to recover)")
	notify := f.String("notify", os.Getenv("MYCE_NOTIFY"), "local notifier executable; must support --check and JSON on stdin")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *profile != "" {
		return runProfile(ctx, *profile, f.Args(), *key, *notify, *config, *joining, *address, *token, out)
	}
	explicitNotify := false
	f.Visit(func(v *flag.Flag) {
		if v.Name == "notify" {
			explicitNotify = true
		}
	})
	if *key != "" || explicitNotify {
		return errors.New("--key and --notify require --profile")
	}
	if f.NArg() == 0 {
		return errors.New("usage: myce run [flags] -- <command> [args]")
	}
	if *cpu <= 0 {
		return errors.New("cpu must be positive")
	}
	memory, err := runMemory(*mem)
	if err != nil {
		return err
	}
	hostMemory, err := runMemory(*hostMem)
	if err != nil {
		return err
	}
	platform := "cpu"
	if *gpu != "" {
		if *gpu != "cuda" && *gpu != "xpu" && *gpu != "metal" {
			return errors.New("gpu must be cuda, xpu or metal")
		}
		platform = *gpu
	}
	if *name == "" {
		*name = filepath.Base(f.Arg(0))
	}
	s := myce.Spec{Name: *name, Project: *project, Class: *class, Priority: *priority,
		SubmissionKey: "run-" + rand.Text(), Run: myce.RunSpec{Command: f.Args()},
		Needs: myce.ResourceNeeds{CPUCores: *cpu, MemoryGB: memory, Platform: myce.Platform{Required: []string{platform}}}}
	if *gpu != "" {
		s.Needs.HostMemoryGB = hostMemory
	}
	if *deadline != "" {
		d, e := time.Parse(time.RFC3339, *deadline)
		if e != nil {
			return errors.New("deadline must be RFC3339")
		}
		s.Deadline = &d
	}
	if err = s.Validate(); err != nil {
		return err
	}
	if *token == "" {
		*token = os.Getenv("MYCE_TOKEN")
	}
	client, err := runClient(*config, *joining, *address, *token)
	if err != nil {
		return err
	}
	if client.HTTP != nil {
		defer client.HTTP.CloseIdleConnections()
	}
	// Never repeat credentials from transport/remote diagnostic text.
	defer func() {
		if resultErr != nil {
			resultErr = &exitError{exitCode(resultErr), errors.New(strings.ReplaceAll(resultErr.Error(), client.Token, "[redacted]"))}
		}
	}()
	cancelJob := func() error {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		ack, e := client.CancelSubmission(cleanup, s.Project, s.SubmissionKey)
		if e != nil || !ack.WillNotRun {
			return waitError(130, "run interrupted; cancellation unconfirmed; cancel via /api/jobs/cancel with project "+s.Project+" and submission_key "+s.SubmissionKey)
		}
		return waitError(130, "run interrupted; cancellation acknowledged (running cleanup may still be pending)")
	}
	j, err := client.Submit(ctx, s)
	if err != nil {
		if ctx.Err() != nil {
			return cancelJob()
		}
		return fmt.Errorf("submit failed (project %s, submission key %s): %w", s.Project, s.SubmissionKey, err)
	}
	if *detach {
		_, err = fmt.Fprintln(out, j.ID)
		return err
	}
	var after int64
	lastStatus := ""
	j, err = client.Wait(ctx, j.ID, func(j *myce.Job) error {
		line := j.ID + " " + j.State + " " + myce.JobReason(j)
		if line != lastStatus {
			if _, e := fmt.Fprintln(errOut, strings.ReplaceAll(line, client.Token, "[redacted]")); e != nil {
				return e
			}
			lastStatus = line
		}
		return drainJobLogs(ctx, client, j.ID, &after, out)
	})
	if ctx.Err() != nil {
		return cancelJob()
	}
	if err != nil {
		return err
	}
	if j.State == "succeeded" {
		return nil
	}
	code := 1
	if cause := j.ExecutionCause; j.State == "failed" && cause != nil && cause.Class == "command_exit" && cause.ExitCode != nil && *cause.ExitCode > 0 && *cause.ExitCode <= 255 {
		code = *cause.ExitCode
	}
	return waitError(code, fmt.Sprintf("job %s %s: %s", j.ID, j.State, myce.JobReason(j)))
}

func runMemory(value string) (float64, error) {
	raw := strings.ToUpper(value)
	scale := 1.0
	for _, suffix := range []string{"GIB", "GB", "G", "MIB", "MB", "M"} {
		if strings.HasSuffix(raw, suffix) {
			raw = strings.TrimSuffix(raw, suffix)
			if strings.HasPrefix(suffix, "M") {
				scale = 1.0 / 1024
			}
			break
		}
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 {
		return 0, errors.New("memory must be positive, for example 8G or 512M (binary units)")
	}
	return n * scale, nil
}

// Explicit credentials select the existing direct client; otherwise reuse the
// installation's authority, including its certificate pin. A queue override
// changes only the endpoint, never removes installed trust.
func runClient(config, joining, address, token string) (myce.Client, error) {
	authority := runAuthority{Config: config, Joining: joining}
	return authority.client(address, token)
}

// Retain only private authority references for detached observers, never credentials.
// Both foreground and detached clients resolve through this same implementation.
type runAuthority struct{ Config, Joining string }

func (authority *runAuthority) client(address, token string) (myce.Client, error) {
	config, joining := authority.Config, authority.Joining
	if config != "" || joining == "" && token == "" {
		paths := []string{config}
		if config == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return myce.Client{}, err
			}
			paths = []string{filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json"), filepath.Join(home, ".local", "share", "mycelium", "queue", "config.json")}
		}
		var saved myce.ServiceConfig
		for i, path := range paths {
			c, err := myce.ReadServiceConfig(path)
			if err == nil {
				saved = c
				authority.Config, err = filepath.Abs(path)
				if err != nil {
					return myce.Client{}, err
				}
				break
			}
			if !os.IsNotExist(err) || i == len(paths)-1 {
				return myce.Client{}, errors.New("cannot read installed config; use --config, --joining-file or MYCE_QUEUE/MYCE_TOKEN")
			}
		}
		if joining == "" {
			joining = saved.JoiningFile
		}
		if joining == "" && token == "" {
			var err error
			if saved.EnvironmentFile != "" {
				var queue string
				token, queue, err = myce.ReadFleetEnvironment(saved.EnvironmentFile)
				if address == "" {
					address = queue
				}
			} else {
				token, err = myce.ReadServiceToken(saved)
			}
			if err != nil {
				return myce.Client{}, errors.New("cannot read installed queue credentials")
			}
		}
		if address == "" {
			address = saved.Queue
		}
	}
	if joining != "" {
		var err error
		authority.Joining, err = filepath.Abs(joining)
		if err != nil {
			return myce.Client{}, err
		}
		invitation, err := myce.ReadInvitation(authority.Joining)
		if err != nil {
			return myce.Client{}, errors.New("cannot read private joining authority")
		}
		if address == "" {
			address = invitation.Address
		}
		return invitation.Client(address)
	}
	if address == "" {
		address = "http://127.0.0.1:7331"
	}
	u, err := url.Parse(address)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return myce.Client{}, errors.New("queue must be an HTTP(S) URL without credentials, query or fragment")
	}
	if token == "" {
		return myce.Client{}, errors.New("queue credential is missing")
	}
	return myce.Client{URL: address, Token: token}, nil
}

// Drain every available page, advancing only after a successful write. The same
// cursor is retained across transient read errors and the final terminal drain.
func drainJobLogs(ctx context.Context, c myce.Client, id string, after *int64, out io.Writer) error {
	for {
		var chunks []myce.Log
		if err := c.Call(ctx, "GET", fmt.Sprintf("/api/jobs/%s/logs?after=%d", id, *after), nil, &chunks); err != nil {
			return err
		}
		for _, chunk := range chunks {
			if _, err := io.WriteString(out, strings.ReplaceAll(chunk.Text, c.Token, "[redacted]")); err != nil {
				return err
			}
			*after = chunk.Seq
		}
		if len(chunks) < 256 {
			return nil
		}
	}
}
