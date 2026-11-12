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
	parentPID := os.Getppid()
	f := flag.NewFlagSet("run", flag.ContinueOnError)
	f.SetOutput(errOut)
	address := f.String("queue", os.Getenv("MYCE_QUEUE"), "queue address (default: installed config)")
	token := f.String("token", "", "fleet token (prefer installed config or MYCE_TOKEN)")
	config := f.String("config", "", "installed queue/worker config (default: installed worker, then queue)")
	joining := f.String("joining-file", os.Getenv("MYCE_JOINING_FILE"), "private pinned joining authority")
	gpu := f.String("gpu", "", "required accelerator: cuda, xpu or metal (default: CPU only)")
	cpu := f.Int("cpu", 1, "logical CPU cores to reserve")
	mem := f.String("mem", "", "host RAM for CPU jobs; accelerator RAM with --gpu (G/GiB or M/MiB); omit to let Mycelium learn it")
	hostMem := f.String("host-mem", "1G", "additional host RAM for GPU jobs")
	project := f.String("project", "default", "project")
	class := f.String("class", "", "job class (default: experiment)")
	priority := f.String("priority", "", "legacy priority: production, deadline or experiment")
	deadline := f.String("deadline", "", "RFC3339 deadline (required for deadline priority/class)")
	name := f.String("name", "", "job name (default: command basename)")
	detach := f.Bool("detach", false, "print job ID and return without waiting")
	wait := f.Bool("wait", false, "with an adapter entry or profile: wait, stream logs and return the command's exit code (125: Mycelium/run failure)")
	outDir := f.String("out", "", "download verified outputs into DIR after success; implies --wait")
	var outputs []string
	f.Func("output", "inline command output NAME to return with --out (repeatable)", func(value string) error {
		outputs = append(outputs, value)
		return nil
	})
	adapter := f.String("adapter", "", "project adapter YAML (default: .mycelium/adapter.yaml)")
	entry := f.String("entry", "", "named adapter entry (default: adapter default)")
	profile := f.String("profile", "", "project execution profile name or YAML path (returns a JSON receipt)")
	key := f.String("key", "", "profile submission key (default: content-derived; reuse to recover)")
	var inputFlags []string
	f.Func("input", "per-submission file DEST=PATH (repeatable; requires --profile or an adapter entry)", func(value string) error {
		inputFlags = append(inputFlags, value)
		return nil
	})
	notify := f.String("notify", os.Getenv("MYCE_NOTIFY"), "local notifier executable; must support --check and JSON on stdin")
	if err := f.Parse(args); err != nil {
		return err
	}
	// --wait and --out opt into the run_wait.go exit contract.
	waitMode := *wait || *outDir != ""
	if waitMode && *detach {
		return waitError(64, "--detach cannot be combined with --wait or --out")
	}
	itemInputs := map[string]string{}
	for _, value := range inputFlags {
		dest, local, ok := strings.Cut(value, "=")
		if !ok || !profileInputName(dest) || local == "" {
			return waitError(64, "profile_input_invalid: expected a clean DEST=PATH")
		}
		if _, exists := itemInputs[dest]; exists {
			return waitError(64, "profile_input_invalid: duplicate destination")
		}
		itemInputs[dest] = local
	}
	if *profile != "" && (*adapter != "" || *entry != "") {
		return errors.New("choose profile or adapter")
	}
	useAdapter := *adapter != "" || *entry != "" || f.NArg() == 0
	if *profile != "" || useAdapter {
		var overrides profileOverrides
		var unsupported string
		f.Visit(func(v *flag.Flag) {
			switch v.Name {
			case "class":
				overrides.Class = class
			case "deadline":
				overrides.Deadline = deadline
			case "gpu", "cpu", "mem", "host-mem", "project", "priority", "name", "detach", "output":
				unsupported = v.Name
			}
		})
		if unsupported != "" {
			return fmt.Errorf("--%s is not supported with a profile or adapter", unsupported)
		}
		notifyGiven := false
		f.Visit(func(v *flag.Flag) { notifyGiven = notifyGiven || v.Name == "notify" })
		// A submit that does not wait here wakes its submitter on completion by
		// default (MY-52); `--notify=` opts out. A foreground --wait already
		// returns the result, so it adds no second wake.
		if !notifyGiven && *notify == "" && !waitMode {
			*notify = harnessWakeDefault
		}
		submit := func(receipt io.Writer) error {
			if *profile == "" {
				return runAdapter(ctx, *adapter, *entry, f.Args(), itemInputs, *key, *notify, *config, *joining, *address, *token, receipt, errOut, overrides)
			}
			return runProfile(ctx, *profile, f.Args(), itemInputs, *key, *notify, *config, *joining, *address, *token, receipt, errOut, overrides)
		}
		if waitMode {
			return runReceiptWait(ctx, submit, parentPID, *outDir, *config, *joining, *address, *token, out, errOut)
		}
		return submit(out)
	}
	explicitNotify, explicitHostMem := false, false
	f.Visit(func(v *flag.Flag) {
		explicitNotify = explicitNotify || v.Name == "notify"
		explicitHostMem = explicitHostMem || v.Name == "host-mem"
	})
	if *key != "" || explicitNotify || len(itemInputs) > 0 {
		return errors.New("--key, --notify and --input require --profile or an adapter entry")
	}
	if f.NArg() == 0 {
		return errors.New("usage: myce run [flags] -- <command> [args]")
	}
	if (*outDir == "") != (len(outputs) == 0) {
		return waitError(64, "--out and --output go together: myce run --out DIR --output NAME -- <command>")
	}
	if *cpu <= 0 {
		return errors.New("cpu must be positive")
	}
	memory, err := 0.0, error(nil) // Omitted: the queue learns it (memory: auto).
	if *mem != "" {
		if memory, err = runMemory(*mem); err != nil {
			return err
		}
	}
	hostMemory, err := runMemory(*hostMem)
	if err != nil {
		return err
	}
	platform := "cpu"
	if *gpu != "" {
		if !myce.PhysicalAcceleratorPlatform(*gpu) {
			return errors.New("gpu must be cuda, xpu or metal")
		}
		platform = *gpu
	}
	if *name == "" {
		*name = filepath.Base(f.Arg(0))
	}
	s := myce.Spec{AuthoringOrigin: myce.OriginCommand, Name: *name, Project: *project, Class: *class, Priority: *priority,
		SubmissionKey: "run-" + rand.Text(), Run: myce.RunSpec{Command: f.Args()},
		Needs: myce.ResourceNeeds{CPUCores: *cpu, MemoryGB: memory, Platform: myce.Platform{Required: []string{platform}}}}
	var manifest string
	if len(outputs) > 0 {
		var cleanup func()
		if manifest, cleanup, err = attachRunOutputs(&s, outputs); err != nil {
			return err
		}
		defer cleanup()
	}
	if *gpu != "" && (*mem != "" || explicitHostMem) {
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
	defer func() { resultErr = redactRunError(resultErr, client.Token) }()
	if manifest != "" {
		if err = client.UploadJobInput(myce.WithFileStoreSubmission(ctx, s), manifest, s.Files[runOutputManifest]); err != nil {
			if ctx.Err() != nil {
				return errors.New("run interrupted before submission; nothing submitted")
			}
			return fmt.Errorf("output manifest upload failed: %w", err)
		}
	}
	j, err := client.Submit(ctx, s)
	if err != nil {
		if ctx.Err() != nil {
			return cancelRunJob(client, s.Project, s.SubmissionKey, waitMode)
		}
		return fmt.Errorf("submit failed (project %s, submission key %s): %w", s.Project, s.SubmissionKey, err)
	}
	if *detach {
		_, err = fmt.Fprintln(out, j.ID)
		return err
	}
	return followRunJob(ctx, client, j, s.Project, s.SubmissionKey, parentPID, waitMode, *outDir, out, errOut)
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

// Reuse installed trust even when credentials are supplied explicitly. A queue override
// changes only the endpoint, never removes installed trust.
func runClient(config, joining, address, token string) (myce.Client, error) {
	authority := runAuthority{Config: config, Joining: joining}
	return authority.client(address, token)
}

// Retain only private authority references for detached observers, never credentials.
// Both foreground and detached clients resolve through this same implementation.
type runAuthority struct{ Config, Joining string }

func (authority *runAuthority) client(address, token string) (myce.Client, error) {
	client, err := authority.resolve(address, token)
	if err != nil {
		return client, err
	}
	// A dev session's token reaches only its own queue, and its queue receives
	// only that token (#1270). Neither fleet nor dev authority crosses over.
	if queue, token, ok := devSessionQueue(); ok && queue != "" && token != "" && (client.Token == token) != (strings.TrimRight(client.URL, "/") == queue) {
		if client.HTTP != nil {
			client.HTTP.CloseIdleConnections()
		}
		return myce.Client{}, errors.New("inside myce dev, commands use only the dev session queue and its token")
	}
	return client, nil
}

// devSessionQueue reports a `myce dev -- COMMAND` session and the queue it
// exported. Detached observers keep only the marker and carry their own
// registered dev authority.
func devSessionQueue() (queue, token string, ok bool) {
	if os.Getenv("MYCE_DEV_SESSION_ROOT") == "" {
		return "", "", false
	}
	return strings.TrimRight(os.Getenv("MYCE_QUEUE"), "/"), os.Getenv("MYCE_TOKEN"), true
}

func (authority *runAuthority) resolve(address, token string) (myce.Client, error) {
	config, joining := authority.Config, authority.Joining
	if queue, sessionToken, ok := devSessionQueue(); ok && config == "" && joining == "" {
		// The dev session outranks installed fleet authority: never read the
		// installed config, joining file or fleet token here (#1270).
		if address == "" {
			address = queue
		}
		if token == "" {
			token = sessionToken
		}
	} else if config != "" || joining == "" {
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
			if config == "" && token != "" && os.IsNotExist(err) {
				continue // Explicit credentials still support an uninstalled dev client.
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
		client, err := invitation.Client(address)
		return client.WithProgressWatch(), err
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
	// Queue progress, not a fixed timeout, bounds each control call (#1016).
	return myce.Client{URL: address, Token: token}.WithProgressWatch(), nil
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
