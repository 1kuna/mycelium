package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"mycelium/internal/myce"
)

type projectDoctorCheck struct {
	Name     string `json:"name"`
	State    string `json:"state"`
	Owner    string `json:"owner,omitempty"`
	Action   string `json:"action"`
	Evidence string `json:"evidence,omitempty"`
}

// Project diagnosis shares run's decoder/source contract and resolve's read-only
// request. It never invokes installation probes, commands from the adapter,
// result checks, notifiers, uploads or worker NAS inspections.
func projectDoctor(ctx context.Context, args []string, out, errOut io.Writer) error {
	return projectDoctorWithAuthority(ctx, args, out, errOut, func(f clientFlags) (myce.Client, error) { return f.client() })
}

func projectDoctorWithAuthority(ctx context.Context, args []string, out, errOut io.Writer, authority func(clientFlags) (myce.Client, error)) error {
	f := flag.NewFlagSet("doctor --project", flag.ContinueOnError)
	var optionOutput bytes.Buffer
	f.SetOutput(&optionOutput)
	project := f.Bool("project", false, "diagnose a consumer project, read-only; no installation checks or changes")
	adapter := f.String("adapter", ".mycelium/adapter.yaml", "project adapter (paths resolve from the current project directory, as with run)")
	entry := f.String("entry", "", "adapter entry (default: adapter default)")
	notify := f.String("notify", os.Getenv("MYCE_NOTIFY"), "optional completion notifier executable; checked but never executed")
	asJSON := f.Bool("json", false, "structured checks and next action")
	credentials := installedClientFlags(f)
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, writeErr := io.WriteString(errOut, projectDoctorRedact(optionOutput.String(), os.Getenv("MYCE_TOKEN"), *credentials.token, *credentials.queue, *credentials.config, *credentials.joining))
			return writeErr
		}
		return waitError(2, "invalid project doctor options; use myce doctor --project --help")
	}
	if !*project || f.NArg() != 0 {
		return waitError(2, "usage: myce doctor --project [--adapter FILE] [--entry NAME] [--notify EXECUTABLE] [--json]")
	}
	checks := []projectDoctorCheck{{Name: "client support", State: "ready", Action: "project doctor, adapter schema 1 and read-only resolve are supported"}}
	add := func(name, state, owner, action string) {
		checks = append(checks, projectDoctorCheck{Name: name, State: state, Owner: owner, Action: action})
	}
	addFinding := func(prefix string, finding myce.ResolveFinding) {
		state, owner := "advice", "consumer"
		if b := myce.ResolveFindingHold([]myce.ResolveFinding{finding}); b != nil {
			state, owner = "blocked", b.Owner
		}
		add(prefix+finding.Field, state, owner, finding.Fix)
		checks[len(checks)-1].Evidence = finding.Problem
	}
	var client myce.Client
	finish := func() error {
		// Apply to every string before either renderer, including fields and fixes
		// echoed by a remote report. Never serialize installed authority.
		for i := range checks {
			c := &checks[i]
			for _, value := range []*string{&c.Name, &c.State, &c.Owner, &c.Action, &c.Evidence} {
				*value = projectDoctorRedact(*value, client.Token, client.URL, os.Getenv("MYCE_TOKEN"), *credentials.token, *credentials.queue, *credentials.config, *credentials.joining)
			}
		}
		blocked := slices.ContainsFunc(checks, func(c projectDoctorCheck) bool { return c.State == "blocked" })
		next := "run the selected entry, wait for completion, then fetch and verify its result"
		owner := "consumer"
		for _, c := range checks {
			if c.State == "blocked" {
				next, owner = c.Action, c.Owner
				break
			}
		}
		state := "ready"
		if blocked {
			state = "blocked"
		}
		if *asJSON {
			if err := json.NewEncoder(out).Encode(struct {
				State  string               `json:"state"`
				Owner  string               `json:"owner"`
				Action string               `json:"action"`
				Checks []projectDoctorCheck `json:"checks"`
			}{state, owner, next, checks}); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(out, "Project %s; next owner: %s; action: %s\n", state, owner, next)
			for _, c := range checks {
				fmt.Fprintf(out, "  %s: %s — %s", c.Name, c.State, c.Action)
				if c.Owner != "" {
					fmt.Fprintf(out, " (owner: %s)", c.Owner)
				}
				if c.Evidence != "" {
					fmt.Fprintf(out, "; %s", c.Evidence)
				}
				fmt.Fprintln(out)
			}
		}
		if blocked {
			return waitError(1, "project diagnosis found a blocker")
		}
		return nil
	}
	// Authority failures deliberately discard error strings: they may contain a
	// queue URL, config path, TLS details or credentials from a remote response.
	var authErr error
	client, authErr = authority(credentials)
	if authErr != nil {
		add("installed authority", "blocked", "installation owner", "restore installed queue authority; do not look up or copy the fleet token")
	} else {
		if client.HTTP != nil {
			defer client.HTTP.CloseIdleConnections()
		}
		var ping struct {
			Version int `json:"version"`
		}
		if err := client.Call(ctx, "GET", "/api/ping", nil, &ping); err != nil {
			authErr = err
			add("installed authority", "blocked", "queue owner", "restore authenticated queue connectivity and installed TLS authority")
		} else if ping.Version != 1 {
			authErr = errors.New("unsupported protocol")
			add("server support", "blocked", "queue owner", "upgrade the queue to the supported API protocol")
		} else {
			add("installed authority", "ready", "", "authenticated queue contact succeeded without exposing authority")
		}
	}
	root, err := os.Getwd()
	if err != nil {
		add("adapter", "blocked", "consumer", "run doctor from a readable project directory")
		return finish()
	}
	data, err := os.ReadFile(*adapter)
	if err != nil {
		add("adapter", "blocked", "consumer", "create a readable .mycelium/adapter.yaml, or select it with --adapter")
		return finish()
	}
	a, err := decodeAdapter(data)
	if err != nil {
		add("adapter", "blocked", "consumer", "fix adapter YAML/schema and field types; run.command requires a list of strings; raw YAML values are withheld")
		return finish()
	}
	p, _, err := adapterProfile(a, *entry)
	if err != nil {
		add("adapter", "blocked", "consumer", "select an existing entry and fix its declaration: "+err.Error())
		return finish()
	}
	adapterPath, _ := filepath.Abs(*adapter)
	if rel, err := filepath.Rel(root, adapterPath); err == nil && profileRelative(rel) {
		p.Source.AdapterFile = rel
	}
	add("adapter", "ready", "", "selected entry is valid")
	if _, _, err := identifyProfileSource(ctx, root, p.Source); err != nil {
		add("source cleanliness", "blocked", "consumer", "commit selected Git HEAD source changes, or fix the declared readable source; live adapter edits and receipts are excluded as in run")
		checks[len(checks)-1].Evidence = err.Error()
	} else {
		add("source cleanliness", "ready", "", "source satisfies run's source contract")
	}
	if *notify == "" {
		add("completion", "ready", "", "foreground myce wait is available; notifier is not configured (no delivery will be sent)")
	} else if _, err := resolveProfileNotifier(*notify); err != nil {
		add("completion", "blocked", "consumer", "provide an executable notifier owned by this thread, or omit --notify and use foreground myce wait")
	} else {
		add("completion", "ready", "", "notifier executable exists; delivery and application result checks are not executed by doctor")
	}
	if len(p.Check) == 0 && p.Native == nil {
		add("result acceptance", "advice", "consumer", "declare checks.result to verify application output after download; doctor does not execute it")
	} else {
		add("result acceptance", "ready", "", "result checks are declared; doctor does not execute them")
	}
	for _, name := range slices.Sorted(maps.Keys(p.Inputs)) {
		path := p.Inputs[name]
		if !projectFileReadable(root, path) {
			add("dependencies: inputs."+name, "blocked", "consumer", "make this input a readable regular file before run")
		}
	}
	for _, name := range slices.Sorted(maps.Keys(p.SharedInputs)) {
		input := p.SharedInputs[name]
		if input.Local != "" && !projectFileReadable(root, input.Local) {
			add("dependencies: shared_inputs."+name, "blocked", "consumer", "make the declared local shared input readable before run")
		}
	}
	request, local, err := resolveInput(*adapter, "", *entry, true)
	if err != nil {
		add("dependencies", "blocked", "consumer", "correct the selected entry and make its source and local dependencies readable")
		checks[len(checks)-1].Evidence = err.Error()
		return finish()
	}
	if request.Recipe == nil {
		if err := request.Spec.Validate(); err != nil {
			add("adapter", "blocked", "consumer", "correct the entry job fields: "+err.Error())
			return finish()
		}
	}
	for _, finding := range local.findings {
		addFinding("dependencies: ", finding)
	}
	if len(local.findings) == 0 {
		add("dependencies", "ready", "", "local resolve checks passed; run stages local inputs and source automatically")
	}
	if authErr != nil {
		add("resolve", "blocked", "queue owner", "restore queue authority before checking target coverage and queue-side dependencies")
		return finish()
	}
	var report myce.ResolveReport
	if err := client.Call(ctx, "POST", "/api/resolve", request, &report); err != nil {
		var h *myce.HTTPError
		if errors.As(err, &h) {
			switch {
			case h.Code == "shared_input_cache_capacity":
				add("dependencies: shared input cache", "blocked", "consumer", "reduce simultaneous shared-input bytes, or ask the queue owner for matching worker cache capacity; do not retry unchanged inputs")
				checks[len(checks)-1].Evidence = h.Message
			case h.PathDiagnostic != nil:
				owner := "consumer"
				if h.PathDiagnostic.Side != "submitter" {
					owner = h.PathDiagnostic.Side + " owner"
				}
				add("dependencies", "blocked", owner, "make the declared dependency readable on its reading side, or stage it through managed transport")
				checks[len(checks)-1].Evidence = h.PathDiagnostic.String()
			case h.Code == "no_qualified_target":
				add("target coverage", "blocked", "consumer", "correct the declared target requirements, or ask the queue owner to qualify a compatible target")
				checks[len(checks)-1].Evidence = h.Message
			case h.Status == 404 || h.Code == "queue_upgrade_required":
				add("server support", "blocked", "queue owner", "upgrade the queue for this client's read-only resolve request, then repeat doctor")
			case h.Status == 400:
				add("resolve request", "blocked", "consumer", "correct the adapter's job fields; if valid fields are unsupported, ask the queue owner to check client/server compatibility")
			default:
				add("resolve", "blocked", "queue owner", "restore queue resolve service; inspect the authenticated queue's diagnostics")
			}
		} else {
			add("resolve", "blocked", "queue owner", "restore queue resolve service; inspect the authenticated queue's diagnostics")
		}
		return finish()
	}
	add("server support", "ready", "", "queue accepts this client's read-only resolve request")
	for _, finding := range report.Findings {
		addFinding("resolve: ", finding)
	}
	if b := report.Hold; b != nil {
		state := "blocked"
		if b.Kind == "unprepared" && b.Owner == "queue" || b.Kind == "building" || b.Kind == "waiting_for_worker" {
			state = "advice"
		}
		add("resolve: "+b.Reason, state, b.Owner, b.Next)
		checks[len(checks)-1].Evidence = b.Detail
	}
	for _, name := range slices.Sorted(maps.Keys(request.Spec.SharedInputs)) {
		input := request.Spec.SharedInputs[name]
		if input.NAS != "" {
			add("dependencies: shared_inputs."+name+".nas", "blocked", "storage owner", "verify worker readability and immutable bytes at the declared NAS path, or stage through managed file transport; doctor does not inspect worker NAS")
		}
	}
	for _, input := range report.FileInputs {
		if input.State == "unknown" {
			add("dependencies: "+input.Field, "blocked", "queue owner", "restore a complete read-only file-store observation, then repeat doctor")
		}
	}
	if !slices.ContainsFunc(report.Targets, func(t myce.ResolveTarget) bool { return t.Eligible }) {
		if b := report.Hold; b != nil {
			add("target coverage", "blocked", b.Owner, b.Next)
			checks[len(checks)-1].Evidence = b.Detail
		} else {
			add("target coverage", "blocked", "queue owner", "restore a compatible qualified target or correct the declared needs")
			for _, target := range report.Targets {
				if target.Reason != "" {
					checks[len(checks)-1].Evidence = target.Reason
					break
				}
			}
		}
	} else {
		add("target coverage", "ready", "", "at least one eligible target; current load may delay launch and submit revalidates")
	}
	// Resolve findings are the source of remedies; never render arbitrary remote
	// errors, installed addresses, command/environment bytes or notifier paths.
	return finish()
}

func projectFileReadable(root, path string) bool {
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	var b [1]byte
	_, err = f.Read(b[:])
	return err == nil || errors.Is(err, io.EOF)
}

// Redact known authority before rendering typed diagnostics. Parse/transport
// errors that can embed arbitrary values are withheld at their source.
func projectDoctorRedact(value string, authority ...string) string {
	for _, secret := range authority {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[redacted]")
		}
	}
	return value
}
