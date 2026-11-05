package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"mycelium/internal/myce"
)

type projectDoctorCheck struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Owner  string `json:"owner,omitempty"`
	Action string `json:"action"`
}

// Project diagnosis shares run's decoder/source contract and resolve's read-only
// request. It never invokes installation probes, commands from the adapter,
// result checks, notifiers, uploads or worker NAS inspections.
func projectDoctor(ctx context.Context, args []string, out, errOut io.Writer) error {
	return projectDoctorWithAuthority(ctx, args, out, errOut, func(f clientFlags) (myce.Client, error) { return f.client() })
}

func projectDoctorWithAuthority(ctx context.Context, args []string, out, errOut io.Writer, authority func(clientFlags) (myce.Client, error)) error {
	f := flag.NewFlagSet("doctor --project", flag.ContinueOnError)
	f.SetOutput(errOut)
	project := f.Bool("project", false, "diagnose a consumer project, read-only; no installation checks or changes")
	adapter := f.String("adapter", ".mycelium/adapter.yaml", "project adapter (paths resolve from the current project directory, as with run)")
	entry := f.String("entry", "", "adapter entry (default: adapter default)")
	notify := f.String("notify", os.Getenv("MYCE_NOTIFY"), "optional completion notifier executable; checked but never executed")
	asJSON := f.Bool("json", false, "structured checks and next action")
	credentials := installedClientFlags(f)
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return waitError(2, "invalid project doctor options")
	}
	if !*project || f.NArg() != 0 {
		return waitError(2, "usage: myce doctor --project [--adapter FILE] [--entry NAME] [--notify EXECUTABLE] [--json]")
	}
	checks := []projectDoctorCheck{{Name: "client support", State: "ready", Action: "project doctor, adapter schema 1 and read-only resolve are supported"}}
	add := func(name, state, owner, action string) {
		checks = append(checks, projectDoctorCheck{name, state, owner, action})
	}
	finish := func() error {
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
	client, authErr := authority(credentials)
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
		add("adapter", "blocked", "consumer", "fix adapter YAML/schema with myce spec check --resolve --entry NAME FILE")
		return finish()
	}
	p, _, err := adapterProfile(a, *entry)
	if err != nil {
		add("adapter", "blocked", "consumer", "select an existing entry and fix its run, targets and result-check declaration")
		return finish()
	}
	adapterPath, _ := filepath.Abs(*adapter)
	if rel, err := filepath.Rel(root, adapterPath); err == nil && profileRelative(rel) {
		p.Source.AdapterFile = rel
	}
	add("adapter", "ready", "", "selected entry is valid")
	if _, _, err := identifyProfileSource(ctx, root, p.Source); err != nil {
		add("source cleanliness", "blocked", "consumer", "commit selected Git HEAD source changes, or fix the declared readable source; live adapter edits and receipts are excluded as in run")
	} else {
		add("source cleanliness", "ready", "", "source satisfies run's source contract")
	}
	if *notify == "" {
		add("completion", "ready", "", "foreground myce wait is available; notifier is not configured (no delivery will be sent)")
	} else if info, err := os.Stat(*notify); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		add("completion", "blocked", "consumer", "provide an executable notifier owned by this thread, or omit --notify and use foreground myce wait")
	} else {
		add("completion", "ready", "", "notifier executable exists; delivery and application result checks are not executed by doctor")
	}
	for name, path := range p.Inputs {
		if !projectFileReadable(root, path) {
			add("dependencies: inputs."+name, "blocked", "consumer", "make this input a readable regular file before run")
		}
	}
	for name, input := range p.SharedInputs {
		if input.Local != "" && !projectFileReadable(root, input.Local) {
			add("dependencies: shared_inputs."+name, "blocked", "consumer", "make the declared local shared input readable before run")
		}
	}
	request, local, err := resolveInput(*adapter, "", *entry, true)
	if err != nil {
		add("dependencies", "blocked", "consumer", "correct the selected entry and make its source and local dependencies readable")
		return finish()
	}
	if request.Recipe == nil {
		if err := request.Spec.Validate(); err != nil {
			add("adapter", "blocked", "consumer", "correct the entry job fields using myce spec check --resolve")
			return finish()
		}
	}
	for _, finding := range local.findings {
		add("dependencies: "+finding.Field, projectFindingState(finding), "consumer", finding.Fix)
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
		if errors.As(err, &h) && (h.Status == 404 || h.Status == 400) {
			add("server support", "blocked", "queue owner", "upgrade the queue for this client's read-only resolve request, then repeat doctor")
		} else {
			add("resolve", "blocked", "queue owner", "restore queue resolve service; inspect the authenticated queue's diagnostics")
		}
		return finish()
	}
	add("server support", "ready", "", "queue accepts this client's read-only resolve request")
	for _, finding := range report.Findings {
		owner := "consumer"
		if finding.Side == "queue" && finding.PathDiagnostic != nil {
			owner = "queue owner"
		}
		add("resolve: "+finding.Field, projectFindingState(finding), owner, finding.Fix)
	}
	for _, input := range report.FileInputs {
		if input.State == "unknown" {
			add("dependencies: "+input.Field, "blocked", "queue owner", "restore a complete read-only file-store observation, then repeat doctor")
		}
	}
	if !slices.ContainsFunc(report.Targets, func(t myce.ResolveTarget) bool { return t.Eligible }) {
		add("target coverage", "blocked", "queue owner", "restore a compatible qualified target; inspect myce spec check --resolve for the exact failing constraint")
	} else {
		add("target coverage", "ready", "", "at least one eligible target; current load may delay launch and submit revalidates")
	}
	// Resolve findings are the source of remedies; never render arbitrary remote
	// errors, installed addresses, command/environment bytes or notifier paths.
	return finish()
}

func projectFindingState(f myce.ResolveFinding) string {
	if f.Field == "source" || strings.HasPrefix(f.Field, "run.command[") || strings.HasPrefix(f.Field, "args[") || f.Field == "allowed_machines" && strings.Contains(f.Problem, "host pin limits") {
		return "advice"
	}
	return "blocked"
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
