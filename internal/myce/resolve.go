package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// ResolveRequest asks which targets could run a job as authored. Resolve is
// explanation only: no job, upload, build, reservation or execution permit.
type ResolveRequest struct {
	SourceUnverified   bool              `json:"source_unverified,omitempty"`
	UnstagedInputs     []string          `json:"unstaged_inputs,omitempty"`
	InspectNAS         bool              `json:"inspect_nas,omitempty"`
	Recipe             *Extension        `json:"recipe,omitempty"`
	Spec               Spec              `json:"spec"`
	RequirementReasons map[string]string `json:"requirement_reasons,omitempty"`
}

type ResolveReport struct {
	Linkage     []MachOLinkage     `json:"linkage,omitempty"`
	Hold        *Hold              `json:"hold,omitempty"`
	FileInputs  []ResolveFileInput `json:"file_inputs,omitempty"`
	ObservedAt  time.Time          `json:"observed_at"`
	Resolved    []string           `json:"resolved"`
	Targets     []ResolveTarget    `json:"targets"`
	Findings    []ResolveFinding   `json:"findings"`
	Portability *PortabilityReport `json:"portability,omitempty"`
	Notes       []string           `json:"notes"`
}

type ResolveEnvironment struct {
	Key     string `json:"key"`
	Backend string `json:"backend"`
	State   string `json:"state"`
}

type ResolveTarget struct {
	session       string
	freshUntil    time.Time
	cachedInputs  map[string]bool
	nasInspection bool
	Environment   *ResolveEnvironment `json:"environment,omitempty"`
	NASInputs     []ResolveNASInput   `json:"nas_inputs,omitempty"`
	Machine       string              `json:"machine"`
	Target        ExecutionTarget     `json:"target"`
	Freshness     string              `json:"freshness"`
	Eligible      bool                `json:"eligible"`
	Reason        string              `json:"reason"`
}

// ResolveFinding names the field, the side that cannot see or run it
// (submitter, queue or worker) and a concrete fix.
type ResolveFinding struct {
	Field          string          `json:"field"`
	Side           string          `json:"side"`
	Problem        string          `json:"problem"`
	Fix            string          `json:"fix"`
	PathDiagnostic *PathDiagnostic `json:"path_diagnostic,omitempty"`
}

// StagingFix is the managed-transport alternative to reading local paths or
// pinning a job to the host that holds its data.
const StagingFix = "Stage it through managed transport: small private files as `inputs: {NAME: RELATIVE_PATH}` submitted with `myce submit --files-from DIR` (the command reads NAME in its working folder); large immutable data with `myce file put PATH`, then `shared_inputs: {NAME: {sha256: HASH, size_bytes: N}}` (or `{nas: RELATIVE, sha256: HASH, size_bytes: N}` for NAS data) read through $MYCE_INPUT_NAME; code through a profile source snapshot."

// Resolve snapshots the queue, then reads environment recipes and evaluates
// targets outside q.mu with the same fit checks submission uses.
func (q *Queue) Resolve(req ResolveRequest) (ResolveReport, error) {
	return q.ResolveContext(context.Background(), req)
}

func (q *Queue) ResolveContext(ctx context.Context, req ResolveRequest) (ResolveReport, error) {
	if req.RequirementReasons == nil {
		req.RequirementReasons = req.Spec.RequirementReasons
	}
	if req.Recipe != nil {
		return q.resolveNative(req, ctx)
	}
	view, err := q.captureResolveView(ctx)
	if err != nil {
		return ResolveReport{}, err
	}
	s := cloneJob(&Job{Spec: req.Spec}).Spec
	if err := s.Validate(); err != nil {
		return ResolveReport{}, specRejection(err)
	}
	if err := view.defaultMemory(&s); err != nil {
		return ResolveReport{}, specRejection(err)
	}
	if err := normalizeResources(&s); err != nil {
		return ResolveReport{}, specRejection(err)
	}
	if !s.extensionJob() {
		if err := lowerExecution(&s); err != nil {
			return ResolveReport{}, specRejection(err)
		}
	}
	r := ResolveReport{ObservedAt: view.clock.Now(), Resolved: []string{}, Targets: []ResolveTarget{}, Findings: []ResolveFinding{}, Notes: []string{
		"Read-only: no job, upload, build, reservation or placement change. Eligibility uses permanent capacity; current load decides when work starts. Submission revalidates everything.",
	}}
	q.resolveFileInputs(ctx, req, &r)
	r.Notes = append(r.Notes, PlacementAdvisories(s, req.RequirementReasons)...)
	if plainMemoryWork(s) && s.Needs.Memory == "auto" && s.ProfileMemory == "" {
		r.Notes = append(r.Notes, "Auto memory learns from accepted samples of the same identity: project, name, exact command, environment and inputs. Changing a name, command argument or input can start a new identity; arbitrary arguments may change memory need. For comparable runs of one program with different items, use an adapter entry or execution profile with a stable name/base command and pass item arguments after -- or files with --input. Leave memory_class omitted to share the profile's base memory class; use memory_class: per_item when item arguments or bytes change memory need. This is guidance, not a submission error.")
	}
	if err := view.validateMachineNames(s.AllowedMachines); err != nil {
		r.Findings = append(r.Findings, ResolveFinding{Field: "allowed_machines", Side: "submitter", Problem: err.Error(), Fix: "Use the current machine name, or omit allowed_machines and declare needs."})
	}
	if err := view.validateJobExclusions(s); err != nil {
		r.Findings = append(r.Findings, ResolveFinding{Field: "excluded_machines", Side: "submitter", Problem: err.Error(), Fix: "Use current machine names and leave a machine permitted by the project policy."})
	}
	// Extension jobs resolve against the captured registry, as submit does.
	extensions, err := view.resolveExtensions(&s)
	if err != nil {
		r.Findings = append(r.Findings, ResolveFinding{Field: "extension", Side: "queue", Problem: err.Error(), Fix: "Reference exactly one registered extension (name@version or extension_digest); `myce portability --extension NAME@VERSION` shows its targets."})
	}
	recipes, sidecars, ok := view.resolveRecipes(s, &r)
	r.Findings = append(r.Findings, resolvePathFindings(s)...)
	var nasInputs []string
	for name, input := range s.SharedInputs {
		if input.NAS != "" {
			nasInputs = append(nasInputs, "shared_inputs."+name+".nas")
		}
	}
	if len(nasInputs) > 0 {
		slices.Sort(nasInputs)
		r.Notes = append(r.Notes, strings.Join(nasInputs, ", ")+": worker NAS reads are unverified. Paths are relative to each worker's --checkpoint-root; without --inspect-nas, resolve does not check their existence or bytes. For data elsewhere, use profile local: or myce file put PATH and a shared input with sha256 and size_bytes, without nas.")
	}
	if _, err := view.modelLimits(s); err != nil {
		r.Findings = append(r.Findings, ResolveFinding{Field: "model_snapshots", Side: "queue", Problem: err.Error(), Fix: "This queue has explicit models.sources restrictions; declare a repository matching those restrictions."})
	} else if len(s.ModelSnapshots) > 0 {
		r.Notes = append(r.Notes, "Model snapshots are verified by the worker before launch; a missing or changed snapshot fails the attempt with a named model_snapshot reason.")
	}
	if !ok || err != nil {
		r.Hold = ResolveFindingHold(r.Findings)
		r.Notes = append(r.Notes, "Targets are evaluated once the environment and extension resolve.")
		return r, nil
	}
	if len(s.Files) > 0 {
		r.Notes = append(r.Notes, "file_inputs reports ready only after a read-only SHA-256/size verification of file-store bytes, which waits for the shared verification slot for as long as the caller waits; local placeholders need staging, and a check the caller abandoned stays unknown. Submit verifies all bytes again before admission.")
	}
	var discovery *CommandDiscovery
	if WantsCommandDiscovery(s) {
		var err error
		discovery, err = deriveCommandDiscovery(s, recipes, sidecars)
		if err != nil {
			r.Findings = append(r.Findings, ResolveFinding{Field: "portable", Side: "queue", Problem: err.Error(), Fix: "Select managed member locks with known intersecting targets, or author explicit targets."})
			discovery = &CommandDiscovery{}
		}
	}
	job := func(s Spec) *Job {
		j := &Job{ID: "resolve", State: "queued", Spec: s, Extensions: extensions, EnvironmentRecipes: recipes, SidecarEnvironmentRecipes: sidecars, CommandDiscovery: discovery}
		_ = normalizeJobClass(&j.Spec)
		_ = normalizeResources(&j.Spec)
		if !j.Spec.extensionJob() {
			_ = lowerExecution(&j.Spec)
		}
		view.applyLearnedNeeds(j)
		return j
	}
	j := job(s)
	if err := view.sharedInputCapacityRefusal(j); err != nil {
		return ResolveReport{}, err
	}
	if !environmentPlatformsMatch(j.Spec.Needs, recipes) {
		r.Findings = append(r.Findings, ResolveFinding{Field: "environment.platforms", Side: "queue", Problem: "the environment has no recipe for the platform this job's needs select", Fix: "Add a recipe for that platform (for CPU work, a cpu recipe), or change needs to match an existing recipe."})
	}
	r.Targets = view.resolveTargets(j)
	if !s.extensionJob() {
		sets := map[string]map[string]EnvironmentRecipe{}
		if s.Environment.Key() != "" {
			sets["main"] = recipes
		}
		for name, recipes := range sidecars {
			sets[name] = recipes
		}
		p := view.portabilityWithRecipes(PortabilityRequest{Spec: &s, RequirementReasons: req.RequirementReasons}, sets)
		r.Portability = &p
	}
	r.Hold = view.jobHold(j)
	if r.Hold != nil && r.Hold.Kind == "waiting_for_worker" {
		r.Hold = nil
	}

	eligible := 0
	for _, t := range r.Targets {
		if t.Eligible {
			eligible++
		}
	}
	if len(s.AllowedMachines) > 0 && strings.TrimSpace(req.RequirementReasons["host"]) == "" {
		unpinned := s
		unpinned.AllowedMachines = nil
		var extra []string
		for _, t := range view.resolveTargets(job(unpinned)) {
			if t.Eligible && !slices.Contains(s.AllowedMachines, t.Machine) {
				extra = append(extra, t.Machine)
			}
		}
		if len(extra) > 0 {
			r.Findings = append(r.Findings, ResolveFinding{Field: "allowed_machines", Side: "submitter", Problem: fmt.Sprintf("host pin limits this job to %d eligible target(s); without it %s could also run it", eligible, strings.Join(extra, ", ")), Fix: "If the pin exists only because the data is on that host, drop allowed_machines. " + StagingFix + " Keep the pin, with requirement_reasons.host, for an explicit owner restriction, specific physical hardware or qualification. The project's machine-kind production approval needs no matching job pin."})
		}
	}
	if eligible == 0 && len(r.Targets) > 0 {
		r.Notes = append(r.Notes, "No eligible target: fix the first blocker on the closest target; submission with require_target would refuse.")
	}
	if finding := ResolveFindingHold(r.Findings); finding != nil {
		r.Hold = finding
	}
	return r, nil
}

// resolveRecipes reads catalog or inline recipes exactly as submit does,
// without freezing them into the store. Inline lock files are small reads
// relative to the queue's working directory, done outside q.mu.
func (q *Queue) resolveRecipes(s Spec, r *ResolveReport) (map[string]EnvironmentRecipe, map[string]map[string]EnvironmentRecipe, bool) {
	library := sortedPortabilityKeys(q.environments.Environments)
	fix := "Select a recipe from the queue-visible environment library with `environment: NAME`"
	if len(library) > 0 {
		fix += " (available: " + strings.Join(library, ", ") + ")"
	} else {
		fix += " (the queue has no library configured; ask its operator to add one with --environment-config)"
	}
	fix += ", or run without a managed environment using tools already on the worker PATH."
	recipes, err := q.environments.resolveEnvironment(s.Environment)
	var sidecars map[string]map[string]EnvironmentRecipe
	if err == nil {
		sidecars, err = q.environments.resolveSidecarEnvironments(s)
	}
	if err != nil {
		f := ResolveFinding{Field: "environment", Side: "queue", Problem: "the queue cannot read this environment", Fix: fix, PathDiagnostic: pathDiagnostic(err)}
		if d := f.PathDiagnostic; d != nil {
			f.Field, f.Side, f.Problem = d.Field, d.Side, d.String()
			if d.ResolutionBase == "queue-working-directory" {
				f.Problem += "; inline environment paths are read on the queue host, not the submitter"
			}
		} else {
			f.Problem = err.Error()
			f.Fix = "Correct the environment declaration to satisfy this validation error, then run `myce spec check --resolve` again."
			var npmErr *NPMLockError
			if errors.As(err, &npmErr) {
				f.Fix = "Run `npm install` so package-lock.json matches package.json, commit both, then run `myce spec check --resolve` again."
			}
			if isPixiLockError(err) {
				f.Fix = "Run `pixi lock` with Pixi >= " + workerPixiVersion() + " (the workers' version), then run `myce spec check --resolve` again."
			}
		}
		r.Findings = append(r.Findings, f)
		return nil, nil, false
	}
	platforms := sortedPortabilityKeys(recipes)
	switch {
	case s.Environment.Name != "" && len(recipes) > 0:
		r.Resolved = append(r.Resolved, fmt.Sprintf("environment %s: queue library recipes for %s", s.Environment.Name, strings.Join(platforms, ", ")))
	case s.Environment.Key() != "" && len(recipes) > 0:
		source := "inline lock read on the queue host"
		if len(s.Environment.Files) > 0 {
			source = "inline lock uploaded by the submitter"
		}
		r.Resolved = append(r.Resolved, "environment: "+source+", recipes for "+strings.Join(platforms, ", "))
	case s.Environment.Key() != "":
		r.Resolved = append(r.Resolved, "environment "+s.Environment.Key()+": legacy worker-advertised name; readiness depends on each worker's --environments")
	default:
		r.Resolved = append(r.Resolved, "environment: none; the command uses tools on each worker's PATH")
	}
	return recipes, sidecars, true
}

// resolveTargets reports the closest fit per target. Stale inventory is
// unknown rather than eligible.
func (q *Queue) resolveTargets(j *Job) []ResolveTarget {
	best := map[string]targetFit{}
	rows := map[string]*ResolveTarget{}
	q.evaluateTargets(j, func(fit targetFit, candidate *Job, m *Machine) bool {
		if j.Spec.Needs.cpuOnly() && m.Device != "" {
			return true
		}
		if old, ok := best[fit.key]; ok && !fit.better(old) {
			return true
		}
		best[fit.key] = fit
		target := machineTarget(m)
		if j.Spec.Needs.cpuOnly() {
			target.Platform = "cpu"
		}
		row := &ResolveTarget{Machine: m.ID, Target: portabilityTarget(target), Freshness: "fresh", Eligible: fit.failures == 0}
		if key := environmentKey(candidate, m); key != "" {
			state := "prepare-needed"
			if environmentReady(candidate, m) {
				state = "ready"
			}
			r := candidate.EnvironmentRecipes[m.GPU]
			if extension := qualifiedExtension(candidate, m); extension != nil {
				if recipe, ok := extensionRecipe(*extension, m); ok {
					r = recipe
				}
			}
			backend := r.Backend
			if backend == "" {
				if r.LockFile != "" {
					backend = "uv"
				} else if r.Key != "" {
					backend = "binary"
				} else {
					backend = "worker-advertised"
				}
			}
			row.Environment = &ResolveEnvironment{Key: key, Backend: backend, State: state}
		}
		if row.Eligible {
			row.Reason = "eligible"
			if j.Spec.Environment.Key() != "" {
				if environmentReady(candidate, m) {
					row.Reason += "; environment prepared"
				} else {
					row.Reason += "; environment prepare-needed (built before first launch)"
				}
			}
		} else {
			row.Reason = targetFitReason(fit.first, candidate, m)
		}
		row.nasInspection = slices.Contains(m.Capabilities, "nas_inspection_v1")
		row.cachedInputs = map[string]bool{}
		for _, key := range m.CachedSharedInputs {
			row.cachedInputs[key] = true
		}
		live := q.s.Machines[m.ID]
		if live != nil {
			row.session = live.Session
			row.freshUntil = q.lastContact(live, q.clock.Now()).Add(q.workerArrivalAllowance(live))
		}
		if live == nil || live.LastSeen.IsZero() {
			row.Freshness = "unknown"
		} else if !q.workerFresh(live, q.clock.Now()) {
			row.Freshness = "stale"
		}
		if row.Freshness != "fresh" && row.Eligible {
			row.Eligible, row.Reason = false, "unknown: worker inventory "+row.Freshness
		}
		if row.Environment != nil {
			if row.Freshness != "fresh" {
				row.Environment.State = "unknown"
			} else if !row.Eligible && row.Environment.State != "ready" {
				row.Environment.State = "blocked"
			}
		}
		rows[fit.key] = row
		return true
	})
	out := make([]ResolveTarget, 0, len(rows))
	for _, key := range sortedPortabilityKeys(rows) {
		out = append(out, *rows[key])
	}
	return out
}

// systemPath covers paths every worker's OS supplies; other absolute paths
// name one machine's filesystem.
func systemPath(p string) bool {
	for _, prefix := range []string{"/usr/", "/bin/", "/sbin/", "/etc/", "/dev/", "/System/"} {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// resolvePathFindings flags authored absolute paths and the system
// interpreter. It never rewrites arguments; a shell string is only scanned.
func resolvePathFindings(s Spec) []ResolveFinding {
	var out []ResolveFinding
	for i, arg := range append(slices.Clone(s.Run.Command), s.Args...) {
		field := fmt.Sprintf("run.command[%d]", i)
		if i >= len(s.Run.Command) {
			field = fmt.Sprintf("args[%d]", i-len(s.Run.Command))
		}
		for _, word := range strings.Fields(arg) {
			word = strings.Trim(word, `"'`)
			if filepath.IsAbs(word) && !systemPath(word) {
				out = append(out, ResolveFinding{Field: field, Side: "worker", Problem: fmt.Sprintf("absolute path %q is read on whichever worker runs the job; only hosts with the same path and bytes can run it", word), Fix: StagingFix})
				break
			}
		}
	}
	if len(s.Run.Command) > 0 && s.Environment.Key() == "" && strings.HasPrefix(filepath.Base(s.Run.Command[0]), "python") {
		out = append(out, ResolveFinding{Field: "run.command[0]", Side: "worker", Problem: "the command uses each worker's system Python, whose version and packages vary by host", Fix: "Select a locked environment from the queue library (`environment: NAME`), or declare `environment: {lock_file: ..., python: '3.12'}` so uv provides the interpreter; never install into system Python."})
	}
	return out
}

func WriteResolve(dest io.Writer, r ResolveReport, asJSON bool) error {
	if asJSON {
		e := json.NewEncoder(dest)
		e.SetIndent("", "  ")
		return e.Encode(r)
	}
	var out strings.Builder
	if r.Portability == nil {
		for _, d := range r.Linkage {
			fmt.Fprintln(&out, "Warning: "+d.String())
		}
	}
	if r.Hold != nil {
		fmt.Fprintln(&out, r.Hold.FullText())
	}
	fmt.Fprintf(&out, "Resolution observed %s\n", r.ObservedAt.Format(time.RFC3339))
	for _, line := range r.Resolved {
		fmt.Fprintln(&out, "  resolved "+line)
	}
	for _, t := range r.Targets {
		fmt.Fprintf(&out, "  %s %s (%s): %s\n", t.Machine, targetLabel(t.Target), t.Freshness, t.Reason)
	}
	if len(r.Targets) == 0 && len(r.Findings) == 0 {
		fmt.Fprintln(&out, "  no known machine targets")
	}
	for _, target := range r.Targets {
		for _, input := range target.NASInputs {
			observed := "unknown"
			if !input.ObservedAt.IsZero() {
				observed = input.ObservedAt.Format(time.RFC3339)
			}
			fmt.Fprintf(&out, "  %s shared_inputs.%s.nas [worker]: %s (observed %s; existence only, bytes unverified)\n", target.Machine, input.Name, input.State, observed)
		}
	}
	for _, input := range r.FileInputs {
		fmt.Fprintf(&out, "  %s [queue]: %s\n", input.Field, input.State)
	}
	for _, f := range r.Findings {
		fmt.Fprintf(&out, "\n%s [%s]: %s\n  fix: %s\n", f.Field, f.Side, f.Problem, f.Fix)
	}
	if r.Portability != nil {
		var native strings.Builder
		if err := WritePortability(&native, *r.Portability, false); err != nil {
			return err
		}
		out.WriteString(native.String())
	}
	for _, note := range r.Notes {
		fmt.Fprintln(&out, "\n"+note)
	}
	_, err := io.WriteString(dest, out.String())
	return err
}
