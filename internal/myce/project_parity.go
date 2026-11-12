package myce

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Project-owned production parity approvals (#1341, MY-44) name kinds of
// machine, never hosts (MY-51, #1503). A project approves a kind with its own
// passing output comparison (MY-19); every host of that kind may then run its
// production. Host-named approvals from the previous release are converted to
// their host's kinds (MY-42); the host-form store and the queue policy field
// production_parity_hosts stay as read-only mirrors for one release so a
// rollback keeps enforcing the same hosts, and are then removed.

// ParityEvidence names the comparison that approved a kind. Migrated entries
// came from host-named approvals; they keep that approval's job when it had one.
type ParityEvidence struct {
	Job      string `json:"job,omitempty"`
	Attempt  int    `json:"attempt,omitempty"`
	Migrated bool   `json:"migrated,omitempty"`
}

// ParityApproval approves one kind of machine for one work identity.
type ParityApproval struct {
	Kind     MachineKind    `json:"kind,omitzero"`
	Evidence ParityEvidence `json:"evidence"`
	AddedAt  time.Time      `json:"added_at,omitzero"`
	// MigratedFrom is the host whose previous-release approval became this kind.
	MigratedFrom string `json:"migrated_from,omitempty"`
	// PendingHost is a previous-release host approval whose host has not yet
	// reported its OS and architecture. It admits that host only and becomes
	// kinds when the host checks in, so no production path is dropped.
	PendingHost string `json:"pending_host,omitempty"`
}

func (a ParityApproval) label() string {
	if a.PendingHost != "" {
		return "pending conversion of a host approval"
	}
	return a.Kind.String()
}

// ProjectParity is one project's approvals, keyed by exact work identity.
type ProjectParity struct {
	Project  string                      `json:"project"`
	Kinds    map[string][]ParityApproval `json:"kinds"`
	Warnings []string                    `json:"warnings,omitempty"`
}

// ParityRequest approves (with Evidence) or removes one kind for one project.
// Kind may be omitted on add: the evidence run's own kind is approved. Host is
// accepted from previous-release clients for one release and converted (MY-42).
type ParityRequest struct {
	Project  string `json:"project"`
	Work     string `json:"work"`
	Kind     string `json:"kind,omitempty"`
	Host     string `json:"host,omitempty"`
	Evidence string `json:"evidence,omitempty"`
}

type parityKindStore struct {
	Projects map[string]map[string][]ParityApproval `json:"projects"`
}

func (s *parityKindStore) clone() *parityKindStore {
	out := &parityKindStore{Projects: map[string]map[string][]ParityApproval{}}
	if s != nil {
		for project, works := range s.Projects {
			out.Projects[project] = cloneParityWorks(works)
		}
	}
	return out
}

func cloneParityWorks[T any](works map[string][]T) map[string][]T {
	out := make(map[string][]T, len(works))
	for work, list := range works {
		out[work] = slices.Clone(list)
	}
	return out
}

// Host-form mirror read by the previous release (#1341). Written only from
// the kind store; never read for placement here except before migration.
type ParityHost struct {
	Host     string         `json:"host"`
	Evidence ParityEvidence `json:"evidence"`
	AddedAt  time.Time      `json:"added_at,omitzero"`
}

type projectParityStore struct {
	Projects map[string]map[string][]ParityHost `json:"projects"`
}

func parityHostNames(hosts []ParityHost) []string {
	names := make([]string, 0, len(hosts))
	for _, h := range hosts {
		names = append(names, h.Host)
	}
	return names
}

func validParityWork(work string) bool {
	prefix, name, _ := strings.Cut(work, ":")
	return (prefix == "kind" || prefix == "program" || prefix == "extension") && strings.TrimSpace(name) != "" && work == strings.TrimSpace(work)
}

// workRunsAs reports whether registered extensions provide work through
// declared variants, and whether any of them runs as target t. Unknown work
// runs anywhere, so conversion keeps every kind for it.
func (q *Queue) workRunsAs(work string, t ExecutionTarget) (variants, runs bool) {
	known := false
	for _, e := range q.s.Extensions {
		if !slices.Contains(extensionWorks(e), work) {
			continue
		}
		known = true
		if len(e.Variants) == 0 {
			runs = true
			continue
		}
		variants = true
		runs = runs || e.variant(t) != nil
	}
	return variants, runs || !known
}

// hostKinds are the exact kinds a host runs work as, as placement sees its
// views: the CPU view and each accelerator. A machine without device
// inventory is one view; CPU-only jobs still run as cpu on it unless the work
// is variant-based. With work, kinds that work cannot run as are dropped
// unless that would drop all of them. Empty while the host has not reported
// its OS and architecture.
func (q *Queue) hostKinds(host, work string) []MachineKind {
	var all, fit []MachineKind
	add := func(list *[]MachineKind, t ExecutionTarget) {
		if k, ok := exactKind(t); ok && !slices.Contains(*list, k) {
			*list = append(*list, k)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(q.s.Machines)) {
		m := q.s.Machines[id]
		if m.Replacement != nil || hostID(m) != host {
			continue
		}
		targets := []ExecutionTarget{machineTarget(m)}
		if m.Devices != nil {
			targets = []ExecutionTarget{machineTarget(cpuMachineView(m))}
			for _, d := range m.Devices {
				view := d.machine(*m)
				targets = append(targets, machineTarget(&view))
			}
		} else if variants, _ := q.workRunsAs(work, targets[0]); work == "" || !variants {
			cpu := targets[0]
			cpu.Platform = "cpu"
			targets = append(targets, cpu)
		}
		for _, t := range targets {
			add(&all, t)
			if _, runs := q.workRunsAs(work, t); work == "" || runs {
				add(&fit, t)
			}
		}
	}
	if len(fit) > 0 {
		all = fit
	}
	slices.SortFunc(all, func(a, b MachineKind) int { return strings.Compare(a.String(), b.String()) })
	return all
}

// convertHostApprovals turns one work's host-named approvals into the kinds
// that work runs as on each host.
func (q *Queue) convertHostApprovals(work string, hosts []ParityHost) []ParityApproval {
	out := make([]ParityApproval, 0, len(hosts))
	for _, h := range hosts {
		evidence := h.Evidence
		evidence.Migrated = true
		kinds := q.hostKinds(h.Host, work)
		if len(kinds) == 0 {
			out = appendApproval(out, ParityApproval{PendingHost: h.Host, Evidence: evidence, AddedAt: h.AddedAt})
		}
		for _, k := range kinds {
			out = appendApproval(out, ParityApproval{Kind: k, Evidence: evidence, AddedAt: h.AddedAt, MigratedFrom: h.Host})
		}
	}
	return out
}

// appendApproval keeps one entry per kind (and per pending host); the first wins.
func appendApproval(list []ParityApproval, a ParityApproval) []ParityApproval {
	if slices.ContainsFunc(list, func(b ParityApproval) bool { return b.Kind == a.Kind && b.PendingHost == a.PendingHost }) {
		return list
	}
	return append(list, a)
}

func hostApprovals(hosts []string) []ParityHost {
	out := make([]ParityHost, 0, len(hosts))
	for _, host := range hosts {
		out = append(out, ParityHost{Host: host, Evidence: ParityEvidence{Migrated: true}})
	}
	return out
}

// convertedParity is the kind store, converting previous-release state when
// none exists yet. It does not write state.
func (q *Queue) convertedParity() *parityKindStore {
	if q.s.ParityKinds != nil {
		return q.s.ParityKinds
	}
	store := &parityKindStore{Projects: map[string]map[string][]ParityApproval{}}
	convert := func(project string, works map[string][]ParityHost) {
		out := map[string][]ParityApproval{}
		for work, hosts := range works {
			out[work] = q.convertHostApprovals(work, hosts)
		}
		store.Projects[project] = out
	}
	if q.s.ProjectParity != nil {
		for project, works := range q.s.ProjectParity.Projects {
			convert(project, works)
		}
		return store
	}
	for project, policy := range q.s.Policies {
		if policy.ProductionParityHosts != nil {
			works := map[string][]ParityHost{}
			for work, hosts := range policy.ProductionParityHosts {
				works[work] = hostApprovals(hosts)
			}
			convert(project, works)
		}
	}
	return store
}

// migrateParityKinds stores the kind store once, converting previous-release
// host approvals, then converts any pending host that has since reported its
// kind. Call inside a transaction; it replaces state values, never shared maps.
func (q *Queue) migrateParityKinds() {
	if q.s.ParityKinds == nil {
		q.s.ParityKinds = q.convertedParity()
		for project, works := range q.s.ParityKinds.Projects {
			for work, approvals := range works {
				for _, a := range approvals {
					if a.MigratedFrom != "" {
						log.Printf("production parity: project %s work %s: host-named approval of %s converted to machine kind %s (MY-51)", project, work, a.MigratedFrom, a.Kind)
					} else if a.PendingHost != "" {
						log.Printf("production parity: project %s work %s: host-named approval of %s kept until that host reports its machine kind (MY-51)", project, work, a.PendingHost)
					}
				}
			}
		}
	}
	q.convertPendingParity()
}

// convertPendingParity replaces pending host approvals whose host now reports
// its kind. Cheap when nothing is pending; check-in calls it.
func (q *Queue) convertPendingParity() {
	if q.s.ParityKinds == nil {
		return
	}
	var store *parityKindStore
	for project, works := range q.s.ParityKinds.Projects {
		for work, approvals := range works {
			ready := slices.ContainsFunc(approvals, func(a ParityApproval) bool {
				return a.PendingHost != "" && len(q.hostKinds(a.PendingHost, work)) > 0
			})
			if !ready {
				continue
			}
			if store == nil {
				store = q.s.ParityKinds.clone()
			}
			list := []ParityApproval{}
			for _, a := range approvals {
				if a.PendingHost == "" || len(q.hostKinds(a.PendingHost, work)) == 0 {
					list = appendApproval(list, a)
					continue
				}
				for _, c := range q.convertHostApprovals(work, []ParityHost{{Host: a.PendingHost, Evidence: a.Evidence, AddedAt: a.AddedAt}}) {
					list = appendApproval(list, c)
					log.Printf("production parity: project %s work %s: pending approval of %s converted to machine kind %s (MY-51)", project, work, a.PendingHost, c.Kind)
				}
			}
			store.Projects[project][work] = list
		}
	}
	if store != nil {
		q.s.ParityKinds = store
		q.mirrorProjectParity()
	}
}

// parityApprovals is the one read of a project's approvals for a work
// identity. configured distinguishes an explicit empty list from none.
func (q *Queue) parityApprovals(project, work string) (approvals []ParityApproval, configured bool) {
	approvals, configured = q.convertedParity().Projects[project][work]
	return approvals, configured
}

func approvalAdmits(a ParityApproval, j *Job, m *Machine) bool {
	if a.PendingHost != "" {
		return a.PendingHost == hostID(m)
	}
	return a.Kind.Matches(jobTargetOn(j, m))
}

// parityApproved reports whether the project's approvals name the kind this
// job runs as on view m.
func (q *Queue) parityApproved(j *Job, m *Machine) (approved, configured bool) {
	approvals, configured := q.parityApprovals(j.Spec.Project, j.Spec.workType())
	return slices.ContainsFunc(approvals, func(a ParityApproval) bool { return approvalAdmits(a, j, m) }), configured
}

// An explicit approval list constrains every production placement, including
// historical registrations and variants with measured quality evidence.
func (q *Queue) productionParityAllowed(j *Job, m *Machine) bool {
	approved, configured := q.parityApproved(j, m)
	return !configured || approved
}

// mirrorProjectParity writes the hosts the kind approvals currently admit into
// the host-form store and the queue policy field, so a rollback to the
// previous release keeps enforcing them (MY-42).
func (q *Queue) mirrorProjectParity() {
	if q.s.ParityKinds == nil {
		return
	}
	mirror := &projectParityStore{Projects: map[string]map[string][]ParityHost{}}
	hosts := []string{}
	for _, m := range q.s.Machines {
		if m.Replacement == nil && !slices.Contains(hosts, hostID(m)) {
			hosts = append(hosts, hostID(m))
		}
	}
	slices.Sort(hosts)
	kinds := map[string][]MachineKind{}
	for _, host := range hosts {
		kinds[host] = q.hostKinds(host, "")
	}
	for project, works := range q.s.ParityKinds.Projects {
		out := map[string][]ParityHost{}
		for work, approvals := range works {
			list := []ParityHost{}
			for _, host := range hosts {
				for _, a := range approvals {
					admits := a.PendingHost == host || a.PendingHost == "" && slices.ContainsFunc(kinds[host], func(k MachineKind) bool {
						return a.Kind.Matches(ExecutionTarget{OS: k.OS, Arch: k.Arch, Platform: k.Platform})
					})
					if admits {
						list = append(list, ParityHost{Host: host, Evidence: a.Evidence, AddedAt: a.AddedAt})
						break
					}
				}
			}
			for _, a := range approvals {
				if a.PendingHost != "" && !slices.Contains(hosts, a.PendingHost) {
					list = append(list, ParityHost{Host: a.PendingHost, Evidence: a.Evidence, AddedAt: a.AddedAt})
				}
			}
			slices.SortFunc(list, func(a, b ParityHost) int { return strings.Compare(a.Host, b.Host) })
			out[work] = list
		}
		mirror.Projects[project] = out
	}
	q.s.ProjectParity = mirror
	if len(q.s.Policies) == 0 {
		return
	}
	policies := maps.Clone(q.s.Policies)
	for project, policy := range policies {
		var field map[string][]string
		if works, ok := mirror.Projects[project]; ok {
			field = make(map[string][]string, len(works))
			for work, list := range works {
				field[work] = parityHostNames(list)
			}
		}
		policy.ProductionParityHosts = field
		policies[project] = policy
	}
	q.s.Policies = policies
}

// seedProjectParity converts host lists from a startup or dev project config
// into kind approvals, replacing only the projects that config lists. Host
// names there are accepted for one release with a warning (MY-42).
func (q *Queue) seedProjectParity(policies map[string]ProjectPolicy) {
	q.migrateParityKinds()
	var store *parityKindStore
	for _, project := range slices.Sorted(maps.Keys(policies)) {
		policy := policies[project]
		if policy.ProductionParityHosts == nil {
			continue
		}
		if store == nil {
			store = q.s.ParityKinds.clone()
		}
		works := map[string][]ParityApproval{}
		for work, hosts := range policy.ProductionParityHosts {
			works[work] = q.convertHostApprovals(work, hostApprovals(hosts))
			for _, a := range works[work] {
				log.Printf("production parity: project config %s work %s: production_parity_hosts names hosts; converted to %s (MY-51). Host names are accepted for one release; use myce parity add --kind", project, work, a.label())
			}
		}
		store.Projects[project] = works
	}
	if store != nil {
		q.s.ParityKinds = store
	}
}

// checkPolicyParityUnchanged keeps the policy field read-only: an omitted field
// keeps the approvals, the current mirror is accepted, a change is refused.
func (q *Queue) checkPolicyParityUnchanged(project string, approvals map[string][]string) error {
	if approvals == nil {
		return nil
	}
	current := map[string][]string{}
	if q.s.ProjectParity != nil {
		for work, hosts := range q.s.ProjectParity.Projects[project] {
			current[work] = parityHostNames(hosts)
		}
	}
	same := len(current) == len(approvals)
	for work, hosts := range approvals {
		have, ok := current[work]
		same = same && ok && slices.Equal(have, hosts)
	}
	if same {
		return nil
	}
	return &HTTPError{Status: http.StatusConflict, Code: "parity_hosts_project_owned", Message: fmt.Sprintf("production_parity_hosts is a read-only mirror; production approval names machine kinds, owned by the project (MY-51); leave the field unchanged and run myce parity add --project %s --work WORK --evidence JOB (or myce parity remove --kind KIND) instead", project)}
}

func (q *Queue) projectParityRecord(project string) ProjectParity {
	out := ProjectParity{Project: project, Kinds: cloneParityWorks(q.convertedParity().Projects[project])}
	for _, work := range slices.Sorted(maps.Keys(out.Kinds)) {
		for _, a := range out.Kinds[work] {
			if a.PendingHost != "" {
				out.Warnings = append(out.Warnings, fmt.Sprintf("%s: a previous-release host approval is waiting for its host to report its machine kind; until then it admits that host only", work))
			}
		}
	}
	return out
}

func parityRequestError(message string) error {
	return &HTTPError{Status: http.StatusBadRequest, Code: "parity_request_invalid", Message: message}
}

func validateParityRequest(r ParityRequest) (MachineKind, error) {
	if r.Project == "" {
		return MachineKind{}, parityRequestError("project required; pass the project that owns this production work")
	}
	if err := validateProjectIdentifier(r.Project); err != nil {
		return MachineKind{}, parityRequestError(err.Error())
	}
	if !validParityWork(r.Work) {
		return MachineKind{}, parityRequestError("work must be an exact kind:<name>, program:<type> or extension:<name> identity")
	}
	if r.Host != "" && r.Kind != "" {
		return MachineKind{}, parityRequestError("name a machine kind, not a host (MY-51); drop host")
	}
	if r.Host != "" && r.Host != strings.TrimSpace(r.Host) {
		return MachineKind{}, parityRequestError("host must be exact")
	}
	if r.Kind == "" {
		return MachineKind{}, nil
	}
	kind, err := ParseMachineKind(r.Kind)
	if err != nil {
		return MachineKind{}, parityRequestError(err.Error())
	}
	return kind, nil
}

func hostFieldWarning(kinds string) string {
	return "host-named approval converted to machine kind " + kinds + " (MY-51); the host field is accepted for one release; name a kind with --kind, or omit it to approve the evidence run's own kind"
}

// ProjectParity returns one project's production approvals.
func (q *Queue) ProjectParity(project string, requestContexts ...context.Context) (ProjectParity, error) {
	if err := validateProjectIdentifier(project); err != nil || project == "" {
		return ProjectParity{}, parityRequestError("project required; pass the project that owns this production work")
	}
	unlock := requestLock(requestContext(requestContexts), &q.mu)
	defer unlock()
	return q.projectParityRecord(project), nil
}

// AddParity approves a kind of machine for the request project's production
// after its own passing output comparison on a machine of that kind. Without
// a kind it approves the exact kind the comparison ran as. Only the named
// project's approvals change.
func (q *Queue) AddParity(r ParityRequest, requestContexts ...context.Context) (ProjectParity, error) {
	kind, err := validateParityRequest(r)
	if err != nil {
		return ProjectParity{}, err
	}
	var out ProjectParity
	err = q.updateRequest(requestContext(requestContexts), func() error {
		if r.Host != "" {
			if err := q.validateProductionParityHosts(map[string][]string{r.Work: {r.Host}}); err != nil {
				return &HTTPError{Status: http.StatusBadRequest, Code: "parity_host_unknown", Message: fmt.Sprintf("unknown host %q; production approval names a machine kind (MY-51): omit host", r.Host)}
			}
		}
		evidence, ran, err := q.parityEvidence(r)
		if err != nil {
			return err
		}
		if kind == (MachineKind{}) {
			exact, ok := exactKind(ran)
			if !ok {
				return parityRequestError(fmt.Sprintf("evidence job %s ran on a host that has not reported its OS and architecture; wait for its next check-in, or name the kind with --kind", r.Evidence))
			}
			kind = exact
		} else if !kind.Matches(ran) {
			return &HTTPError{Status: http.StatusBadRequest, Code: "parity_evidence_required", Message: fmt.Sprintf("MY-19 parity evidence required: evidence job %s ran as %s, which is not kind %s; next: run the comparison on a machine of kind %s (declare its needs so Mycelium places it there), or approve %s, then repeat myce parity add with --evidence JOB_ID", r.Evidence, targetLabel(ran), kind, kind, targetLabel(ran))}
		}
		q.migrateParityKinds()
		store := q.s.ParityKinds.clone()
		works := store.Projects[r.Project]
		if works == nil {
			works = map[string][]ParityApproval{}
			store.Projects[r.Project] = works
		}
		approvals := slices.DeleteFunc(works[r.Work], func(a ParityApproval) bool { return a.PendingHost == "" && a.Kind == kind })
		works[r.Work] = append(approvals, ParityApproval{Kind: kind, Evidence: evidence, AddedAt: q.clock.Now()})
		q.s.ParityKinds = store
		q.mirrorProjectParity()
		out = q.projectParityRecord(r.Project)
		if r.Host != "" {
			out.Warnings = append(out.Warnings, hostFieldWarning(kind.String()))
		}
		return nil
	}, nil)
	return out, err
}

// RemoveParity withdraws one kind; narrowing needs no evidence. Removing the
// last kind keeps an explicit empty list, which admits no machine.
func (q *Queue) RemoveParity(r ParityRequest, requestContexts ...context.Context) (ProjectParity, error) {
	kind, err := validateParityRequest(r)
	if err != nil {
		return ProjectParity{}, err
	}
	if r.Kind == "" && r.Host == "" {
		return ProjectParity{}, parityRequestError("kind required; run myce parity list --project " + r.Project + " and remove one of its kinds")
	}
	var out ProjectParity
	err = q.updateRequest(requestContext(requestContexts), func() error {
		q.migrateParityKinds()
		approvals, ok := q.s.ParityKinds.Projects[r.Project][r.Work]
		// A previous-release client names a host: withdraw that host's
		// pending approval and the exact kinds it runs as.
		hostKinds := q.hostKinds(r.Host, r.Work)
		match := func(a ParityApproval) bool {
			if r.Host != "" {
				return a.PendingHost == r.Host || a.PendingHost == "" && slices.Contains(hostKinds, a.Kind)
			}
			return a.PendingHost == "" && a.Kind == kind
		}
		if !ok || !slices.ContainsFunc(approvals, match) {
			return parityRequestError(fmt.Sprintf("%s has no production approval for %s matching %s; run myce parity list --project %s", r.Project, r.Work, cmp.Or(r.Kind, r.Host), r.Project))
		}
		store := q.s.ParityKinds.clone()
		store.Projects[r.Project][r.Work] = slices.DeleteFunc(store.Projects[r.Project][r.Work], match)
		q.s.ParityKinds = store
		q.mirrorProjectParity()
		out = q.projectParityRecord(r.Project)
		if r.Host != "" {
			labels := []string{}
			for _, k := range hostKinds {
				labels = append(labels, k.String())
			}
			out.Warnings = append(out.Warnings, "host-named removal withdrew that host's machine kinds ("+strings.Join(labels, ", ")+") (MY-51); the host field is accepted for one release; remove by --kind")
		}
		return nil
	}, nil)
	return out, err
}

// parityEvidence accepts the project's own succeeded check or experiment for
// the exact work identity and returns the target its accepted attempt ran as.
// The project owns its comparison criteria; the job succeeds only when
// outputs match.
func (q *Queue) parityEvidence(r ParityRequest) (ParityEvidence, ExecutionTarget, error) {
	run := fmt.Sprintf("run a check or experiment of %s as project %s that compares outputs with your reference on a machine of the kind to approve (declare its needs so Mycelium places it there), then repeat myce parity add with --evidence JOB_ID", r.Work, r.Project)
	refuse := func(why string) (ParityEvidence, ExecutionTarget, error) {
		return ParityEvidence{}, ExecutionTarget{}, &HTTPError{Status: http.StatusBadRequest, Code: "parity_evidence_required", Message: "MY-19 parity evidence required: " + why + "; next: " + run}
	}
	if r.Evidence == "" {
		return refuse("no evidence job named")
	}
	j := q.evidenceJob(r.Evidence)
	switch {
	case j == nil:
		return refuse(fmt.Sprintf("evidence job %s is unknown", r.Evidence))
	case j.Spec.Project != r.Project:
		return refuse(fmt.Sprintf("evidence job %s belongs to another project", r.Evidence))
	case j.Spec.workType() != r.Work:
		return refuse(fmt.Sprintf("evidence job %s ran %s, not %s", r.Evidence, j.Spec.workType(), r.Work))
	case j.Spec.ExtensionCheck == nil && (j.Spec.Priority == "production" || jobClass(j.Spec) == "production"):
		return refuse(fmt.Sprintf("evidence job %s is production work, not a comparison check or experiment", r.Evidence))
	case j.State != "succeeded" || cancelled(j):
		return refuse(fmt.Sprintf("evidence job %s is %s; only a succeeded comparison counts", r.Evidence, JobState(j)))
	}
	m := q.s.Machines[j.Machine]
	if m == nil {
		return refuse(fmt.Sprintf("evidence job %s ran on a machine the queue no longer knows, so its kind cannot be read", r.Evidence))
	}
	if r.Host != "" && hostID(m) != r.Host {
		return refuse(fmt.Sprintf("evidence job %s ran on host %s, not %s", r.Evidence, hostID(m), r.Host))
	}
	view := allocationMachine(m, j.AllocatedDevices)
	if m.Devices != nil && len(j.AllocatedDevices) == 0 {
		view = cpuMachineView(m)
	}
	return ParityEvidence{Job: j.ID, Attempt: j.Attempt}, jobTargetOn(j, view), nil
}

func (q *Queue) evidenceJob(id string) *Job {
	if j := q.s.Jobs[id]; j != nil {
		return j
	}
	for _, record := range q.s.Submissions {
		if record.Kind == submissionBinding && record.JobID == id && record.Job != nil {
			return record.Job
		}
	}
	return nil
}

// The /api/parity-hosts routes serve previous-release clients for one release
// (MY-42); their host field is converted with a warning.
func (a API) registerProjectParity(mux *requestMux) {
	list := func(w http.ResponseWriter, r *http.Request) {
		record, err := a.Queue.ProjectParity(r.URL.Query().Get("project"), r.Context())
		respond(w, record, err)
	}
	add := func(w http.ResponseWriter, r *http.Request) {
		var request ParityRequest
		if !decode(w, r, &request) {
			return
		}
		record, err := a.Queue.AddParity(request, r.Context())
		respondOperation(w, record, err, "parity_request_invalid")
	}
	remove := func(w http.ResponseWriter, r *http.Request) {
		var request ParityRequest
		if !decode(w, r, &request) {
			return
		}
		if request.Evidence != "" {
			respond(w, nil, parityRequestError("evidence is not used when removing a kind"))
			return
		}
		record, err := a.Queue.RemoveParity(request, r.Context())
		respondOperation(w, record, err, "parity_request_invalid")
	}
	for _, base := range []string{"/api/parity", "/api/parity-hosts"} {
		mux.HandleFunc("GET "+base, list)
		mux.HandleFunc("POST "+base, add)
		mux.HandleFunc("POST "+base+"/remove", remove)
	}
}
