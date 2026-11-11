package myce

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Project-owned production parity host lists (#1341, MY-44). A project widens
// its own list with its own passing output comparison (MY-19); no queue-owner
// policy edit is involved. The queue policy field production_parity_hosts is a
// read-only mirror for one release (MY-42) and is then removed.

// ParityEvidence names the comparison that admitted a host. Migrated entries
// were copied from queue policy and carry no job.
type ParityEvidence struct {
	Job      string `json:"job,omitempty"`
	Attempt  int    `json:"attempt,omitempty"`
	Migrated bool   `json:"migrated,omitempty"`
}

type ParityHost struct {
	Host     string         `json:"host"`
	Evidence ParityEvidence `json:"evidence"`
	AddedAt  time.Time      `json:"added_at,omitzero"`
}

// ProjectParityHosts is one project's lists, keyed by exact work identity.
type ProjectParityHosts struct {
	Project string                  `json:"project"`
	Hosts   map[string][]ParityHost `json:"hosts"`
}

// ParityHostRequest adds (with Evidence) or removes one host for one project.
type ParityHostRequest struct {
	Project  string `json:"project"`
	Work     string `json:"work"`
	Host     string `json:"host"`
	Evidence string `json:"evidence,omitempty"`
}

// A non-nil store means the policy lists were copied; it is never re-copied.
type projectParityStore struct {
	Projects map[string]map[string][]ParityHost `json:"projects"`
}

func (s *projectParityStore) clone() *projectParityStore {
	out := &projectParityStore{Projects: map[string]map[string][]ParityHost{}}
	if s != nil {
		for project, works := range s.Projects {
			out.Projects[project] = cloneParityWorks(works)
		}
	}
	return out
}

func cloneParityWorks(works map[string][]ParityHost) map[string][]ParityHost {
	out := make(map[string][]ParityHost, len(works))
	for work, hosts := range works {
		out[work] = slices.Clone(hosts)
	}
	return out
}

func parityHostNames(hosts []ParityHost) []string {
	names := make([]string, 0, len(hosts))
	for _, h := range hosts {
		names = append(names, h.Host)
	}
	return names
}

func migratedParityHosts(approvals map[string][]string) map[string][]ParityHost {
	works := make(map[string][]ParityHost, len(approvals))
	for work, hosts := range approvals {
		works[work] = make([]ParityHost, 0, len(hosts))
		for _, host := range hosts {
			works[work] = append(works[work], ParityHost{Host: host, Evidence: ParityEvidence{Migrated: true}})
		}
	}
	return works
}

func validParityWork(work string) bool {
	prefix, name, _ := strings.Cut(work, ":")
	return (prefix == "kind" || prefix == "program" || prefix == "extension") && strings.TrimSpace(name) != "" && work == strings.TrimSpace(work)
}

// parityHostList is the one read of a project's production host list for a
// work identity. configured distinguishes an explicit empty list from none.
func (q *Queue) parityHostList(project, work string) (hosts []string, configured bool) {
	if q.s.ProjectParity == nil {
		hosts, configured = q.s.Policies[project].ProductionParityHosts[work]
		return hosts, configured
	}
	entries, configured := q.s.ProjectParity.Projects[project][work]
	if !configured {
		return nil, false
	}
	return parityHostNames(entries), true
}

// migrateProjectParity copies the queue-policy lists once. Call inside a
// transaction; it replaces state values rather than mutating shared maps.
func (q *Queue) migrateProjectParity() {
	if q.s.ProjectParity != nil {
		return
	}
	store := &projectParityStore{Projects: map[string]map[string][]ParityHost{}}
	for project, policy := range q.s.Policies {
		if policy.ProductionParityHosts != nil {
			store.Projects[project] = migratedParityHosts(policy.ProductionParityHosts)
		}
	}
	q.s.ProjectParity = store
}

// seedProjectParity copies lists from a startup or dev project config into the
// project store, replacing only the projects that config lists.
func (q *Queue) seedProjectParity(policies map[string]ProjectPolicy) {
	q.migrateProjectParity()
	var store *projectParityStore
	for project, policy := range policies {
		if policy.ProductionParityHosts == nil {
			continue
		}
		if store == nil {
			store = q.s.ProjectParity.clone()
		}
		store.Projects[project] = migratedParityHosts(policy.ProductionParityHosts)
	}
	if store != nil {
		q.s.ProjectParity = store
	}
}

// mirrorProjectParity writes the project lists into the stored policy field so
// a rollback to the previous release keeps enforcing them (MY-42).
func (q *Queue) mirrorProjectParity() {
	if q.s.ProjectParity == nil || len(q.s.Policies) == 0 {
		return
	}
	policies := maps.Clone(q.s.Policies)
	for project, policy := range policies {
		var mirror map[string][]string
		if works, ok := q.s.ProjectParity.Projects[project]; ok {
			mirror = make(map[string][]string, len(works))
			for work, hosts := range works {
				mirror[work] = parityHostNames(hosts)
			}
		}
		policy.ProductionParityHosts = mirror
		policies[project] = policy
	}
	q.s.Policies = policies
}

// checkPolicyParityUnchanged keeps the policy field read-only: an omitted field
// keeps the project list, an identical one is accepted, a change is refused.
func (q *Queue) checkPolicyParityUnchanged(project string, approvals map[string][]string) error {
	if approvals == nil {
		return nil
	}
	current := map[string][]string{}
	if q.s.ProjectParity == nil {
		current = q.s.Policies[project].ProductionParityHosts
	} else {
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
	return &HTTPError{Status: http.StatusConflict, Code: "parity_hosts_project_owned", Message: fmt.Sprintf("production_parity_hosts is project-owned and read-only in queue policy; leave it unchanged and run myce parity add --project %s --work WORK --host HOST --evidence JOB (or myce parity remove) instead", project)}
}

func (q *Queue) projectParityRecord(project string) ProjectParityHosts {
	out := ProjectParityHosts{Project: project, Hosts: map[string][]ParityHost{}}
	if q.s.ProjectParity == nil {
		for work, hosts := range migratedParityHosts(q.s.Policies[project].ProductionParityHosts) {
			out.Hosts[work] = hosts
		}
		return out
	}
	maps.Copy(out.Hosts, cloneParityWorks(q.s.ProjectParity.Projects[project]))
	return out
}

func parityRequestError(message string) error {
	return &HTTPError{Status: http.StatusBadRequest, Code: "parity_request_invalid", Message: message}
}

func validateParityRequest(r ParityHostRequest) error {
	if r.Project == "" {
		return parityRequestError("project required; pass the project that owns this production work")
	}
	if err := validateProjectIdentifier(r.Project); err != nil {
		return parityRequestError(err.Error())
	}
	if !validParityWork(r.Work) {
		return parityRequestError("work must be an exact kind:<name>, program:<type> or extension:<name> identity")
	}
	if r.Host == "" || r.Host != strings.TrimSpace(r.Host) {
		return parityRequestError("host must be an exact physical host ID from myce status")
	}
	return nil
}

// ProjectParityHosts returns one project's production parity host lists.
func (q *Queue) ProjectParityHosts(project string, requestContexts ...context.Context) (ProjectParityHosts, error) {
	if err := validateProjectIdentifier(project); err != nil || project == "" {
		return ProjectParityHosts{}, parityRequestError("project required; pass the project that owns this production work")
	}
	unlock := requestLock(requestContext(requestContexts), &q.mu)
	defer unlock()
	return q.projectParityRecord(project), nil
}

// AddParityHost widens the request project's own list after its passing
// output comparison on that host. Only the named project's list changes.
func (q *Queue) AddParityHost(r ParityHostRequest, requestContexts ...context.Context) (ProjectParityHosts, error) {
	if err := validateParityRequest(r); err != nil {
		return ProjectParityHosts{}, err
	}
	var out ProjectParityHosts
	err := q.updateRequest(requestContext(requestContexts), func() error {
		if err := q.validateProductionParityHosts(map[string][]string{r.Work: {r.Host}}); err != nil {
			return &HTTPError{Status: http.StatusBadRequest, Code: "parity_host_unknown", Message: fmt.Sprintf("unknown host %q; use a physical host ID from myce status", r.Host)}
		}
		evidence, err := q.parityEvidence(r)
		if err != nil {
			return err
		}
		q.migrateProjectParity()
		store := q.s.ProjectParity.clone()
		works := store.Projects[r.Project]
		if works == nil {
			works = map[string][]ParityHost{}
			store.Projects[r.Project] = works
		}
		hosts := slices.DeleteFunc(works[r.Work], func(h ParityHost) bool { return h.Host == r.Host })
		works[r.Work] = append(hosts, ParityHost{Host: r.Host, Evidence: evidence, AddedAt: q.clock.Now()})
		q.s.ProjectParity = store
		q.mirrorProjectParity()
		out = q.projectParityRecord(r.Project)
		return nil
	}, nil)
	return out, err
}

// RemoveParityHost narrows the project's list; narrowing needs no evidence.
// Removing the last host keeps an explicit empty list, which admits no host.
func (q *Queue) RemoveParityHost(r ParityHostRequest, requestContexts ...context.Context) (ProjectParityHosts, error) {
	if err := validateParityRequest(r); err != nil {
		return ProjectParityHosts{}, err
	}
	var out ProjectParityHosts
	err := q.updateRequest(requestContext(requestContexts), func() error {
		q.migrateProjectParity()
		hosts, ok := q.s.ProjectParity.Projects[r.Project][r.Work]
		if !ok || !slices.ContainsFunc(hosts, func(h ParityHost) bool { return h.Host == r.Host }) {
			return parityRequestError(fmt.Sprintf("host %q is not in %s's production parity list for %s; run myce parity list --project %s", r.Host, r.Project, r.Work, r.Project))
		}
		store := q.s.ProjectParity.clone()
		store.Projects[r.Project][r.Work] = slices.DeleteFunc(store.Projects[r.Project][r.Work], func(h ParityHost) bool { return h.Host == r.Host })
		q.s.ProjectParity = store
		q.mirrorProjectParity()
		out = q.projectParityRecord(r.Project)
		return nil
	}, nil)
	return out, err
}

// parityEvidence accepts the project's own succeeded check or experiment for
// the exact work identity whose accepted attempt ran on the host. The project
// owns its comparison criteria; the job succeeds only when outputs match.
func (q *Queue) parityEvidence(r ParityHostRequest) (ParityEvidence, error) {
	run := fmt.Sprintf("run a check or experiment of %s as project %s on host %s that compares outputs with your reference, then repeat myce parity add with --evidence JOB_ID", r.Work, r.Project, r.Host)
	refuse := func(why string) (ParityEvidence, error) {
		return ParityEvidence{}, &HTTPError{Status: http.StatusBadRequest, Code: "parity_evidence_required", Message: "MY-19 parity evidence required: " + why + "; next: " + run}
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
	host := j.Machine
	if m := q.s.Machines[j.Machine]; m != nil {
		host = hostID(m)
	}
	if host != r.Host {
		return refuse(fmt.Sprintf("evidence job %s ran on host %s, not %s", r.Evidence, host, r.Host))
	}
	return ParityEvidence{Job: j.ID, Attempt: j.Attempt}, nil
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

func (a API) registerProjectParity(mux *requestMux) {
	mux.HandleFunc("GET /api/parity-hosts", func(w http.ResponseWriter, r *http.Request) {
		record, err := a.Queue.ProjectParityHosts(r.URL.Query().Get("project"), r.Context())
		respond(w, record, err)
	})
	mux.HandleFunc("POST /api/parity-hosts", func(w http.ResponseWriter, r *http.Request) {
		var request ParityHostRequest
		if !decode(w, r, &request) {
			return
		}
		record, err := a.Queue.AddParityHost(request, r.Context())
		respondOperation(w, record, err, "parity_request_invalid")
	})
	mux.HandleFunc("POST /api/parity-hosts/remove", func(w http.ResponseWriter, r *http.Request) {
		var request ParityHostRequest
		if !decode(w, r, &request) {
			return
		}
		if request.Evidence != "" {
			respond(w, nil, parityRequestError("evidence is not used when removing a host"))
			return
		}
		record, err := a.Queue.RemoveParityHost(request, r.Context())
		respondOperation(w, record, err, "parity_request_invalid")
	})
}
