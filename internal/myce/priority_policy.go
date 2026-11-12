package myce

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"path"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

type PriorityTier struct {
	Name      string   `json:"name" yaml:"name"`
	Selectors []string `json:"selectors" yaml:"selectors"`
}
type PriorityPolicy struct {
	Version int            `json:"version" yaml:"version"`
	Tiers   []PriorityTier `json:"tiers" yaml:"tiers"`
	// Keep an explicit empty map on the wire: nil preserves settings, {} clears them.
	Projects map[string]ProjectPolicy `json:"projects" yaml:"projects"`
	// Models is local acquisition policy for declared model snapshots; absent permits all repositories.
	Models *ModelPolicy `json:"models,omitempty" yaml:"models,omitempty"`
	// Nil hosts stay off the wire and out of exports, so an older queue accepts
	// the request and a file exported from it never clears holds after upgrade.
	Hosts map[string]HostPolicy `json:"hosts,omitzero" yaml:"hosts,omitempty"`
	// LearnedMemory nil means on (MY-31, #585). Like hosts, an omitted field
	// preserves the saved switch so an older CLI's round trip cannot flip it.
	LearnedMemory *bool `json:"learned_memory,omitempty" yaml:"learned_memory,omitempty"`
}
type PriorityPolicyRecord struct {
	Revision uint64         `json:"revision" yaml:"revision"`
	Policy   PriorityPolicy `json:"policy" yaml:"policy"`
	Warnings []string       `json:"warnings,omitempty" yaml:"warnings,omitempty"`
}
type PriorityMatch struct {
	Tier     string `json:"tier"`
	Index    int    `json:"index"`
	Selector string `json:"selector"`
	Revision uint64 `json:"revision"`
}

var ErrPolicyRevision = errors.New("priority policy revision conflict")

func defaultPriorityPolicy() PriorityPolicy {
	return PriorityPolicy{Version: 1, Tiers: []PriorityTier{{"production", []string{"*/production"}}, {"deadline", []string{"*/deadline"}}, {"experiment", []string{"*/*"}}}}
}
func jobClass(s Spec) string {
	if s.Class != "" {
		return s.Class
	}
	if s.Priority != "" {
		return s.Priority
	}
	return "experiment"
}

// Empty projects are allowed for internal scheduling demands and unscoped models;
// entry points that require a project check that separately.
func validateProjectIdentifier(project string) error {
	if strings.Contains(project, "/") {
		return errors.New("project cannot contain slash")
	}
	return nil
}
func normalizeJobClass(s *Spec) error {
	if err := validateProjectIdentifier(s.Project); err != nil {
		return reject("job_project_invalid", err)
	}
	if strings.Contains(s.Class, "/") {
		return reject("job_class_invalid", errors.New("class cannot contain slash"))
	}
	if s.Priority != "" && legacyRank(s.Priority) == 3 {
		return reject("job_priority_invalid", errors.New("priority must be production, deadline or experiment"))
	}
	if s.Class != "" && s.Priority != "" && s.Class != s.Priority {
		return reject("job_class_invalid", errors.New("class and legacy priority must agree"))
	}
	s.Class = jobClass(*s)
	// Priority retains legacy reliability/deadline semantics, independent of tiers.
	if s.Priority == "" {
		if legacyRank(s.Class) < 3 {
			s.Priority = s.Class
		}
	}
	return nil
}
func (p PriorityPolicy) validate() ([]string, error) {
	if p.Version != 1 || len(p.Tiers) == 0 {
		return nil, errors.New("policy requires version 1 and ordered tiers")
	}
	names, seen := map[string]bool{}, map[string]bool{}
	var warnings []string
	catchall := false
	for _, t := range p.Tiers {
		if strings.TrimSpace(t.Name) == "" || names[t.Name] || len(t.Selectors) == 0 {
			return nil, errors.New("tiers require unique nonempty names and selectors")
		}
		names[t.Name] = true
		for _, selector := range t.Selectors {
			parts := strings.Split(selector, "/")
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return nil, fmt.Errorf("selector %q requires project/class", selector)
			}
			for _, part := range parts {
				if _, err := path.Match(part, ""); err != nil {
					return nil, fmt.Errorf("invalid selector %q: %w", selector, err)
				}
			}
			shadowed := seen[selector] || catchall
			for earlier := range seen {
				previous := strings.Split(earlier, "/")
				if (previous[0] == "*" || previous[0] == parts[0]) && (previous[1] == "*" || previous[1] == parts[1]) {
					shadowed = true
				}
			}
			if shadowed {
				warnings = append(warnings, "shadowed selector: "+selector)
			}
			seen[selector] = true
			if selector == "*/*" {
				catchall = true
			}
		}
	}
	if !slices.Contains(p.Tiers[len(p.Tiers)-1].Selectors, "*/*") {
		return nil, errors.New("final tier requires catchall */*")
	}
	if err := p.Models.validate(); err != nil {
		return nil, err
	}
	return warnings, nil
}
func ReadPriorityPolicy(r io.Reader) (PriorityPolicy, error) {
	var p PriorityPolicy
	d := yaml.NewDecoder(r)
	d.KnownFields(true)
	if err := d.Decode(&p); err != nil {
		return p, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return p, errors.New("expected one policy document")
	}
	_, err := p.validate()
	return p, err
}
func (q *Queue) effectivePriorityPolicy() PriorityPolicy {
	if q.s.PriorityPolicy == nil {
		return defaultPriorityPolicy()
	}
	return q.s.PriorityPolicy.Policy
}
func (q *Queue) policyRevision() uint64 {
	if q.s.PriorityPolicy != nil {
		return q.s.PriorityPolicy.Revision
	}
	return 0
}
func (q *Queue) priorityMatch(s Spec) PriorityMatch {
	for i, t := range q.effectivePriorityPolicy().Tiers {
		for _, selector := range t.Selectors {
			parts := strings.Split(selector, "/")
			project, _ := path.Match(parts[0], s.Project)
			class, _ := path.Match(parts[1], jobClass(s))
			if project && class {
				return PriorityMatch{t.Name, i, selector, q.policyRevision()}
			}
		}
	}
	// Older persisted identities may predate admission validation. Keep them in
	// the final catchall tier rather than panicking during status or scheduling.
	p := q.effectivePriorityPolicy()
	i := len(p.Tiers) - 1
	return PriorityMatch{p.Tiers[i].Name, i, "*/*", q.policyRevision()}
}

// comparePriority is the only scheduling rank comparator. Reliability and
// loss-timeout classification deliberately remain independent legacy semantics.
func (q *Queue) comparePriority(a, b Spec) int {
	return q.priorityMatch(a).Index - q.priorityMatch(b).Index
}
func (q *Queue) before(a, b *Job, m *Machine) bool {
	return q.beforeWithFair(a, b, m, q.fairScore, q.fairActive)
}

func (q *Queue) beforeWithFair(a, b *Job, m *Machine, score func(*Job, *Machine) float64, active func(*Job) float64) bool {
	if n := q.comparePriority(a.Spec, b.Spec); n != 0 {
		return n < 0
	}
	// Settle an already selected release before another same-tier demand can
	// borrow it. Its target is retained, so borrowing strands both demands on
	// one target instead of planning a separate release for the newcomer.
	if (a.PreemptionPlacement != nil) != (b.PreemptionPlacement != nil) {
		return a.PreemptionPlacement != nil
	}
	// CPU commands backfill accelerator workers after same-tier GPU demand.
	if m.GPU != "cpu" && a.Spec.Needs.cpuOnly() != b.Spec.Needs.cpuOnly() {
		return !a.Spec.Needs.cpuOnly()
	}
	if a.Spec.Project != b.Spec.Project && fairResourceFamily(a.Spec) == fairResourceFamily(b.Spec) {
		x, y := score(a, m), score(b, m)
		if math.Abs(x-y) > 1e-6 {
			return x < y
		}
		x, y = active(a), active(b)
		if math.Abs(x-y) > 1e-6 {
			return x < y
		}
	}
	return beforeWithinTier(a, b, m)
}
func (q *Queue) PriorityPolicy(requestContexts ...context.Context) PriorityPolicyRecord {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestLock(requestCtx, &q.mu)
	defer requestUnlock1()
	return q.priorityPolicyRecord()
}
func (q *Queue) priorityPolicyRecord() PriorityPolicyRecord {
	p := q.effectivePriorityPolicy()
	p.Tiers = slices.Clone(p.Tiers)
	for i := range p.Tiers {
		p.Tiers[i].Selectors = slices.Clone(p.Tiers[i].Selectors)
	}
	if p.Models != nil {
		models := *p.Models
		models.Sources = slices.Clone(models.Sources)
		p.Models = &models
	}
	if p.LearnedMemory != nil {
		learned := *p.LearnedMemory
		p.LearnedMemory = &learned
	}
	p.Projects = map[string]ProjectPolicy{}
	for name, v := range q.s.Policies {
		v.AllowedMachines = slices.Clone(v.AllowedMachines)
		v.ProductionParityHosts = cloneProductionParityHosts(v.ProductionParityHosts)
		p.Projects[name] = v
	}
	if hosts := q.hostPolicies(); len(hosts) > 0 {
		p.Hosts = hosts
	}
	warnings, _ := p.validate()
	return PriorityPolicyRecord{q.policyRevision(), p, warnings}
}
func (q *Queue) SetPriorityPolicy(expected uint64, p PriorityPolicy, requestContexts ...context.Context) (PriorityPolicyRecord, error) {
	return q.setPriorityPolicy(expected, p, false, requestContexts...)
}

// keepFileLimits preserves a project's file-store limits that a caller left at
// zero. An older CLI cannot carry those fields, so its get/set round-trip would
// otherwise clear them silently. Current CLIs mark their requests instead.
func (q *Queue) setPriorityPolicy(expected uint64, p PriorityPolicy, keepFileLimits bool, requestContexts ...context.Context) (PriorityPolicyRecord, error) {
	requestCtx := requestContext(requestContexts)

	var out PriorityPolicyRecord
	if _, err := p.validate(); err != nil {
		return out, err
	}
	// Reuse the existing project settings, validation and store.
	projects := p.Projects
	if projects != nil {
		projectsCopy := map[string]ProjectPolicy{}
		for name, v := range projects {
			if v.Weight == 0 {
				v.Weight = 1
			}
			v.AllowedMachines = slices.Clone(v.AllowedMachines)
			v.ProductionParityHosts = cloneProductionParityHosts(v.ProductionParityHosts)
			projectsCopy[name] = v
		}
		projects = projectsCopy
		if err := validateProjectPolicies(projects); err != nil {
			return out, err
		}
	}
	hosts := p.Hosts
	if hosts != nil {
		var err error
		if hosts, err = normalizeHostPolicies(hosts, false); err != nil {
			return out, err
		}
	}
	p.Tiers = slices.Clone(p.Tiers)
	for i := range p.Tiers {
		p.Tiers[i].Selectors = slices.Clone(p.Tiers[i].Selectors)
	}
	p.Projects, p.Hosts = nil, nil
	if p.LearnedMemory != nil {
		learned := *p.LearnedMemory
		p.LearnedMemory = &learned
	}
	var previous map[string]ProjectPolicy
	err := q.updateTransaction(requestCtx, func() error {
		previous = q.s.Policies
		if expected == ^uint64(0) {
			return errors.New("policy revision exhausted")
		}
		if expected != q.policyRevision() {
			return ErrPolicyRevision
		}
		q.migrateProjectParity()
		for name, v := range projects {
			if err := q.checkPolicyParityUnchanged(name, v.ProductionParityHosts); err != nil {
				return err
			}
			if keepFileLimits {
				old := q.s.Policies[name]
				v.MaxRetainedFileBytes = cmp.Or(v.MaxRetainedFileBytes, old.MaxRetainedFileBytes)
				v.MaxAddedFileBytes24h = cmp.Or(v.MaxAddedFileBytes24h, old.MaxAddedFileBytes24h)
				v.ReservedFileBytes = cmp.Or(v.ReservedFileBytes, old.ReservedFileBytes)
				v.FileRetention = cmp.Or(v.FileRetention, old.FileRetention)
				projects[name] = v
			}
		}
		if err := q.validateMachineNames(slices.Collect(maps.Keys(hosts))); err != nil {
			return err
		}
		if p.LearnedMemory == nil {
			p.LearnedMemory = q.effectivePriorityPolicy().LearnedMemory
		}
		q.s.PriorityPolicy = &PriorityPolicyRecord{Revision: expected + 1, Policy: p}
		if projects != nil {
			q.s.Policies = projects
			q.mirrorProjectParity()
		}
		if hosts != nil {
			q.replaceHostPolicies(hosts)
		}
		out = q.priorityPolicyRecord()
		return nil // Normal planner reevaluates on its next decision; no bulk stop here.
	}, nil, false, func() { q.reindexProjectFileDeadlines(previous) })
	return out, err
}

// Marks a policy PUT whose project file-store limits are authoritative.
const policyFileLimitsParam = "file_store_limits"

func (a API) registerPriorityPolicy(mux *requestMux) {
	mux.HandleFunc("GET /api/policy", func(w http.ResponseWriter, r *http.Request) { respond(w, a.Queue.PriorityPolicy(r.Context()), nil) })
	mux.HandleFunc("PUT /api/policy", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ExpectedRevision *uint64        `json:"expected_revision"`
			Policy           PriorityPolicy `json:"policy"`
		}
		if !decode(w, r, &request) {
			return
		}
		if request.ExpectedRevision == nil {
			respond(w, nil, reject("policy_invalid", errors.New("expected_revision required")))
			return
		}
		// Older queues ignore this query; older clients omit it (see setPriorityPolicy).
		keep := r.URL.Query().Get(policyFileLimitsParam) != "1"
		record, err := a.Queue.setPriorityPolicy(*request.ExpectedRevision, request.Policy, keep, r.Context())
		if errors.Is(err, ErrPolicyRevision) {
			revision := a.Queue.PriorityPolicy(r.Context()).Revision
			respond(w, nil, &HTTPError{Status: http.StatusConflict, Code: "policy_revision_conflict", CurrentRevision: &revision, Message: fmt.Sprintf("policy revision changed; current revision %d; export policy and retry with --if-revision %d", revision, revision)})
			return
		}
		respondOperation(w, record, err, "policy_invalid")
	})
}

// Demand identities accompany legacy stop-grace arguments through existing
// reclamation helpers. Tests/older callers without an identity use the default.
func priorityDemand(priority string, identity []Spec) Spec {
	if len(identity) > 0 {
		return identity[0]
	}
	return Spec{Priority: priority}
}
func (q *Queue) slotSpec(s *ModelSlot) Spec {
	best := Spec{Project: s.Project, Priority: s.Priority, Class: s.Class}
	found := false
	for _, c := range q.calls {
		if c.Model == s.Model && c.Machine == s.Machine && c.Session == s.Session && c.Attempt == s.Attempt && c.Error == "" {
			candidate := callJob(c).Spec
			if !found || q.comparePriority(candidate, best) < 0 {
				best = candidate
				found = true
			}
		}
	}
	for _, p := range q.s.Pins {
		if p.Model == s.Model && p.Assigned == s.Machine && p.AssignedDevice == s.Device && p.active(q.clock.Now()) {
			candidate := Spec{Project: p.Project, Priority: "production"}
			if !found || q.comparePriority(candidate, best) < 0 {
				best = candidate
				found = true
			}
		}
	}
	return best
}
