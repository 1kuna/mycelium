package myce

import (
	"slices"
	"strings"
	"time"
)

// EntryCoverage is derived at submission for a non-production adapter or
// profile entry that names no targets (#853, #1341). Such an entry runs on any
// machine type its platform, Pixi lock and declared tools fit. A failure on a
// type the recipe has never succeeded on is a coverage finding: the queue skips
// that type for the entry until its recipe changes, and requeues the job onto
// another type. Production entries keep explicit targets (MY-19).
type EntryCoverage struct {
	Key    string `json:"key"`
	Recipe string `json:"recipe"`
	// OS/arch pairs from a declared Pixi lock; empty means the lock does not
	// restrict placement (no lock, uv, or undeclared coverage).
	LockTargets []ExecutionTarget `json:"lock_targets,omitempty"`
	Findings    []CoverageFinding `json:"findings,omitempty"`
}

// CoverageFinding names one skipped machine type and its one action. It never
// carries worker error text; the attempt history keeps the failure itself.
type CoverageFinding struct {
	Target  ExecutionTarget `json:"target"`
	Job     string          `json:"job"`
	Attempt int             `json:"attempt"`
	At      time.Time       `json:"at"`
	Action  string          `json:"action"`
}

// Persisted per project entry, so a changed recipe replaces the record and
// clears its skips. Bounded by the number of entries, not submissions.
type entryCoverageRecord struct {
	Recipe    string            `json:"recipe"`
	Succeeded []ExecutionTarget `json:"succeeded,omitempty"`
	Skipped   []CoverageFinding `json:"skipped,omitempty"`
}

// deriveEntryCoverage runs outside q.mu, before frozen lock bytes move to the
// file store. Only entries that omit targets and are not portable discovery
// qualify; production is refused separately under the lock.
func deriveEntryCoverage(s Spec, recipes map[string]EnvironmentRecipe, discovery *CommandDiscovery) *EntryCoverage {
	if s.extensionJob() || !s.RequireTarget || len(s.Targets) > 0 || discovery != nil || s.ProfileMemory == "" || s.Name == "" ||
		s.AuthoringOrigin != OriginAdapter && s.AuthoringOrigin != OriginProfile {
		return nil
	}
	keys := map[string]string{}
	for platform, r := range recipes {
		keys[platform] = r.Key
	}
	environment := s.Environment
	environment.Files = nil
	c := &EntryCoverage{Key: s.Project + "/" + s.Name, Recipe: identityHash(struct {
		Project, Name, Profile string
		Environment            Environment
		Recipes                map[string]string
		Tools, Platform        []string
	}{s.Project, s.Name, s.ProfileMemory, environment, keys, s.Needs.Tools, s.Needs.Platform.Required})}
	if r, ok := recipes["cpu"]; ok {
		if coverage := environmentLockCoverage(r, nil, "cpu"); strings.HasPrefix(coverage.State, "declared:") && !coverage.UnmappedPlatforms {
			for _, t := range coverage.Targets {
				c.LockTargets = append(c.LockTargets, ExecutionTarget{OS: t.OS, Arch: t.Arch})
			}
		}
	}
	return c
}

func coverageTarget(j *Job, m *Machine) ExecutionTarget {
	t := machineTarget(m)
	if j.Spec.Needs.cpuOnly() {
		t.Platform = "cpu"
	}
	return t
}

func (q *Queue) entryCoverageProduction(j *Job) bool {
	return jobClass(j.Spec) == "production" || q.priorityMatch(j.Spec).Tier == "production"
}

// entryCoverageAllows is part of jobAllowsMachine, so placement, qualification
// and guidance all see the same skips. It reads only small frozen job fields.
func (q *Queue) entryCoverageAllows(j *Job, machine string) bool {
	c := j.EntryCoverage
	if c == nil {
		return true
	}
	m := q.s.Machines[machine]
	if m == nil {
		return true
	}
	t := coverageTarget(j, m)
	if len(c.LockTargets) > 0 && !slices.Contains(c.LockTargets, ExecutionTarget{OS: t.OS, Arch: t.Arch}) {
		return false
	}
	rec, ok := q.s.EntryCoverage[c.Key]
	return !ok || rec.Recipe != c.Recipe || !slices.ContainsFunc(rec.Skipped, func(f CoverageFinding) bool { return f.Target == t })
}

func (q *Queue) entryCoverageRecord(c *EntryCoverage) entryCoverageRecord {
	rec, ok := q.s.EntryCoverage[c.Key]
	if !ok || rec.Recipe != c.Recipe {
		return entryCoverageRecord{Recipe: c.Recipe} // A changed recipe clears old skips.
	}
	return rec
}

func (q *Queue) putEntryCoverage(key string, rec entryCoverageRecord) {
	if q.s.EntryCoverage == nil {
		q.s.EntryCoverage = map[string]entryCoverageRecord{}
	}
	q.s.EntryCoverage[key] = rec
}

func (q *Queue) recordEntryCoverageSuccess(j *Job, machine string) {
	c, m := j.EntryCoverage, q.s.Machines[machine]
	if c == nil || m == nil {
		return
	}
	rec, t := q.entryCoverageRecord(c), coverageTarget(j, m)
	if !slices.Contains(rec.Succeeded, t) {
		rec.Succeeded = append(rec.Succeeded, t)
	}
	q.putEntryCoverage(c.Key, rec)
}

// entryCoverageFailure decides a failed attempt that would otherwise end the
// job. It returns false when the failure stays the job's own: production, a
// type the recipe already succeeded on, or a failure on every type it could use
// while it has never succeeded anywhere (not type-specific; skips are cleared).
func (q *Queue) entryCoverageFailure(j *Job, machine string) bool {
	c, m := j.EntryCoverage, q.s.Machines[machine]
	if c == nil || m == nil || q.entryCoverageProduction(j) {
		return false
	}
	rec, t := q.entryCoverageRecord(c), coverageTarget(j, m)
	if slices.Contains(rec.Succeeded, t) {
		return false
	}
	label := targetLabel(t)
	finding := CoverageFinding{Target: t, Job: j.ID, Attempt: j.Attempt, At: q.clock.Now(),
		Action: "make the entry's command, tools or lock file work on " + label + "; any change to the entry retries " + label + ", otherwise it stays skipped"}
	if !slices.ContainsFunc(rec.Skipped, func(f CoverageFinding) bool { return f.Target == t }) {
		rec.Skipped = append(rec.Skipped, finding)
	}
	q.putEntryCoverage(c.Key, rec)
	q.touchJob(j.ID)
	updated := *c
	updated.Findings = append(slices.Clone(c.Findings), finding)
	j.EntryCoverage = &updated
	reason := "coverage finding: entry never succeeded on " + label + "; skipped for this entry until its recipe changes"
	if ok, _ := q.qualifiedTarget(j); ok {
		q.requeue(j, reason+"; trying another machine type", false)
		return true
	}
	if len(rec.Succeeded) == 0 {
		delete(q.s.EntryCoverage, c.Key)
		return false
	}
	now := q.clock.Now()
	j.State, j.Reason, j.FinishedAt = "failed", reason+"; no machine type it has succeeded on is in the fleet", &now
	return true
}
