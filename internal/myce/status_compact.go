package myce

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

// StatusSummary identifies the deliberate projection. A missing diagnostic is
// not absent evidence; detail=full and sections recover the existing document.
type StatusSummary struct {
	RetainedJobs int                `json:"retained_jobs"`
	Alerts       []StatusAlertGroup `json:"alert_groups"`
	Leases       []StatusLease      `json:"production_leases,omitempty"`
	NotQuiescent []StatusQuiescence `json:"not_quiescent,omitempty"`
	Diagnostics  string             `json:"diagnostics"`
}

// StatusQuiescence explains one host that a drain, disable or running fleet
// upgrade is waiting on: hostMaintenanceStatus(host).Quiescent is false (the
// same predicate drains and upgrades use), with the attempts holding it and any
// other unmet condition (stale workers, cleanup, calls, sidecars, preparation).
type StatusQuiescence struct {
	Host     string         `json:"host"`
	Mode     string         `json:"mode"`
	Attempts []StatusHolder `json:"attempts,omitempty"`
	Other    []string       `json:"other,omitempty"`
}
type StatusHolder struct {
	JobID   string `json:"job_id"`
	Name    string `json:"name,omitempty"`
	Attempt int    `json:"attempt"`
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
}

// notQuiescentHosts runs on the compact capture's source: only hosts a
// drain, disable or the active fleet step is waiting on, so its cost is bounded
// by in-progress maintenance, not fleet size.
func (q *Queue) notQuiescentHosts() []StatusQuiescence {
	upgrading := ""
	if h := q.fleetActiveHost(); h != nil {
		upgrading = h.Host
	}
	var hosts []string
	for _, m := range q.s.Machines {
		if host := hostID(m); m.Replacement == nil && !slices.Contains(hosts, host) && (q.maintenanceBlocked(host) || host == upgrading) {
			hosts = append(hosts, host)
		}
	}
	slices.Sort(hosts)
	var out []StatusQuiescence
	for _, host := range hosts {
		st := q.hostMaintenanceStatus(host)
		if st.Quiescent {
			continue
		}
		row := StatusQuiescence{Host: host, Mode: st.Policy.Mode}
		if host == upgrading {
			row.Mode += "; fleet upgrade"
		}
		for _, a := range st.Attempts {
			holder := StatusHolder{JobID: a.ID, Attempt: a.Attempt, State: a.State, Reason: a.StopCause}
			if j := q.s.Jobs[a.ID]; j != nil {
				holder.Name = j.Spec.Name
			}
			row.Attempts = append(row.Attempts, holder)
		}
		if len(st.Workers) == 0 {
			row.Other = append(row.Other, "no worker has reported")
		}
		for _, w := range st.Workers {
			if !w.Fresh {
				row.Other = append(row.Other, "worker "+w.Machine.ID+" has no fresh custody report")
			}
		}
		row.Other = append(row.Other, st.PendingCleanup...)
		for _, n := range []struct {
			count int
			what  string
		}{{st.Calls, "model calls"}, {st.Sidecars, "sidecars"}, {st.Preparations, "preparations"}} {
			if n.count > 0 {
				row.Other = append(row.Other, fmt.Sprintf("%d %s", n.count, n.what))
			}
		}
		if !q.maintenanceBlocked(host) {
			row.Other = append(row.Other, "admission still enabled until its next idle moment")
		}
		out = append(out, row)
	}
	return out
}

// StatusLease is one open production lease (#1009): its project and key,
// expiry, and the hosts able to start a declared stage, which planned drains
// and fleet upgrades keep at least one of enabled until it closes or expires.
type StatusLease struct {
	Project   string    `json:"project"`
	Key       string    `json:"key"`
	ExpiresAt time.Time `json:"expires_at"`
	Hosts     []string  `json:"hosts"`
}

// StatusAlertGroup folds active conditions of one kind into one row: the same
// regression on 18 work profiles is one fact with a count, not 18 lines.
// Scopes count members per host or project; hash-like subjects (work profiles,
// targets, recipes) are counted only. The owning section names each member.
type StatusAlertGroup struct {
	Kind   string         `json:"kind"`
	Count  int            `json:"count"`
	Since  time.Time      `json:"since"`
	Scopes map[string]int `json:"scopes,omitempty"`
}

// statusAlertKind splits a condition key into its kind and member scope.
// "efficiency/<subject>/<metric>" and "reports/<host>/<code>" are kinded by
// their final code; other keys by their first segment, scoped by the second.
func statusAlertKind(key string) (kind, scope string) {
	parts := strings.Split(key, "/")
	kind = parts[0]
	if len(parts) >= 3 && (kind == "efficiency" || kind == "reports") {
		kind += "/" + parts[len(parts)-1]
		scope = strings.Join(parts[1:len(parts)-1], "/")
	} else if len(parts) >= 2 {
		scope = parts[1]
	}
	if strings.HasPrefix(scope, "work:") || hashLikeStatusScope(scope) {
		scope = ""
	}
	return kind, scope
}
func hashLikeStatusScope(s string) bool {
	if len(s) < 16 {
		return false
	}
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}
func statusAlertGroups(conditions map[string]healthCondition) []StatusAlertGroup {
	groups := map[string]*StatusAlertGroup{}
	for key, c := range conditions {
		if !c.Active {
			continue
		}
		kind, scope := statusAlertKind(key)
		g := groups[kind]
		if g == nil {
			g = &StatusAlertGroup{Kind: kind, Since: c.Since}
			groups[kind] = g
		}
		g.Count++
		if c.Since.Before(g.Since) {
			g.Since = c.Since
		}
		if scope != "" {
			if g.Scopes == nil {
				g.Scopes = map[string]int{}
			}
			g.Scopes[scope]++
		}
	}
	out := make([]StatusAlertGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	slices.SortFunc(out, func(a, b StatusAlertGroup) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return strings.Compare(a.Kind, b.Kind)
	})
	return out
}

// A replaced per-card identity is history, not a host; only --history shows it.
func dropReplacedStatusMachines(machines []*Machine) []*Machine {
	return slices.DeleteFunc(machines, func(m *Machine) bool { return m.Replacement != nil })
}

func (o *statusOptions) compact() bool {
	return o != nil && !o.full && len(o.sections) == 0 && o.jobs == "active" && o.before == "" && o.limit == 0
}
func (o *statusOptions) includes(section string) bool {
	return o == nil || o.full || slices.Contains(o.sections, section)
}

// Whitelist current lifecycle facts before copying. Retained attempts, ledgers,
// fan-out member manifests and evidence never enter the default capture.
func compactStatusJob(j *Job) Job {
	started := commandStarted(j)
	return Job{ID: j.ID, State: j.State, Attempt: j.Attempt, Machine: j.Machine, Session: j.Session, CommandStarted: &started,
		Copies: compactStatusCopies(j.Copies),
		Spec: Spec{Workload: j.Spec.Workload, QualityTolerance: j.Spec.QualityTolerance, ExtensionCheck: compactStatusCheck(j.Spec.ExtensionCheck), Logs: j.Spec.Logs, AuthoringOrigin: j.Spec.Origin(), AllowedMachines: j.Spec.AllowedMachines, ExcludedMachines: j.Spec.ExcludedMachines, Name: j.Spec.Name, Project: j.Spec.Project, Class: j.Spec.Class, Priority: j.Spec.Priority,
			Deadline: j.Spec.Deadline, StartWindow: j.Spec.StartWindow, Resumable: j.Spec.Resumable, Preemptible: j.Spec.Preemptible, Needs: j.Spec.Needs, StartBy: j.Spec.StartBy,
			Program: compactStatusProgram(j.Spec.Program), Kind: j.Spec.Kind, WallTime: j.Spec.WallTime},
		Queued: j.Queued, AssignedAt: j.AssignedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt,
		WaitingSince: j.WaitingSince, Starts: j.Starts, AdmissionRequeues: j.AdmissionRequeues, Losses: j.Losses, PreemptionCount: j.PreemptionCount,
		AllocatedDevices: j.AllocatedDevices, Hold: j.Hold, Finding: j.Finding, DeviceFaultStartWait: j.DeviceFaultStartWait, StartCapacityWaiting: j.StartCapacityWaiting, Delivery: j.Delivery, Reason: j.Reason, DiagnosticCode: j.DiagnosticCode, Pause: j.Pause, CancelledAt: j.CancelledAt,
		ReleaseConfirmed: j.ReleaseConfirmed, LastStopOutcome: j.LastStopOutcome, StopAt: j.StopAt,
		PriorityPolicy: j.PriorityPolicy, QueueOrder: j.QueueOrder, ApplicationProgress: j.ApplicationProgress,
		ExecutionCause: j.ExecutionCause, CleanupCause: j.CleanupCause, ProductionHost: j.ProductionHost,
		Timing: j.Timing, Liveness: j.Liveness, WallTime: j.WallTime, Progress: j.Progress,
		// The whitelist drops the frozen reason maps (raw text). Carry their
		// sanitized markers so the summary matches the per-job read (#748).
		RequirementReasons: compactRequirementReasons(j)}
}

func compactRequirementReasons(j *Job) []RequirementReasonStatus {
	if j.RequirementReasons != nil {
		return j.RequirementReasons
	}
	return requirementReasonStatus(j)
}

// Unconfirmed executions are current custody, even when a later attempt runs.
func compactStatusCopies(copies []Copy) []Copy {
	var out []Copy
	for _, c := range copies {
		if !c.ReleaseConfirmed {
			out = append(out, Copy{Machine: c.Machine, Session: c.Session, Attempt: c.Attempt, StartedAt: c.StartedAt, StoppedAt: c.StoppedAt, AllocatedDevices: c.AllocatedDevices, StopCause: c.StopCause, Progress: c.Progress, Liveness: c.Liveness})
		}
	}
	return out
}
func compactStatusCheck(c *ExtensionCheckRequest) *ExtensionCheckRequest {
	if c == nil {
		return nil
	}
	return &ExtensionCheckRequest{Work: c.Work, Timeout: c.Timeout}
}
func compactStatusProgram(p *Program) *Program {
	if p == nil {
		return nil
	}
	return &Program{Type: p.Type}
}
func compactStatusMachine(m *Machine) Machine {
	// Preserve current capacity/availability fields and the latest health sample
	// (the live view uses load), omitting retained telemetry and inventories.
	c := *m
	c.ToolInventory, c.OutputDestinationIDs = nil, nil
	c.HygieneCollection, c.ExternalMemory, c.MemoryObservation = nil, nil, nil
	c.CachedSharedInputs, c.SharedInputEvidence, c.SharedInputCache = nil, nil, nil
	c.ExtensionReports, c.EnvironmentReports, c.CopyReports = nil, nil, nil
	for _, r := range m.EnvironmentReports {
		if r.State == "building" {
			c.EnvironmentReports = append(c.EnvironmentReports, EnvironmentReport{Key: r.Key, State: r.State, Attempt: r.Attempt, Kind: r.Kind})
		}
	}
	for _, r := range m.CopyReports {
		if r.State == "copying" {
			c.CopyReports = append(c.CopyReports, CopyReport{Model: r.Model, State: r.State, Attempt: r.Attempt, Bytes: r.Bytes, Total: r.Total})
		}
	}
	c.EnvironmentBuildSeconds, c.EnvironmentBytes, c.ModelCopySeconds = nil, nil, nil
	c.Environments, c.CachedModels, c.Models = nil, nil, nil
	c.ModelApprovalStates = nil
	c.CapabilityProfile = compactStatusProfile(c.CapabilityProfile)
	c.Devices = slices.Clone(c.Devices)
	for i := range c.Devices {
		d := &c.Devices[i]
		d.FaultEvidence = nil
		d.ExtensionReports, d.Models, d.Releases, d.Attempts = nil, nil, nil, nil
		d.CapabilityProfile = compactStatusProfile(d.CapabilityProfile)
	}
	if c.HostHealth != nil {
		h := *c.HostHealth
		h.Baseline, h.Foreign, h.ForeignServices = nil, nil, nil
		if len(h.History) > 0 {
			h.History = h.History[len(h.History)-1:]
		}
		c.HostHealth = &h
	}
	return c
}

func compactStatusCall(c *ModelCall) *Job {
	j := callJob(c)
	j.Spec.Name = "model call: " + c.Model
	j.Machine, j.Session, j.StopAt = c.Machine, c.Session, c.StopAt
	j.State, j.Reason = "queued", "waiting for model "+c.Model+" on an eligible worker"
	if c.Machine != "" {
		j.State, j.Reason = "running", "model call in flight"
	}
	if c.Error != "" {
		j.State, j.Reason = "failed", "model call failed"
	}
	return j
}

func compactStatusProfile(profile *CapabilityProfile) *CapabilityProfile {
	if profile == nil {
		return nil
	}
	p := *profile
	return &p
}

// A summary reads live work, not the full-history materialization or
// diagnostic planner. It is projected from the last committed publication
// without q.mu: only current work, machines and the reservations' own job
// records are decoded. Retained file-store accounting is available through
// the storage section.
func (q *Queue) compactStatusObservation(o *statusOptions, ctx context.Context) (*Queue, *StatusSummary, *HealthAlertDelivery, error) {
	started := readWallStart(q.observeRead)
	if index := q.readIndex(ctx); index != nil && index.capture != nil && index.capture.generation != nil && !q.simulation && !q.unpublishedRecords() {
		source, err := index.capture.statusQueue(ctx, q.clock.Now(), q)
		observeRead(q.observeRead, "", started, started)
		if err != nil {
			return nil, nil, nil, err
		}
		delivery := index.capture.delivery
		return source.compactStatusView(o, ctx, index.capture.retained, &delivery)
	}
	// Before the first commit (or with unpublished imported records) copy
	// the same small inputs from the live queue under q.mu.
	unlock, err := requestObservationLock(ctx, &q.mu)
	if err != nil {
		return nil, nil, nil, err
	}
	locked := readWallStart(q.observeRead)
	hold := q.statusHolds.start()
	defer func() {
		duration := queueHoldDuration(hold)
		unlock()
		observeRead(q.observeRead, "", started, locked)
		logQueueHold(duration, "compactStatusObservation", nil)
	}()
	return q.compactStatusView(o, ctx, len(q.s.Jobs)+len(q.calls), q.healthAlertDelivery())
}

// compactStatusView copies the summary inputs from q: either the live queue
// under q.mu or a private detached source built from a publication.
func (q *Queue) compactStatusView(o *statusOptions, ctx context.Context, retained int, delivery *HealthAlertDelivery) (*Queue, *StatusSummary, *HealthAlertDelivery, error) {
	v := &Queue{clock: q.detachedClock(q.clock.Now()), nasAvailable: q.nasAvailable, simulation: q.simulation,
		restartUntil: q.restartUntil, networkQuiet: q.networkQuiet, root: q.root, detachedRead: true}
	q.captureContactInputs(v.clock).freezeContactView(v)
	s := state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}, Hosts: q.s.Hosts,
		HostReleases: q.s.HostReleases, HostMaintenance: q.s.HostMaintenance, DeviceMaintenance: q.s.DeviceMaintenance,
		Policies: q.s.Policies, PriorityPolicy: q.s.PriorityPolicy,
		ComponentExclusions: map[string]ComponentExclusion{}, Reservations: map[string]Reservation{},
		ModelSlots: q.s.ModelSlots, Pins: q.s.Pins, SyncRequests: q.s.SyncRequests, PreparationPlans: q.s.PreparationPlans}
	for key, r := range q.s.Reservations {
		if r.active() || q.reservationCleanupPending(r, q.retainedJob(r.Job)) {
			s.Reservations[key] = r
		}
	}
	for key, p := range q.s.ComponentExclusions {
		if p.Active {
			s.ComponentExclusions[key] = p
		}
	}
	jobs := q.s.Jobs
	if q.records != nil && !q.simulation && q.records.jobCount == len(q.s.Jobs) {
		jobs = q.records.live
	}
	selection := &statusSelection{ids: map[string]bool{}, page: StatusJobsPage{Mode: "active"}, compact: true}
	for id, j := range jobs {
		if !mutableJob(j) {
			continue
		}
		copy := compactStatusJob(j)
		s.Jobs[id] = &copy
		// Filters affect returned rows, never the fleet's capacity/hold context.
		if o.project != "" && j.Spec.Project != o.project || !o.since.IsZero() && j.Queued.Before(o.since) {
			continue
		}
		selection.ids[id] = true
	}
	for id, c := range q.calls {
		if o.project != "" && c.Project != o.project || !o.since.IsZero() && c.Queued.Before(o.since) {
			continue
		}
		selection.ids[id] = true
	}
	for id, m := range q.s.Machines {
		copy := compactStatusMachine(m)
		s.Machines[id] = &copy
	}
	summary := &StatusSummary{RetainedJobs: retained, Alerts: statusAlertGroups(q.s.Health.Conditions), Diagnostics: "summary; use sections or detail=full; job reasons are last committed, not a new placement plan"}
	for _, l := range q.openProductionLeases() {
		summary.Leases = append(summary.Leases, StatusLease{Project: l.Project, Key: l.Key, ExpiresAt: l.ExpiresAt, Hosts: q.productionLeaseHosts(l)})
	}
	summary.NotQuiescent = q.notQuiescentHosts()
	selection.page.Total, selection.page.Returned = len(selection.ids), len(selection.ids)
	inputs := observationState{State: s, Models: q.models, Calls: q.calls, Unreported: q.unreported}
	if err := chargeProjectionValue(ctx, inputs); err != nil {
		return nil, nil, nil, err
	}
	detached := copyObservation(inputs)
	v.s, v.models, v.calls, v.unreported = detached.State, detached.Models, detached.Calls, detached.Unreported
	for id, m := range v.s.Machines {
		original := s.Machines[id]
		m.cpuLedger = original.cpuLedger
		m.modelReservationDebtGB, m.hostReservationDebtGB = original.modelReservationDebtGB, original.hostReservationDebtGB
	}
	v.observationSelection = selection
	v.observationLive = v.s.Jobs
	// Recent admission history is diagnostic, never relabel active-only counts
	// as retained admissions.
	admissions := []HostAdmissions{}
	v.statusAdmissions = &admissions
	// A fleet operation's current projection is small; never copy its retained
	// signed release/manifest or supersession history into the summary.
	upgrade := q.fleetUpgradeStatus()
	if err := chargeProjectionValue(ctx, upgrade); err != nil {
		return nil, nil, nil, err
	}
	selection.upgrade = copyObservationValue(reflect.ValueOf(upgrade)).Interface().(*FleetUpgradeStatus)
	return v, summary, delivery, nil
}

func trimStatusSections(s *Snapshot, o *statusOptions) {
	if o == nil || o.full {
		return
	}
	if !o.includes("storage") {
		s.StorageObservations = nil
		s.FileStore = nil
		s.CheckpointExpiry = nil
	}
	if !o.includes("work") {
		s.WorkCurves = nil
	}
	if !o.includes("health") {
		s.HealthAlerts = nil
	}
	if !o.includes("efficiency") {
		s.Efficiency = nil
	}
	if !o.includes("usage") {
		s.Usage = UsageReport{}
	}
	if !o.includes("machines") {
		for i, m := range s.Machines {
			c := compactStatusMachine(m)
			s.Machines[i] = &c
		}
	}
	if !o.includes("jobs") && o.jobs == "active" {
		for i, j := range s.Jobs {
			c := compactStatusJob(j)
			c.Runtime = j.Runtime
			c.Timing = nil
			s.Jobs[i] = &c
		}
	}
	if !o.includes("diagnostics") {
		s.RequestLatencies = nil
		s.EvidenceRetention = nil
		s.Admissions = nil
	}
}
