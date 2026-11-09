package myce

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const scarceHostYieldCause = "scarce_host_yield"
const scarceHostYieldCapability = "scarce_host_yield_v1"

// These are author assertions backed by the named result/resume tests, not
// inferred portability. Admission still requires the frozen recipe's ordinary
// environment and qualification evidence on each exact target.
type ResumeCompatibility struct {
	CheckpointFormat string         `json:"checkpoint_format" yaml:"checkpoint_format"`
	ResultContract   string         `json:"result_contract" yaml:"result_contract"`
	Targets          []ResumeTarget `json:"targets" yaml:"targets"`
}
type ScarceYieldProjection struct {
	Host         string             `json:"host,omitempty"`
	Requirements []string           `json:"requirements,omitempty"`
	Victims      []*ScarceHostYield `json:"victims,omitempty"`
	Reason       string             `json:"reason"`
}

type ResumeTarget struct {
	Target      ExecutionTarget `json:"target" yaml:"target"`
	Environment string          `json:"environment" yaml:"environment"`
	ResultCheck string          `json:"result_check" yaml:"result_check"`
}

// Intent belongs to exactly one victim attempt. Authorized means the worker
// may deliver the stop, never that a signal, final save or release happened.
type ScarceHostYield struct {
	Cause             string          `json:"cause"`
	Demand            string          `json:"demand"`
	Victim            string          `json:"victim"`
	Attempt           int             `json:"attempt"`
	Machine           string          `json:"machine"`
	Session           string          `json:"session"`
	Destination       string          `json:"destination"`
	DestinationDevice string          `json:"destination_device"`
	DestinationTarget ExecutionTarget `json:"destination_target"`
	Generation        int64           `json:"generation"`
	Authorized        bool            `json:"authorized"`
	FinalReceived     bool            `json:"final_received"`
	Hold              string          `json:"hold,omitempty"`
}

func validateYieldContract(s Spec) error {
	if s.Preemptible && (s.Resumable || s.Priority != "experiment" && s.Class != "experiment" && (s.Priority != "" || s.Class != "")) {
		return errors.New("preemptible: true is only for disposable, non-resumable experiments")
	}
	r := s.ResumeCompatibility
	if r == nil {
		return nil
	}
	if !s.Resumable && !s.Preemptible {
		return errors.New("resume_compatibility requires resumable or preemptible work")
	}
	if !extensionName.MatchString(r.CheckpointFormat) || !extensionName.MatchString(r.ResultContract) || len(r.Targets) == 0 {
		return errors.New("resume_compatibility requires checkpoint_format, result_contract and proven exact targets")
	}
	seen := map[ExecutionTarget]bool{}
	for _, p := range r.Targets {
		t := p.Target
		if !extensionName.MatchString(t.OS) || !extensionName.MatchString(t.Arch) || !validGPU(t.Platform) || seen[t] || strings.TrimSpace(p.Environment) == "" || !extensionName.MatchString(p.ResultCheck) {
			return errors.New("resume targets require unique exact OS/arch/platform, environment identity (none or qualified for native recipes), and a result_check evidence identifier")
		}
		seen[t] = true
	}
	return nil
}

func finiteYieldJob(j *Job) bool {
	return j.Spec.Needs.Model == "" && j.Spec.FanOut == nil && j.Batch == nil && j.Spec.ExtensionCheck == nil
}
func (q *Queue) yieldContractMatches(j *Job, m *Machine) bool {
	if j.Spec.Needs.cpuOnly() {
		m = cpuMachineView(m)
	}
	r := j.Spec.ResumeCompatibility
	if r == nil {
		return false
	}
	environment := environmentKey(j, m)
	if j.Spec.extensionJob() {
		environment = "qualified"
	} else if environment == "" {
		environment = "none"
	}
	return slices.ContainsFunc(r.Targets, func(p ResumeTarget) bool { return p.Target == machineTarget(m) && p.Environment == environment })
}

// Unknown software support cannot manufacture hard uniqueness. Compare all
// physical hosts at permanent capacity, including busy/stale/maintained peers.
// Authored pins without a hard host reason and target labels without an OS/arch
// reason are deliberately not evidence for excluding another capable host.
func (q *Queue) scarceDemandHost(j *Job) (string, string) {
	if !finiteYieldJob(j) || j.State != "queued" || cancelled(j) || !dependencyReady(j) {
		return "", ""
	}
	reasons := j.Spec.RequirementReasons
	for _, e := range j.Extensions {
		if len(reasons) == 0 {
			reasons = e.RequirementReasons
		}
	}
	if len(reasons) == 0 {
		return "", ""
	}
	hard := false
	for k, v := range reasons {
		if strings.TrimSpace(v) != "" && (k == "host" || k == "device" || strings.HasPrefix(k, "backend:") || strings.HasPrefix(k, "os:") || strings.HasPrefix(k, "arch:")) {
			hard = true
		}
	}
	if !hard {
		return "", ""
	}
	hosts := map[string]bool{}
	for _, m := range q.plannerMachines() {
		if m.CapabilityProfile == nil {
			return "", "scarce_host_yield: hardware compatibility unknown"
		}
		if !q.projectAllows(j.Spec.Project, m.ID) {
			continue
		}
		copy := *j
		if strings.TrimSpace(reasons["host"]) == "" {
			copy.Spec.AllowedMachines = nil
			copy.Spec.ExcludedMachines = nil
		}
		if !machineAllowed(copy.Spec.AllowedMachines, m.ID) || contains(copy.Spec.ExcludedMachines, m.ID) {
			continue
		}
		capacity := q.qualificationCapacity(m)
		// Start from only hardware/resource constraints. Copying the full job
		// and removing known software fields lets future protocol/recipe fields
		// accidentally turn a mixed-version rollout into hardware uniqueness.
		physical := Job{Spec: Spec{Needs: copy.Spec.Needs, AllowedMachines: copy.Spec.AllowedMachines, ExcludedMachines: copy.Spec.ExcludedMachines}}
		physical.Spec.Needs.Tools = nil // Missing software is not a hardware exclusion.
		physical.Spec.Needs.Network = false
		if gpu := physical.Spec.Needs.GPU; gpu != nil && strings.TrimSpace(reasons["device"]) == "" {
			selection := *gpu
			selection.Devices = nil
			physical.Spec.Needs.GPU = &selection
		}
		if !fitsTargetResourcesChecked(&physical, &capacity, capacity.TotalMemoryGB, nil) || !hostFits(&capacity, physical.Spec.Needs.MemoryGB, physical.Spec.Needs.HostMemoryGB) {
			continue
		}
		target := machineTarget(m)
		excluded := false
		for _, dimension := range []struct{ key, value string }{{"os:", target.OS}, {"arch:", target.Arch}} {
			constrained := false
			matches := false
			for k, v := range reasons {
				if strings.HasPrefix(k, dimension.key) && strings.TrimSpace(v) != "" {
					constrained = true
					matches = matches || k == dimension.key+dimension.value
				}
			}
			excluded = excluded || constrained && !matches
		}
		if excluded {
			continue
		}
		// An unqualified peer or a missing target recipe is unknown, not a hard
		// hardware exclusion. Count it so it cannot create a single-host claim.
		hosts[hostID(m)] = true
		if len(hosts) > 1 {
			return "", "scarce_host_yield: demand has more than one structurally capable host (occupancy or unknown software support is not uniqueness)"
		}
	}
	for host := range hosts {
		return host, ""
	}
	return "", "scarce_host_yield: hard single-host requirement not established"
}

func yieldMatches(j *Job) bool {
	y := j.ScarceHostYield
	return y != nil && y.Attempt == j.Attempt && y.Machine == j.Machine && y.Session == j.Session
}
func (q *Queue) yieldDemandNeedsHost(y *ScarceHostYield) bool {
	d := q.s.Jobs[y.Demand]
	if d == nil || cancelled(d) || terminal(d.State) {
		return false
	}
	if d.State == "running" || d.State == "stopping" {
		return q.executionHost(d.Machine) == q.executionHost(y.Machine)
	}
	host, _ := q.scarceDemandHost(d)
	return host != "" && host == q.executionHost(y.Machine)
}

// A held final save never becomes a restart. Route only while the recorded
// demand still needs the scarce host; cancellation immediately removes that
// routing exclusion. Saved compatibility remains required on every target.
func (q *Queue) yieldPlacementAllows(j *Job, m *Machine) bool {
	y := j.ScarceHostYield
	if y == nil {
		return true
	}
	if !slices.Contains(m.Capabilities, scarceHostYieldCapability) {
		return false
	}
	if y.Hold != "" || !y.FinalReceived || q.yieldReleasePending(y) {
		return false
	}
	if !q.yieldContractMatches(j, m) {
		return false
	}
	return !q.yieldDemandNeedsHost(y) || hostID(m) != q.executionHost(y.Machine)
}

// Evaluate a destination without preempting its residents or reserving it. The
// ordinary comparator preserves ready work's fair turn. Projections for several
// selected victims debit only this local plan, never queue admission capacity.
func (q *Queue) yieldAlternative(v *Job, scarce string, loads map[string][]*Job) (*Machine, *Job) {
	previousCapture := q.memoryAdmissionCapture
	q.memoryAdmissionCapture = nil // Hypothetical destinations cannot replace durable waiting evidence.
	defer func() { q.memoryAdmissionCapture = previousCapture }()
	copy := *v
	copy.State = "queued"
	copy.Machine = ""
	copy.Session = ""
	copy.StartedAt = nil
	copy.MemoryEnvelope = nil
	copy.PreemptionPlacement = nil
	copy.ScarceHostYield = nil
	scan := q.placementScan()
	live := q.placementLiveness(scan)
	var best *Machine
	var bestCandidate *Job
	for _, m := range q.plannerMachines() {
		if hostID(m) == scarce || !slices.Contains(m.Capabilities, scarceHostYieldCapability) || !live[m.ID] || !q.hostLaunchAllowed(m, m.Session) || q.preparationActive(m) || q.preparationStopping(m) {
			continue
		}
		projected := *m
		for _, load := range loads[hostID(m)] {
			projected.CPUInUseCores += load.Spec.Needs.cores()
			projected.FreeCPUCores = max(0, projected.EffectiveCPUCores-projected.CPUInUseCores)
			projected.FreeHostMemoryGB -= hostNeed(m, load.Spec.Needs.MemoryGB, load.Spec.Needs.HostMemoryGB)
			if !load.Spec.Needs.cpuOnly() {
				projected.FreeMemoryGB -= load.Spec.Needs.MemoryGB
			}
		}
		for _, candidate := range q.memoryCandidates(&copy, &projected) {
			fence := q.groupDemandFence(&projected)
			if fence != nil && fence.ID != v.ID && q.comparePriority(candidate.Spec, fence.Spec) >= 0 {
				continue
			}
			if !q.yieldContractMatches(candidate, &projected) || !q.placementAllowed(candidate, &projected) || !q.checkpointReady(candidate, &projected) || !q.workerIdle(&projected, candidate) || !environmentReady(candidate, &projected) || q.environmentFailureBlocked(candidate, &projected) || !fits(candidate, &projected, projected.FreeMemoryGB) || q.capReasonFor(candidate, &projected) != "" {
				continue
			}
			set := &candidateSet{q: q, m: &projected, jobs: []*Job{candidate}, scores: map[string]float64{}, actives: map[string]float64{}}
			for _, other := range q.liveJobs() {
				if other.ID == v.ID || other.State != "queued" || !q.placementAllowed(other, &projected) || !q.preemptionPlacementAllows(other, &projected) || q.preemptionReleasesPending(other) || !q.checkpointReady(other, &projected) || q.environmentFailureBlocked(other, &projected) {
					continue
				}
				for _, c := range q.memoryCandidates(other, &projected) {
					if q.capReasonFor(c, &projected) == "" && fits(c, &projected, projected.FreeMemoryGB) && environmentReady(c, &projected) {
						set.jobs = append(set.jobs, c)
						break
					}
				}
			}
			for _, call := range q.calls {
				if q.callCanUse(&projected, call) {
					set.jobs = append(set.jobs, callJob(call))
				}
			}
			if set.pick(nil).ID != candidate.ID {
				continue
			}
			if best == nil || q.targetPlacementBetter(candidate, m, best, scan) {
				best, bestCandidate = m, candidate
			}
		}
	}
	return best, bestCandidate
}

func (q *Queue) scarceYieldPlan(d *Job, m *Machine) (*placementPlan, string) {
	if !finiteYieldJob(d) || len(d.Spec.RequirementReasons) == 0 && len(d.Extensions) == 0 {
		return nil, ""
	}
	host, why := q.scarceDemandHost(d)
	if host == "" || host != hostID(m) {
		return nil, why
	}
	if !q.yieldDemandTargetReady(d, m) {
		return nil, "scarce_host_yield: demand target is not ready"
	}
	yields := map[string]*ScarceHostYield{}
	reason := "scarce_host_yield: no portable victim with proven target/environment/result compatibility"
	for _, v := range q.liveJobs() {
		if !batchReservesCapacity(v) || v.State != "running" || q.executionHost(v.Machine) != host || !finiteYieldJob(v) || cancelled(v) || q.comparePriority(d.Spec, v.Spec) > 0 || v.Spec.Priority == "production" && d.Spec.Priority != "production" || v.Spec.Class == "production" && d.Spec.Class != "production" || q.preemptionProtected(v.PreemptionCount, v.StartedAt) {
			continue
		}
		// Ordinary higher-tier preemption retains its original contract.
		if q.batchPreemptible(v, d.Spec) {
			continue
		}
		if !v.Spec.Resumable && !(v.Spec.Preemptible && v.Spec.Priority == "experiment") {
			continue
		}
		source := allocationMachine(q.s.Machines[v.Machine], v.AllocatedDevices)
		if source != nil && v.Spec.Needs.cpuOnly() {
			source = cpuMachineView(source)
		}
		if source == nil || !slices.Contains(source.Capabilities, scarceHostYieldCapability) || !q.yieldContractMatches(v, source) {
			continue
		}
		generation := v.YieldGeneration
		if v.FileTransport {
			generation = 0
			if save, ok := v.FileCheckpoints[v.Attempt]; ok {
				generation = save.Generation
			}
		}
		if v.Spec.Resumable && generation <= 0 {
			reason = "scarce_host_yield: waiting for a verified portable checkpoint generation"
			continue
		}
		dest, _ := q.yieldAlternative(v, host, nil)
		if dest == nil {
			reason = "scarce_host_yield: no qualified currently admissible destination (resources, readiness or fair turn)"
			continue
		}
		yields[v.ID] = &ScarceHostYield{Cause: scarceHostYieldCause, Demand: d.ID, Victim: v.ID, Attempt: v.Attempt, Machine: v.Machine, Session: v.Session, Generation: generation, Destination: dest.ID, DestinationDevice: dest.Device, DestinationTarget: machineTarget(dest)}
	}
	if len(yields) == 0 {
		return nil, reason
	}
	p := q.roomPlanWithBatch(d, m, true, true, yields)
	if p == nil || !q.addDeviceTierStops(p, d) {
		return nil, reason
	}
	selected := false
	loads := map[string][]*Job{}
	for _, v := range p.jobs {
		y := yields[v.ID]
		if y == nil {
			continue
		}
		dest, candidate := q.yieldAlternative(v, host, loads)
		if dest == nil {
			return nil, "scarce_host_yield: selected victims have no simultaneous destination capacity"
		}
		y.Destination, y.DestinationDevice, y.DestinationTarget = dest.ID, dest.Device, machineTarget(dest)
		loads[hostID(dest)] = append(loads[hostID(dest)], candidate)
		selected = true
	}
	if !selected {
		return nil, ""
	}
	return p, ""
}

func (q *Queue) stopForScarceYield(v, d *Job, y *ScarceHostYield) {
	if v.State != "running" || cancelled(v) || v.Attempt != y.Attempt || v.Machine != y.Machine || v.Session != y.Session {
		return
	}
	v.ScarceHostYield = y
	for i := range v.Copies {
		c := &v.Copies[i]
		if c.Attempt == v.Attempt && c.Machine == v.Machine && c.Session == v.Session {
			c.StopCause = scarceHostYieldCause
			c.PriorityStop = q.preemption(d, v.ID)
			c.PriorityStop.Cause = scarceHostYieldCause
			c.Recoverable = false
		}
	}
	v.PreemptionCount++
	q.stopFor(v, d.Spec.Priority, "job")
	v.Reason = "scarce_host_yield: checkpoint and exact release for " + d.ID + "; alternative " + y.Destination
}

// Recheck immediately before delivery. A successful authorization is durable
// stop custody; cancellation thereafter cannot undo a potentially delivered stop.
func (q *Queue) authorizeYieldStop(ctx context.Context, r Result) (bool, error) {
	allowed := false
	err := q.updateRequest(ctx, func() error {
		v, err := q.current(r)
		if err != nil {
			return err
		}
		if !yieldMatches(v) || v.State != "stopping" || cancelled(v) {
			return nil
		}
		y := v.ScarceHostYield
		if y.Authorized {
			allowed = true
			return nil
		}
		d := q.s.Jobs[y.Demand]
		destination, _ := q.yieldAlternative(v, q.executionHost(y.Machine), nil)
		if d == nil || !q.yieldDemandNeedsHost(y) || !q.yieldDemandReady(d, y) || destination == nil {
			if d != nil && d.PreemptionPlacement != nil {
				key := obligationKey(HostObligation{Result: resultIdentity(v)})
				d.PreemptionPlacement.Releases = slices.DeleteFunc(d.PreemptionPlacement.Releases, func(s string) bool { return s == key })
				if len(d.PreemptionPlacement.Releases) == 0 {
					d.PreemptionPlacement = nil
				}
			}
			v.State = "running"
			v.StopAt = nil
			v.Reason = "scarce_host_yield: stop withdrawn before delivery; demand or destination unavailable"
			v.PreemptionCount = max(0, v.PreemptionCount-1)
			for i := range v.Copies {
				c := &v.Copies[i]
				if c.Attempt == v.Attempt && c.Machine == v.Machine && c.Session == v.Session {
					c.PriorityStop = nil
					c.StopCause = ""
					c.StoppedAt = nil
				}
			}
			v.ScarceHostYield = nil
			return nil
		}
		y.Destination, y.DestinationDevice, y.DestinationTarget = destination.ID, destination.Device, machineTarget(destination)
		if v.FileTransport {
			if save, ok := v.FileCheckpoints[v.Attempt]; ok {
				y.Generation = max(y.Generation, save.Generation)
			}
		} else {
			y.Generation = max(y.Generation, v.YieldGeneration)
		}
		y.Authorized = true
		allowed = true
		return nil
	}, nil)
	return allowed, err
}

// The demand can lose readiness independently of its structural uniqueness
// (maintenance, owner return, qualification or environment changes). Stop
// authorization reuses admission fences on its committed exact target, without
// claiming that the still-occupied capacity has already been released.
func (q *Queue) yieldDemandReady(d *Job, y *ScarceHostYield) bool {
	if d.State != "queued" || cancelled(d) {
		return false
	}
	live := q.placementLiveness(q.placementScan())
	for _, m := range q.plannerMachines() {
		if hostID(m) != q.executionHost(y.Machine) || !live[m.ID] || !q.hostLaunchAllowed(m, m.Session) || !q.preemptionPlacementAllows(d, m) {
			continue
		}
		for _, candidate := range q.memoryCandidates(d, m) {
			if q.yieldDemandTargetReady(candidate, m) {
				return true
			}
		}
	}
	return false
}

func (q *Queue) yieldDemandTargetReady(d *Job, m *Machine) bool {
	if m.HostMemoryError != "" || !d.Spec.Needs.cpuOnly() && m.MemoryError != "" {
		return false
	}
	capacity := q.qualificationCapacity(m)
	return q.placementAllowed(d, m) && q.checkpointReady(d, m) && environmentReady(d, m) &&
		q.capReasonFor(d, m) == "" && sharedInputCacheReason(d.Spec, m, false) == "" &&
		fitsTargetResourcesChecked(d, &capacity, capacity.TotalMemoryGB, nil) &&
		hostFits(&capacity, d.Spec.Needs.MemoryGB, d.Spec.Needs.HostMemoryGB)
}

func (q *Queue) yieldWaitReason(j *Job) string {
	y := j.ScarceHostYield
	if y != nil {
		if y.Hold != "" {
			return "scarce_host_yield: " + y.Hold
		}
		if !y.FinalReceived {
			return "scarce_host_yield: final checkpoint/transport pending; exact release tracked separately"
		}
		if q.yieldDemandNeedsHost(y) {
			return fmt.Sprintf("scarce_host_yield: durable generation %d retained; waiting for a qualified alternative to %s", y.Generation, q.executionHost(y.Machine))
		}
	}
	return ""
}

func scarceYieldStop(p *PriorityPreemption) bool { return p != nil && p.Cause == scarceHostYieldCause }
func (q *Queue) yieldReleasePending(y *ScarceHostYield) bool {
	_, ok := q.s.HostReleases[releaseKey(HostRelease{Job: y.Victim, Machine: y.Machine, Session: y.Session, Attempt: y.Attempt})]
	return ok
}
func (q *Queue) yieldResumeStartAllowed(j *Job, m *Machine) bool {
	y := j.ScarceHostYield
	return y == nil || slices.Contains(m.Capabilities, scarceHostYieldCapability) && y.FinalReceived && y.Hold == "" && !q.yieldReleasePending(y) && q.yieldContractMatches(j, m) && (!q.yieldDemandNeedsHost(y) || hostID(m) != q.executionHost(y.Machine))
}
func (q *Queue) retainYieldFinal(j *Job, r Result) {
	if !yieldMatches(j) {
		return
	}
	if r.ReleasePending && r.Interrupted {
		return // Cleanup custody must retain the identity for its final receipt.
	}
	y := j.ScarceHostYield
	y.FinalReceived = true
	if r.Success || !r.Interrupted {
		return
	} // Preserve independent outcomes.
	if !j.Spec.Resumable && j.Spec.Preemptible {
		y.Hold = ""
		return
	}
	valid := y.Authorized && !r.Forced && r.FinalCheckpointGeneration != nil && *r.FinalCheckpointGeneration > y.Generation
	if valid && j.FileTransport {
		save, ok := j.FileCheckpoints[y.Attempt]
		valid = ok && save.Generation == *r.FinalCheckpointGeneration
	}
	if valid {
		y.Hold = ""
		y.Generation = *r.FinalCheckpointGeneration
	} else {
		y.Hold = "final checkpoint incomplete, forced or unverified; committed generations retained; repair checkpoint before resume"
	}
}

func (q *Queue) yieldProjection(j *Job) *ScarceYieldProjection {
	if j.State != "queued" || j.ScarceHostYield != nil {
		return nil
	}
	host, why := q.scarceDemandHost(j)
	if host == "" {
		if why == "" {
			return nil
		}
		return &ScarceYieldProjection{Reason: why}
	}
	out := &ScarceYieldProjection{Host: host, Reason: "scarce_host_yield: no safe relocation plan"}
	for k := range j.Spec.RequirementReasons {
		out.Requirements = append(out.Requirements, requirementConstraint(k))
	}
	slices.Sort(out.Requirements)
	out.Requirements = slices.Compact(out.Requirements)
	for _, m := range q.plannerMachines() {
		if hostID(m) != host {
			continue
		}
		for _, candidate := range q.memoryCandidates(j, m) {
			capacity := q.qualificationCapacity(m)
			if !fitsResources(candidate, &capacity, m.TotalMemoryGB) {
				continue
			}
			p, reason := q.scarceYieldPlan(candidate, m)
			if p != nil {
				out.Reason = "scarce_host_yield: verified portable progress; checkpoint, exact release, then qualified alternative"
				for _, v := range p.jobs {
					if y := p.yields[v.ID]; y != nil {
						out.Victims = append(out.Victims, y)
					}
				}
				return out
			}
			if reason != "" {
				out.Reason = reason
			}
		}
	}
	return out
}
