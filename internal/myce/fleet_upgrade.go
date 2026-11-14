package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// One rollout path (MY-21, MY-35, MY-49), the sequence the per-host release
// rollouts proved: for each host in turn, drain it, wait for its running work,
// let each Mycelium service on it fetch, verify and swap in the approved
// release and restart, check health, resume the drain, then advance. The
// queue's own host goes last. An unhealthy result stops advancing and keeps the
// host drained; nothing rolls back. The operation lives in the queue state.
type FleetUpgradeStatus struct {
	Urgency   string            `json:"urgency"`
	Operation string            `json:"operation"`
	Release   string            `json:"release"`
	State     string            `json:"state"`
	Hosts     []FleetHostStatus `json:"hosts"`
}
type FleetHostStatus struct {
	StorageWarning *StoragePreflightFailure `json:"storage_warning,omitempty"`
	StorageFailure *StoragePreflightFailure `json:"storage_failure,omitempty"`
	StopRequests   []FleetStopRequest       `json:"stop_requests,omitempty"`
	Host           string                   `json:"host"`
	Phase          string                   `json:"phase"`
	Wait           string                   `json:"wait,omitempty"`
	Failure        string                   `json:"failure,omitempty"`
	FailureCode    string                   `json:"failure_code,omitempty"`
	ProgressAt     time.Time                `json:"progress_at"`
}
type fleetUpgrade struct {
	Urgency string `json:"urgency,omitempty"`
	// Revision stays 1. Helpers fence on operation and session; the field
	// keeps the instruction and acknowledgement wire format unchanged.
	Revision     int                 `json:"revision"`
	Operation    string              `json:"operation"`
	Release      EnrollmentRelease   `json:"release"`
	PublisherKey []byte              `json:"publisher_key"`
	State        string              `json:"state"`
	Hosts        []*fleetUpgradeHost `json:"hosts"`
	CreatedAt    time.Time           `json:"created_at,omitempty"`
}
type fleetUpgradeHost struct {
	CriticalStopAt *time.Time `json:"critical_stop_at,omitempty"`
	StepAt         time.Time  `json:"step_at,omitempty"`
	HealthSince    time.Time  `json:"health_since,omitempty"`
	HealthIdentity string     `json:"health_identity,omitempty"`
	FleetHostStatus
	Queue       bool               `json:"queue"`
	QueueDone   bool               `json:"queue_done"`
	WorkerDone  bool               `json:"worker_done"`
	Machine     string             `json:"machine,omitempty"`
	Before      *FleetRuntimeProof `json:"worker_before,omitempty"`
	QueueBefore *FleetRuntimeProof `json:"queue_before,omitempty"`
	Policy      HostMaintenance    `json:"policy"`
	Owned       bool               `json:"owned"`
}

// fleetAbandoned is what remains of operations retired by releases before
// MY-49: only their IDs, so the next upgrade can adopt drains they left.
type fleetAbandoned struct {
	Operation string `json:"operation"`
}

type fleetQueueRuntime struct {
	Session    string
	Host       string
	OS         string
	Arch       string
	Proof      *FleetRuntimeProof
	ObservedAt time.Time
}
type FleetUpgradeRequest struct {
	Urgency string `json:"urgency,omitempty"`
	Release string `json:"release,omitempty"`
}
type FleetUpgradeAck struct {
	StorageWarning *StoragePreflightFailure `json:"storage_warning,omitempty"`
	StorageFailure *StoragePreflightFailure `json:"storage_failure,omitempty"`
	Runtime        *FleetRuntimeProof       `json:"runtime,omitempty"`
	Revision       int                      `json:"revision"`
	Operation      string                   `json:"operation"`
	Host           string                   `json:"host"`
	Machine        string                   `json:"machine"`
	Session        string                   `json:"session"`
	Phase          string                   `json:"phase"`
	Failure        string                   `json:"failure,omitempty"`
}

// The longest one service update can take: the helper's 10-minute artifact
// transfer, a 3-minute manager restart command and 1-minute readiness wait,
// plus the 30-second stability window, rounded up. A step older than this has
// outlived every bound the helper has, so it died or hung: stop advancing.
const fleetStepTimeout = 15 * time.Minute

func fleetError(code string) error { return &HTTPError{Status: 409, Code: code, Message: code} }

func fleetUrgency(value string) string {
	if value == "" {
		return "normal"
	}
	return value
}

func (q *Queue) fleetUpgradeStatus() *FleetUpgradeStatus {
	u := q.s.FleetUpgrade
	if u == nil {
		return nil
	}
	out := &FleetUpgradeStatus{Urgency: fleetUrgency(u.Urgency), Operation: u.Operation, Release: u.Release.Manifest.Version, State: u.State, Hosts: []FleetHostStatus{}}
	for _, h := range u.Hosts {
		out.Hosts = append(out.Hosts, h.FleetHostStatus)
	}
	return out
}

// StartFleetUpgrade starts a new operation for the approved release. Rerunning
// while one runs returns it unchanged; after it stopped or completed, a rerun
// is a new operation that adopts the drains the stopped one left and skips
// services already on the release.
func (q *Queue) StartFleetUpgrade(request FleetUpgradeRequest, requestContexts ...context.Context) (*FleetUpgradeStatus, error) {
	var out *FleetUpgradeStatus
	request.Urgency = fleetUrgency(request.Urgency)
	if request.Urgency != "normal" && request.Urgency != "critical" {
		return nil, &HTTPError{Status: 400, Code: "fleet_urgency_invalid", Message: "fleet urgency must be normal or critical"}
	}
	err := q.updateRequest(requestContext(requestContexts), func() error {
		approved := q.s.EnrollmentRelease
		if approved == nil || len(approved.SignedRelease) == 0 {
			return fleetError("fleet_release_reapproval_required")
		}
		if request.Release != "" && request.Release != approved.Manifest.Version {
			return fleetError("fleet_release_not_approved")
		}
		if !bytes.Equal(approved.PublisherKey, q.s.EnrollmentReleaseKey) {
			return fleetError("fleet_publisher_changed")
		}
		previous := q.s.FleetUpgrade
		if previous != nil && previous.State == "running" {
			if previous.Release.Manifest.Version == approved.Manifest.Version && fleetUrgency(previous.Urgency) == request.Urgency {
				out = q.fleetUpgradeStatus()
				return nil
			}
			return q.fleetOperationRefusal(previous)
		}
		if previous != nil && CheckReleaseUpgrade(approved.Manifest.Version, previous.Release.Manifest.Version, false) != nil {
			return fleetError("fleet_downgrade_refused")
		}
		queue := q.fleetQueue
		if queue == nil || !q.fleetProofFresh(queue.Proof) {
			return q.fleetProofRefusal("fleet_queue_unmanaged")
		}
		a, err := approved.Manifest.Select(queue.OS, queue.Arch)
		if err != nil {
			return fleetError("fleet_queue_target_incompatible")
		}
		if CheckReleaseUpgrade(approved.Manifest.Version, queue.Proof.Version, false, queue.Proof.SHA256 == a.SHA256) != nil {
			return fleetError("fleet_queue_downgrade_refused")
		}
		u := &fleetUpgrade{Urgency: request.Urgency, Operation: ID(), Revision: 1, Release: *approved, PublisherKey: bytes.Clone(q.s.EnrollmentReleaseKey), State: "running", CreatedAt: q.clock.Now()}
		byHost := map[string]*fleetUpgradeHost{queue.Host: {FleetHostStatus: FleetHostStatus{Host: queue.Host, Phase: "pending"}, Queue: true, QueueDone: q.fleetCurrent(queue.Proof, queue.OS, queue.Arch)}}
		for _, m := range q.s.Machines {
			if m.Replacement != nil {
				continue
			}
			h := byHost[hostID(m)]
			if h == nil {
				h = &fleetUpgradeHost{FleetHostStatus: FleetHostStatus{Host: hostID(m), Phase: "pending"}}
				byHost[h.Host] = h
			}
			if h.Machine != "" {
				return fleetError("fleet_legacy_siblings")
			}
			h.Machine = m.ID
			if reason := q.fleetWorkerFailure(m, approved); reason != "" {
				if !q.maintenanceBlocked(h.Host) {
					if reason == "worker_unmanaged" {
						return q.fleetProofRefusal("fleet_worker_unmanaged")
					}
					return fleetError("fleet_" + reason)
				}
				// An out-of-service host the operator drained or disabled is
				// left alone, visibly; rerun once it is back.
				h.Phase, h.Failure = "skipped", reason
				h.Wait = "out of service without a usable runtime proof; rerun myce fleet upgrade once it is back"
				continue
			}
			h.WorkerDone = q.fleetCurrent(m.Fleet, m.CapabilityProfile.OS, m.CapabilityProfile.Arch)
		}
		// Abandoned operations' drains belong to this operation now: it
		// verifies those hosts and then resumes them.
		abandoned := map[string]bool{}
		if previous != nil && previous.State != "complete" {
			abandoned[previous.Operation] = true
		}
		for _, a := range q.s.FleetSuperseded {
			abandoned[a.Operation] = true
		}
		for _, h := range byHost {
			if policy := q.s.HostMaintenance[h.Host]; policy.Mode == "drained" && policy.DrainOwner != "" && abandoned[policy.DrainOwner] {
				h.Policy, h.Owned = policy, true
			}
			if h.Phase == "pending" && (h.Machine == "" || h.WorkerDone) && (!h.Queue || h.QueueDone) {
				h.Phase, h.Wait = "complete", "already on the release"
				if h.Owned {
					h.Phase, h.Wait = "verified", ""
				}
			}
			u.Hosts = append(u.Hosts, h)
		}
		sort.Slice(u.Hosts, func(i, j int) bool { return u.Hosts[i].Host < u.Hosts[j].Host })
		if !slicesContainsPhase(u.Hosts, "pending") && !slicesContainsPhase(u.Hosts, "verified") {
			u.State = "complete"
		}
		q.s.FleetSuperseded = nil
		q.s.FleetUpgrade = u
		out = q.fleetUpgradeStatus()
		return nil
	}, nil)
	return out, err
}

func cloneFleetProof(p *FleetRuntimeProof) *FleetRuntimeProof {
	if p == nil {
		return nil
	}
	out := *p
	if p.InstalledSlot != nil {
		slot := *p.InstalledSlot
		out.InstalledSlot = &slot
	}
	return &out
}

// fleetCurrent reports a service already running the operation's release.
func (q *Queue) fleetCurrent(p *FleetRuntimeProof, goos, arch string) bool {
	release := q.s.EnrollmentRelease
	if u := q.s.FleetUpgrade; u != nil && u.State == "running" {
		release = &u.Release
	}
	if p == nil || release == nil {
		return false
	}
	a, err := release.Manifest.Select(goos, arch)
	return err == nil && p.Version == release.Manifest.Version && p.SHA256 == a.SHA256
}

func (q *Queue) fleetWorkerFailure(m *Machine, release *EnrollmentRelease) string {
	if m == nil || !q.fleetProofFresh(m.Fleet) || m.CapabilityProfile == nil {
		return "worker_unmanaged"
	}
	artifact, err := release.Manifest.Select(m.CapabilityProfile.OS, m.CapabilityProfile.Arch)
	if err != nil {
		return "worker_target_incompatible"
	}
	if CheckReleaseUpgrade(release.Manifest.Version, m.Fleet.Version, false, m.Fleet.SHA256 == artifact.SHA256) != nil {
		return "worker_downgrade_refused"
	}
	return ""
}
func fleetProofReady(p *FleetRuntimeProof) bool {
	return p != nil && p.PID > 0 && p.Session != "" && validPin(p.SHA256) && p.ServiceHealthy && p.RunnerSmoke && p.StartGate == startGateProtocol && p.AttemptProtocol == keyedAttemptProtocol
}

// fleetStable requires the same healthy process for fleetStableFor, the
// enrollment stability window, so a service that crash-loops shortly after
// start stops the rollout instead of passing it. The window runs between the
// process's own observations, so it passes only on a proof collected at least
// fleetStableFor after its first: a process that stopped proving never passes,
// however long its proofs stay fresh (#1450).
func (q *Queue) fleetStable(h *fleetUpgradeHost, p *FleetRuntimeProof) bool {
	identity := fleetHealthIdentity(p)
	if h.HealthIdentity != identity || h.HealthSince.IsZero() {
		h.HealthIdentity, h.HealthSince = identity, p.ObservedAt
	}
	if p.ObservedAt.Sub(h.HealthSince) < fleetStableFor {
		h.Wait = "waiting for stable service health"
		return false
	}
	return true
}

func fleetHealthIdentity(p *FleetRuntimeProof) string {
	return fmt.Sprintf("%s/%d/%s", p.Session, p.PID, p.SHA256)
}

func (q *Queue) fleetProduction(host string) bool {
	for _, j := range q.liveJobs() {
		if !terminal(j.State) && (j.Spec.Priority == "production" || j.Spec.Class == "production") && q.executionHost(j.Machine) == host {
			return true
		}
	}
	for _, c := range q.calls {
		if (c.Priority == "production" || c.Class == "production") && q.executionHost(c.Machine) == host {
			return true
		}
	}
	return false
}
func (q *Queue) fleetActiveHost() *fleetUpgradeHost {
	if u := q.s.FleetUpgrade; u != nil {
		for _, h := range u.Hosts {
			if h.Phase != "pending" && h.Phase != "complete" && h.Phase != "skipped" {
				return h
			}
		}
	}
	return nil
}
func (q *Queue) fleetPolicyMatches(h *fleetUpgradeHost) bool {
	return reflect.DeepEqual(h.Policy, q.s.HostMaintenance[h.Host])
}

// fleetStop records an unhealthy or refused step and stops advancing. The
// host keeps its drain; the operator fixes forward and reruns.
func (q *Queue) fleetStop(h *fleetUpgradeHost, reason string) {
	q.s.FleetUpgrade.State = "stopped"
	h.Failure = reason
	h.Wait = "fix forward, then rerun myce fleet upgrade"
	h.ProgressAt = q.clock.Now()
}
func (q *Queue) fleetHostQuiet(h *fleetUpgradeHost) bool {
	for _, m := range q.s.Machines {
		if hostID(m) == h.Host && m.Replacement == nil {
			return q.hostMaintenanceStatus(h.Host).Quiescent
		}
	}
	return len(q.hostObligations(h.Host)) == 0
}

// fleetDrainTemporary reports a drain the running upgrade owns. It ends when
// that host's step verifies; a stopped operation's retained drain does not.
func (q *Queue) fleetDrainTemporary(host string) bool {
	u := q.s.FleetUpgrade
	if u == nil || u.State != "running" {
		return false
	}
	for _, h := range u.Hosts {
		if h.Host == host && h.Owned && q.fleetPolicyMatches(h) && h.Policy.Mode == "drained" {
			return true
		}
	}
	return false
}

// The queue restart ends every model HTTP stream, so new calls wait while its
// host's step runs.
func (q *Queue) fleetPausesCalls() bool {
	h := q.fleetActiveHost()
	return h != nil && h.Queue && !h.QueueDone
}

// fleetSelectHost picks the next pending host: hosts idle now first, hosts
// running production after, the queue's host last. A normal update leaves a
// host enabled while draining it would strand open production work (#1387).
func (q *Queue) fleetSelectHost(u *fleetUpgrade) *fleetUpgradeHost {
	var pending []*fleetUpgradeHost
	for _, h := range u.Hosts {
		if h.Phase == "pending" {
			pending = append(pending, h)
		}
	}
	idle := map[*fleetUpgradeHost]bool{}
	production := map[*fleetUpgradeHost]bool{}
	for _, h := range pending {
		idle[h], production[h] = len(q.hostObligations(h.Host)) == 0, q.fleetProduction(h.Host)
	}
	sort.SliceStable(pending, func(i, j int) bool {
		a, b := pending[i], pending[j]
		if a.Queue != b.Queue {
			return !a.Queue
		}
		if production[a] != production[b] {
			return !production[a]
		}
		if idle[a] != idle[b] {
			return idle[a]
		}
		return a.Host < b.Host
	})
	critical := fleetUrgency(u.Urgency) == "critical"
	for _, h := range pending {
		wait := ""
		failure := ""
		if h.Machine != "" && !h.WorkerDone {
			failure = q.fleetWorkerFailure(q.s.Machines[h.Machine], &u.Release)
		}
		if failure != "" && !h.Owned && q.maintenanceBlocked(h.Host) {
			// Taken out of service while waiting: leave it, visibly.
			h.Phase, h.Failure, h.ProgressAt = "skipped", failure, q.clock.Now()
			h.Wait = "out of service without a usable runtime proof; rerun myce fleet upgrade once it is back"
			continue
		}
		if h.Queue && q.fleetOthersPending(u, h) {
			wait = "the queue's host goes last"
		} else if failure != "" {
			wait = "waiting for a fresh worker runtime proof"
		} else if reason := q.productionDrainBlocker(h.Host); reason != "" && !critical && !h.Owned && !q.maintenanceBlocked(h.Host) {
			wait = "waiting for open production work: " + reason + "; host admission remains enabled"
		}
		if wait != h.Wait {
			h.Wait, h.ProgressAt = wait, q.clock.Now()
		}
		if wait == "" {
			return h
		}
	}
	return nil
}

// fleetBeginHost drains a selected host. An operator's drain or disable stays
// theirs: the operation updates the host but never resumes it.
func (q *Queue) fleetBeginHost(u *fleetUpgrade, h *fleetUpgradeHost) bool {
	h.Phase, h.ProgressAt = "draining", q.clock.Now()
	if h.Owned {
		return true
	}
	policy := q.s.HostMaintenance[h.Host]
	switch policy.Mode {
	case "", "enabled":
		if err := q.changeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "drain", IfMode: policy.Mode, IfUpdatedAt: &policy.UpdatedAt, DrainOwner: u.Operation}); err != nil {
			q.fleetStop(h, "drain_conflict")
			return false
		}
		h.Owned = true
	case "drained", "disabled":
	default:
		q.fleetStop(h, "maintenance_unavailable")
		return false
	}
	h.Policy = q.s.HostMaintenance[h.Host]
	return true
}

// Called from the normal queue transaction; only in-memory fences and custody
// are inspected here. Hashing, signature checks and manager probes stay outside.
func (q *Queue) maintainFleetUpgrade() {
	q.pruneProductionLeases()
	u := q.s.FleetUpgrade
	if u == nil || u.State != "running" {
		return
	}
	h := q.fleetActiveHost()
	if h == nil {
		if h = q.fleetSelectHost(u); h == nil {
			if !slicesContainsPhase(u.Hosts, "pending") {
				u.State = "complete"
			}
			return
		}
		if !q.fleetBeginHost(u, h) {
			return
		}
	}
	if !q.fleetPolicyMatches(h) {
		q.fleetStop(h, "maintenance_changed")
		h.Wait = "maintenance was replaced; the operator's policy stays; rerun myce fleet upgrade to continue"
		return
	}
	if h.Phase == "draining" && !q.fleetDrain(u, h) {
		return
	}
	if h.Phase == "worker_update" || h.Phase == "worker_health" {
		m := q.s.Machines[h.Machine]
		if m == nil || m.CapabilityProfile == nil {
			q.fleetStop(h, "worker_unmanaged")
			return
		}
		if h.Before == nil && h.Phase == "worker_update" && q.fleetProofFresh(m.Fleet) {
			h.Before = cloneFleetProof(m.Fleet)
		}
		if !q.fleetVerify(h, m.Fleet, h.Before, m.CapabilityProfile.OS, m.CapabilityProfile.Arch, "worker") {
			return
		}
		h.WorkerDone = true
		if h.Phase = "draining"; !q.fleetDrain(u, h) {
			return
		}
	}
	if h.Phase == "queue_update" || h.Phase == "queue_health" {
		p := q.fleetQueue
		var proof *FleetRuntimeProof
		goos, arch := "", ""
		if p != nil && p.Host == h.Host {
			proof, goos, arch = p.Proof, p.OS, p.Arch
		}
		if h.QueueBefore == nil && h.Phase == "queue_update" && q.fleetProofFresh(proof) {
			h.QueueBefore = cloneFleetProof(proof)
		}
		if !q.fleetVerify(h, proof, h.QueueBefore, goos, arch, "queue") {
			return
		}
		h.QueueDone, h.Phase = true, "verified"
	}
	if h.Phase == "verified" {
		if h.Owned {
			if err := q.changeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &h.Policy.UpdatedAt, IfDrainOwner: h.Policy.DrainOwner}); err != nil {
				q.fleetStop(h, "resume_conflict")
				return
			}
		}
		h.Phase, h.Wait, h.ProgressAt = "complete", "", q.clock.Now()
		if !slicesContainsPhase(u.Hosts, "pending") && q.fleetActiveHost() == nil {
			u.State = "complete"
		}
	}
}

func (q *Queue) fleetOthersPending(u *fleetUpgrade, self *fleetUpgradeHost) bool {
	for _, h := range u.Hosts {
		if h != self && h.Phase == "pending" {
			return true
		}
	}
	return false
}

func slicesContainsPhase(hosts []*fleetUpgradeHost, phase string) bool {
	for _, h := range hosts {
		if h.Phase == phase {
			return true
		}
	}
	return false
}

// fleetDrain waits for the drained host's work to finish (critical: stops it
// at a checkpoint first), then starts the next service's update: the worker,
// then the queue. It reports whether the host moved past draining.
func (q *Queue) fleetDrain(u *fleetUpgrade, h *fleetUpgradeHost) bool {
	if fleetUrgency(u.Urgency) == "critical" && !q.maintainFleetCriticalStops(h) {
		return false
	}
	if !q.fleetHostQuiet(h) {
		h.Wait = "waiting for running work on the host to finish"
		return false
	}
	if h.Queue && !h.QueueDone && len(q.calls) > 0 {
		h.Wait = "waiting for model calls and streams to finish"
		return false
	}
	h.Wait, h.StepAt, h.HealthSince, h.HealthIdentity = "", q.clock.Now(), time.Time{}, ""
	// The pre-update identity comes from a fresh proof only; a late one is
	// captured once it arrives, before any instruction can be issued.
	switch {
	case h.Machine != "" && !h.WorkerDone:
		h.Phase, h.Before = "worker_update", nil
		if m := q.s.Machines[h.Machine]; m != nil && q.fleetProofFresh(m.Fleet) {
			h.Before = cloneFleetProof(m.Fleet)
		}
	case h.Queue && !h.QueueDone:
		var proof *FleetRuntimeProof
		if q.fleetQueue != nil && q.fleetProofFresh(q.fleetQueue.Proof) {
			proof = cloneFleetProof(q.fleetQueue.Proof)
		}
		h.Phase, h.QueueBefore = "queue_update", proof
	default:
		h.Phase = "verified"
	}
	h.ProgressAt = q.clock.Now()
	return true
}

// fleetVerify reports a service healthy on the release: a fresh runtime proof
// from a new process with the release's exact bytes, stable for
// fleetStableFor. One late proof from that process keeps its window; a missing
// or unhealthy proof restarts it. A different new process, or no verification
// within fleetStepTimeout, stops the operation.
func (q *Queue) fleetVerify(h *fleetUpgradeHost, p, before *FleetRuntimeProof, goos, arch, role string) bool {
	fresh := q.fleetProofFresh(p)
	restarted := fleetProofReady(p) && before != nil && p.Session != before.Session && p.PID != before.PID
	current := restarted && q.fleetCurrent(p, goos, arch)
	if fresh && current {
		return q.fleetStable(h, p)
	}
	if current && q.fleetProofLate(h, p) {
		h.Wait = "waiting for the " + role + "'s next runtime proof"
		return false
	}
	h.HealthSince, h.HealthIdentity = time.Time{}, ""
	if fresh && restarted {
		q.fleetStop(h, role+"_running_hash_mismatch")
		return false
	}
	if q.clock.Now().Sub(h.StepAt) >= fleetStepTimeout {
		q.fleetStop(h, role+"_update_not_verified")
		return false
	}
	h.Wait = "waiting for the " + role + " to install the release, restart and report healthy"
	return false
}

func (q *Queue) fleetInstruction(host, machine, session string) (FleetUpdateInstruction, error) {
	u, h := q.s.FleetUpgrade, q.fleetActiveHost()
	if u == nil || u.State != "running" || h == nil || h.Host != host {
		return FleetUpdateInstruction{}, nil
	}
	if !q.fleetPolicyMatches(h) {
		return FleetUpdateInstruction{}, fleetError("fleet_maintenance_changed")
	}
	ok := false
	switch {
	case machine == "@queue":
		ok = h.Phase == "queue_update" && h.QueueBefore != nil && h.QueueBefore.Session == session && q.fleetQueue != nil && q.fleetProofFresh(q.fleetQueue.Proof) && q.fleetQueue.Proof.Session == session && len(q.calls) == 0
	case machine == h.Machine:
		m := q.s.Machines[machine]
		ok = h.Phase == "worker_update" && h.Before != nil && h.Before.Session == session && m != nil && m.Session == session && q.fleetProofFresh(m.Fleet)
	}
	if !ok || !q.fleetHostQuiet(h) {
		return FleetUpdateInstruction{}, nil
	}
	return FleetUpdateInstruction{StorageDiagnostics: true, StorageWarnings: true, Revision: u.Revision, Operation: u.Operation, Host: host, Machine: machine, Session: session, Release: u.Release, PublisherKey: bytes.Clone(u.PublisherKey)}, nil
}

func (q *Queue) authorizeFleetUpdate(operation, host, machine, session string) error {
	i, err := q.fleetInstruction(host, machine, session)
	if err != nil {
		return err
	}
	if i.Operation == "" || i.Operation != operation {
		return fleetError("fleet_instruction_stale")
	}
	return nil
}

// AckFleetUpgrade records a helper's report. Only "restarting" and "failed"
// change anything; health is verified from runtime proofs, not from acks.
func (q *Queue) AckFleetUpgrade(ack FleetUpgradeAck, requestContexts ...context.Context) error {
	return q.updateRequest(requestContext(requestContexts), func() error {
		u, h := q.s.FleetUpgrade, q.fleetActiveHost()
		if u == nil || h == nil || u.State != "running" || u.Operation != ack.Operation || h.Host != ack.Host {
			return fleetError("fleet_ack_stale")
		}
		queue := ack.Machine == "@queue" && h.QueueBefore != nil && ack.Session == h.QueueBefore.Session && strings.HasPrefix(h.Phase, "queue_")
		worker := ack.Machine == h.Machine && h.Before != nil && ack.Session == h.Before.Session && strings.HasPrefix(h.Phase, "worker_")
		if !queue && !worker {
			return fleetError("fleet_ack_stale")
		}
		switch ack.Phase {
		case "prepared", "installed":
		case "restarting":
			h.StorageWarning = sanitizeStorageFailure(ack.StorageWarning)
			h.Phase = strings.Replace(h.Phase, "_update", "_health", 1)
			h.ProgressAt = q.clock.Now()
		case "failed":
			h.FailureCode = fleetFailureCode(ack.Failure)
			h.StorageFailure = fleetStorageFailure(h.FailureCode, ack.StorageFailure)
			reason := h.FailureCode
			switch h.FailureCode {
			case "fleet_storage_preflight_failed":
				reason = "storage_preflight_failed"
			case "fleet_health_failed":
				reason = "service_health_failed"
			}
			q.fleetStop(h, reason)
		default:
			return errors.New("invalid fleet acknowledgement")
		}
		return nil
	}, nil)
}

// fleetOperationRefusal names the running operation a different request
// would replace.
func (q *Queue) fleetOperationRefusal(u *fleetUpgrade) error {
	msg := fmt.Sprintf("fleet operation %s is running (release %s, urgency %s); wait for it to finish or stop (myce status), then rerun", u.Operation, u.Release.Manifest.Version, fleetUrgency(u.Urgency))
	return &HTTPError{Status: 409, Code: "fleet_upgrade_target_conflict", Message: msg}
}

func (q *Queue) fleetProofRefusal(code string) error {
	var gaps []string
	if q.fleetQueue == nil {
		gaps = append(gaps, "queue: no fleet runtime proof")
	} else if why := q.fleetProofGap(q.fleetQueue.Proof); why != "" {
		gaps = append(gaps, "queue on host "+q.fleetQueue.Host+": "+why)
	}
	var workers []string
	for _, m := range q.s.Machines {
		if m.Replacement != nil || q.maintenanceBlocked(hostID(m)) {
			continue
		}
		why := q.fleetProofGap(m.Fleet)
		if why == "" && m.CapabilityProfile == nil {
			why = "no capability profile"
		}
		if why != "" {
			workers = append(workers, fmt.Sprintf("worker %s on host %s: %s", m.ID, hostID(m), why))
		}
	}
	sort.Strings(workers)
	msg := strings.Join(append(gaps, workers...), "; ")
	msg += "; install the release as a service on each named host (docs/operations.md#rolling-fleet-upgrades), or drain it to leave it out; a healthy service presents its proof on its next poll"
	return &HTTPError{Status: 409, Code: code, Message: msg}
}

func (q *Queue) fleetProofGap(p *FleetRuntimeProof) string {
	switch {
	case p == nil:
		return "no fleet runtime proof"
	case !fleetProofReady(p):
		why := fmt.Sprintf("fleet runtime proof not ready (service_healthy=%t, runner_smoke=%t", p.ServiceHealthy, p.RunnerSmoke)
		if p.Failure != "" {
			why += ", failure=" + p.Failure
		}
		return why + ")"
	case !q.fleetProofFresh(p):
		return "fleet runtime proof stale (observed " + q.clock.Now().Sub(p.ObservedAt).Round(time.Second).String() + " ago, fresh for " + q.fleetProofBound(p).Round(time.Second).String() + ")"
	}
	return ""
}

// Marshal-copy the frozen operation before handing it to network code.
func cloneFleetRelease(r EnrollmentRelease) EnrollmentRelease {
	var out EnrollmentRelease
	b, _ := json.Marshal(r)
	_ = json.Unmarshal(b, &out)
	return out
}
