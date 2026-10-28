package myce

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

// The operation is part of the queue state transaction, never a second queue.
// Public status deliberately excludes publisher locations and signed envelopes.
type FleetUpgradeStatus struct {
	Operation string            `json:"operation"`
	Release   string            `json:"release"`
	State     string            `json:"state"`
	Hosts     []FleetHostStatus `json:"hosts"`
}
type FleetHostStatus struct {
	Host       string    `json:"host"`
	Phase      string    `json:"phase"`
	Wait       string    `json:"wait,omitempty"`
	Failure    string    `json:"failure,omitempty"`
	ProgressAt time.Time `json:"progress_at"`
}
type fleetUpgrade struct {
	Revision     int                 `json:"revision"`
	Operation    string              `json:"operation"`
	Release      EnrollmentRelease   `json:"release"`
	PublisherKey []byte              `json:"publisher_key"`
	State        string              `json:"state"`
	Hosts        []*fleetUpgradeHost `json:"hosts"`
}
type fleetUpgradeHost struct {
	HealthSince    time.Time `json:"health_since,omitempty"`
	HealthIdentity string    `json:"health_identity,omitempty"`
	HelperRevision int       `json:"helper_revision,omitempty"`
	FleetHostStatus
	Queue       bool               `json:"queue"`
	QueueDone   bool               `json:"queue_done"`
	WorkerDone  bool               `json:"worker_done"`
	Machine     string             `json:"machine,omitempty"`
	Before      *Machine           `json:"before,omitempty"`
	QueueBefore *FleetRuntimeProof `json:"queue_before,omitempty"`
	Policy      HostMaintenance    `json:"policy"`
	Owned       bool               `json:"owned"`
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
	Release string `json:"release,omitempty"`
}
type FleetUpgradeAck struct {
	Runtime   *FleetRuntimeProof `json:"runtime,omitempty"`
	Revision  int                `json:"revision"`
	Operation string             `json:"operation"`
	Host      string             `json:"host"`
	Machine   string             `json:"machine"`
	Session   string             `json:"session"`
	Phase     string             `json:"phase"`
	Failure   string             `json:"failure,omitempty"`
}

func fleetError(code string) error { return &HTTPError{Status: 409, Code: code, Message: code} }

func (q *Queue) fleetUpgradeStatus() *FleetUpgradeStatus {
	u := q.s.FleetUpgrade
	if u == nil {
		return nil
	}
	out := &FleetUpgradeStatus{Operation: u.Operation, Release: u.Release.Manifest.Version, State: u.State, Hosts: []FleetHostStatus{}}
	for _, h := range u.Hosts {
		out.Hosts = append(out.Hosts, h.FleetHostStatus)
	}
	return out
}

func (q *Queue) StartFleetUpgrade(request FleetUpgradeRequest) (*FleetUpgradeStatus, error) {
	var out *FleetUpgradeStatus
	err := q.update(func() error {
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
		if previous := q.s.FleetUpgrade; previous != nil {
			if previous.Release.Manifest.Version == approved.Manifest.Version && reflect.DeepEqual(previous.Release.Manifest, approved.Manifest) {
				// An explicit identical rerun clears only a failure. The exact drain
				// and old session remain fenced; health is always re-observed.
				if previous.State != "complete" {
					previous.Revision++
				}
				if h := q.fleetActiveHost(); h != nil {
					if previous.State == "stopped" {
						// Explicit recovery supersedes the already reported failure.
						h.HelperRevision = previous.Revision
					}
					if h.Phase == "queue_health" && h.QueueBefore.Version != approved.Manifest.Version && q.fleetQueue != nil && q.fleetQueue.Proof != nil && q.fleetQueue.Proof.Session == h.QueueBefore.Session && q.fleetQueue.Proof.SHA256 == h.QueueBefore.SHA256 {
						h.Phase = "queue_update"
					}
					if h.Phase == "worker_health" && h.Before.Fleet.Version != approved.Manifest.Version {
						if m := q.s.Machines[h.Machine]; m != nil && m.Fleet != nil && m.Session == h.Before.Session && m.Fleet.SHA256 == h.Before.Fleet.SHA256 {
							h.Phase = "worker_update"
						}
					}
				}
				if previous.State == "stopped" {
					previous.State = "running"
					for _, h := range previous.Hosts {
						if h.Failure != "" {
							h.Failure = ""
							h.Wait = "reconciling interrupted step"
						}
					}
				}
				out = q.fleetUpgradeStatus()
				return nil
			}
			if previous.State != "complete" && (previous.State != "stopped" || request.Release == "") {
				return fleetError("fleet_upgrade_target_conflict")
			}
			if err := CheckReleaseUpgrade(approved.Manifest.Version, previous.Release.Manifest.Version, false); err != nil {
				return fleetError("fleet_downgrade_refused")
			}
		}
		if q.fleetQueue == nil || !q.fleetProofFresh(q.fleetQueue.Proof) {
			return fleetError("fleet_queue_unmanaged")
		}
		u := &fleetUpgrade{Operation: ID(), Revision: 1, Release: *approved, PublisherKey: bytes.Clone(q.s.EnrollmentReleaseKey), State: "running"}
		queue := q.fleetQueue
		a, err := approved.Manifest.Select(queue.OS, queue.Arch)
		if err != nil {
			return fleetError("fleet_queue_target_incompatible")
		}
		if err = CheckReleaseUpgrade(approved.Manifest.Version, queue.Proof.Version, false, queue.Proof.SHA256 == a.SHA256); err != nil {
			return fleetError("fleet_queue_downgrade_refused")
		}
		qh := &fleetUpgradeHost{FleetHostStatus: FleetHostStatus{Host: queue.Host, Phase: "pending"}, Queue: true, QueueBefore: cloneFleetProof(queue.Proof)}
		byHost := map[string]*fleetUpgradeHost{queue.Host: qh}
		for _, m := range q.s.Machines {
			if m.Replacement != nil || q.migrationHistoricalMachine(m) {
				continue
			}
			if !q.fleetProofFresh(m.Fleet) || m.CapabilityProfile == nil {
				return fleetError("fleet_worker_unmanaged")
			}
			artifact, e := approved.Manifest.Select(m.CapabilityProfile.OS, m.CapabilityProfile.Arch)
			if e != nil {
				return fleetError("fleet_worker_target_incompatible")
			}
			if e = CheckReleaseUpgrade(approved.Manifest.Version, m.Fleet.Version, false, m.Fleet.SHA256 == artifact.SHA256); e != nil {
				return fleetError("fleet_worker_downgrade_refused")
			}
			h := byHost[hostID(m)]
			if h == nil {
				h = &fleetUpgradeHost{FleetHostStatus: FleetHostStatus{Host: hostID(m), Phase: "pending"}}
				byHost[h.Host] = h
			}
			if h.Machine != "" {
				return fleetError("fleet_legacy_siblings")
			}
			before := cloneMachine(m)
			h.Machine = m.ID
			h.Before = &before
		}
		for _, h := range byHost {
			u.Hosts = append(u.Hosts, h)
		}
		sort.Slice(u.Hosts, func(i, j int) bool { return u.Hosts[i].Host < u.Hosts[j].Host })
		// Manifest validation already requires the current start/attempt protocols.
		// Thus workers preceding a production queue remain compatible with it.
		// New protocol migrations cannot enter approval under this implementation.
		if old := q.s.FleetUpgrade; old != nil && old.State == "stopped" {
			// Carry unresolved drain custody into explicit fix-forward intent.
			for _, h := range u.Hosts {
				for _, p := range old.Hosts {
					if h.Host == p.Host && p.Phase != "pending" && p.Phase != "complete" {
						h.Policy = p.Policy
						h.Owned = p.Owned
						if !q.fleetPolicyMatches(p) {
							current := q.s.HostMaintenance[h.Host]
							if current.Mode != "drained" && current.Mode != "disabled" {
								return &HTTPError{Status: 409, Code: "fleet_maintenance_changed", Message: "stopped upgrade maintenance was replaced; explicitly drain or disable the host before selecting a newer approved release"}
							}
							// Explicit newer-release intent may use the operator's current
							// admission fence, but never inherit authority to resume it.
							h.Policy = current
							h.Owned = false
						}
						h.Phase = "draining"
					}
				}
			}
		}
		q.s.FleetUpgrade = u
		out = q.fleetUpgradeStatus()
		return nil
	})
	return out, err
}

func cloneFleetProof(p *FleetRuntimeProof) *FleetRuntimeProof {
	if p == nil {
		return nil
	}
	out := *p
	return &out
}
func fleetProofReady(p *FleetRuntimeProof) bool {
	return p != nil && p.PID > 0 && p.Session != "" && validPin(p.SHA256) && p.ServiceHealthy && p.RunnerSmoke && p.StartGate == startGateProtocol && p.AttemptProtocol == keyedAttemptProtocol
}
func (q *Queue) fleetProofFresh(p *FleetRuntimeProof) bool {
	if !fleetProofReady(p) || p.ObservedAt.IsZero() {
		return false
	}
	now := q.clock.Now()
	age := now.Sub(p.ObservedAt)
	return age >= 0 && age < min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2]))
}

// Match the existing enrollment stability window. It catches service crash loops
// before restoring admission; its cost is 30 seconds per updated service.
func (q *Queue) fleetStable(h *fleetUpgradeHost, p *FleetRuntimeProof) bool {
	identity := fmt.Sprintf("%s/%d/%s", p.Session, p.PID, p.SHA256)
	now := q.clock.Now()
	if h.HealthIdentity != identity || h.HealthSince.IsZero() {
		h.HealthIdentity = identity
		h.HealthSince = now
	}
	if now.Sub(h.HealthSince) < 30*time.Second {
		h.Wait = "waiting for stable service and running-image health"
		return false
	}
	return true
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
			if h.Phase != "pending" && h.Phase != "complete" {
				return h
			}
		}
	}
	return nil
}
func (q *Queue) fleetPolicyMatches(h *fleetUpgradeHost) bool {
	return reflect.DeepEqual(h.Policy, q.s.HostMaintenance[h.Host])
}
func (q *Queue) fleetStop(h *fleetUpgradeHost, reason string) {
	q.s.FleetUpgrade.State = "stopped"
	h.Failure = reason
	h.Wait = "fix forward, then rerun myce fleet upgrade"
	if reason == "maintenance_changed" {
		h.Wait = "maintenance was replaced; preserve the current operator policy and select an approved newer release with myce fleet upgrade --release VERSION to fix forward"
	}
	h.ProgressAt = q.clock.Now()
}
func (q *Queue) fleetHostQuiet(h *fleetUpgradeHost) bool {
	if h.Machine == "" {
		return h.Queue && len(q.hostObligations(h.Host)) == 0
	}
	return q.hostMaintenanceStatus(h.Host).Quiescent
}
func (q *Queue) fleetPausesCalls() bool {
	h := q.fleetActiveHost()
	return h != nil && h.Queue && !h.QueueDone && q.s.FleetUpgrade.State != "complete"
}

// Called from the normal queue transaction; only in-memory fences and custody
// are inspected here. Hashing, signature checks and manager probes stay outside.
func (q *Queue) maintainFleetUpgrade() {
	u := q.s.FleetUpgrade
	if u == nil || u.State != "running" {
		return
	}
	h := q.fleetActiveHost()
	if h == nil {
		var pending []*fleetUpgradeHost
		for _, v := range u.Hosts {
			if v.Phase == "pending" {
				pending = append(pending, v)
			}
		}
		if len(pending) == 0 {
			u.State = "complete"
			return
		}
		sort.SliceStable(pending, func(i, j int) bool {
			pi, pj := q.fleetProduction(pending[i].Host), q.fleetProduction(pending[j].Host)
			if pi != pj {
				return !pi
			}
			if pending[i].Queue != pending[j].Queue {
				return pending[i].Queue
			}
			return pending[i].Host < pending[j].Host
		})
		h = pending[0]
		// Reflect execution order in status while retaining completed history.
		var ordered []*fleetUpgradeHost
		for _, v := range u.Hosts {
			if v.Phase == "complete" {
				ordered = append(ordered, v)
			}
		}
		u.Hosts = append(ordered, pending...)
		policy := q.s.HostMaintenance[h.Host]
		if policy.Mode == "" && h.Machine == "" {
			policy.Mode = "enabled"
			q.s.HostMaintenance[h.Host] = policy
		}
		h.Policy = policy
		h.Phase = "draining"
		h.ProgressAt = q.clock.Now()
		if policy.Mode == "enabled" {
			if err := q.changeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "drain", IfMode: policy.Mode, IfUpdatedAt: &policy.UpdatedAt, DrainOwner: u.Operation}); err != nil {
				q.fleetStop(h, "drain_conflict")
				return
			}
			h.Owned = true
			h.Policy = q.s.HostMaintenance[h.Host]
		} else if policy.Mode != "drained" && policy.Mode != "disabled" {
			q.fleetStop(h, "maintenance_unavailable")
			return
		}
	}
	if !q.fleetPolicyMatches(h) {
		q.fleetStop(h, "maintenance_changed")
		return
	}
	if h.Phase == "draining" {
		if !q.fleetHostQuiet(h) {
			h.Wait = "waiting for fresh exact execution and release custody"
			return
		}
		if h.Queue && !h.QueueDone && len(q.calls) > 0 {
			h.Wait = "waiting for model calls and streams to finish"
			return
		}
		if h.Queue && !h.QueueDone {
			h.Phase = "queue_update"
			h.HelperRevision = u.Revision
			if h.QueueBefore.Version == u.Release.Manifest.Version {
				h.Phase = "queue_health"
			}
		} else if h.Machine != "" && !h.WorkerDone {
			h.Phase = "worker_update"
			h.HelperRevision = u.Revision
			if h.Before.Fleet.Version == u.Release.Manifest.Version {
				h.Phase = "worker_health"
			}
		} else {
			h.Phase = "verified"
		}
		h.Wait = ""
		h.ProgressAt = q.clock.Now()
	}
	if h.Phase == "queue_update" || h.Phase == "queue_health" {
		p := q.fleetQueue
		if p == nil || p.Host != h.Host || !q.fleetProofFresh(p.Proof) {
			h.HealthSince = time.Time{}
			h.HealthIdentity = ""
			h.Wait = "waiting for healthy queue runtime proof"
			return
		}
		a, _ := u.Release.Manifest.Select(p.OS, p.Arch)
		if p.Proof.Version == u.Release.Manifest.Version && p.Proof.SHA256 == a.SHA256 {
			if h.QueueBefore.Version != u.Release.Manifest.Version && (p.Proof.Session == h.QueueBefore.Session || p.Proof.PID == h.QueueBefore.PID) {
				h.Wait = "waiting for a new queue process and session"
				return
			}
			if !q.fleetStable(h, p.Proof) {
				return
			}
			h.HealthSince = time.Time{}
			h.HealthIdentity = ""
			h.QueueDone = true
			h.Phase = "draining"
			h.Wait = ""
			h.ProgressAt = q.clock.Now()
			return
		}
		if h.Phase == "queue_health" && p.Proof.Session != h.QueueBefore.Session {
			q.fleetStop(h, "queue_running_hash_mismatch")
			return
		}
	}
	if h.Phase == "worker_update" || h.Phase == "worker_health" {
		m := q.s.Machines[h.Machine]
		if m == nil || !q.fleetProofFresh(m.Fleet) {
			h.HealthSince = time.Time{}
			h.HealthIdentity = ""
			h.Wait = "waiting for worker runtime proof"
			return
		}
		a, _ := u.Release.Manifest.Select(h.Before.CapabilityProfile.OS, h.Before.CapabilityProfile.Arch)
		if m.Fleet.Version == u.Release.Manifest.Version && m.Fleet.SHA256 == a.SHA256 {
			if m.Fleet.Session != m.Session || h.Before.Fleet.Version != u.Release.Manifest.Version && (m.Session == h.Before.Session || m.Fleet.PID == h.Before.Fleet.PID) {
				h.Wait = "waiting for new worker process and session"
				return
			}
			profile := h.Before.CapabilityProfile
			r := EnrollmentReceipt{Session: m.Session, Version: u.Release.Manifest.Version, Devices: h.Before.Devices, LogicalCPUs: profile.LogicalCPUs, SystemMemoryBytes: profile.SystemMemoryBytes}
			status := q.hostMaintenanceStatus(h.Host)
			fresh := false
			for _, v := range status.Workers {
				if v.Machine.ID == m.ID {
					fresh = v.Fresh
				}
			}
			observed := cloneMachine(m)
			if observed.Enrollment == nil {
				p := m.Fleet
				observed.Enrollment = &EnrollmentProof{Version: p.Version, StartGate: p.StartGate, AttemptProtocol: p.AttemptProtocol, RunnerSmoke: p.RunnerSmoke}
			}
			_, err := evaluateEnrollmentVersion(r, EnrollmentObservation{Machine: observed, Fresh: fresh, StartGate: startGateProtocol, AttemptProtocol: keyedAttemptProtocol, Policy: status.Policy}, u.Release.Manifest.Version)
			if err != nil {
				h.HealthSince = time.Time{}
				h.HealthIdentity = ""
				h.Wait = "waiting for enrollment inventory, budgets and runner health"
				return
			}
			if !q.fleetStable(h, m.Fleet) {
				return
			}
			h.HealthSince = time.Time{}
			h.HealthIdentity = ""
			h.WorkerDone = true
			h.Phase = "verified"
			h.Wait = ""
			h.ProgressAt = q.clock.Now()
		} else if h.Phase == "worker_health" && m.Session != h.Before.Session {
			q.fleetStop(h, "worker_running_hash_mismatch")
			return
		}
	}
	if h.Phase == "verified" {
		if h.Owned {
			if err := q.changeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &h.Policy.UpdatedAt, IfDrainOwner: h.Policy.DrainOwner}); err != nil {
				q.fleetStop(h, "resume_conflict")
				return
			}
		}
		h.Phase = "complete"
		h.Wait = ""
		h.ProgressAt = q.clock.Now()
	}
}

func (q *Queue) fleetInstruction(host, machine, session string) (FleetUpdateInstruction, error) {
	u := q.s.FleetUpgrade
	h := q.fleetActiveHost()
	if u == nil || u.State != "running" || h == nil || h.Host != host {
		return FleetUpdateInstruction{}, nil
	}
	if !q.fleetPolicyMatches(h) {
		return FleetUpdateInstruction{}, fleetError("fleet_maintenance_changed")
	}
	if h.Phase == "queue_update" && machine == "@queue" && h.QueueBefore.Session == session || h.Phase == "worker_update" && h.Machine == machine && h.Before.Session == session {
		if machine == "@queue" {
			if q.fleetQueue == nil || !q.fleetProofFresh(q.fleetQueue.Proof) || q.fleetQueue.Proof.Session != session {
				return FleetUpdateInstruction{}, nil
			}
		} else {
			m := q.s.Machines[machine]
			if m == nil || m.Session != session || !q.fleetProofFresh(m.Fleet) {
				return FleetUpdateInstruction{}, nil
			}
		}
		if !q.fleetHostQuiet(h) || h.Queue && !h.QueueDone && len(q.calls) > 0 {
			return FleetUpdateInstruction{}, nil
		}
		return FleetUpdateInstruction{Revision: u.Revision, Operation: u.Operation, Host: host, Machine: machine, Session: session, Release: u.Release, PublisherKey: bytes.Clone(u.PublisherKey)}, nil
	}
	return FleetUpdateInstruction{}, nil
}
func (q *Queue) authorizeFleetUpdate(operation, host, machine, session string, revision int) error {
	if q.s.FleetUpgrade == nil || (revision <= 0 || revision > q.s.FleetUpgrade.Revision) {
		return fleetError("fleet_instruction_stale")
	}
	i, err := q.fleetInstruction(host, machine, session)
	if err != nil {
		return err
	}
	if i.Operation == "" || i.Operation != operation {
		return fleetError("fleet_instruction_stale")
	}
	if revision < q.fleetActiveHost().HelperRevision {
		return fleetError("fleet_instruction_stale")
	}
	return nil
}

// Runtime failures carry their receipt plus the exact current process identity.
// A prior helper revision can report a new process's cached smoke failure, but
// an obsolete process/session cannot poison a later recovery.
func (q *Queue) fleetRuntimeFailureCurrent(h *fleetUpgradeHost, ack FleetUpgradeAck) bool {
	p := ack.Runtime
	if p == nil || p.Failure != "runner_smoke_failed" || ack.Failure != "runner_smoke_failed" || p.RunnerSmoke || p.PID <= 0 || p.Session == "" || p.Version != q.s.FleetUpgrade.Release.Manifest.Version {
		return false
	}
	var goos, arch string
	if ack.Machine == "@queue" {
		if q.fleetQueue == nil || q.fleetQueue.Host != h.Host || q.fleetQueue.Session != p.Session {
			return false
		}
		goos, arch = q.fleetQueue.OS, q.fleetQueue.Arch
	} else {
		m := q.s.Machines[ack.Machine]
		if m == nil || m.Session != p.Session || h.Machine != m.ID || m.CapabilityProfile == nil {
			return false
		}
		goos, arch = m.CapabilityProfile.OS, m.CapabilityProfile.Arch
	}
	artifact, err := q.s.FleetUpgrade.Release.Manifest.Select(goos, arch)
	return err == nil && p.SHA256 == artifact.SHA256
}

func (q *Queue) AckFleetUpgrade(ack FleetUpgradeAck) error {
	return q.update(func() error {
		u := q.s.FleetUpgrade
		h := q.fleetActiveHost()
		if u == nil || h == nil || u.Operation != ack.Operation || ack.Revision <= 0 || ack.Revision > u.Revision || h.Host != ack.Host || u.State != "running" {
			return fleetError("fleet_ack_stale")
		}
		// An operator rerun permits recovery; it does not replace an active
		// helper. Only a durable replacement receipt claims the newer attempt.
		// Thus that helper's failure remains authoritative until superseded.
		runtimeFailure := ack.Phase == "failed" && q.fleetRuntimeFailureCurrent(h, ack)
		if !runtimeFailure && (ack.Revision < h.HelperRevision || ack.Phase == "failed" && ack.Revision != h.HelperRevision && ack.Revision != u.Revision) {
			return fleetError("fleet_ack_stale")
		}
		queue := ack.Machine == "@queue" && h.QueueBefore != nil && ack.Session == h.QueueBefore.Session && (h.Phase == "queue_update" || h.Phase == "queue_health")
		worker := ack.Machine == h.Machine && h.Before != nil && ack.Session == h.Before.Session && (h.Phase == "worker_update" || h.Phase == "worker_health")
		if !queue && !worker {
			return fleetError("fleet_ack_stale")
		}
		if !q.fleetPolicyMatches(h) {
			q.fleetStop(h, "maintenance_changed")
			return nil
		}
		switch ack.Phase {
		case "prepared":
			if err := q.authorizeFleetUpdate(ack.Operation, ack.Host, ack.Machine, ack.Session, ack.Revision); err != nil {
				return err
			}
			h.HelperRevision = ack.Revision
		case "failed":
			reason := "update_failed"
			if ack.Failure == "runner_smoke_failed" {
				if !q.fleetRuntimeFailureCurrent(h, ack) {
					return fleetError("fleet_ack_stale")
				}
				reason = "runner_smoke_failed"
			}
			if ack.Failure == "fleet_health_failed" {
				reason = "service_health_failed"
			}
			q.fleetStop(h, reason)
		case "installed": // Not proof of activation, and no phase regression on delayed ACK.
			h.HelperRevision = ack.Revision
		case "restarting":
			h.HelperRevision = ack.Revision
			if queue {
				h.Phase = "queue_health"
			} else {
				h.Phase = "worker_health"
			}
			h.ProgressAt = q.clock.Now()
		default:
			return errors.New("invalid fleet acknowledgement")
		}
		return nil
	})
}

// Marshal-copy the frozen operation before handing it to network code.
func cloneFleetRelease(r EnrollmentRelease) EnrollmentRelease {
	var out EnrollmentRelease
	b, _ := json.Marshal(r)
	_ = json.Unmarshal(b, &out)
	return out
}
