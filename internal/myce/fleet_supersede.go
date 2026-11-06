package myce

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

// FleetSupersession is the durable audit record of a retired fleet operation.
// It also retains critical stop evidence when explicit fix-forward replaces it.
type FleetSupersession struct {
	Urgency            string            `json:"urgency"`
	Operation          string            `json:"operation"`
	Release            string            `json:"release"`
	State              string            `json:"state"`
	QueueOnly          bool              `json:"queue_only"`
	WaitProductionIdle bool              `json:"wait_production_idle"`
	Reason             string            `json:"reason"`
	SupersededAt       time.Time         `json:"superseded_at"`
	Hosts              []FleetHostStatus `json:"hosts"`
	RetainedDrains     []string          `json:"retained_drains,omitempty"`
	Next               string            `json:"next"`
}

type FleetSupersedeRequest struct {
	Operation string `json:"operation"`
	Reason    string `json:"reason"`
}

func (q *Queue) fleetSupersession(u *fleetUpgrade, reason string) FleetSupersession {
	record := FleetSupersession{Urgency: fleetUrgency(u.Urgency), Operation: u.Operation, Release: u.Release.Manifest.Version, State: u.State, QueueOnly: u.QueueOnly, WaitProductionIdle: u.WaitProductionIdle, Reason: reason, SupersededAt: q.clock.Now(), Hosts: []FleetHostStatus{}}
	for _, h := range u.Hosts {
		record.Hosts = append(record.Hosts, h.FleetHostStatus)
		if p := q.s.HostMaintenance[h.Host]; p.Mode == "drained" && p.DrainOwner == u.Operation {
			record.RetainedDrains = append(record.RetainedDrains, h.Host)
		}
	}
	return record
}

func fleetAge(d time.Duration) string {
	d = d.Round(time.Minute)
	if d < time.Minute {
		return "under 1m"
	}
	if d >= 24*time.Hour {
		return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
	}
	return strings.TrimSuffix(d.String(), "0s")
}

func fleetFlags(queueOnly, waitIdle bool, urgency string) string {
	var flags []string
	if fleetUrgency(urgency) == "critical" {
		flags = append(flags, "--urgency critical")
	}
	if queueOnly {
		flags = append(flags, "--queue-only")
	}
	if waitIdle {
		flags = append(flags, "--wait-production-idle")
	}
	if len(flags) == 0 {
		return "no scope flags"
	}
	return strings.Join(flags, " ")
}

// fleetOperationRefusal names the unfinished operation blocking a new request,
// its age and failure, every missing runtime proof, and the supported next step.
func (q *Queue) fleetOperationRefusal(code string, request FleetUpgradeRequest) error {
	u := q.s.FleetUpgrade
	at, label, failure := u.CreatedAt, "started", ""
	var last time.Time
	for _, h := range u.Hosts {
		if h.ProgressAt.After(last) {
			last = h.ProgressAt
		}
		if h.Failure != "" {
			failure = fmt.Sprintf(", host %s failed: %s", h.Host, h.Failure)
		}
	}
	if at.IsZero() {
		at, label = last, "last progress"
	}
	age := "age unknown"
	if !at.IsZero() {
		age = label + " " + fleetAge(q.clock.Now().Sub(at)) + " ago"
	}
	msg := fmt.Sprintf("fleet operation %s is %s (release %s, %s, %s%s)", u.Operation, u.State, u.Release.Manifest.Version, fleetFlags(u.QueueOnly, u.WaitProductionIdle, u.Urgency), age, failure)
	if u.State == "stopped" {
		msg += fmt.Sprintf("; to resume or fix forward rerun myce fleet upgrade with %s (and --release NEWER to fix forward); to retire it run myce fleet supersede --operation %s --reason TEXT", fleetFlags(u.QueueOnly, u.WaitProductionIdle, u.Urgency), u.Operation)
	} else {
		msg += "; wait for it to finish (myce status), then rerun"
	}
	if gaps := q.fleetProofGaps(request); len(gaps) > 0 {
		msg += "; also blocking: " + strings.Join(gaps, "; ")
	}
	return &HTTPError{Status: 409, Code: code, Message: msg}
}

func (q *Queue) fleetProofRefusal(code string, request FleetUpgradeRequest) error {
	msg := strings.Join(q.fleetProofGaps(request), "; ")
	msg += "; install a fleet-capable release as a service on each named host (docs/operations.md#rolling-fleet-upgrades); a healthy service presents its proof on its next poll"
	return &HTTPError{Status: 409, Code: code, Message: msg}
}

// fleetProofGaps lists every service whose runtime proof refuses a new operation.
func (q *Queue) fleetProofGaps(request FleetUpgradeRequest) []string {
	var gaps []string
	if q.fleetQueue == nil {
		gaps = append(gaps, "queue: no fleet runtime proof")
	} else if why := q.fleetProofGap(q.fleetQueue.Proof); why != "" {
		gaps = append(gaps, "queue on host "+q.fleetQueue.Host+": "+why)
	}
	if request.QueueOnly {
		return gaps
	}
	var workers []string
	for _, m := range q.s.Machines {
		if m.Replacement != nil || q.migrationHistoricalMachine(m) {
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
	return append(gaps, workers...)
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
		return "fleet runtime proof stale (observed " + fleetAge(q.clock.Now().Sub(p.ObservedAt)) + " ago)"
	}
	return ""
}

// SupersedeFleetUpgrade retires a stopped operation with an audit record so a
// new one can start. It never resumes a drain: hosts the operation drained stay
// drained, named in the record, until the operator resumes each one.
func (q *Queue) SupersedeFleetUpgrade(request FleetSupersedeRequest, requestContexts ...context.Context) (*FleetSupersession, error) {
	reason := strings.TrimSpace(request.Reason)
	if reason == "" || len(reason) > 500 {
		return nil, &HTTPError{Status: 400, Code: "fleet_supersede_reason_required", Message: "--reason must explain the supersession in 1 to 500 characters"}
	}
	var out *FleetSupersession
	err := q.updateRequest(requestContext(requestContexts), func() error {
		u := q.s.FleetUpgrade
		if u == nil || u.Operation != request.Operation {
			current := "none"
			if u != nil {
				current = u.Operation + " (" + u.State + ")"
			}
			return &HTTPError{Status: 409, Code: "fleet_operation_mismatch", Message: fmt.Sprintf("operation %q is not the current fleet operation; current: %s", request.Operation, current)}
		}
		if u.State != "stopped" {
			return &HTTPError{Status: 409, Code: "fleet_operation_not_stopped", Message: fmt.Sprintf("operation %s is %s; only a stopped operation can be superseded", u.Operation, u.State)}
		}
		record := q.fleetSupersession(u, reason)
		record.Next = "run myce fleet upgrade to start a new operation"
		if len(record.RetainedDrains) > 0 {
			record.Next += "; retained drains stay in place: resume each host with myce host resume HOST once it is healthy"
		}
		q.s.FleetSuperseded = append(q.s.FleetSuperseded, record)
		q.s.FleetUpgrade = nil
		out = &record
		return nil
	}, nil)
	if err != nil {
		return nil, err
	}
	log.Printf("fleet operation %s superseded by operator; retained drains=%d", out.Operation, len(out.RetainedDrains))
	return out, nil
}
