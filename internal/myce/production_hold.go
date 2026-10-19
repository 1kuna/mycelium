package myce

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Policies are keyed by physical host_id (worker name when host_id is unset).
type HostPolicy struct {
	ProductionHold bool `json:"production_hold" yaml:"production_hold"`
}
type HostState struct {
	Policy HostPolicy `json:"policy"`
	Active bool       `json:"active,omitempty"`
}

// Release obligations survive requeue/loss; only an execution acknowledgement or
// a fresh worker's completed cleanup can settle them.
type HostRelease struct {
	CleanupSince                *time.Time `json:"cleanup_since,omitempty"`
	EscalatedAt                 *time.Time `json:"escalated_at,omitempty"`
	HostChargeGB                float64    `json:"host_charge_gb,omitempty"`
	Docker                      bool       `json:"docker,omitempty"`
	CleanupFailed               bool       `json:"cleanup_failed,omitempty"`
	Device                      string     `json:"device,omitempty"`
	MemoryGB, HostMemoryGB      float64
	Preparation                 bool `json:"preparation,omitempty"`
	Host, Job, Machine, Session string
	Attempt                     int
	Until                       time.Time
}

func ReadHostPolicies(path string) (map[string]HostPolicy, error) {
	if path == "" {
		return nil, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	var v struct {
		Hosts map[string]HostPolicy `yaml:"hosts"`
	}
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if e = d.Decode(&v); e != nil {
		return nil, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("expected one host policy document")
	}
	return v.Hosts, nil
}
func (q *Queue) ConfigureHosts(p map[string]HostPolicy) error {
	normalized := map[string]HostPolicy{}
	for host, v := range p {
		if strings.TrimSpace(host) == "" {
			return errors.New("host name is required")
		}
		normalized[host] = v
	}
	return q.update(func() error {
		if len(q.s.Hosts) > 0 {
			saved := map[string]HostPolicy{}
			for h, s := range q.s.Hosts {
				saved[h] = s.Policy
			}
			if !reflect.DeepEqual(saved, normalized) {
				return errors.New("host configuration differs from saved policy; refusing to relax or replace it")
			}
			return nil
		}
		q.s.Hosts = map[string]*HostState{}
		for h, v := range normalized {
			q.s.Hosts[h] = &HostState{Policy: v}
		}
		return nil
	})
}
func (q *Queue) held(host string) bool {
	s := q.s.Hosts[host]
	return s != nil && s.Policy.ProductionHold && s.Active
}
func releaseKey(r HostRelease) string {
	return fmt.Sprintf("%s/%s/%s/%d", r.Job, r.Machine, r.Session, r.Attempt)
}
func (q *Queue) retainRelease(j *Job) {
	if j.ReleaseConfirmed {
		return
	}
	m := q.s.Machines[j.Machine]
	if m == nil {
		return
	}
	policy := q.s.Hosts[hostID(m)]
	if j.Spec.StartBy == nil && !j.Spec.Run.Docker && m.HostMemoryBudgetGB == 0 && m.hostBudgetGB == 0 && q.machineJobLimit(m) == 0 && (policy == nil || !policy.Policy.ProductionHold) {
		return
	}
	if q.s.HostReleases == nil {
		q.s.HostReleases = map[string]HostRelease{}
	}
	r := HostRelease{HostChargeGB: hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), Docker: j.Spec.Run.Docker, Device: m.Device, MemoryGB: j.Spec.Needs.MemoryGB, HostMemoryGB: j.Spec.Needs.HostMemoryGB, Host: hostID(m), Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt}
	if j.StopAt != nil {
		r.Until = *j.StopAt
	}
	old := q.s.HostReleases[releaseKey(r)]
	r.CleanupFailed = old.CleanupFailed
	r.CleanupSince = old.CleanupSince
	r.EscalatedAt = old.EscalatedAt
	// A new stop resets an un-escalated boot/create timer (#74), but must
	// preserve an existing escalation until the release resolves.
	if r.Docker && (q.dockerReleaseLive(r) || old.EscalatedAt == nil && old.Until.IsZero() && !r.Until.IsZero()) {
		r.CleanupSince, r.EscalatedAt = nil, nil
	}
	q.s.HostReleases[releaseKey(r)] = r
}
func (q *Queue) settleRelease(r Result) bool {
	q.confirmCopyRelease(r)
	key := releaseKey(HostRelease{Job: r.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt})
	_, ok := q.s.HostReleases[key]
	delete(q.s.HostReleases, key)
	return ok
}
func (q *Queue) reconcileHostReleases(c Checkin) {
	q.confirmPreviousCopies(c)
	for key, r := range q.s.HostReleases {
		if r.Docker || r.Machine != c.Machine.ID {
			continue
		}
		old := q.s.Machines[c.Machine.ID]
		fresh := c.Fresh && old != nil && c.PreviousSession == old.Session && c.Machine.Session != old.Session && r.Session == c.PreviousSession
		stopped := r.Session == c.Machine.Session && c.Running == nil && !r.Until.IsZero() && !r.Preparation && !r.CleanupFailed
		if r.Preparation {
			stopped = r.Session == c.Machine.Session && !q.backgroundPreparing(&c.Machine)
		}
		if fresh || stopped {
			q.confirmCopyRelease(Result{Execution: Execution{ID: r.Job, Attempt: r.Attempt}, Machine: r.Machine, Session: r.Session})
			delete(q.s.HostReleases, key)
		}
	}
}

// Placement policy is shared by batch, model, preparation and projections.
func (q *Queue) placementAllowed(j *Job, m *Machine) bool {
	return q.placementAllowedForUse(j, m, false)
}

func (q *Queue) placementAllowedForUse(j *Job, m *Machine, resident bool) bool {
	if cancelled(j) || !dockerRuntimeFits(j, m) || !logPolicyFits(j.Spec.Logs, m) {
		return false
	}
	if model, ok := q.model(j.Spec.Needs.Model); ok {
		if resident {
			model = q.residentModel(model, m)
		}
		if !logPolicyFits(model.Logs, m) || !modelFenceAllows(model, m) || !q.projectAllows(model.Project, m.ID) {
			return false
		}
	}

	if hostID(m) == q.localHostID && q.localHostErr != nil {
		return false
	}
	if availabilityBlocked(m) || (q.machineLimitReason(m) != "" && !(j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping"))) || q.startupReleaseBlocker(hostID(m)) != "" || q.dockerBlocker(hostID(m)) != "" || m.Disabled != "" || q.hostClosedReason(hostID(m)) != "" || !q.jobAllowsMachine(j, m.ID) {
		return false
	}
	if j.ProductionHost != "" && j.ProductionHost != hostID(m) {
		return false
	}
	if !q.held(hostID(m)) {
		return true
	}
	if j.Spec.Priority != "production" {
		return false
	}
	return q.hostBlockers(hostID(m)) == ""
}
func (q *Queue) hostBlockers(host string) string {
	text, _ := q.hostBlockerState(host)
	return text
}

// Unknown release gets attention only after ordinary kill/report latency.
const releaseAttentionDelay = 2 * time.Minute

type hostBlockerClass struct {
	Kind                   string
	OperatorActionRequired bool
}

func (q *Queue) releaseNeedsOperator(m *Machine, until *time.Time, failed bool) bool {
	if m == nil || m.Gone || failed {
		return true
	}
	now := q.clock.Now()
	if q.unreported[m.ID] && now.Before(q.restartUntil) {
		return false
	}
	return until != nil && !until.IsZero() && !now.Before(until.Add(releaseAttentionDelay))
}

func (q *Queue) hostBlockerState(host string) (string, hostBlockerClass) {
	count := 0
	remaining := 0.0
	unknown := []string{}
	needsOperator := false
	for _, r := range q.s.HostReleases {
		if r.Host != host {
			continue
		}
		// Live production is demand, not a blocker for another production job.
		j := q.s.Jobs[r.Job]
		if j != nil && j.Spec.Priority == "production" && j.Machine == r.Machine && j.Attempt == r.Attempt && (j.State == "running" || j.State == "stopping") && !q.unreported[r.Machine] && q.s.Machines[r.Machine] != nil && !q.s.Machines[r.Machine].Gone {
			continue
		}
		count++
		remaining = max(remaining, r.Until.Sub(q.clock.Now()).Seconds())
		if m := q.s.Machines[r.Machine]; m == nil || m.Gone || q.unreported[r.Machine] || r.CleanupFailed || r.Until.IsZero() || !q.clock.Now().Before(r.Until) {
			unknown = append(unknown, r.Machine)
			needsOperator = needsOperator || q.releaseNeedsOperator(m, &r.Until, r.CleanupFailed)
		}
	}
	for _, c := range q.calls {
		m := q.s.Machines[c.Machine]
		if m != nil && hostID(m) == host && c.Priority != "production" {
			count++
			if c.StopAt != nil {
				remaining = max(remaining, c.StopAt.Sub(q.clock.Now()).Seconds())
				if expired(c.StopAt, q.clock.Now()) {
					unknown = append(unknown, m.ID)
					needsOperator = needsOperator || q.releaseNeedsOperator(m, c.StopAt, false)
				}
			}
		}
	}
	for _, s := range q.s.ModelSlots {
		m := q.s.Machines[s.Machine]
		if m != nil && hostID(m) == host && (s.ReleasingCalls || s.Recovering || m.Gone || q.unreported[m.ID] || !q.slotPinned(s) && s.State == "draining" || s.State == "unloading" || s.State == "unload_failed") {
			count++
			if s.ReleasingCalls || s.Recovering || m.Gone || q.unreported[m.ID] || s.State == "unload_failed" || expired(s.StopAt, q.clock.Now()) {
				unknown = append(unknown, m.ID)
				needsOperator = needsOperator || q.releaseNeedsOperator(m, s.StopAt, s.State == "unload_failed")
			}
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return "release unknown: worker " + strings.Join(unknown, ", "), hostBlockerClass{Kind: "production_release_unknown", OperatorActionRequired: needsOperator}
	}
	if count > 0 {
		return fmt.Sprintf("stopping %d attempts; grace remaining %.0fs", count, remaining), hostBlockerClass{Kind: "production_stopping"}
	}
	return "", hostBlockerClass{}
}
func (q *Queue) holdReason(host string) string { return q.productionHoldDetail(host).Text }
func (q *Queue) productionHoldDetail(host string) HoldDetail {
	detail := HoldDetail{Host: host, Kind: "production_hold", Text: host + ": production hold"}
	if b, class := q.hostBlockerState(host); b != "" {
		return productionBlockerDetail(host, b, class)
	}
	return detail
}

// Classification is explicit: new blocker paths cannot silently become routine.
func productionBlockerDetail(host, text string, class hostBlockerClass) HoldDetail {
	detail := HoldDetail{Host: host, Kind: "unclassified", OperatorActionRequired: true, Text: host + ": production hold; " + text}
	switch class.Kind {
	case "production_stopping", "production_release_unknown":
		detail.Kind = class.Kind
		detail.OperatorActionRequired = class.OperatorActionRequired
	}
	return detail
}

func (q *Queue) holdWaitReason(j *Job) string {
	for _, m := range q.s.Machines {
		if q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			if b := q.dockerBlocker(hostID(m)); b != "" {
				return b
			}
		}
	}
	if j.ProductionHost != "" && q.held(j.ProductionHost) {
		return q.holdReason(j.ProductionHost)
	}
	if j.Spec.Priority == "production" && j.ProductionHost == "" {
		for _, m := range q.s.Machines {
			h := q.s.Hosts[hostID(m)]
			if h == nil || !h.Policy.ProductionHold || !q.jobAllowsMachine(j, m.ID) || !fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
				continue
			}
			for _, slot := range q.s.ModelSlots {
				if slot.Machine == m.ID && q.slotPinned(slot) && slot.Model != j.Spec.Needs.Model {
					model, _ := q.model(slot.Model)
					if model.MemoryGB+j.Spec.Needs.MemoryGB > m.TotalMemoryGB {
						return "pinned model " + slot.Model + " on " + m.ID + " prevents production placement"
					}
				}
			}
		}
	}
	if j.Spec.Priority != "production" {
		for _, m := range q.s.Machines {
			if q.held(hostID(m)) && q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
				return "host " + hostID(m) + " reserved for production"
			}
		}
	}
	return ""
}

// Select once, before stopping anything. Occupancy is reclaimable; immutable
// capacity, environments, project/job restrictions and pins are not.
func (q *Queue) productionTarget(j *Job, call *ModelCall) string {
	machines := []*Machine{}
	for _, m := range q.s.Machines {
		machines = append(machines, m)
	}
	sort.Slice(machines, func(a, b int) bool { return machines[a].ID < machines[b].ID })
	var best *Machine
	bestStops := int(^uint(0) >> 1)
	for _, m := range machines {
		if q.dockerBlocker(hostID(m)) != "" || q.unreported[m.ID] || !q.jobAllowsMachine(j, m.ID) || !fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			continue
		}
		model, hasModel := q.model(j.Spec.Needs.Model)
		if hasModel && call != nil {
			model = q.residentModel(model, m)
		}
		if hasModel && !q.modelFitsMachine(model, m) {
			continue
		}
		if call != nil && (m.ModelURL == "" || m.ID == call.Exclude) {
			continue
		}
		pinned := 0.0
		for _, s := range q.s.ModelSlots {
			if s.Machine == m.ID && (q.slotPinned(s) || s.State == "unload_failed") {
				model, _ := q.model(s.Model)
				if call == nil || s.Model != call.Model {
					pinned += model.MemoryGB
				}
			}
		}
		if m.TotalMemoryGB-pinned < j.Spec.Needs.MemoryGB {
			continue
		}
		slot := q.s.ModelSlots[slotKey(m.ID, j.Spec.Needs.Model)]
		// Calls reuse a ready/loading model's existing host-memory reservation.
		if call == nil || slot == nil || slot.State != "ready" && slot.State != "loading" {
			if _, _, ok := q.hostReleases(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB, "production"); !ok {
				continue
			}
		}
		stops := 0
		for _, r := range q.s.Jobs {
			if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") {
				stops++
			}
		}
		// A first program still chooses the most capable free hypha before the
		// hold binds it to a host. Existing minimal-preemption ordering remains.
		preferFreeProgram := best != nil && stops == 0 && bestStops == 0 && j.Spec.extensionJob() && fits(j, m, m.FreeMemoryGB) && (!fits(j, best, best.FreeMemoryGB) || q.capabilityPlacementBetter(j, m, best))
		if best == nil || stops < bestStops || preferFreeProgram {
			best = m
			bestStops = stops
		}
	}
	if best == nil {
		return ""
	}
	return hostID(best)
}
func (q *Queue) maintainHolds() {
	enabled := false
	for _, h := range q.s.Hosts {
		enabled = enabled || h.Policy.ProductionHold
	}
	if !enabled {
		return
	}
	now := q.clock.Now()
	demand := map[string]bool{}
	jobs := []*Job{}
	for _, j := range q.s.Jobs {
		if j.Spec.Priority == "production" && !terminal(j.State) && !cancelled(j) && !(j.State == "queued" && expired(j.Spec.GiveUpAt, now)) {
			jobs = append(jobs, j)
		}
	}
	sort.Slice(jobs, func(a, b int) bool { return before(jobs[a], jobs[b], &Machine{}) })
	for _, j := range jobs {
		if j.State == "queued" && j.ProductionHost != "" && q.dockerBlocker(j.ProductionHost) != "" {
			if target := q.productionTarget(j, nil); target != "" {
				j.ProductionHost = target
			}
		}
		if j.ProductionHost == "" {
			if m := q.s.Machines[j.Machine]; m != nil {
				if h := q.s.Hosts[hostID(m)]; h != nil && h.Policy.ProductionHold {
					j.ProductionHost = hostID(m)
				}
			} else {
				target := q.productionTarget(j, nil)
				if h := q.s.Hosts[target]; h != nil && h.Policy.ProductionHold {
					j.ProductionHost = target
				}
			}
		}
		demand[j.ProductionHost] = true
	}
	for _, c := range q.calls {
		if c.Priority == "production" && c.Error == "" {
			if c.ProductionHost == "" {
				j := callJob(c)
				model, _ := q.model(c.Model)
				j.Spec.Needs.MemoryGB = model.MemoryGB
				j.Spec.Needs.HostMemoryGB = model.HostMemoryGB
				j.Spec.Needs.Platform = Platform{Required: model.Platforms}
				target := q.productionTarget(j, c)
				if h := q.s.Hosts[target]; h != nil && h.Policy.ProductionHold {
					c.ProductionHost = target
				}
			}
			demand[c.ProductionHost] = true
		}
	}
	for host, h := range q.s.Hosts {
		if !h.Policy.ProductionHold {
			continue
		}
		if demand[host] {
			h.Active = true
		}
		if !h.Active {
			continue
		}
		for _, m := range q.s.Machines {
			if hostID(m) == host && q.backgroundPreparing(m) {
				if q.s.HostReleases == nil {
					q.s.HostReleases = map[string]HostRelease{}
				}
				r := HostRelease{Host: host, Job: "preparation", Machine: m.ID, Session: m.Session, Preparation: true, Until: now.Add(q.productionGrace)}
				key := releaseKey(r)
				if _, exists := q.s.HostReleases[key]; !exists {
					q.s.HostReleases[key] = r
				}
			}
		}
		for _, j := range q.s.Jobs {
			m := q.s.Machines[j.Machine]
			if m != nil && hostID(m) == host && (j.State == "running" || j.State == "stopping") {
				if j.Spec.Priority != "production" {
					q.stopFor(j, "production", "host hold")
				}
				q.retainRelease(j)
			}
		}
		for _, c := range q.calls {
			m := q.s.Machines[c.Machine]
			if m != nil && hostID(m) == host && c.Priority != "production" && c.Error == "" {
				if slot := q.s.ModelSlots[slotKey(c.Machine, c.Model)]; slot != nil {
					slot.ReleasingCalls = true
				}
				if c.StopAt == nil {
					t := now.Add(q.productionGrace)
					c.StopAt = &t
				}
				if expired(c.StopAt, now) {
					c.Error = "preempted by production host hold"
				}
			}
		}
		for _, s := range q.s.ModelSlots {
			m := q.s.Machines[s.Machine]
			if m == nil || hostID(m) != host || q.slotPinned(s) || s.State == "failed" {
				continue
			}
			if s.State != "unload_failed" && s.State != "unloading" && (rank(s.Priority) > 0 && (!s.ProductionUsed || s.InFlight > 0) || q.unreported[m.ID]) {
				s.State = "draining"
			}
			if s.State == "draining" {
				if s.StopAt == nil {
					t := now.Add(q.productionGrace)
					s.StopAt = &t
				}
				if expired(s.StopAt, now) {
					for _, c := range q.calls {
						if c.Machine == s.Machine && c.Model == s.Model && c.Priority != "production" {
							c.Error = "preempted by production host hold"
						}
					}
				}
			}
		}
		unsettled := false
		for _, r := range q.s.HostReleases {
			if r.Host == host {
				unsettled = true
			}
		}
		for _, s := range q.s.ModelSlots {
			m := q.s.Machines[s.Machine]
			if m != nil && hostID(m) == host && (s.ReleasingCalls || s.Recovering || m.Gone || q.unreported[m.ID] || s.State == "loading" || s.State == "draining" || s.State == "unloading" || s.State == "unload_failed" || s.InFlight > 0) {
				unsettled = true
			}
		}
		if demand[host] || unsettled {
			continue
		}
		// Ready idle residency remains available under the engine's own idle
		// timer. Only actual demand or unresolved releases keep takeover.
		h.Active = false
	}
}

func (q *Queue) backgroundPreparing(m *Machine) bool {
	for _, r := range m.EnvironmentReports {
		if r.State == "building" {
			production := false
			for _, j := range q.s.Jobs {
				if j.State == "queued" && j.Spec.Priority == "production" && j.ProductionHost == hostID(m) && q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) && environmentKey(j, m) == r.Key {
					production = true
					break
				}
			}
			if !production {
				return true
			}
		}
	}
	for _, r := range m.CopyReports {
		if r.State == "copying" {
			s := q.s.ModelSlots[slotKey(m.ID, r.Model)]
			if s == nil || s.Priority != "production" {
				return true
			}
		}
	}
	return false
}

// Lost/stopping Docker executions fence the physical host, even without host policy.
func (q *Queue) dockerBlocker(host string) string { return q.dockerHoldDetail(host).Text }

func (q *Queue) dockerHoldDetail(host string) HoldDetail {
	best := HoldDetail{}
	severity := 0
	choose := func(detail HoldDetail, rank int) {
		if rank > severity || rank == severity && detail.Text < best.Text {
			best, severity = detail, rank
		}
	}
	for _, m := range q.s.Machines {
		if hostID(m) != host || m.DockerCleanup == "" {
			continue
		}
		kind, rank, action := "unclassified", 4, true
		if strings.HasPrefix(m.DockerCleanup, "cleanup unconfirmed:") {
			kind, rank, action = "docker_cleanup_waiting", 3, false
		}
		switch m.DockerCleanup {
		case "creating", "stopping", "released", "create pending", "start pending", "wait pending", "inspect pending", "stop pending", "kill pending", "logs pending", "remove pending", "network-create pending", "network-remove pending":
			kind, rank, action = "docker_transition", 1, false
		}
		choose(HoldDetail{Host: host, Kind: kind, OperatorActionRequired: action, Text: host + ": Docker cleanup unconfirmed; " + m.DockerCleanup}, rank)
	}
	for _, r := range q.s.HostReleases {
		if !r.Docker || r.Host != host || q.dockerReleaseLive(r) {
			continue
		}
		detail := HoldDetail{Host: host, Kind: "docker_cleanup_waiting", Text: host + ": Docker cleanup unconfirmed; waiting for attempt " + r.Job + fmt.Sprintf("/%d", r.Attempt)}
		rank := 2
		if r.EscalatedAt != nil {
			detail.Kind = "docker_cleanup_operator_action"
			detail.OperatorActionRequired = true
			detail.Text += "; operator action required (myce resolve-docker)"
			rank = 5
		}
		choose(detail, rank)
	}
	return best
}

// Escalation bounds silent waiting, never custody or physical memory ownership.
func (q *Queue) escalateDockerCleanup() {
	now := q.clock.Now()
	for key, r := range q.s.HostReleases {
		if !r.Docker {
			continue
		}
		if q.dockerReleaseLive(r) {
			r.CleanupSince, r.EscalatedAt = nil, nil
			q.s.HostReleases[key] = r
			continue
		}
		if r.CleanupSince == nil || !r.Until.IsZero() && r.CleanupSince.Before(r.Until) {
			since := now
			// Stopped attempts retain ordinary grace before the cleanup bound.
			if !r.Until.IsZero() {
				since = r.Until
			}
			r.CleanupSince = &since
		}
		if r.EscalatedAt == nil && !now.Before(r.CleanupSince.Add(2*time.Minute)) {
			r.EscalatedAt = &now
			log.Printf("event=docker_cleanup_escalated host=%q job=%q attempt=%d machine=%q session=%q operator_action_required=true", r.Host, r.Job, r.Attempt, r.Machine, r.Session)
		}
		q.s.HostReleases[key] = r
	}
}

func (q *Queue) dockerReleaseLive(r HostRelease) bool {
	j := q.s.Jobs[r.Job]
	m := q.s.Machines[r.Machine]
	return j != nil && j.Attempt == r.Attempt && j.Session == r.Session && j.Machine == r.Machine && j.State == "running" && j.StopAt == nil && m != nil && m.Session == r.Session && !m.Gone && !q.unreported[m.ID] && dockerStartingOrRunning(m.DockerCleanup) && !r.CleanupFailed
}

// Only healthy startup/running reports keep the exact current execution live.
// Stopping, errors, replacement sessions and lost workers retain custody.
func dockerStartingOrRunning(status string) bool {
	switch status {
	case "", "creating", "create pending", "network-create pending", "start pending":
		return true
	default:
		return false
	}
}
