package myce

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
)

// ComponentExclusion is an additive admission fence, independent of worker
// health and per-device maintenance. Inactive records retain revision fences.
type ComponentExclusion struct {
	Selector  string            `json:"selector"`
	Reason    string            `json:"reason"`
	SetBy     string            `json:"set_by"`
	UpdatedAt time.Time         `json:"updated_at"`
	Revision  uint64            `json:"revision"`
	Active    bool              `json:"active"`
	Host      string            `json:"host,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Platform  string            `json:"platform,omitempty"`
	Devices   []ExclusionDevice `json:"devices,omitempty"`
}

type ExclusionDevice struct {
	Platform string `json:"platform"`
	Identity string `json:"identity"`
}

type ComponentExclusionRequest struct {
	Action     string  `json:"action"`
	Selector   string  `json:"selector"`
	Reason     string  `json:"reason,omitempty"`
	SetBy      string  `json:"set_by"`
	IfRevision *uint64 `json:"if_revision"`
}

// Selectors deliberately use a small, literal grammar rather than globs or a
// general expression language. Device names in durable selectors are identities.
func parseComponentSelector(selector string) (ComponentExclusion, []string, error) {
	p := ComponentExclusion{}
	fields := map[string]string{}
	for _, field := range strings.Split(selector, ";") {
		key, value, ok := strings.Cut(field, "=")
		if !ok || value == "" || fields[key] != "" || strings.TrimSpace(value) != value {
			return p, nil, errors.New("selector requires host=HOST, kind=gpu, platform=PLATFORM, or host=HOST;devices=ID,ID")
		}
		switch key {
		case "host", "kind", "platform", "devices":
			fields[key] = value
		default:
			return p, nil, errors.New("unknown component selector field")
		}
	}
	p.Host, p.Kind, p.Platform = fields["host"], fields["kind"], fields["platform"]
	if p.Kind != "" && p.Kind != "gpu" {
		return p, nil, errors.New("kind must be gpu (all accelerators); use host budget for CPU/RAM reservations")
	}
	if p.Platform != "" && (!validGPU(p.Platform) || p.Platform == "cpu") {
		return p, nil, errors.New("platform must be an accelerator platform; use host budget for CPU/RAM reservations")
	}
	var devices []string
	if fields["devices"] != "" {
		devices = strings.Split(fields["devices"], ",")
		if p.Host == "" || p.Kind != "" || p.Platform != "" || slices.Contains(devices, "") {
			return p, nil, errors.New("devices requires a host and cannot combine with kind or platform")
		}
	}
	if p.Kind != "" && p.Platform != "" {
		return p, nil, errors.New("choose kind or platform")
	}
	p.Selector = p.canonicalSelector(devices)
	return p, devices, nil
}

func (p ComponentExclusion) canonicalSelector(devices []string) string {
	var fields []string
	if p.Host != "" {
		fields = append(fields, "host="+p.Host)
	}
	if p.Kind != "" {
		fields = append(fields, "kind="+p.Kind)
	}
	if p.Platform != "" {
		fields = append(fields, "platform="+p.Platform)
	}
	if len(devices) > 0 {
		devices = slices.Clone(devices)
		slices.Sort(devices)
		fields = append(fields, "devices="+strings.Join(slices.Compact(devices), ","))
	}
	return strings.Join(fields, ";")
}

func (p ComponentExclusion) wholeHost() bool {
	return p.Host != "" && p.Kind == "" && p.Platform == "" && len(p.Devices) == 0
}

// Resolve once against stable inventory, retaining absent identities from
// earlier settings so operators can remove an exclusion after hardware leaves.
func (q *Queue) resolveComponentSelector(selector string) (ComponentExclusion, error) {
	p, devices, err := parseComponentSelector(selector)
	if err != nil {
		return p, err
	}
	if p.Host != "" {
		if err := q.validateMachineNames([]string{p.Host}); err != nil {
			return p, err
		}
	}
	if len(devices) == 0 {
		return p, nil
	}
	status := q.hostMaintenanceStatus(p.Host)
	for _, old := range q.s.ComponentExclusions {
		if old.Host == p.Host {
			for _, d := range old.Devices {
				status.DeviceMaintenance = append(status.DeviceMaintenance, DeviceMaintenance{Platform: d.Platform, Identity: d.Identity})
			}
		}
	}
	var identities []string
	for _, name := range devices {
		d, err := status.ResolveDevice(name)
		if err != nil {
			return p, err
		}
		ref := ExclusionDevice{Platform: d.Platform, Identity: d.Identity}
		if !slices.Contains(p.Devices, ref) {
			p.Devices = append(p.Devices, ref)
		}
		identities = append(identities, d.Identity)
	}
	p.Selector = p.canonicalSelector(identities)
	slices.SortFunc(p.Devices, func(a, b ExclusionDevice) int {
		return strings.Compare(a.Platform+":"+a.Identity, b.Platform+":"+b.Identity)
	})
	return p, nil
}

func (q *Queue) componentExclusions() []ComponentExclusion {
	out := []ComponentExclusion{}
	for _, p := range q.s.ComponentExclusions {
		p.Devices = slices.Clone(p.Devices)
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b ComponentExclusion) int { return strings.Compare(a.Selector, b.Selector) })
	return out
}

func (q *Queue) ComponentExclusions() []ComponentExclusion {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.componentExclusions()
}

func (q *Queue) ResolveComponentSelector(selector string) (ComponentExclusion, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	p, err := q.resolveComponentSelector(selector)
	if old, ok := q.s.ComponentExclusions[p.Selector]; err == nil && ok {
		old.Devices = slices.Clone(old.Devices)
		return old, nil
	}
	return p, err
}

func (q *Queue) ChangeComponentExclusion(r ComponentExclusionRequest) (ComponentExclusion, error) {
	var out ComponentExclusion
	if r.IfRevision == nil || strings.TrimSpace(r.SetBy) == "" || r.Action != "add" && r.Action != "remove" || r.Action == "remove" && r.Reason != "" {
		return out, errors.New("add/remove requires if_revision and set_by; reason applies only to add")
	}
	err := q.update(func() error {
		p, err := q.resolveComponentSelector(r.Selector)
		if err != nil {
			return err
		}
		old := q.s.ComponentExclusions[p.Selector]
		if old.Revision != *r.IfRevision {
			return ErrStale
		}
		if r.Action == "remove" && !old.Active {
			return errors.New("exclusion is not active")
		}
		p.Active, p.Revision, p.Reason, p.SetBy, p.UpdatedAt = r.Action == "add", old.Revision+1, strings.TrimSpace(r.Reason), strings.TrimSpace(r.SetBy), q.clock.Now()
		if p.wholeHost() {
			policy := q.s.HostMaintenance[p.Host]
			owner := "exclusion:" + p.Selector
			if p.Active && !old.Active {
				if q.maintenanceBlocked(p.Host) {
					return errors.New("host already drained/disabled; use existing host maintenance controls")
				}
				if err := q.changeHostMaintenance(p.Host, HostMaintenanceRequest{Action: "drain", DrainOwner: owner}); err != nil {
					return err
				}
			}
			// A later drain/disable owns its own authority. Removing this
			// selector must never resume that independently maintained host.
			if !p.Active && policy.Mode == "drained" && policy.DrainOwner == owner {
				q.s.ComponentExclusions[p.Selector] = p
				if err := q.changeHostMaintenance(p.Host, HostMaintenanceRequest{Action: "resume", IfMode: policy.Mode, IfUpdatedAt: &policy.UpdatedAt, IfDrainOwner: owner}); err != nil {
					return err
				}
			}
		}
		if q.s.ComponentExclusions == nil {
			q.s.ComponentExclusions = map[string]ComponentExclusion{}
		}
		q.s.ComponentExclusions[p.Selector] = p
		q.maintain()
		out = p
		out.Devices = slices.Clone(p.Devices)
		return nil
	})
	return out, err
}

func (q *Queue) componentExcluded(m *Machine, platform, identity string) bool {
	// The profile uses the same kind=gpu matching/admission path, independently
	// of operator exclusions; clearing either cannot clear the other.
	if q.s.HostMaintenance[hostID(m)].SharedHost != nil && platform != "cpu" {
		return true
	}
	for _, p := range q.s.ComponentExclusions {
		if p.Active && !p.wholeHost() && p.matchesComponent(hostID(m), platform, identity) {
			return true
		}
	}
	return false
}

func (p ComponentExclusion) matchesComponent(host, platform, identity string) bool {
	if p.Host != "" && p.Host != host {
		return false
	}
	return p.wholeHost() || platform != "cpu" && (p.Kind == "gpu" || p.Platform != "" && p.Platform == platform || slices.Contains(p.Devices, ExclusionDevice{Platform: platform, Identity: identity}))
}

func (a API) registerComponentExclusions(mux *requestMux) {
	mux.HandleFunc("GET /api/exclusions", func(w http.ResponseWriter, r *http.Request) {
		respondOperation(w, a.Queue.ComponentExclusions(), nil, "exclusion_invalid")
	})
	mux.HandleFunc("GET /api/exclusions/resolve", func(w http.ResponseWriter, r *http.Request) {
		p, err := a.Queue.ResolveComponentSelector(r.URL.Query().Get("selector"))
		respondOperation(w, p, err, "exclusion_invalid")
	})
	mux.HandleFunc("POST /api/exclusions", func(w http.ResponseWriter, r *http.Request) {
		var request ComponentExclusionRequest
		if !decode(w, r, &request) {
			return
		}
		p, err := a.Queue.ChangeComponentExclusion(request)
		respondOperation(w, p, err, "exclusion_invalid")
	})
}
