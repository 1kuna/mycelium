package myce

import (
	"errors"
	"net/url"
	"path/filepath"
)

// ModelCustody contains no local process details. The worker verifies process
// birth and independent service membership before reporting this exact owner.
type ModelCustody struct {
	Protocol int    `json:"protocol"`
	ID       string `json:"id"`
	// Queue-stamped coordinator identity, already public in FleetRuntimeProof.
	// A replacement must present its own exact model report before activation.
	WorkerPID int `json:"worker_pid,omitempty"`
}

func validModelCustody(v *ModelCustody) bool {
	return v != nil && v.Protocol == custodianProtocol && validCustodianID(v.ID)
}

func validModelBridgeURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "http" && u.Host != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

// Decode the previous release's keeper records for one rolling upgrade.
type modelCustodianRequest struct {
	Protocol  int                 `json:"protocol"`
	ID        string              `json:"id"`
	Execution HostEngineExecution `json:"execution"`
	Original  struct {
		Name string `json:"name"`
	} `json:"original"`
}

type modelCustodianSnapshot struct {
	custodianSnapshot
	Execution HostEngineExecution `json:"execution"`
	Report    ModelReport         `json:"report"`
}

func modelRequests(dir string) ([]modelCustodianRequest, error) {
	paths, err := filepath.Glob(filepath.Join(custodianDir(dir), "custodian-*.model-request.json"))
	if err != nil {
		return nil, err
	}
	var requests []modelCustodianRequest
	for _, path := range paths {
		var r modelCustodianRequest
		if err := readCustodianJSON(filepath.Dir(path), filepath.Base(path), &r); err != nil {
			return nil, err
		}
		if r.Protocol != custodianProtocol || !validCustodianID(r.ID) || filepath.Base(path) != custodianName(r.ID, "model-request") || r.Execution.Session == "" || r.Execution.Attempt == "" || r.Execution.Model != r.Original.Name {
			return nil, errors.New("invalid detached model record")
		}
		var v modelCustodianSnapshot
		if readCustodianJSON(filepath.Dir(path), custodianName(r.ID, "snapshot"), &v) == nil && v.Protocol == custodianProtocol && v.ID == r.ID && v.Execution == r.Execution && v.Report.Name == r.Execution.Model && v.Report.Device == r.Execution.Device && v.Report.Attempt == r.Execution.Attempt && v.Finished && (v.Report.State == "failed" || v.Report.State == "unloaded") {
			continue
		}
		requests = append(requests, r)
	}
	return requests, nil
}

func (q *Queue) modelAdoptionReason(m *Machine, p HostObligation) string {
	e := p.Engine
	if p.Call != nil {
		e = &p.Call.HostEngineExecution
	}
	if e == nil || e.Machine != m.ID || e.Session != m.Session {
		return "an older or different worker session retains model custody"
	}
	s := q.s.ModelSlots[slotKey(e.Machine, e.Model, e.Device)]
	if s == nil || s.Session != e.Session || s.Attempt != e.Attempt || !validModelCustody(s.Custody) || !validModelBridgeURL(s.URL) {
		return "model engine or response bridge has no detached custody"
	}
	if m.Fleet == nil || m.Fleet.PID == 0 || s.Custody.WorkerPID != m.Fleet.PID {
		return "current worker has not proved adoption of the detached model"
	}
	if s.State != "ready" && s.State != "draining" {
		return "detached model readiness or release is unconfirmed"
	}
	if p.Call != nil && q.obligationCall(p.Call) == nil {
		return "model call no longer belongs to the retained engine"
	}
	return ""
}
