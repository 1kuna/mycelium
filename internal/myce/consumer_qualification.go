package myce

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const consumerCheckPrefix = "consumer-"
const consumerCheckWorkdir = "{myce-check-work}"
const consumerCheckCapability = "consumer_native_check_v1"

// ConsumerQualification requests one native baseline on an exact worker target.
type ConsumerQualification struct {
	Project   string `json:"project"`
	Extension string `json:"extension"`
	Digest    string `json:"digest"`
	Worker    string `json:"worker"`
	Platform  string `json:"platform"`
	Device    string `json:"device,omitempty"`
	Work      string `json:"work,omitempty"`
	Recheck   bool   `json:"recheck,omitempty"`
}

// QualificationVerdict remains readable after the worker leaves. A verdict
// requires accepted terminal evidence and exact execution release.
type QualificationVerdict struct {
	CheckID    string    `json:"check_id"`
	JobID      string    `json:"job_id"`
	Extension  string    `json:"extension"`
	Digest     string    `json:"digest"`
	Worker     string    `json:"worker"`
	Platform   string    `json:"platform"`
	Device     string    `json:"device,omitempty"`
	Work       string    `json:"work"`
	State      string    `json:"state"`
	Verdict    string    `json:"verdict"`
	Attempt    int       `json:"attempt"`
	FinishedAt time.Time `json:"finished_at,omitzero"`
	Reused     bool      `json:"reused,omitempty"`
}

func consumerExtensionCheck(s Spec) bool {
	return s.ExtensionCheck != nil && s.ExtensionCheck.Consumer
}

func qualificationVerdict(j *Job) QualificationVerdict {
	s := j.Spec
	v := QualificationVerdict{
		CheckID: s.SubmissionKey, JobID: j.ID, Extension: s.Extension, Digest: s.ExtensionDigest,
		Worker: s.AllowedMachines[0], Work: s.ExtensionCheck.Work, State: j.State, Attempt: j.Attempt,
		Verdict: "pending",
	}
	if consumerExtensionCheck(s) {
		v.CheckID = consumerCheckPrefix + j.ID
	}
	if r := s.ExtensionCheck.Automatic; r != nil {
		v.Platform, v.Device = r.Platform, r.Device
	} else if len(s.Needs.Platform.Required) == 1 {
		v.Platform = s.Needs.Platform.Required[0]
		if v.Platform != "cpu" {
			if len(j.AllocatedDevices) == 1 {
				v.Device = j.AllocatedDevices[0]
			} else if s.Needs.GPU != nil && len(s.Needs.GPU.Devices) == 1 {
				v.Device = s.Needs.GPU.Devices[0]
			}
		}
	}
	if terminal(j.State) && (j.Attempt == 0 || j.ReleaseConfirmed) {
		if j.FinishedAt != nil {
			v.FinishedAt = j.FinishedAt.UTC()
		}
		// The owner CLI seals the key during cleanup even after a pass.
		// Post-terminal cancellation retains the already committed verdict.
		switch {
		case j.State == "succeeded" && j.Attempt != 0 && j.Machine == v.Worker:
			v.Verdict = "passed"
		case j.State == "failed" && j.Attempt != 0 && !automaticCheckNoVerdict(j):
			v.Verdict = "failed"
		default:
			v.Verdict = "no_verdict"
		}
	}
	return v
}

func qualificationRequestError(status int, code, message string) error {
	return &HTTPError{Status: status, Code: code, Message: message}
}

func (q *Queue) requestQualification(ctx context.Context, request ConsumerQualification) (QualificationVerdict, error) {
	unlock := requestReadLock(ctx, &q.mu)
	spec, existing, err := q.qualificationSubmission(request)
	unlock()
	if err != nil || existing.JobID != "" {
		return existing, err
	}
	j, err := q.submitContext(ctx, spec, nil, true)
	if err != nil {
		return QualificationVerdict{}, err
	}
	if !consumerExtensionCheck(j.Spec) {
		return QualificationVerdict{}, qualificationRequestError(409, "qualification_cancelled", "check key was cancelled before submission")
	}
	v := qualificationVerdict(j)
	v.Reused = j.SubmissionReused
	return v, nil
}

// Choose the frozen payload and its replay key from one committed view.
// Submission rechecks target identity and policy in the write transaction.
func (q *Queue) qualificationSubmission(request ConsumerQualification) (Spec, QualificationVerdict, error) {
	var missing QualificationVerdict
	var empty Spec
	s := Spec{Project: request.Project, Extension: request.Extension, ExtensionDigest: request.Digest,
		Name: "extension-check", Class: "experiment", AllowedMachines: []string{request.Worker}, RequireTarget: true}
	if s.Project == "" || request.Worker == "" || validateProjectIdentifier(s.Project) != nil || validateExtensionSelector(s) != nil ||
		!strings.Contains(s.Extension, "@") || (FileRef{SHA256: s.ExtensionDigest}).validate() != nil ||
		!validGPU(request.Platform) || (request.Platform == "cpu") != (request.Device == "") {
		return empty, missing, qualificationRequestError(400, "qualification_invalid", "require project, NAME@VERSION, exact SHA-256, worker, platform and device for an accelerator")
	}
	if err := q.validateProjectPlacement(s.Project, s.AllowedMachines); err != nil {
		return empty, missing, qualificationRequestError(403, "qualification_target_denied", "project is not permitted on the requested worker")
	}
	e, ok := q.s.Extensions[s.Extension]
	if !ok || e.Digest != s.ExtensionDigest {
		return empty, missing, qualificationRequestError(409, "qualification_digest_mismatch", "extension version and digest do not match the registered extension")
	}
	m := q.qualificationTarget(request.Worker, request.Platform, request.Device)
	if m == nil {
		return empty, missing, qualificationRequestError(404, "qualification_target_unknown", "unknown worker, platform or device")
	}
	if m.ExtensionFingerprint == "" {
		return empty, missing, qualificationRequestError(409, "qualification_identity_unavailable", "worker target has not reported its native fingerprint")
	}
	view, ok := e.forMachine(m)
	if !ok || !slices.Contains(e.platforms(), m.GPU) || len(view.Check) == 0 {
		return empty, missing, qualificationRequestError(409, "qualification_check_unavailable", "extension has no native check on the requested target")
	}
	work := request.Work
	if work == "" {
		works := extensionWorks(e)
		switch {
		case slices.Contains(works, "extension:"+e.Name):
			work = "extension:" + e.Name
		case len(works) == 1:
			work = works[0]
		}
	}
	if _, present := view.baseline(work, m.GPU); !present || !slices.Contains(extensionWorks(e), work) {
		return empty, missing, qualificationRequestError(400, "qualification_work_invalid", "select a declared work type with a native baseline on this target")
	}

	r := newExtensionReport(e, m, work)
	s.ExtensionCheck = &ExtensionCheckRequest{Consumer: true, Work: work, Automatic: &r, Directory: consumerCheckWorkdir, Timeout: "10m"}
	if variant := e.variant(machineTarget(m)); variant != nil {
		s.Needs.Memory = variant.Needs.Memory
	}
	if m.GPU != "cpu" {
		s.Needs.GPU = &GPUSelection{Devices: []string{r.Device}}
	}
	base := consumerCheckPrefix + identityHash([2]string{m.ID, extensionReportIdentity(r)})
	s.SubmissionKey = base
	var previous *Job
	for {
		record := q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)]
		if record == nil {
			break
		}
		j := q.s.Jobs[record.JobID]
		if j == nil {
			j = record.Job
		}
		if j == nil || !consumerExtensionCheck(j.Spec) {
			return empty, missing, submissionFailure(record.Kind == submissionExpired)
		}
		previous = j
		s.SubmissionKey = consumerCheckPrefix + identityHash([2]string{base, j.ID})
	}
	if previous != nil && (!terminal(previous.State) || previous.Attempt != 0 && !previous.ReleaseConfirmed) {
		v := qualificationVerdict(previous)
		v.Reused = true
		return empty, v, nil
	}
	if !request.Recheck {
		var latest QualificationVerdict
		if previous != nil {
			latest = qualificationVerdict(previous)
		}
		for _, report := range m.ExtensionReports {
			if report.Work != work || !matchingExtensionReport(e, m, report) {
				continue
			}
			j := q.checkJobByID(report.CheckID)
			if j == nil || j.Spec.ExtensionDigest != e.Digest || j.Spec.ExtensionCheck == nil ||
				j.Spec.ExtensionCheck.Work != work || j.Machine != m.ID || len(j.Spec.AllowedMachines) != 1 {
				continue
			}
			v := qualificationVerdict(j)
			passed := v.Verdict == "passed" && extensionQualificationReason(e, report, m) == ""
			failed := v.Verdict == "failed" && consumerExtensionCheck(j.Spec) && report.CheckState == "failed"
			if (passed || failed) && (latest.JobID == "" || v.FinishedAt.After(latest.FinishedAt)) {
				latest = v
				latest.Platform, latest.Device = report.Platform, report.Device
			}
		}
		if latest.JobID != "" {
			latest.Reused = true
			return empty, latest, nil
		}
	}
	if !slices.Contains(m.Capabilities, consumerCheckCapability) {
		return empty, missing, qualificationRequestError(409, "qualification_worker_upgrade_required", "worker does not support queue-requested native checks")
	}
	return s, missing, nil
}

func (q *Queue) qualificationTarget(worker, platform, device string) *Machine {
	owner := q.s.Machines[worker]
	if owner == nil || owner.Replacement != nil {
		return nil
	}
	if owner.Devices == nil {
		if owner.GPU == platform && extensionReportDevice(platform, owner.Device) == device {
			return owner
		}
		return nil
	}
	if platform == "cpu" {
		return cpuMachineView(owner)
	}
	for _, card := range owner.Devices {
		if card.Platform == platform && card.ID == device {
			target := card.machine(*owner)
			return &target
		}
	}
	return nil
}

func (q *Queue) validateConsumerCheck(s *Spec, extensions []Extension) error {
	r := s.ExtensionCheck.Automatic
	m := q.qualificationTarget(s.AllowedMachines[0], r.Platform, r.Device)
	if m == nil || !slices.Contains(m.Capabilities, consumerCheckCapability) || !matchingExtensionReport(extensions[0], m, *r) {
		return ErrStale
	}
	s.Needs.Platform = Platform{Required: []string{r.Platform}}
	return nil
}

func consumerCheckTargetMatches(j *Job, e Extension, m *Machine) bool {
	return slices.Contains(m.Capabilities, consumerCheckCapability) && matchingExtensionReport(e, m, *automaticExtensionCheck(j))
}

func (q *Queue) checkJobByID(id string) *Job {
	if job, ok := strings.CutPrefix(id, consumerCheckPrefix); ok {
		j := q.s.Jobs[job]
		if j != nil && consumerExtensionCheck(j.Spec) {
			return j
		}
		return nil
	}
	return q.qualificationCheckJob(id)
}

func (q *Queue) readQualification(ctx context.Context, id string, byJob bool) (QualificationVerdict, error) {
	unlock := requestReadLock(ctx, &q.mu)
	defer unlock()
	var j *Job
	if byJob {
		j = q.s.Jobs[id]
	} else {
		j = q.checkJobByID(id)
	}
	if j == nil || j.Spec.ExtensionCheck == nil || len(j.Spec.AllowedMachines) != 1 {
		return QualificationVerdict{}, qualificationRequestError(404, "qualification_unknown", "unknown extension check")
	}
	return qualificationVerdict(j), nil
}

func (a API) registerConsumerQualifications(mux *requestMux) {
	mux.HandleFunc("POST /api/extension-checks", func(w http.ResponseWriter, r *http.Request) {
		var request ConsumerQualification
		if !decode(w, r, &request) {
			return
		}
		v, err := a.Queue.requestQualification(r.Context(), request)
		respondOperation(w, v, err, "qualification_invalid")
	})
	read := func(byJob bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			v, err := a.Queue.readQualification(r.Context(), r.PathValue("id"), byJob)
			respondOperation(w, v, err, "qualification_unknown")
		}
	}
	mux.HandleFunc("GET /api/extension-checks/{id}", read(false))
	mux.HandleFunc("GET /api/jobs/{id}/extension-check", read(true))
}

// Only the worker that ran the check may publish its local report, after the
// queue confirms the exact attempt's verdict and physical release.
func (w Worker) recordConsumerQualification(ctx context.Context, j Job) error {
	var verdict QualificationVerdict
	if err := w.Client.Call(ctx, "GET", "/api/jobs/"+j.ID+"/extension-check", nil, &verdict); err != nil {
		return err
	}
	if verdict.Verdict == "pending" {
		return errWorkerReleasePending
	}
	if (verdict.Verdict != "passed" && verdict.Verdict != "failed") || verdict.Worker != w.Machine.ID || verdict.Attempt != j.Attempt {
		return nil
	}
	r := *j.Spec.ExtensionCheck.Automatic
	r.CheckID, r.CheckState, r.Setup, r.Qualified, r.Reason = verdict.CheckID, verdict.State, true, verdict.Verdict == "passed", ""
	if !r.Qualified {
		r.Reason = "check failed; explicit check required to retry"
	}
	e := Extension{Name: r.Name, Version: r.Version, Digest: r.Digest}
	unlock, err := extensionLock(w.ExtensionRoot, e, r.Platform+"-check", false)
	if err != nil {
		return err
	}
	defer unlock()
	reports := readExtensionReports(w.ExtensionRoot, e, r.Platform)
	for i, old := range reports {
		if !old.Setup || extensionReportIdentity(old) != extensionReportIdentity(r) || old.CheckID == r.CheckID {
			continue
		}
		if old.CheckID != "" {
			var prior QualificationVerdict
			err := w.Client.Call(ctx, "GET", "/api/extension-checks/"+url.PathEscape(old.CheckID), nil, &prior)
			var refusal *HTTPError
			if err != nil && !(errors.As(err, &refusal) && refusal.Status == 404) {
				return err
			}
			// An acknowledgement can replay after a later result. Keep the
			// newer report, including a failure following an earlier pass.
			if !prior.FinishedAt.IsZero() && !prior.FinishedAt.Before(verdict.FinishedAt) {
				return nil
			}
		}
		reports[i] = r
		return writeExtensionReports(w.ExtensionRoot, e, r.Platform, reports)
	}
	return nil
}
