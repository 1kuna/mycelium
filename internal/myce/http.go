package myce

import (
	"compress/gzip"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const startGateProtocol = "host-launch-v3"

type API struct {
	Queue *Queue
	Token string
}

func (a API) Handler() http.Handler { return a.handler("") }

func (a API) HandlerWithJoiningIdentity(pin string) http.Handler { return a.handler(pin) }

func (a API) handler(joiningPin string, observationClocks ...Clock) http.Handler {
	mux := newRequestMux()
	jobGets := newJobGetVolume(a.Queue.clock)
	jobGets.observe = a.Queue.requestHealth.recordClient
	// All adaptive admission decisions use an injected clock. The optional
	// override is retained for transport/fake-clock fixtures.
	var observationClock Clock = a.Queue.clock
	if len(observationClocks) > 0 && observationClocks[0] != nil {
		observationClock = observationClocks[0]
	}
	observationLimit := newObservationLimit(observationClock, a.Queue)
	a.registerWait(mux)
	waitStates := a.Queue.waitIndex()
	nas := a.Queue.resolveNASInspections()
	// Joining needs authenticated identity/version only, never the growing status
	// snapshot. Transport pinning authenticates this identity to joining clients.
	// Clients waiting on a slow call read queue progress here. No queue mutex.
	mux.HandleFunc("GET /api/ping", func(w http.ResponseWriter, r *http.Request) {
		progress := a.Queue.mu.progress()
		progress.P99MS = float64(a.Queue.requestLatencies.p99("GET /api/ping")) / float64(time.Millisecond)
		respond(w, map[string]any{"version": 1, "fingerprint": joiningPin, "queue_progress": progress}, nil)
	})
	mux.HandleFunc("GET /api/environments", func(w http.ResponseWriter, r *http.Request) {
		respond(w, a.Queue.EnvironmentEntries(), nil)
	})
	mux.HandleFunc("POST /api/environments/{key}/retry", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Worker string          `json:"worker"`
			Target ExecutionTarget `json:"target"`
		}
		if !decode(w, r, &request) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.RetryEnvironment(request.Worker, r.PathValue("key"), request.Target, r.Context()), "environment_retry_invalid")
	})
	a.registerCacheReferences(mux)
	mux.HandleFunc("GET /api/environments/{name}", func(w http.ResponseWriter, r *http.Request) {
		for _, entry := range a.Queue.EnvironmentEntries() {
			if entry.Name == r.PathValue("name") {
				respond(w, entry, nil)
				return
			}
		}
		respond(w, nil, &HTTPError{Status: 404, Code: "environment_unknown", Message: "unknown environment"})
	})
	mux.HandleFunc("GET /api/environments/{key}/reference", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if (FileRef{SHA256: key}).validate() != nil {
			respond(w, nil, errors.New("invalid environment key"))
			return
		}
		respond(w, map[string]bool{"referenced": a.Queue.environmentReferenced(r.Context(), key)}, nil)
	})
	mux.HandleFunc("GET /api/portability", func(w http.ResponseWriter, r *http.Request) {
		report, err := a.Queue.Portability(PortabilityRequest{Extension: r.URL.Query().Get("extension"), Project: r.URL.Query().Get("project")}, r.Context())
		respondOperation(w, report, err, "portability_invalid")
	})
	mux.HandleFunc("POST /api/portability", func(w http.ResponseWriter, r *http.Request) {
		var request PortabilityRequest
		if !decode(w, r, &request) {
			return
		}
		report, err := a.Queue.Portability(request, r.Context())
		respondOperation(w, report, err, "portability_invalid")
	})
	mux.HandleFunc("POST /api/resolve", func(w http.ResponseWriter, r *http.Request) {
		var request ResolveRequest
		if !decode(w, r, &request) {
			return
		}
		report, err := a.Queue.ResolveContext(r.Context(), request)
		if err == nil {
			nas.resolve(request, &report)
		}
		respondOperation(w, report, err, "resolve_invalid")
	})
	a.registerInvites(mux, joiningPin)
	a.registerEnrollmentRelease(mux)
	a.registerEnrollment(mux)
	a.registerFleetUpgrade(mux)
	a.registerDevClock(mux)
	mux.HandleFunc("POST /api/files/result", a.deliveryResultHandler)
	a.registerCompletions(mux)
	a.registerRecoveredResults(mux)
	a.registerHostMaintenance(mux)
	a.registerDeviceMaintenance(mux)
	a.registerComponentExclusions(mux)
	a.registerReservations(mux)
	a.registerPriorityPolicy(mux)
	a.registerProjectParity(mux)
	a.registerFiles(mux)
	a.registerFileRelease(mux)
	a.registerAttemptFiles(mux)
	mux.HandleFunc("POST /api/extensions", func(w http.ResponseWriter, r *http.Request) {
		var e Extension
		if !decode(w, r, &e) {
			return
		}
		if err := a.Queue.AddExtension(e, r.Context()); err != nil {
			respondOperation(w, nil, err, "extension_invalid")
			return
		}
		respond(w, ExtensionRegistration{OK: true, Warnings: a.Queue.extensionRegistrationWarnings(e, r.Context())}, nil)
	})
	mux.HandleFunc("GET /api/extensions/{digest}", func(w http.ResponseWriter, r *http.Request) {
		e, err := a.Queue.extensionByDigest(r.PathValue("digest"), r.Context())
		respondOperation(w, e, err, "extension_unknown")
	})
	mux.HandleFunc("GET /api/extensions", func(w http.ResponseWriter, r *http.Request) { respond(w, a.Queue.Extensions(r.Context()), nil) })
	mux.HandleFunc("GET /api/usage", func(w http.ResponseWriter, r *http.Request) {
		d, e := time.ParseDuration(r.URL.Query().Get("since"))
		if e != nil || d <= 0 {
			respond(w, nil, reject("usage_invalid", nil))
			return
		}
		report, e := a.Queue.usageSnapshot(a.Queue.clock.Now().Add(-d), r.Context())
		respondOperation(w, report, e, "usage_unavailable")
	})
	mux.HandleFunc("/models/{name}/{path...}", a.modelCall)
	mux.HandleFunc("/api/workers/resident/{id}/{path...}", func(w http.ResponseWriter, r *http.Request) {
		respond(w, nil, retiredResidentError())
	})
	mux.HandleFunc("GET /api/workers/{id}/events", a.workerEvent)
	mux.HandleFunc("POST /api/models/sync", func(w http.ResponseWriter, r *http.Request) {
		respond(w, nil, &HTTPError{Status: http.StatusGone, Code: "model_sync_retired", Message: "stage verified weights under the model root and call the catalog model to load it on demand (docs/services.md)"})
	})
	for _, command := range []string{"pin", "unpin"} {
		mux.HandleFunc("POST /api/models/"+command, func(w http.ResponseWriter, r *http.Request) {
			respond(w, nil, &HTTPError{Status: http.StatusGone, Code: "model_pin_retired", Message: "call the catalog model to load it on demand and set idle_timeout for retention (docs/services.md)"})
		})
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		mux.HandleFunc("POST "+path, a.modelCall)
	}
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		var s Snapshot
		var err error
		if len(r.URL.Query()) == 0 {
			s, err = a.Queue.cachedPublicSnapshot(r.Context())
		} else {
			var options statusOptions
			options, err = parseStatusOptions(r.URL.Query())
			if err == nil {
				s, err = a.Queue.publicStatusSnapshot(&options, r.Context())
			}
		}
		if err != nil {
			http.Error(w, "status unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = statusPage.Execute(w, s)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		options, err := parseStatusOptions(r.URL.Query())
		if err != nil {
			respond(w, nil, err)
			return
		}
		// The default is the CompactStatus view; view=snapshot is the
		// compatibility path to the summary snapshot. Explicit sections, job
		// pages and detail=full return snapshots as before.
		query := r.URL.Query()
		view := query.Get("view")
		if view != "" && view != "snapshot" {
			respond(w, nil, reject("status_query_invalid", nil))
			return
		}
		// Explicit peer byte ceilings retain the bounded, buffered contract.
		// Ordinary full-history readers consume a single streamed document.
		_, peerLimited := r.Context().Value(peerResponseBudgetKey{}).(int64)
		if options.full && !peerLimited {
			a.Queue.serveStatusHistory(w, r, options)
			return
		}
		query.Del("view")
		var data string
		if len(query) == 0 && view == "" {
			data, err = a.Queue.cachedCompactStatusJSON(r.Context())
		} else if len(query) == 0 {
			data, err = a.Queue.cachedStatusJSON(r.Context())
		} else {
			var snapshot Snapshot
			snapshot, err = a.Queue.publicStatusSnapshot(&options, r.Context())
			var encoded []byte
			if err == nil && view == "" && options.compact() {
				encoded, err = json.Marshal(NewCompactStatus(snapshot))
			} else if err == nil {
				encoded, err = json.Marshal(snapshot)
			}
			data = string(encoded)
		}
		// Current work cannot be paged away to fit a smaller peer. Apply its
		// negotiated allowance to the actual compact document, including cache hits.
		if peer, ok := r.Context().Value(peerResponseBudgetKey{}).(int64); ok && err == nil && int64(len(data)) > peer {
			err = &HTTPError{Status: 503, Code: "projection_capacity_exceeded", Message: fmt.Sprintf("status response requires %d bytes; fitting bytes=%d", len(data), peer)}
		}
		w.Header().Add("Vary", "Accept-Encoding")
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			for _, encoding := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
				if strings.EqualFold(strings.TrimSpace(encoding), "gzip") {
					// The stock Go client negotiates and decodes gzip automatically.
					// Compress only the detached public summary, outside the queue lock.
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Encoding", "gzip")
					compressed := gzip.NewWriter(w)
					_, _ = io.WriteString(compressed, data)
					_ = compressed.Close()
					return
				}
			}
			_, _ = io.WriteString(w, data)
			return
		}
		respondOperation(w, nil, err, "status_unavailable")
	})
	// Administrative inspection must remain fresh for migration custody and
	// measurement safety. It never consumes the observational status cache.
	mux.HandleFunc("GET /api/efficiency", func(w http.ResponseWriter, r *http.Request) {
		a.Queue.mu.RLock()
		report := a.Queue.efficiencyView
		a.Queue.mu.RUnlock()
		respond(w, report, nil) // immutable projection; encoding stays outside q.mu.
	})
	mux.HandleFunc("GET /api/inspection", func(w http.ResponseWriter, r *http.Request) {
		s, err := a.Queue.publicSnapshot(r.Context())
		respondOperation(w, s, err, "status_unavailable")
	})
	mux.HandleFunc("GET /api/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Myce-Wait", "batch-v1")
		var j *Job
		var freshness *SnapshotFreshness
		var err error
		if r.URL.Query().Get("view") == "wait" {
			jobGets.record(r.UserAgent(), true)
			states, _, _, readErr := waitStates.readContext(r.Context(), WaitRequest{IDs: []string{r.PathValue("id")}})
			err = readErr
			if err == nil {
				state := publicWaitReply(states).Jobs[0]
				j = &Job{ID: state.ID, State: state.State, Reason: state.Reason}
				if terminal(j.State) {
					j, freshness, err = a.Queue.jobSnapshotRead(j.ID, r.Context())
				}
			}
			if err == nil && !terminal(j.State) {
				respondOperation(w, struct {
					ID     string `json:"id"`
					State  string `json:"state"`
					Reason string `json:"reason,omitempty"`
				}{j.ID, j.State, j.Reason}, nil, "status_unavailable")
				return
			}
		} else {
			j, freshness, err = a.Queue.jobSnapshotRead(r.PathValue("id"), r.Context())
			var refusal *HTTPError
			if !errors.As(err, &refusal) || refusal.Code != "client_rate_limited" {
				jobGets.record(r.UserAgent(), false)
			}
		}
		err = a.Queue.expiredJobError(r.Context(), err)
		// Keep the GET execution payload empty without changing Spec's JSON
		// encoding, which also defines durable submission fingerprints.
		type replySpec struct {
			Spec
			Run struct{} `json:"run"`
		}
		// Freshness is the age of the committed state this receipt projects.
		reply := struct {
			*Job
			Spec       replySpec          `json:"spec"`
			Completion *CompletionState   `json:"completion"`
			Freshness  *SnapshotFreshness `json:"freshness,omitempty"`
		}{Job: j, Freshness: freshness}
		if j != nil {
			reply.Completion = j.Completion
			if reply.Completion == nil {
				reply.Completion = &CompletionState{}
			}
			reply.Spec.Spec = j.Spec
		}
		respondOperation(w, reply, err, "status_unavailable")
	})
	mux.HandleFunc("GET /api/models/intervals", func(w http.ResponseWriter, r *http.Request) {
		respond(w, nil, &HTTPError{Status: http.StatusGone, Code: "model_intervals_retired", Message: "read loaded_models from /api/status for current model state"})
	})
	mux.HandleFunc("GET /api/jobs/{id}/ledger", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		values := []int{0, -1, 0}
		for i, key := range []string{"after", "high_water", "limit"} {
			if query.Has(key) {
				v, err := strconv.Atoi(query.Get(key))
				if err != nil || v < 0 {
					respondOperation(w, nil, &HTTPError{Status: 400, Code: "job_ledger_cursor_invalid", Message: "invalid ledger pagination"}, "")
					return
				}
				values[i] = v
			}
		}
		page, err := a.Queue.JobLedger(r.PathValue("id"), values[0], values[1], values[2], r.Context())
		respondOperation(w, page, err, "job_ledger_unavailable")
	})
	mux.HandleFunc("POST /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Spec
			SubmissionKey json.RawMessage `json:"submission_key"`
		}
		if !decode(w, r, &request) {
			return
		}
		if request.SubmissionKey != nil {
			if err := json.Unmarshal(request.SubmissionKey, &request.Spec.SubmissionKey); err != nil {
				respond(w, nil, validateSubmissionKey(""))
				return
			}
			if err := validateSubmissionKey(request.Spec.SubmissionKey); err != nil {
				respond(w, nil, err)
				return
			}
		}
		j, err := a.Queue.submitWithDeadlineGuidance(r.Context(), request.Spec)
		if j != nil {
			j.LedgerEntries = nil
		}
		respondOperation(w, j, err, "job_submit_failed")
	})
	mux.HandleFunc("POST /api/jobs/{id}/pause", func(w http.ResponseWriter, r *http.Request) {
		var request PauseJobRequest
		if !decode(w, r, &request) {
			return
		}
		j, err := a.Queue.PauseJob(r.PathValue("id"), request, r.Context())
		respondOperation(w, j, err, "job_pause_invalid")
	})
	mux.HandleFunc("POST /api/jobs/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		var request ResumeJobRequest
		if !decode(w, r, &request) {
			return
		}
		j, err := a.Queue.ResumeJob(r.PathValue("id"), request.IfRevision, r.Context())
		respondOperation(w, j, err, "job_resume_invalid")
	})
	mux.HandleFunc("POST /api/jobs/{id}/control/pause", func(w http.ResponseWriter, r *http.Request) {
		var request FencedPauseRequest
		if !decode(w, r, &request) {
			return
		}
		j, err := a.Queue.FencedPause(r.Context(), r.PathValue("id"), request)
		respondOperation(w, j, err, "job_pause_invalid")
	})
	mux.HandleFunc("POST /api/jobs/{id}/control/resume", func(w http.ResponseWriter, r *http.Request) {
		var request FencedResumeRequest
		if !decode(w, r, &request) {
			return
		}
		j, err := a.Queue.FencedResume(r.Context(), r.PathValue("id"), request)
		respondOperation(w, j, err, "job_resume_invalid")
	})
	mux.HandleFunc("POST /api/jobs/resize", func(w http.ResponseWriter, r *http.Request) {
		var request ResizeJobsRequest
		if !decode(w, r, &request) {
			return
		}
		receipt, err := a.Queue.ResizeJobs(r.Context(), request)
		respondOperation(w, receipt, err, "job_resize_failed")
	})
	mux.HandleFunc("POST /api/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		ack, err := a.Queue.CancelJob(r.PathValue("id"), r.Context())
		respondOperation(w, ack, err, "job_cancel_invalid")
	})
	cancelSubmission := func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Project       string          `json:"project"`
			SubmissionKey json.RawMessage `json:"submission_key"`
		}
		if !decode(w, r, &request) {
			return
		}
		var key string
		if err := json.Unmarshal(request.SubmissionKey, &key); err != nil {
			respond(w, nil, validateSubmissionKey(""))
			return
		}
		ack, err := a.Queue.CancelSubmission(request.Project, key, r.Context())
		respondOperation(w, ack, err, "job_cancel_invalid")
	}
	mux.HandleFunc("POST /api/jobs/cancel", cancelSubmission)
	mux.HandleFunc("POST /api/jobs/control/cancel-by-key", cancelSubmission)
	mux.HandleFunc("GET /api/jobs/{id}/logs", func(w http.ResponseWriter, r *http.Request) {
		after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		if r.URL.Query().Get("after") == "" {
			after = 0
			err = nil
		}
		if err != nil || after < 0 {
			respond(w, nil, reject("job_logs_invalid_cursor", err))
			return
		}
		ls, err := a.Queue.Logs(r.PathValue("id"), after, r.Context())
		respondOperation(w, ls, err, "job_logs_unavailable")
	})
	mux.HandleFunc("POST /api/workers/checkin", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Myce-Start-Gate") != startGateProtocol {
			http.Error(w, "worker start-gate protocol mismatch: upgrade queue and workers together", http.StatusUpgradeRequired)
			return
		}
		var c Checkin
		if !decode(w, r, &c) {
			return
		}
		if value := r.Header.Get(checkinPeriodHeader); value != "" {
			period, err := time.ParseDuration(value)
			if err != nil || period <= 0 || period > time.Duration(1<<63-1)/2 {
				respondOperation(w, nil, errors.New("invalid worker check-in period"), "worker_checkin_invalid")
				return
			}
			c.period = period
		}
		c.fleetProofIssue = safeFleetProofIssue(FleetProofIssue(r.Header.Get(fleetProofIssueHeader)))
		c.ephemeralRefusal = safeEphemeralRefusal(ephemeralRefusal(r.Header.Get(ephemeralRefusalHeader)))
		accepted := false
		contact, settle := a.Queue.trackWorkerCheckin(c.Machine.ID, c.Machine.Session, c.period, true, r.Context())
		c.contact = contact
		defer func() { settle(accepted) }()
		_, _ = a.Queue.checkinResponseContext(r.Context(), c, func(v Reply, err error) {
			accepted = err == nil
			if err == nil {
				a.Queue.recordCheckinService(r.Context())
				v.NASPathChecks = nas.checkin(c)
			}
			respondOperation(w, v, err, "worker_checkin_invalid")
		})
	})
	mux.HandleFunc("POST /api/workers/launch", func(w http.ResponseWriter, r *http.Request) {
		var l workerLaunchRequest
		if !decode(w, r, &l) {
			return
		}
		respondOperation(w, map[string]any{"ok": true, "start_gate": startGateProtocol, "native_launch_protocol": nativeLaunchProtocol}, a.Queue.authorizeLaunchWithCustody(r.Context(), l), "worker_launch_invalid")
	})
	mux.HandleFunc("POST /api/workers/native-custody", func(w http.ResponseWriter, r *http.Request) {
		var c NativeCustodyReport
		if !decode(w, r, &c) {
			return
		}
		respondOperation(w, map[string]any{"ok": true, "start_gate": startGateProtocol}, a.Queue.NativeCustody(c, r.Context()), "native_custody_invalid")
	})
	mux.HandleFunc("POST /api/workers/started", func(w http.ResponseWriter, r *http.Request) {
		var c workerStartedRequest
		if !decode(w, r, &c) {
			return
		}
		remaining, err := a.Queue.startedWithCustody(r.Context(), c)
		respondOperation(w, map[string]any{"ok": true, "start_gate": startGateProtocol, "wall_time_remaining_seconds": remaining, "native_launch_protocol": nativeLaunchProtocol}, err, "worker_start_invalid")
	})
	mux.HandleFunc("POST /api/workers/yield-stop", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		allowed, err := a.Queue.authorizeYieldStop(r.Context(), c)
		respondOperation(w, map[string]bool{"allowed": allowed}, err, "worker_yield_invalid")
	})
	mux.HandleFunc("POST /api/workers/warm-piece", func(w http.ResponseWriter, r *http.Request) {
		var request warmPieceRequest
		if !decode(w, r, &request) {
			return
		}
		reply, err := a.Queue.warmPiece(r.Context(), request)
		respondOperation(w, reply, err, "worker_complete_invalid")
	})
	mux.HandleFunc("POST /api/workers/complete", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.CompleteContext(r.Context(), c), "worker_complete_invalid")
	})
	mux.HandleFunc("POST /api/workers/environment-log", func(w http.ResponseWriter, r *http.Request) {
		var c EnvironmentLog
		if !decode(w, r, &c) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.AppendEnvironmentLog(c, r.Context()), "worker_environment_log_invalid")
	})
	mux.HandleFunc("POST /api/workers/log", func(w http.ResponseWriter, r *http.Request) {
		var c LogChunk
		if !decode(w, r, &c) {
			return
		}
		setLogRequestEvidence(r, []LogChunk{c})
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.AppendLog(c, r.Context()), "worker_log_invalid")
	})
	mux.HandleFunc("POST /api/workers/log-batch", func(w http.ResponseWriter, r *http.Request) {
		var batch logBatch
		if !decode(w, r, &batch) {
			return
		}
		setLogRequestEvidence(r, batch.Chunks)
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.appendLogBatch(batch, r.Context()), "worker_log_invalid")
	})
	mux.HandleFunc("POST /api/workers/{id}/leave", func(w http.ResponseWriter, r *http.Request) {
		var c struct {
			Now bool `json:"now"`
		}
		if !decode(w, r, &c) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.Leave(r.PathValue("id"), c.Now, r.Context()), "worker_leave_invalid")
	})
	return a.Queue.shutdownRequests(a.Queue.measureRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, r.Pattern = mux.Handler(r)
		if r.URL.Path == "/api/invites/redeem" {
			a.redeemInvite(w, r, joiningPin)
			return
		}
		credential := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if r.Method == "GET" && (r.URL.Path == "/" || r.URL.Path == "/api/status") {
			if user, password, ok := r.BasicAuth(); ok && user == "myce" {
				credential = password
			}
		}
		if a.Token == "" || subtle.ConstantTimeCompare([]byte(credential), []byte(a.Token)) != 1 {
			if r.URL.Path == "/" {
				w.Header().Set("WWW-Authenticate", `Basic realm="Mycelium", charset="UTF-8"`)
			}
			respond(w, nil, &HTTPError{Status: 401, Code: "authentication_required", Message: rejectionMessages["authentication_required"]})
			return
		}
		if workerProtocolEndpoint(r.URL.Path) && (r.Method == http.MethodPost || r.Method == http.MethodGet) && r.Header.Get("X-Myce-Start-Gate") != startGateProtocol {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUpgradeRequired)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "worker_protocol_mismatch", "error": "current host-launch-v3 worker/report protocol required; upgrade queue and workers together"})
			return
		}
		if r.URL.Path == "/api/jobs" && r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			respond(w, nil, &HTTPError{Status: 405, Code: "request_method_invalid", Message: rejectionMessages["request_method_invalid"]})
			return
		}
		if workerProtocolEndpoint(r.URL.Path) {
			w.Header().Set(executionEvidenceProtocolHeader, executionEvidenceProtocol)
			w.Header().Set(resourceProtocolHeader, resourceProtocol)
			w.Header().Set(cpuPressureProtocolHeader, cpuPressureProtocol)
			w.Header().Set(faultEvidenceHeader, faultEvidenceProtocol)
			w.Header().Set(stagingPhasesHeader, stagingPhasesProtocol)
			w.Header().Set(launchTimingsHeader, launchTimingsWire)
			w.Header().Set(hostRAMGateHeader, hostRAMGateProtocol)
		}
		// Liveness is a fixed-size authenticated response, not a projection.
		// A progress probe must not wait on the OS memory observation used to
		// admit growing request/response allocations: that would let the probe
		// time out while the control request it watches is still progressing.
		if r.Method == http.MethodGet && r.URL.Path == "/api/ping" {
			mux.ServeHTTP(w, r)
			return
		}
		var finishBudget func()
		var budgetErr error
		r, finishBudget, budgetErr = a.Queue.withRequestBudget(w, r)
		if budgetErr != nil {
			respond(w, nil, budgetErr)
			return
		}
		defer finishBudget()
		if deferredObservationProjection(r) {
			projection := &observationProjection{limit: observationLimit, request: r, rejected: func() { jobGets.recordRejection(r) }}
			defer func() {
				if projection.finish != nil {
					projection.finish()
				}
			}()
			r = r.WithContext(context.WithValue(r.Context(), observationProjectionKey{}, projection))
			mux.ServeHTTP(w, r)
			return
		}
		finishObservation, admitted := observationLimit.admit(w, r)
		if !admitted {
			jobGets.recordRejection(r)
			return
		}
		defer finishObservation()
		mux.ServeHTTP(w, r)
	}), mux.patterns))
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	err := decodeBudget(w, r, v)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respond(w, nil, budgetRefusal(tooLarge.Limit))
			return false
		}
		var capacityError *HTTPError
		if errors.As(err, &capacityError) {
			respond(w, nil, err)
			return false
		}
		var rejected *rejectionError
		if !errors.As(err, &rejected) {
			err = reject("request_invalid", err)
		}
		respond(w, nil, err)
		return false
	}
	return true
}

// publicErrorCode reports whether a typed HTTPError code reaches callers with
// its server-written reason. Every HTTPError the queue builds carries its own
// public message, so any well-formed code passes; untyped internal errors have
// no HTTPError code and stay request_failed (#593).
func publicErrorCode(code string) bool {
	if code == "" || code == "request_failed" || len(code) > 64 {
		return false
	}
	for _, r := range code {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// knownServerRefusal reports whether a 5xx code is a queue-authored refusal
// with a fixed public reason (queue_stopping, file_store_unavailable, release
// unavailability). Any other 5xx is an internal failure whose message may
// carry private causes, so it stays request_failed (#593 follow-up).
func knownServerRefusal(code string) bool {
	return code == "request_capacity_unavailable" || code == "projection_capacity_exceeded" || rejectionMessages[code] != "" || fileStoreRefusalCode(code)
}

func respond(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	// Transport cancellation is unavailable observation, never invalid input.
	// Preserve a retryable public cause without exposing the private error.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = &HTTPError{Status: http.StatusServiceUnavailable, Code: "status_unavailable"}
	}
	if err != nil {
		status := http.StatusBadRequest
		var writeFailure queueWriteError
		if errors.As(err, &writeFailure) {
			status = http.StatusServiceUnavailable
		}
		if errors.Is(err, ErrStale) {
			status = http.StatusConflict
		}
		code := "request_failed"
		if errors.Is(err, ErrStale) {
			code = "request_stale"
		}
		var rule *rejectionError
		if errors.As(err, &rule) {
			code = rule.code
		}
		body := map[string]any{"error": rejectionMessages[code], "code": code}
		var launchConflict *batchLaunchConflict
		if errors.As(err, &launchConflict) {
			body["cause"] = safeRuntimeCause(&RuntimeCause{Operation: "start_authorization", Class: "admission_changed", Predicate: "launch_authorization", LaunchRefusal: launchConflict.boundary})
		}
		var apiError *HTTPError
		if errors.As(err, &apiError) && publicErrorCode(apiError.Code) && (apiError.Status < 500 || knownServerRefusal(apiError.Code)) {
			if apiError.RetryAfter > 0 {
				w.Header().Set("Retry-After", strconv.FormatInt(max(1, int64(math.Ceil(apiError.RetryAfter.Seconds()))), 10))
			}
			if apiError.Status >= 400 {
				status = apiError.Status
			}
			message := apiError.Message
			if message == "" {
				message = rejectionMessages[apiError.Code]
			}
			body = map[string]any{"error": message, "code": apiError.Code}
			if apiError.Code == "file_store_capacity" {
				if cause := safeRuntimeCause(apiError.Cause); cause != nil && cause.FileCapacity != nil {
					body["cause"] = cause
					snapshot, _ := json.Marshal(cause)
					log.Printf("event=file_store_capacity cause=%s", snapshot)
				}
			}
			if apiError.Code == "client_rate_limited" {
				body["code_alias"] = "observation_shed_control_pressure"
			}
			if apiError.PathDiagnostic != nil {
				body["path_diagnostic"] = apiError.PathDiagnostic
			}
			if apiError.CurrentRevision != nil {
				body["current_revision"] = *apiError.CurrentRevision
			}
			if apiError.Code == "job_unknown" {
				body["will_not_run"] = nil
				body["job_id"], body["project"], body["submission_key"] = apiError.JobID, nil, nil
				body["state"], body["release"], body["acknowledged_at"] = "unknown", nil, nil
			}
		}
		if apiError != nil && apiError.reason != "" {
			log.Printf("API rejection status=%d code=%s reason=%s", status, body["code"], apiError.reason)
		} else {
			log.Printf("API rejection status=%d code=%s", status, body["code"])
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}
func (q *Queue) Run(ctx context.Context) error {
	q.readWarm.enable(&q.publishedReads)
	runtimeCtx, stopRuntime := context.WithCancel(ctx)
	runtimeDone := make(chan struct{})
	go func() { defer close(runtimeDone); q.runFleetQueueRuntime(runtimeCtx) }()
	healthDone := make(chan struct{})
	go func() { defer close(healthDone); q.runHealthNotifier(runtimeCtx) }()
	defer func() { stopRuntime(); <-healthDone }()
	defer func() { stopRuntime(); <-runtimeDone }()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := q.TickContext(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// No installation-specific dial/handshake/request deadline. The operating
// system handles transport liveness; the caller owns the operation deadline.
var clientTransport = &http.Transport{Proxy: http.ProxyFromEnvironment}

type Client struct {
	// SharedQueueProof requires durable acceptance of worker control decisions
	// through the worker's local member.
	SharedQueueProof bool
	// sharedWire lets a worker follow a cutover or rollback without a
	// restart: a shared member advertises its control protocol on refusal,
	// and only a single queue answers worker control without a turn.
	sharedWire       *atomic.Bool
	publication      *publicationProgress
	publicationWire  *atomic.Bool
	inventoryWire    *atomic.Bool
	stagingWire      *atomic.Bool
	launchTimingWire *atomic.Bool
	faultWire        *atomic.Bool
	hostRAMGateWire  *atomic.Bool
	URL              string
	Token            string
	endpoints        *queueEndpoints
	HTTP             *http.Client // Its Timeout bounds API calls; file transfers use progress budgets.
	// Tests shorten the file transfer budgets; zero uses the defaults.
	transferIdle  time.Duration
	uploadTiming  *uploadTiming
	Clock         Clock // Optional observation clock.
	Pause         func(context.Context, time.Duration) error
	Jitter        func() float64
	singleAttempt bool
	watch         bool // see WithProgressWatch
}

func (c Client) Call(ctx context.Context, method, path string, in, out any) error {
	done := c.publication.request(c.clock(), method, path)
	defer done()
	ctx, cancel := c.apiContext(ctx)
	defer cancel()
	clock := c.clock()
	pace := ClientPace{}
	var last error
	for {
		started := clock.Now()
		prepared, stagingFields := stagingWireInput(in, c.stagingWire != nil && c.stagingWire.Load())
		prepared, launchFields := launchTimingWireInput(prepared, c.launchTimingWire != nil && c.launchTimingWire.Load())
		prepared, ramFields := hostRAMGateWireInput(prepared, c.hostRAMGateWire != nil && c.hostRAMGateWire.Load())
		wireIn, faultFields := faultWireInput(prepared, c.faultWire != nil && c.faultWire.Load())
		err := c.callWatched(ctx, method, path, wireIn, out)
		if method == http.MethodPost && path == "/api/workers/complete" && requestDecodeRefused(err) {
			if legacy, changed := legacyCompletionInput(wireIn); changed {
				wireIn = legacy
				err = c.callWatched(ctx, method, path, wireIn, out)
			}
		}
		if (faultFields || stagingFields || launchFields || ramFields) && requestDecodeRefused(err) {
			// A staging-capable predecessor can reject launch timings alone.
			launchDowngrade := launchFields && !c.launchTimingWire.Load()
			keepStaging := launchDowngrade && c.stagingWire != nil && c.stagingWire.Load()
			keepFaults := launchDowngrade && c.faultWire != nil && c.faultWire.Load()
			if c.faultWire != nil && !keepFaults {
				c.faultWire.Store(false)
			}
			if c.stagingWire != nil && !keepStaging {
				c.stagingWire.Store(false)
			}
			if c.launchTimingWire != nil {
				c.launchTimingWire.Store(false)
			}
			if c.hostRAMGateWire != nil {
				c.hostRAMGateWire.Store(false) // The newest fields go with any refusal.
			}
			legacy, _ := hostRAMGateWireInput(wireIn, false)
			legacy, _ = faultWireInput(legacy, keepFaults)
			legacy, _ = stagingWireInput(legacy, keepStaging)
			legacy, _ = launchTimingWireInput(legacy, false)
			err = c.callWatched(ctx, method, path, legacy, out)
		}
		if err == nil {
			c.publication.failed(nil) // A complete, acknowledged request clears history.
			if release, ok := in.(Result); c.publication != nil && method == http.MethodPost && path == "/api/workers/complete" && ok && release.ReleaseOnly && !release.ReleasePending {
				// Before its first upload, this attempt has already measured a
				// durable acknowledgement through the same queue writer.
				c.publication.timing.completed(durableAckRoute, clock.Now().Sub(started), 0, 0, 0, false, clock.Now())
			}
		}
		var api *HTTPError
		safe := method == "GET" || method == "HEAD" || method == "POST" && replaySafePost[path]
		var transport *url.Error
		var stalled *QueueStalledError
		_, streaming := out.(io.Writer)
		// Never replay a response whose bytes have already reached a caller.
		broken := !streaming && !errors.As(err, &stalled) && !IsTLSIdentityError(err) && (errors.As(err, &transport) || errors.Is(err, io.ErrUnexpectedEOF))
		overload := errors.As(err, &api) && (api.Status == 429 || api.Status == 503)
		if c.singleAttempt || !safe || (!broken && !overload) {
			if last != nil && ctx.Err() != nil {
				return &clientRetryStopped{last, ctx.Err()}
			}
			return err
		}
		last = err
		pace.Observe(clock.Now().Sub(started), false)
		if stopped := c.pause(ctx, pace.Delay(clock.Now(), time.Time{}, err, c.Jitter)); stopped != nil {
			return &clientRetryStopped{err, stopped}
		}
	}
}

func requestDecodeRefused(err error) bool {
	var rejected *HTTPError
	return errors.As(err, &rejected) && rejected.Status == http.StatusBadRequest && rejected.Code == "request_invalid"
}

// These POSTs change nothing, or replay identically (an extension registration
// is keyed by its digest), so overload refusals retry under Retry-After.
var replaySafePost = map[string]bool{"/api/resolve": true, "/api/files/preflight": true, "/api/extensions": true}

func (c Client) callOnce(ctx context.Context, method, path string, in, out any) error {
	return c.callOnceShared(ctx, method, path, in, out, "")
}

func (c Client) callOnceShared(ctx context.Context, method, path string, in, out any, handover string) error {
	checkin, workerCheckin := in.(Checkin)
	workerCheckin = workerCheckin && path == "/api/workers/checkin"
	if workerCheckin && checkin.Machine.EphemeralRefusal != "" {
		// The queue's status field is never part of the strict check-in body.
		checkin.Machine.EphemeralRefusal = ""
		in = checkin
	}
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+path, body)
	if err != nil {
		return err
	}
	if evidence, ok := ctx.Value(stalledRequestKey{}).(stalledRequest); ok {
		req.Header.Set(logRequestIDHeader, evidence.RequestID)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	fileStoreSubmissionHeaders(ctx, req)
	if wait, ok := in.(WaitRequest); ok && wait.Collect {
		req.Header.Set(completionCollectHeader, "1")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "myce/"+BuildVersion)
	if workerCheckin {
		if checkin.period > 0 {
			req.Header.Set(checkinPeriodHeader, checkin.period.String())
		}
		if issue := safeFleetProofIssue(checkin.fleetProofIssue); issue != "" {
			req.Header.Set(fleetProofIssueHeader, string(issue))
		}
		if refusal := safeEphemeralRefusal(checkin.ephemeralRefusal); refusal != "" {
			req.Header.Set(ephemeralRefusalHeader, string(refusal))
		}
	}
	if workerProtocolEndpoint(path) {
		req.Header.Set("X-Myce-Start-Gate", startGateProtocol)
	}
	control, effect := sharedWorkerControl(path)
	shared := c.SharedQueueProof || c.sharedWire != nil && c.sharedWire.Load()
	proposal, mutation := shared && control, shared && effect
	confirm := shared && method == http.MethodPost && path == sharedTurnPath
	if proposal || mutation || confirm {
		req.Header.Set(sharedVerifyHeader, sharedControlProtocol)
	}
	if handover != "" {
		req.Header.Set(sharedTurnHeader, handover)
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Transport: clientTransport}
	}
	spec, submit := in.(Spec)
	replaySafe := method == http.MethodGet || method == http.MethodHead ||
		method == http.MethodPost && (replaySafePost[path] || path == "/api/wait" || path == "/api/jobs" && submit && spec.SubmissionKey != "")
	res, err := c.doQueueRequest(hc, req, replaySafe)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if c.sharedWire != nil && !c.SharedQueueProof && (control || effect) {
		if !shared && res.StatusCode == http.StatusUpgradeRequired && res.Header.Get(sharedVerifyHeader) == sharedControlProtocol {
			// The endpoint is now a shared member. Repeat under its protocol.
			c.sharedWire.Store(true)
			_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			return c.callOnceShared(ctx, method, path, in, out, "")
		}
		if shared && res.StatusCode < 300 && res.Header.Get(sharedTurnHeader) == "" {
			// A member always names its turn here; this reply is a single
			// queue's own decision, so take it as one.
			c.sharedWire.Store(false)
			proposal, mutation = false, false
		}
	}
	advertisesWorkerProtocol := workerProtocolEndpoint(path) && !(mutation && handover == "")
	if c.stagingWire != nil && advertisesWorkerProtocol {
		c.stagingWire.Store(res.Header.Get(stagingPhasesHeader) == stagingPhasesProtocol)
	}
	if c.launchTimingWire != nil && advertisesWorkerProtocol {
		c.launchTimingWire.Store(res.Header.Get(launchTimingsHeader) == launchTimingsWire)
	}
	if c.faultWire != nil && advertisesWorkerProtocol {
		c.faultWire.Store(res.Header.Get(faultEvidenceHeader) == faultEvidenceProtocol)
	}
	if c.hostRAMGateWire != nil && workerProtocolEndpoint(path) {
		c.hostRAMGateWire.Store(res.Header.Get(hostRAMGateHeader) == hostRAMGateProtocol)
	}
	if res.StatusCode >= 300 {
		e := &HTTPError{Status: res.StatusCode, RetryAfter: retryAfter(res.Header.Get("Retry-After"), c.clock().Now()), Message: "request failed"}
		var body struct {
			Cause           *RuntimeCause   `json:"cause"`
			PathDiagnostic  *PathDiagnostic `json:"path_diagnostic"`
			Code            string          `json:"code"`
			Error           string          `json:"error"`
			CurrentRevision *uint64         `json:"current_revision"`
		}
		if json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&body) == nil && (publicErrorCode(body.Code) || body.Code == "request_failed") {
			e.Code, e.Message = body.Code, body.Error
			e.Cause = safeRuntimeCause(body.Cause)
			e.PathDiagnostic = body.PathDiagnostic
			e.CurrentRevision = body.CurrentRevision
			// These endpoints return their own sanitized validation reasons.
			specificValidation := method == http.MethodPost && (path == "/api/jobs" || path == "/api/resolve" ||
				body.Code == "file_release_invalid" && (path == "/api/files/release/plan" || path == "/api/files/release/apply" || path == "/api/files/release/claim"))
			if message := rejectionMessages[body.Code]; message != "" && body.Code != "extension_version_conflict" && body.Code != "policy_revision_conflict" && body.Code != "shared_input_cache_capacity" && !specificValidation {
				e.Message = message
			}
		}
		if res.StatusCode == http.StatusNotFound && body.Code == "" && method == http.MethodGet && strings.HasPrefix(path, "/api/jobs/") && !strings.Contains(strings.TrimPrefix(path, "/api/jobs/"), "/") {
			e.Code = "queue_upgrade_required"
			e.Message = "queue too old for this client; upgrade the queue first"
		}
		return e
	}
	turn := res.Header.Get(sharedTurnHeader)
	if confirm {
		if res.StatusCode != http.StatusNoContent || turn != handover {
			return queueStopping()
		}
		return nil
	}
	if proposal || mutation {
		if number, err := strconv.ParseUint(turn, 10, 63); err != nil || number == 0 {
			return queueStopping()
		}
	}
	if mutation {
		if handover == "" {
			if res.StatusCode != http.StatusNoContent {
				return queueStopping()
			}
			return c.callOnceShared(ctx, method, path, in, out, turn)
		}
		if turn != handover || res.StatusCode == http.StatusNoContent {
			return queueStopping()
		}
	}
	if proposal {
		if out == nil {
			// Discard-only callers do not act on a directive.
			_, err = io.Copy(io.Discard, res.Body)
			return err
		}
		dest := reflect.ValueOf(out)
		if dest.Kind() != reflect.Pointer || dest.IsNil() {
			return errors.New("shared queue control requires a response pointer")
		}
		next := reflect.New(dest.Elem().Type())
		if err := json.NewDecoder(res.Body).Decode(next.Interface()); err != nil {
			return err
		}
		if _, err := io.Copy(io.Discard, res.Body); err != nil {
			return err
		}
		// A delayed proposal cannot spend a newer turn. Expose it only
		// after a fresh read confirms the original turn in PostgreSQL.
		if err := c.callOnceShared(ctx, http.MethodPost, sharedTurnPath, nil, nil, turn); err != nil {
			return err
		}
		dest.Elem().Set(next.Elem())
		return nil
	}
	if out != nil {
		if stream, ok := out.(io.Writer); ok {
			return copyJSONDocument(stream, res.Body)
		}
		// Keep a truncated reply from contaminating a retry's destination.
		dest := reflect.ValueOf(out)
		if dest.Kind() == reflect.Pointer && !dest.IsNil() {
			next := reflect.New(dest.Elem().Type())
			if err := json.NewDecoder(res.Body).Decode(next.Interface()); err != nil {
				return err
			}
			dest.Elem().Set(next.Elem())
			return nil
		}
		return json.NewDecoder(res.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, res.Body)
	return err
}

// Forward a JSON document with bounded decoder scratch. Token validation makes
// a truncated successful HTTP body an error even after bytes reached stdout.
func copyJSONDocument(dst io.Writer, src io.Reader) error {
	decoder := json.NewDecoder(io.TeeReader(src, dst))
	first, err := decoder.Token()
	if err != nil {
		return err
	}
	if first != json.Delim('{') && first != json.Delim('[') {
		return errors.New("expected a JSON document")
	}
	depth := 1
	for depth > 0 {
		token, err := decoder.Token()
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("multiple JSON documents")
	}
	return nil
}

type HTTPError struct {
	Cause           *RuntimeCause
	RetryAfter      time.Duration // Server not-before delay, including HTTP-date form.
	PathDiagnostic  *PathDiagnostic
	CurrentRevision *uint64
	JobID           string // Requested or failed job identity, when applicable.
	Code            string
	Status          int
	Message         string
	reason          string // Server-side only: logged with the rejection, never sent.
}

func (e *HTTPError) Error() string {
	interval := ""
	if e.RetryAfter > 0 {
		interval = fmt.Sprintf("; retry after %s (Retry-After)", e.RetryAfter)
	}
	if e.Code == "job_failed" || e.Code == "job_cancelled" {
		return e.Code + ": " + e.Message
	}
	if e.Code != "" {
		return fmt.Sprintf("queue: HTTP %d: %s: %s", e.Status, e.Code, e.Message) + interval
	}
	return fmt.Sprintf("queue: HTTP %d: %s", e.Status, e.Message) + interval
}
func stale(err error) bool {
	var e *HTTPError
	return errors.As(err, &e) && e.Status == http.StatusConflict
}

// Checkpoint publication is a worker report too; GET event streams also bind
// to the current worker protocol. Authentication always runs before this gate.
func workerProtocolEndpoint(path string) bool {
	return strings.HasPrefix(path, "/api/workers/") || path == "/api/files/checkpoint"
}

func respondOperation(w http.ResponseWriter, v any, err error, code string) {
	var rule *rejectionError
	var api *HTTPError
	var writeFailure queueWriteError
	if err != nil && !errors.Is(err, ErrStale) && !errors.As(err, &rule) && !errors.As(err, &api) && !errors.As(err, &writeFailure) {
		err = reject(code, err)
	}
	respond(w, v, err)
}
