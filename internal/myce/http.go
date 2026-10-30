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
	"net/http"
	"strconv"
	"strings"
	"time"
)

const startGateProtocol = "host-launch-v3"

type API struct {
	Queue *Queue
	Token string
}

func (a API) Handler() http.Handler { return a.handler("") }

func (a API) HandlerWithJoiningIdentity(pin string) http.Handler { return a.handler(pin) }

func (a API) handler(joiningPin string) http.Handler {
	mux := newRequestMux()
	// Joining needs authenticated identity/version only, never the growing status
	// snapshot. Transport pinning authenticates this identity to joining clients.
	mux.HandleFunc("GET /api/ping", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]any{"version": 1, "fingerprint": joiningPin}, nil)
	})
	mux.HandleFunc("GET /api/portability", func(w http.ResponseWriter, r *http.Request) {
		report, err := a.Queue.Portability(PortabilityRequest{Extension: r.URL.Query().Get("extension"), Project: r.URL.Query().Get("project")})
		respondOperation(w, report, err, "portability_invalid")
	})
	mux.HandleFunc("POST /api/portability", func(w http.ResponseWriter, r *http.Request) {
		var request PortabilityRequest
		if !decode(w, r, &request) {
			return
		}
		report, err := a.Queue.Portability(request)
		respondOperation(w, report, err, "portability_invalid")
	})
	a.registerInvites(mux, joiningPin)
	a.registerEnrollmentRelease(mux)
	a.registerEnrollment(mux)
	a.registerFleetUpgrade(mux)
	a.registerDevClock(mux)
	mux.HandleFunc("POST /api/files/result", a.deliveryResultHandler)
	a.registerHostMaintenance(mux)
	a.registerDeviceMaintenance(mux)
	a.registerComponentExclusions(mux)
	a.registerReservations(mux)
	a.registerPriorityPolicy(mux)
	a.registerFiles(mux)
	mux.HandleFunc("POST /api/extensions", func(w http.ResponseWriter, r *http.Request) {
		var e Extension
		if !decode(w, r, &e) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.AddExtension(e, r.Context()), "extension_invalid")
	})
	mux.HandleFunc("GET /api/extensions/{digest}", func(w http.ResponseWriter, r *http.Request) {
		e, err := a.Queue.extensionByDigest(r.PathValue("digest"), r.Context())
		respondOperation(w, e, err, "extension_unknown")
	})
	mux.HandleFunc("GET /api/extensions", func(w http.ResponseWriter, r *http.Request) { respond(w, a.Queue.Extensions(r.Context()), nil) })
	mux.HandleFunc("GET /api/usage", func(w http.ResponseWriter, r *http.Request) {
		d, e := time.ParseDuration(r.URL.Query().Get("since"))
		if e != nil || d <= 0 || d > 90*24*time.Hour {
			respond(w, nil, reject("usage_invalid", nil))
			return
		}
		report, e := a.Queue.Usage(a.Queue.clock.Now().Add(-d), r.Context())
		respondOperation(w, report, e, "usage_unavailable")
	})
	mux.HandleFunc("/models/{name}/{path...}", a.modelCall)
	mux.HandleFunc("GET /api/workers/{id}/events", a.workerEvent)
	mux.HandleFunc("POST /api/models/sync", func(w http.ResponseWriter, r *http.Request) {
		var s SyncRequest
		if !decode(w, r, &s) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.SyncModel(s, r.Context()), "model_sync_invalid")
	})
	mux.HandleFunc("POST /api/models/pin", func(w http.ResponseWriter, r *http.Request) {
		var p ModelPin
		if !decode(w, r, &p) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.Pin(p, r.Context()), "model_pin_invalid")
	})
	mux.HandleFunc("POST /api/models/unpin", func(w http.ResponseWriter, r *http.Request) {
		var p ModelPin
		if !decode(w, r, &p) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.Unpin(p.Project, p.Model, r.Context()), "model_unpin_invalid")
	})
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		mux.HandleFunc("POST "+path, a.modelCall)
	}
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		s, err := a.Queue.cachedPublicSnapshot(r.Context())
		if err != nil {
			http.Error(w, "status unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = statusPage.Execute(w, s)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		data, err := a.Queue.cachedStatusJSON(r.Context())
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
	mux.HandleFunc("GET /api/inspection", func(w http.ResponseWriter, r *http.Request) {
		s, err := a.Queue.publicSnapshot(r.Context())
		respondOperation(w, s, err, "status_unavailable")
	})
	mux.HandleFunc("GET /api/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		j, err := a.Queue.jobSnapshot(r.PathValue("id"), r.Context())
		// Keep the GET execution payload empty without changing Spec's JSON
		// encoding, which also defines durable submission fingerprints.
		type replySpec struct {
			Spec
			Run struct{} `json:"run"`
		}
		reply := struct {
			*Job
			Spec replySpec `json:"spec"`
		}{Job: j}
		if j != nil {
			reply.Spec.Spec = j.Spec
		}
		respondOperation(w, reply, err, "status_unavailable")
	})
	mux.HandleFunc("GET /api/jobs/{id}/ledger", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		values := []int{0, -1, 100}
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
		j, err := a.Queue.SubmitContext(r.Context(), request.Spec)
		if j != nil {
			j.LedgerEntries = nil
		}
		respondOperation(w, j, err, "job_submit_failed")
	})
	mux.HandleFunc("POST /api/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		ack, err := a.Queue.CancelJob(r.PathValue("id"), r.Context())
		respondOperation(w, ack, err, "job_cancel_invalid")
	})
	mux.HandleFunc("POST /api/jobs/cancel", func(w http.ResponseWriter, r *http.Request) {
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
	})
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
		v, err := a.Queue.CheckinContext(r.Context(), c)
		respondOperation(w, v, err, "worker_checkin_invalid")
	})
	mux.HandleFunc("POST /api/workers/launch", func(w http.ResponseWriter, r *http.Request) {
		var l HostLaunch
		if !decode(w, r, &l) {
			return
		}
		respondOperation(w, map[string]any{"ok": true, "start_gate": startGateProtocol}, a.Queue.authorizeLaunchContext(r.Context(), l), "worker_launch_invalid")
	})
	mux.HandleFunc("POST /api/workers/native-custody", func(w http.ResponseWriter, r *http.Request) {
		var c NativeCustodyReport
		if !decode(w, r, &c) {
			return
		}
		respondOperation(w, map[string]any{"ok": true, "start_gate": startGateProtocol}, a.Queue.NativeCustody(c, r.Context()), "native_custody_invalid")
	})
	mux.HandleFunc("POST /api/workers/started", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		respondOperation(w, map[string]any{"ok": true, "start_gate": startGateProtocol}, a.Queue.Started(c, r.Context()), "worker_start_invalid")
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
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.AppendLog(c, r.Context()), "worker_log_invalid")
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
	return a.Queue.measureRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		mux.ServeHTTP(w, r)
	}), mux.patterns)
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	limit := int64(1 << 20)
	if r.URL.Path == "/api/extensions" || r.URL.Path == "/api/portability" {
		limit = 2 << 20
	} // base64 transport of a <=1 MiB folder
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	d.DisallowUnknownFields()
	err := d.Decode(v)
	if err == nil {
		var extra any
		if d.Decode(&extra) != io.EOF {
			err = errors.New("expected one JSON object")
		}
	}
	if err != nil {
		var rejected *rejectionError
		if !errors.As(err, &rejected) {
			err = reject("request_invalid", err)
		}
		respond(w, nil, err)
		return false
	}
	return true
}
func respond(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		status := http.StatusBadRequest
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
		var apiError *HTTPError
		if errors.As(err, &apiError) && (fileCapacityCode(apiError.Code) || rejectionMessages[apiError.Code] != "" || apiError.Code == "job_spec_invalid" || apiError.Code == "worker_protocol_invalid" || apiError.Code == "submission_key_conflict" || apiError.Code == "submission_key_expired" || apiError.Code == "submission_key_invalid" || apiError.Code == "job_unknown") {
			status = apiError.Status
			body = map[string]any{"error": apiError.Message, "code": apiError.Code}
			if apiError.CurrentRevision != nil {
				body["current_revision"] = *apiError.CurrentRevision
			}
			if apiError.Code == "job_unknown" {
				body["will_not_run"] = nil
				body["job_id"], body["project"], body["submission_key"] = apiError.JobID, nil, nil
				body["state"], body["release"], body["acknowledged_at"] = "unknown", nil, nil
			}
		}
		log.Printf("API rejection status=%d code=%s", status, body["code"])
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}
func (q *Queue) Run(ctx context.Context) error {
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
			if err := q.Tick(); err != nil {
				return err
			}
		}
	}
}

type Client struct {
	URL   string
	Token string
	HTTP  *http.Client // Its Timeout bounds API calls; file transfers use progress budgets.
	// Tests shorten the file transfer budgets; zero uses the defaults.
	transferIdle time.Duration
}

func (c Client) Call(ctx context.Context, method, path string, in, out any) error {
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
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	if workerProtocolEndpoint(path) {
		req.Header.Set("X-Myce-Start-Gate", startGateProtocol)
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		e := &HTTPError{Status: res.StatusCode, Message: "request failed"}
		var body struct {
			Code            string  `json:"code"`
			Error           string  `json:"error"`
			CurrentRevision *uint64 `json:"current_revision"`
		}
		if json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&body) == nil && (fileCapacityCode(body.Code) || rejectionMessages[body.Code] != "" || body.Code == "job_spec_invalid" || body.Code == "submission_key_invalid" || body.Code == "worker_protocol_mismatch" || body.Code == "worker_protocol_invalid" || body.Code == "submission_key_conflict" || body.Code == "submission_key_expired" || body.Code == "job_unknown") {
			e.Code, e.Message = body.Code, body.Error
			e.CurrentRevision = body.CurrentRevision
			// The queue returns specific, safe submission validation reasons.
			if message := rejectionMessages[body.Code]; message != "" && body.Code != "extension_version_conflict" && body.Code != "policy_revision_conflict" && !(method == http.MethodPost && path == "/api/jobs") {
				e.Message = message
			}
		}
		if res.StatusCode == http.StatusNotFound && body.Code == "" && method == http.MethodGet && strings.HasPrefix(path, "/api/jobs/") && !strings.Contains(strings.TrimPrefix(path, "/api/jobs/"), "/") {
			e.Code = "queue_upgrade_required"
			e.Message = "queue too old for this client; upgrade the queue first"
		}
		return e
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, res.Body)
	return err
}

type HTTPError struct {
	CurrentRevision *uint64
	JobID           string // Requested or failed job identity, when applicable.
	Code            string
	Status          int
	Message         string
}

func (e *HTTPError) Error() string {
	if e.Code == "job_failed" || e.Code == "job_cancelled" {
		return e.Code + ": " + e.Message
	}
	if e.Code != "" {
		return fmt.Sprintf("queue: HTTP %d: %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("queue: HTTP %d: %s", e.Status, e.Message)
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
	if err != nil && !errors.Is(err, ErrStale) && !errors.As(err, &rule) && !errors.As(err, &api) {
		err = reject(code, err)
	}
	respond(w, v, err)
}
