package myce

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const startGateProtocol = "durable-start-v1"

type API struct {
	Queue *Queue
	Token string
}

func (a API) Handler() http.Handler {
	mux := http.NewServeMux()
	a.registerDevClock(mux)
	mux.HandleFunc("GET /api/hosts/{id}/observation", func(w http.ResponseWriter, r *http.Request) {
		v, err := a.Queue.ReadHostObservation(r.Context(), r.PathValue("id"))
		respond(w, v, err)
	})
	a.registerFiles(mux)
	mux.HandleFunc("POST /api/extensions", func(w http.ResponseWriter, r *http.Request) {
		var e Extension
		if !decode(w, r, &e) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.AddExtension(e))
	})
	mux.HandleFunc("GET /api/extensions", func(w http.ResponseWriter, r *http.Request) { respond(w, a.Queue.Extensions(), nil) })
	mux.HandleFunc("GET /api/usage", func(w http.ResponseWriter, r *http.Request) {
		d, e := time.ParseDuration(r.URL.Query().Get("since"))
		if e != nil || d <= 0 || d > 90*24*time.Hour {
			respond(w, nil, errors.New("since must be a positive duration up to 2160h"))
			return
		}
		report, e := a.Queue.Usage(a.Queue.clock.Now().Add(-d))
		respond(w, report, e)
	})
	mux.HandleFunc("/models/{name}/{path...}", a.modelCall)
	mux.HandleFunc("GET /api/workers/{id}/events", a.workerEvent)
	mux.HandleFunc("POST /api/models/sync", func(w http.ResponseWriter, r *http.Request) {
		var s SyncRequest
		if !decode(w, r, &s) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.SyncModel(s))
	})
	mux.HandleFunc("POST /api/models/pin", func(w http.ResponseWriter, r *http.Request) {
		var p ModelPin
		if !decode(w, r, &p) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.Pin(p))
	})
	mux.HandleFunc("POST /api/models/unpin", func(w http.ResponseWriter, r *http.Request) {
		var p ModelPin
		if !decode(w, r, &p) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.Unpin(p.Project, p.Model))
	})
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		mux.HandleFunc("POST "+path, a.modelCall)
	}
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		s, err := a.Queue.publicSnapshot()
		if err != nil {
			http.Error(w, "status unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = statusPage.Execute(w, s)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) { s, err := a.Queue.publicSnapshot(); respond(w, s, err) })
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
		respond(w, j, err)
	})
	mux.HandleFunc("POST /api/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		ack, err := a.Queue.CancelJob(r.PathValue("id"))
		respond(w, ack, err)
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
		ack, err := a.Queue.CancelSubmission(request.Project, key)
		respond(w, ack, err)
	})
	mux.HandleFunc("GET /api/jobs/{id}/logs", func(w http.ResponseWriter, r *http.Request) {
		after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		if r.URL.Query().Get("after") == "" {
			after = 0
			err = nil
		}
		if err != nil {
			respond(w, nil, err)
			return
		}
		ls, err := a.Queue.Logs(r.PathValue("id"), after)
		respond(w, ls, err)
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
		v, err := a.Queue.Checkin(c)
		respond(w, v, err)
	})
	mux.HandleFunc("POST /api/workers/start-permit", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		permit, err := a.Queue.PermitStart(c)
		respond(w, permit, err)
	})
	mux.HandleFunc("POST /api/workers/started", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]any{"ok": true, "start_gate": startGateProtocol}, a.Queue.Started(c))
	})
	mux.HandleFunc("POST /api/workers/complete", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.CompleteContext(r.Context(), c))
	})
	mux.HandleFunc("POST /api/workers/environment-log", func(w http.ResponseWriter, r *http.Request) {
		var c EnvironmentLog
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.AppendEnvironmentLog(c))
	})
	mux.HandleFunc("POST /api/workers/log", func(w http.ResponseWriter, r *http.Request) {
		var c LogChunk
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.AppendLog(c))
	})
	mux.HandleFunc("POST /api/hosts/{id}/reconcile", func(w http.ResponseWriter, r *http.Request) {
		var c HostExchange
		if !decode(w, r, &c) {
			return
		}
		if c.Host != r.PathValue("id") {
			respond(w, nil, &hostReconcileError{cause: errors.New("host identity mismatch"), code: "host_reconcile_rejected"})
			return
		}
		v, err := a.Queue.ReconcileHost(c)
		respond(w, v, err)
	})
	mux.HandleFunc("POST /api/workers/{id}/leave", func(w http.ResponseWriter, r *http.Request) {
		var c struct {
			Now bool `json:"now"`
		}
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.Leave(r.PathValue("id"), c.Now))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			http.Error(w, "invalid fleet token", 401)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	limit := int64(1 << 20)
	if r.URL.Path == "/api/extensions" {
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
		http.Error(w, "invalid JSON request", 400)
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
		body := map[string]any{"error": "request failed"}
		var rejected *hostReconcileError
		if errors.As(err, &rejected) {
			status = http.StatusConflict
			body = map[string]any{"error": "host reconciliation rejected; retry", "code": rejected.code, "queue_revision": rejected.revision}
		}
		var apiError *HTTPError
		if errors.As(err, &apiError) && (apiError.Code == "submission_key_conflict" || apiError.Code == "submission_key_expired" || apiError.Code == "submission_key_invalid" || apiError.Code == "job_unknown") {
			status = apiError.Status
			body = map[string]any{"error": apiError.Message, "code": apiError.Code}
			if apiError.Code == "job_unknown" {
				body["will_not_run"] = nil
				body["job_id"], body["project"], body["submission_key"] = apiError.JobID, nil, nil
				body["state"], body["release"], body["acknowledged_at"] = "unknown", nil, nil
			}
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}
func (q *Queue) Run(ctx context.Context) error {
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
	HTTP  *http.Client
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
	if path == "/api/workers/checkin" {
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
			QueueRevision HostRevision `json:"queue_revision"`
			Code          string       `json:"code"`
			Error         string       `json:"error"`
		}
		if json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&body) == nil && (body.Code == "submission_key_conflict" || body.Code == "submission_key_expired" || body.Code == "job_unknown" || body.Code == "host_reconcile_rejected" || body.Code == "host_revision_behind_queue") {
			e.Code, e.Message, e.QueueRevision = body.Code, body.Error, body.QueueRevision
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
	QueueRevision HostRevision
	JobID         string // Only job_unknown carries the requested cancellation identity.
	Code          string
	Status        int
	Message       string
}

func (e *HTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("queue: HTTP %d: %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("queue: HTTP %d: %s", e.Status, e.Message)
}
func stale(err error) bool {
	var e *HTTPError
	return errors.As(err, &e) && e.Status == http.StatusConflict
}
