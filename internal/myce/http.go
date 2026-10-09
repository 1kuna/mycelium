package myce

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type API struct {
	Queue *Queue
	Token string
}

var page = template.Must(template.New("status").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5"><title>Mycelium queue</title>
<style>
body{font:16px system-ui;margin:2rem;color:#182820;background:#f6f8f5}
.tables{overflow-x:auto}table{border-collapse:collapse;width:100%;margin-bottom:2rem}
td,th{text-align:left;padding:.6rem;border-bottom:1px solid #ccd5cd}
td{overflow-wrap:anywhere}code{font-size:.8rem}h1{font-weight:600}.reason{max-width:30rem}
@media(max-width:650px){body{margin:1rem}table,tbody,tr,td{display:block}
th{display:none}tr{margin-bottom:1rem;border:1px solid #ccd5cd;border-radius:6px}
td{display:grid;grid-template-columns:6rem minmax(0,1fr);gap:.5rem;border-bottom:0}
td:before{content:attr(data-label);font-weight:600}.reason{max-width:none}}
</style></head><body>
<h1>Mycelium queue</h1><p>Batch jobs and model calls · refreshes every 5 seconds</p>
{{if .Warning}}<p role="status">{{.Warning}}</p>{{end}}<h2>Jobs</h2><div class="tables"><table>
<tr><th>Job / ID</th><th>Priority</th><th>State</th><th>Machine</th><th>Attempt</th><th>Why</th></tr>
{{range .Jobs}}<tr>
<td data-label="Job"><div>{{.Spec.Name}}<br><code>{{.ID}}</code></div></td>
<td data-label="Priority">{{.Spec.Priority}}</td><td data-label="State">{{.State}}</td>
<td data-label="Machine">{{.Machine}}</td><td data-label="Attempt">{{.Attempt}}</td>
<td data-label="Why" class="reason">{{.Reason}}{{if .StartedAt}}{{if eq .Spec.Priority "production"}}<br>Start latency: {{printf "%.3f" .StartLatencySeconds}}s{{end}}{{end}}</td></tr>
{{else}}<tr><td colspan="6">No jobs yet. Submit a job.yaml to get started.</td></tr>{{end}}
</table></div><h2>Fleet</h2><div class="tables"><table>
<tr><th>Machine</th><th>GPU</th><th>Free GB</th><th>State</th><th>Ready environments</th></tr>
{{range .Machines}}<tr><td data-label="Machine">{{.ID}}</td><td data-label="GPU">{{.GPU}}</td>
<td data-label="Free GB">{{.FreeMemoryGB}}{{if .MemoryError}}; memory unavailable: {{.MemoryError}}{{end}}</td>
<td data-label="State">{{if .Gone}}gone{{else if .Leaving}}leaving{{else}}joined{{end}}</td>
<td data-label="Ready envs">{{range .Environments}}{{.}} {{end}}<br>Cached: {{range .CachedModels}}{{.}} {{else}}none{{end}}<br>Cache free: {{.CacheFreeBytes}} bytes{{if .CacheError}}<br>Cache error: {{.CacheError}}{{end}}</td></tr>
{{else}}<tr><td colspan="5">No machines joined.</td></tr>{{end}}
</table></div><h2>Models</h2><table><tr><th>Model</th><th>Machine</th><th>State</th><th>Idle seconds</th><th>Calls in flight</th><th>Error</th></tr>{{range .LoadedModels}}<tr><td data-label="Model">{{.Model}}</td><td data-label="Machine">{{.Machine}}</td><td data-label="State">{{.State}}</td><td data-label="Idle seconds">{{printf "%.1f" .IdleSeconds}}</td><td data-label="Calls">{{.InFlight}}</td><td data-label="Error">{{.Error}}</td></tr>{{else}}<tr><td colspan="6">No models loaded.</td></tr>{{end}}</table><h2>Production pins</h2><div class="tables"><table><tr><th>Project / model</th><th>Target</th><th>Assigned</th><th>Window</th></tr>{{range .Pins}}<tr><td data-label="Project">{{.Project}} / {{.Model}}</td><td data-label="Target">{{.Machine}}{{.Platform}}</td><td data-label="Assigned">{{if .Assigned}}{{.Assigned}}{{else}}waiting / outside window{{end}}</td><td data-label="Window">{{if .Start}}{{.Start}}–{{.End}} {{.Timezone}}; weekdays {{.Weekdays}}{{else}}always{{end}}</td></tr>{{else}}<tr><td colspan="4">No production pins.</td></tr>{{end}}</table></div></body></html>`))

func (a API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/workers/{id}/events", a.workerEvent)
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
		s, err := a.Queue.Snapshot()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = page.Execute(w, s)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) { s, err := a.Queue.Snapshot(); respond(w, s, err) })
	mux.HandleFunc("POST /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		var s Spec
		if !decode(w, r, &s) {
			return
		}
		j, err := a.Queue.Submit(s)
		respond(w, j, err)
	})
	mux.HandleFunc("POST /api/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]bool{"ok": true}, a.Queue.Cancel(r.PathValue("id")))
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
		var c Checkin
		if !decode(w, r, &c) {
			return
		}
		v, err := a.Queue.Checkin(c)
		respond(w, v, err)
	})
	mux.HandleFunc("POST /api/workers/started", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.Started(c))
	})
	mux.HandleFunc("POST /api/workers/complete", func(w http.ResponseWriter, r *http.Request) {
		var c Result
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.Complete(c))
	})
	mux.HandleFunc("POST /api/workers/log", func(w http.ResponseWriter, r *http.Request) {
		var c LogChunk
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.AppendLog(c))
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
		if (strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/v1/")) && (a.Token == "" || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(a.Token)) != 1) {
			http.Error(w, "invalid fleet token", 401)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	err := d.Decode(v)
	if err == nil {
		var extra any
		if d.Decode(&extra) != io.EOF {
			err = errors.New("expected one JSON object")
		}
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
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
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
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
		data, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return &HTTPError{Status: res.StatusCode, Message: string(data)}
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, res.Body)
	return err
}

type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("queue: HTTP %d: %s", e.Status, e.Message) }
func stale(err error) bool {
	var e *HTTPError
	return errors.As(err, &e) && e.Status == http.StatusConflict
}
