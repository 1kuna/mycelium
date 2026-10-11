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

type API struct {
	Queue *Queue
	Token string
}

func (a API) Handler() http.Handler {
	mux := http.NewServeMux()
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
		s, err := a.Queue.Snapshot()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = statusPage.Execute(w, s)
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
	mux.HandleFunc("POST /api/hosts/{id}/disable", func(w http.ResponseWriter, r *http.Request) {
		var c struct {
			Reason     string   `json:"reason"`
			Cleared    bool     `json:"cleared"`
			Executions []Result `json:"executions,omitempty"`
		}
		if !decode(w, r, &c) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.DisableHostExecutions(r.PathValue("id"), c.Reason, c.Cleared, c.Executions))
	})
	mux.HandleFunc("POST /api/hosts/{id}/enable", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]bool{"ok": true}, a.Queue.EnableHost(r.PathValue("id")))
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
		if (strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/v1/") || strings.HasPrefix(r.URL.Path, "/models/")) && (a.Token == "" || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(a.Token)) != 1) {
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
