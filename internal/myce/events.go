package myce

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type WorkerEvent struct {
	Cursor string `json:"cursor"`
	Execution
	GraceSeconds float64 `json:"grace_seconds"`
	Kill         bool    `json:"kill"`
}

func (a API) workerEvent(w http.ResponseWriter, r *http.Request) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		a.Queue.mu.Lock()
		wake := a.Queue.wake
		var event WorkerEvent
		for _, j := range a.Queue.s.Jobs {
			if j.Machine == r.PathValue("id") && j.Session == r.URL.Query().Get("session") && j.State == "stopping" && j.StopAt != nil {
				event = WorkerEvent{Cursor: fmt.Sprintf("%s:%d:%s", j.ID, j.Attempt, j.StopAt.Format(time.RFC3339Nano)), Execution: Execution{ID: j.ID, Attempt: j.Attempt}, GraceSeconds: max(0, j.StopAt.Sub(a.Queue.clock.Now()).Seconds()), Kill: expired(j.StopAt, a.Queue.clock.Now())}
				break
			}
		}
		a.Queue.mu.Unlock()
		if event.Cursor != "" && event.Cursor != r.URL.Query().Get("after") {
			respond(w, event, nil)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			respond(w, WorkerEvent{Cursor: r.URL.Query().Get("after")}, nil)
			return
		case <-wake:
		}
	}
}
func (w Worker) watchStops(ctx context.Context, events chan<- WorkerEvent) {
	after := ""
	for ctx.Err() == nil {
		var event WorkerEvent
		e := w.Client.Call(ctx, "GET", "/api/workers/"+url.PathEscape(w.Machine.ID)+"/events?session="+url.QueryEscape(w.Machine.Session)+"&after="+url.QueryEscape(after), nil, &event)
		if e != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if event.ID != "" && event.Cursor != after {
			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
		after = event.Cursor
	}
}
