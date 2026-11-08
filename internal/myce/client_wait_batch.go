package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

// WaitBatch observes up to 256 IDs in one transport wait. After is the previous
// reply's revisions; nil returns immediately. Older queues fall back to their
// wait-view GETs. It never fetches full diagnostic/terminal payloads.
func (c Client) WaitBatch(ctx context.Context, ids []string, after map[string]uint64, seconds int) (WaitReply, error) {
	return c.waitBatch(ctx, ids, after, seconds, true)
}

// WaitBatchOnly probes the batch endpoint without buffering legacy per-job reads.
// Multi-job observers use this to apply legacy observations and deadlines one at a time.
func (c Client) WaitBatchOnly(ctx context.Context, ids []string, after map[string]uint64, seconds int) (WaitReply, error) {
	return c.waitBatch(ctx, ids, after, seconds, false)
}

// WaitBatchUnsupported identifies an old queue without hiding job_unknown replies.
func WaitBatchUnsupported(err error) bool {
	var api *HTTPError
	return errors.As(err, &api) && (api.Status == 404 && api.Code == "" || api.Status == 405)
}

func (c Client) waitBatch(ctx context.Context, ids []string, after map[string]uint64, seconds int, fallback bool) (WaitReply, error) {
	// batch-v1 defines a maximum 20-second hold. Caller budgets reserve a
	// proportional share for transit rather than a fixed one-second margin.
	seconds = min(20, max(0, seconds))
	if c.HTTP != nil && c.HTTP.Timeout > 0 {
		seconds = min(seconds, int(c.HTTP.Timeout/2/time.Second))
	}
	if end, ok := ctx.Deadline(); ok {
		seconds = min(seconds, int(max(0, end.Sub(c.clock().Now()))/2/time.Second))
	}
	var reply WaitReply
	err := c.Call(ctx, "POST", "/api/wait", WaitRequest{IDs: ids, After: after, Seconds: seconds}, &reply)
	if fallback && WaitBatchUnsupported(err) {
		for _, id := range ids {
			read := c.GetJobForWait
			if _, ok := after[id]; !ok {
				read = c.GetJob
			}
			j, e := read(ctx, id)
			if e != nil {
				return WaitReply{}, e
			}
			reply.Jobs = append(reply.Jobs, WaitState{ID: j.ID, State: j.State, Reason: j.Reason})
		}
		return reply, nil
	}
	if err != nil {
		var syntax *json.SyntaxError
		var fieldType *json.UnmarshalTypeError
		if errors.As(err, &syntax) || errors.As(err, &fieldType) || err == io.EOF {
			return WaitReply{}, invalidWaitReply()
		}
		return WaitReply{}, err
	}
	if len(reply.Jobs) != len(ids) {
		return WaitReply{}, invalidWaitReply()
	}
	for i, j := range reply.Jobs {
		if j.ID == "" || j.State == "" || j.Revision == 0 || (j.ID != ids[i] && !(len(ids[i]) >= 8 && strings.HasPrefix(j.ID, ids[i]))) {
			return WaitReply{}, invalidWaitReply()
		}
	}
	return reply, nil
}
func invalidWaitReply() error {
	return &HTTPError{Status: 502, Code: "job_reply_invalid", Message: "queue returned an invalid wait reply"}
}

// GetJobWaitCapability preserves one initial diagnostic read while negotiating
// batch waits through a response header ignored by old clients/queues. A shed
// bootstrap switches to the scalar endpoint; placement/spec metadata is then
// unavailable, while queue execution deadlines and caller bounds still apply.
func (c Client) GetJobWaitCapability(ctx context.Context, id string) (*Job, bool, error) {
	supported := false
	j, err := c.GetJob(context.WithValue(ctx, waitCapabilityKey{}, &supported), id)
	var api *HTTPError
	if errors.As(err, &api) && api.Status == 429 {
		// Diagnostic bootstrap is expendable under control pressure. Probe the
		// shed-exempt scalar endpoint instead of retrying this GET forever.
		reply, waitErr := c.WaitBatch(ctx, []string{id}, nil, 0)
		if waitErr != nil {
			// A transient probe failure must retry the batch route, too.
			return nil, RetryStatusRead(waitErr), waitErr
		}
		state := reply.Jobs[0]
		if state.Revision == 0 {
			// An older queue used the GET fallback; retain the original retry
			// signal rather than mistaking that reply for batch support.
			return nil, false, err
		}
		j = &Job{ID: state.ID, State: state.State, Reason: state.Reason}
		if terminal(state.State) {
			j, waitErr = c.GetJobForWait(ctx, state.ID)
		}
		return j, true, waitErr
	}
	return j, supported, err
}

type waitCapabilityKey struct{}
