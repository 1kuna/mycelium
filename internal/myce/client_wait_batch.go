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
	var api *HTTPError
	if errors.As(err, &api) && (api.Status == 404 && api.Code == "" || api.Status == 405) {
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
// batch waits through a response header ignored by old clients/queues.
func (c Client) GetJobWaitCapability(ctx context.Context, id string) (*Job, bool, error) {
	supported := false
	j, err := c.GetJob(context.WithValue(ctx, waitCapabilityKey{}, &supported), id)
	return j, supported, err
}

type waitCapabilityKey struct{}
