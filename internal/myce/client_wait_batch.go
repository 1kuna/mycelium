package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

// WaitBatchMaxSeconds is the batch-v1 maximum observation hold.
const WaitBatchMaxSeconds = 20

// WaitBatch observes up to 256 IDs using the batch-v1 contract shared by
// current and previous releases. Terminal payloads are fetched separately.
func (c Client) WaitBatch(ctx context.Context, ids []string, after map[string]uint64, seconds int) (WaitReply, error) {
	// batch-v1 defines a maximum 20-second hold. Caller budgets reserve a
	// proportional share for transit rather than a fixed one-second margin.
	seconds = min(WaitBatchMaxSeconds, max(0, seconds))
	if c.HTTP != nil && c.HTTP.Timeout > 0 {
		seconds = min(seconds, int(c.HTTP.Timeout/2/time.Second))
	}
	if end, ok := ctx.Deadline(); ok {
		seconds = min(seconds, int(max(0, end.Sub(c.clock().Now()))/2/time.Second))
	}
	var reply WaitReply
	err := c.Call(ctx, "POST", "/api/wait", WaitRequest{IDs: ids, After: after, Seconds: seconds}, &reply)
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

// GetJobWaitCapability preserves one initial diagnostic read. Both supported
// releases implement batch-v1. A shed bootstrap switches to that endpoint;
// scalar deadline metadata and caller bounds remain authoritative.
func (c Client) GetJobWaitCapability(ctx context.Context, id string) (*Job, bool, error) {
	j, err := c.GetJob(ctx, id)
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
		j = state.Job()
		if terminal(state.State) {
			j, waitErr = c.GetJobForWait(ctx, state.ID)
		}
		return j, true, waitErr
	}
	return j, true, err
}
