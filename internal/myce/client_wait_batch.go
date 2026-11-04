package myce

import (
	"context"
	"errors"
	"strings"
	"time"
)

// WaitBatch observes up to 256 IDs in one transport wait. After is the previous
// reply's revisions; nil returns immediately. Older queues fall back to their
// wait-view GETs. It never fetches full diagnostic/terminal payloads.
func (c Client) WaitBatch(ctx context.Context, ids []string, after map[string]uint64, seconds int) (WaitReply, error) {
	// Stay below the caller transport timeout; an intentional long-poll must
	// not look like a queue outage to a legacy ten-second HTTP client.
	transportBudget := 10 * time.Second
	if c.HTTP != nil && c.HTTP.Timeout > 0 {
		transportBudget = c.HTTP.Timeout
	}
	seconds = min(seconds, int(max(0, transportBudget-time.Second)/time.Second))
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
		return reply, err
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

// DefaultWaitTimeout bounds abandoned synchronous observers even during an
// indefinite transient outage. Long jobs can be observed again by saved ID;
// an earlier caller deadline always wins. This does not cancel queued work.
const DefaultWaitTimeout = 24 * time.Hour

// GetJobWaitCapability preserves one initial diagnostic read while negotiating
// batch waits through a response header ignored by old clients/queues.
func (c Client) GetJobWaitCapability(ctx context.Context, id string) (*Job, bool, error) {
	supported := false
	j, err := c.GetJob(context.WithValue(ctx, waitCapabilityKey{}, &supported), id)
	return j, supported, err
}

type waitCapabilityKey struct{}
