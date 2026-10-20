package myce

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// SubmitAndWait can be called again after a transport error with the same spec
// and submission key. The queue reconciles it to the original job.
func (c Client) SubmitAndWait(ctx context.Context, spec Spec) (*Job, error) {
	if spec.SubmissionKey == "" {
		return nil, errors.New("submit-and-wait requires submission_key")
	}
	var j Job
	if err := c.Call(ctx, "POST", "/api/jobs", spec, &j); err != nil {
		var transport *url.Error
		if !errors.As(err, &transport) || ctx.Err() != nil {
			return nil, err
		}
		// Only an uncertain transport response gets one identical keyed retry.
		if err = c.Call(ctx, "POST", "/api/jobs", spec, &j); err != nil {
			return nil, err
		}
	}
	return c.Wait(ctx, j.ID)
}

func (c Client) GetJob(ctx context.Context, id string) (*Job, error) {
	var s Snapshot
	if err := c.Call(ctx, "GET", "/api/status", nil, &s); err != nil {
		return nil, err
	}
	for _, j := range s.Jobs {
		if j.ID == id {
			return j, nil
		}
	}
	return nil, errors.New("job not found")
}

func (c Client) Wait(ctx context.Context, id string) (*Job, error) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		j, err := c.GetJob(ctx, id)
		if err != nil {
			return nil, err
		}
		if terminal(j.State) {
			return j, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
		}
	}
}

// DownloadResult resolves the accepted current attempt from the queue, rather
// than trusting an output reference supplied by a caller or stale worker.
func (c Client) DownloadResult(ctx context.Context, id, name, dest string) error {
	j, err := c.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if j.State != "succeeded" {
		return fmt.Errorf("job is %s, no accepted result", j.State)
	}
	ref, ok := j.OutputRefs[name]
	if !ok {
		return errors.New("accepted artifact not found")
	}
	return c.DownloadFile(ctx, ref, dest)
}
