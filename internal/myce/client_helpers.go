package myce

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SubmitAndWait can be called again after a transport error with the same spec
// and submission key. The queue reconciles it to the original job.
func (c Client) SubmitAndWait(ctx context.Context, spec Spec) (*Job, error) {
	if spec.SubmissionKey == "" {
		return nil, errors.New("submit-and-wait requires submission_key")
	}
	j, err := c.Submit(ctx, spec)
	if err != nil {
		return nil, err
	}
	return c.Wait(ctx, j.ID)
}

// Submit uses the ordinary jobs API. A keyed submission can safely retry one
// uncertain transport response without creating another job.
func (c Client) Submit(ctx context.Context, spec Spec) (*Job, error) {
	var j Job
	if err := c.Call(ctx, "POST", "/api/jobs", spec, &j); err != nil {
		var transport *url.Error
		if spec.SubmissionKey == "" || !errors.As(err, &transport) || ctx.Err() != nil {
			return nil, err
		}
		if err = c.Call(ctx, "POST", "/api/jobs", spec, &j); err != nil {
			return nil, err
		}
	}
	return &j, nil
}

func (c Client) GetJob(ctx context.Context, id string) (*Job, error) {
	var s Snapshot
	if err := c.Call(ctx, "GET", "/api/status", nil, &s); err != nil {
		return nil, err
	}
	return FindJob(s.Jobs, id)
}

// Wait observes a job through terminal state. Optional updates run on each
// successful status read, including completion, and may drain logs. Transient
// read errors from an update use the same retry policy as status reads.
func (c Client) Wait(ctx context.Context, id string, updates ...func(*Job) error) (*Job, error) {
	return c.wait(ctx, id, updates)
}

func waitStatusPause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// RetryStatusRead shares the CLI wait command's idempotent-read policy.
// Authentication/other permanent HTTP errors must still return immediately.
func RetryStatusRead(err error) bool {
	var httpErr *HTTPError
	return err != nil && (!errors.As(err, &httpErr) || httpErr.Status >= 500 || httpErr.Status == 408 || httpErr.Status == 429)
}

func (c Client) wait(ctx context.Context, id string, updates []func(*Job) error) (*Job, error) {
	const poll = time.Second
	delay := poll
	var last *Job
	for {
		if ctx.Err() != nil {
			return last, jobWaitError(id, last, ctx.Err())
		}
		var snapshot Snapshot
		err := c.Call(ctx, "GET", "/api/status", nil, &snapshot)
		if ctx.Err() != nil {
			return last, jobWaitError(id, last, ctx.Err())
		}
		if err != nil {
			if !RetryStatusRead(err) {
				return last, err
			}
			delay = min(delay*2, 10*time.Second)
		} else {
			j, err := FindJob(snapshot.Jobs, id)
			if err != nil {
				return last, err
			}
			last = j
			for _, update := range updates {
				if err = update(j); err != nil {
					break
				}
			}
			if err != nil {
				var httpErr *HTTPError
				var transport *url.Error
				if !RetryStatusRead(err) || (!errors.As(err, &httpErr) && !errors.As(err, &transport)) {
					return last, err
				}
			}
			if err == nil && terminal(j.State) {
				return j, nil
			}
			delay = poll
			if err != nil {
				delay = 2 * poll
			}
		}
		if err := waitStatusPause(ctx, delay); err != nil {
			return last, jobWaitError(id, last, err)
		}
	}
}

// Waiting never cancels the queued/running job. Preserve context error matching
// and the latest authoritative status reason so callers can recover by job ID.
func jobWaitError(id string, j *Job, cause error) error {
	if j == nil {
		return fmt.Errorf("waiting for job %s: %w; status unavailable; job was not cancelled", id, cause)
	}
	return fmt.Errorf("waiting for job %s: %w; job is still %s: %s", j.ID, cause, j.State, JobReason(j))
}

// DownloadResult resolves the accepted current attempt from the queue, rather
// than trusting an output reference supplied by a caller or stale worker.
func (c Client) DownloadResult(ctx context.Context, id, name, dest string) error {
	j, err := c.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if j.FilesExpired {
		return resultExpired()
	}
	if j.State != "succeeded" {
		return &HTTPError{Status: 409, Code: "request_stale", Message: fmt.Sprintf("job is %s, no accepted result", j.State)}
	}
	ref, ok := j.OutputRefs[name]
	if !ok {
		return &HTTPError{Status: 404, Code: "artifact_unknown", Message: "accepted artifact not found"}
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(dest), ".myce-result-*")
	if err != nil {
		return err
	}
	temporary := f.Name()
	defer os.Remove(temporary)
	if err = f.Close(); err != nil {
		return err
	}
	if err = c.DownloadFile(ctx, ref, temporary); err != nil {
		return err
	}
	return os.Rename(temporary, dest)
}

// Run uploads local inputs, submits one keyed extension job, waits and downloads
// its declared outputs into outputs. Repeat the same key and inputs to recover
// a lost response. The extension declares output filenames; no app adapter is needed.
func (c Client) Run(ctx context.Context, extension string, inputs map[string]string, outputs string, options RunOptions) (*Job, error) {
	if extension == "" || outputs == "" || options.Key == "" || options.Project == "" {
		return nil, errors.New("run requires extension, outputs directory, key and project")
	}
	files := FileManifest{}
	for name, source := range inputs {
		if !safeRelative(name) {
			return nil, errors.New("input names must be clean relative paths")
		}
		ref, err := IdentifyFile(source)
		if err != nil {
			return nil, err
		}
		if err = c.UploadFile(ctx, source, ref); err != nil {
			return nil, err
		}
		files[name] = ref
	}
	j, err := c.SubmitAndWait(ctx, Spec{Name: extension, Project: options.Project, Class: options.Class, Extension: extension, SubmissionKey: options.Key, Files: files})
	if err != nil {
		return j, err
	}
	if j.State != "succeeded" {
		return j, &HTTPError{Status: 409, Code: "job_" + j.State, JobID: j.ID, Message: fmt.Sprintf("job %s %s: %s; run myce logs %s for the traceback", ShortID(j.ID), j.State, j.Reason, ShortID(j.ID))}
	}
	for name := range j.OutputRefs {
		if !safeRelative(name) {
			return j, errors.New("result names must be clean relative paths")
		}
	}
	for name := range j.OutputRefs {
		dest, err := localResultPath(outputs, name)
		if err != nil {
			return j, err
		}
		if err = c.DownloadResult(ctx, j.ID, name, dest); err != nil {
			return j, err
		}
	}
	return j, nil
}

type RunOptions struct{ Key, Project, Class string }

// Refuse symlink ancestors so automatic result placement stays in the caller's
// directory. DownloadResult remains available for explicit arbitrary destinations.
func localResultPath(root, name string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	dest := filepath.Join(root, filepath.FromSlash(name))
	for parent := filepath.Dir(dest); parent != root; parent = filepath.Dir(parent) {
		info, err := os.Lstat(parent)
		if err == nil && !info.IsDir() {
			return "", errors.New("result parent must be a directory, not a symlink")
		}
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
	return dest, nil
}

// FindJob accepts an exact ID or an unambiguous prefix of at least eight characters.
func FindJob(jobs []*Job, id string) (*Job, error) {
	for _, j := range jobs {
		if j.ID == id {
			return j, nil
		}
	}
	var match *Job
	if len(id) >= 8 {
		for _, j := range jobs {
			if strings.HasPrefix(j.ID, id) {
				if match != nil && match.ID != j.ID {
					return nil, &HTTPError{Status: 409, Code: "job_id_ambiguous", Message: "job ID prefix is ambiguous; use a longer ID"}
				}
				match = j
			}
		}
	}
	if match != nil {
		return match, nil
	}
	return nil, &HTTPError{Status: 404, Code: "job_unknown", Message: "job is unknown; use an exact ID or a unique prefix of at least eight characters", JobID: id}
}
