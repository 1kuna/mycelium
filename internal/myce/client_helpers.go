package myce

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	return c.WaitSubmitted(ctx, j)
}

// Submit retries only identical keyed intent after an ambiguous transport
// failure or explicit overload, paced by observations until owner cancellation.
func (c Client) Submit(ctx context.Context, spec Spec) (*Job, error) {
	ctx, cancel := c.apiContext(ctx)
	defer cancel()
	var j Job
	pace := ClientPace{}
	for {
		started := c.clock().Now()
		err := c.callOnce(ctx, "POST", "/api/jobs", spec, &j)
		if err == nil {
			return &j, nil
		}
		var transport *url.Error
		var api *HTTPError
		retry := errors.As(err, &transport) && !IsTLSIdentityError(err) || errors.As(err, &api) && (api.Status == 429 || api.Status == 503)
		if spec.SubmissionKey == "" || !retry || ctx.Err() != nil {
			return nil, err
		}
		pace.Observe(c.clock().Now().Sub(started), false)
		if err := c.pause(ctx, pace.Delay(c.clock().Now(), time.Time{}, err, c.Jitter)); err != nil {
			return nil, err
		}
	}
}

// GetJob reads one Job, including MemoryAttempts with each ended attempt's
// observed peak/counters and resolved envelope, independently of learning.
// MemoryAttempts retains the first and latest 31 rows; MemoryAttemptsDropped
// counts evictions. Peakless prelaunch attempts are omitted.
// An empty, non-JSON or incomplete Job reply is the
// typed nonretryable job_reply_invalid; a reply that ends mid-document returns
// io.ErrUnexpectedEOF, which RetryStatusRead retries. The Python client
// detects truncation only from transport framing (Content-Length or chunking).
func (c Client) GetJob(ctx context.Context, id string) (*Job, error) {
	return c.getJob(ctx, id, "")
}

// GetJobForWait returns only identity, state and stored reason while pending,
// and the ordinary full Job at completion. Older queues ignore the query and
// return full replies. Use GetJob for live placement and deadline diagnostics.
func (c Client) GetJobForWait(ctx context.Context, id string) (*Job, error) {
	return c.getJob(ctx, id, "?view=wait")
}

func (c Client) getJob(ctx context.Context, id, query string) (*Job, error) {
	var j Job
	invalid := &HTTPError{Status: 502, Code: "job_reply_invalid", Message: "queue returned an invalid Job reply: nonempty id and state are required"}
	if err := c.Call(ctx, "GET", "/api/jobs/"+url.PathEscape(id)+query, nil, &j); err != nil {
		var syntax *json.SyntaxError
		var fieldType *json.UnmarshalTypeError
		if errors.As(err, &syntax) || errors.As(err, &fieldType) || err == io.EOF {
			return nil, invalid
		}
		return nil, err
	}
	if j.ID == "" || j.State == "" {
		return nil, invalid
	}
	return &j, nil
}

// Wait observes a job through terminal state. Optional updates run on each
// successful job read, including completion, and may drain logs. The first
// read includes diagnostics; later pending updates carry only state and reason. Transient
// read errors from an update use the same retry policy as status reads.
func (c Client) Wait(ctx context.Context, id string, updates ...func(*Job) error) (*Job, error) {
	return c.wait(ctx, id, nil, updates)
}

// WaitSubmitted uses the accepted receipt to tolerate transient visibility
// failures until the owner deadline. It never resubmits or cancels the job.
func (c Client) WaitSubmitted(ctx context.Context, receipt *Job, updates ...func(*Job) error) (*Job, error) {
	if receipt == nil || receipt.ID == "" {
		return nil, errors.New("wait-submitted requires an accepted job receipt")
	}
	return c.wait(ctx, receipt.ID, receipt, updates)
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

// IsTLSIdentityError identifies permanent TLS identity/handshake failures without
// exposing certificate, endpoint or credential diagnostics to notifications.
func IsTLSIdentityError(err error) bool {
	var verification *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	if errors.Is(err, errJoiningIdentity) || errors.As(err, &verification) || errors.As(err, &record) {
		return true
	}
	// Go also emits unexported TLS alert/handshake errors. Network timeouts
	// (including net/http's TLS handshake timeout) retain their retry policy.
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if strings.HasPrefix(cause.Error(), "tls:") || strings.HasPrefix(cause.Error(), "remote error: tls:") {
			return true
		}
	}
	return false
}

// RetryStatusRead shares the CLI wait command's idempotent-read policy.
// Authentication, TLS identity and other permanent HTTP errors return immediately.
func RetryStatusRead(err error) bool {
	if IsTLSIdentityError(err) {
		return false
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.Code == "job_reply_invalid" {
		// Polling cannot repair a reply from the wrong endpoint or protocol.
		return false
	}
	return err != nil && (!errors.As(err, &httpErr) || httpErr.Status >= 500 || httpErr.Status == 408 || httpErr.Status == 429)
}

func (c Client) wait(ctx context.Context, id string, receipt *Job, updates []func(*Job) error) (*Job, error) {
	pace := ClientPace{}
	c = c.WithoutRetries()
	last := receipt
	batchSupported := false
	revisions := map[string]uint64{}
	var initial *Job
	waitError := func(cause error) error {
		err := jobWaitError(id, last, cause)
		if initial != nil && last != nil && initial.State == last.State && initial.Reason != last.Reason {
			return fmt.Errorf("%w; initial status advice: %s; refresh with myce status %s", err, JobReason(initial), id)
		}
		return err
	}
	clock := c.clock()
	var jobDeadline *time.Time
	if receipt != nil {
		jobDeadline = receipt.Spec.Deadline
	}
	for {
		if ctx.Err() != nil {
			return last, waitError(ctx.Err())
		}
		if jobDeadline != nil && !clock.Now().Before(*jobDeadline) {
			return last, waitError(context.DeadlineExceeded)
		}
		requestCtx, cancel := context.WithCancel(ctx)
		if jobDeadline != nil {
			cancel()
			requestCtx, cancel = context.WithTimeout(ctx, jobDeadline.Sub(clock.Now()))
		}
		started := clock.Now()
		read := c.GetJobForWait
		if initial == nil {
			read = c.GetJob
		}
		var j *Job
		var err error
		held := false
		if batchSupported {
			var reply WaitReply
			seconds := pace.Seconds(clock.Now(), ClientJobEvent(initial, clock.Now()))
			reply, err = c.WaitBatch(requestCtx, []string{id}, revisions, seconds)
			if err == nil {
				state := reply.Jobs[0]
				held = seconds > 0 && state.Revision > 0
				revisions[state.ID] = state.Revision
				j = &Job{ID: state.ID, State: state.State, Reason: state.Reason}
				if terminal(j.State) {
					j, err = c.GetJobForWait(requestCtx, state.ID)
				}
			}
		} else if initial == nil {
			j, batchSupported, err = c.GetJobWaitCapability(requestCtx, id)
		} else {
			j, err = read(requestCtx, id)
		}
		cancel()
		if ctx.Err() != nil {
			return last, waitError(ctx.Err())
		}
		if err != nil {
			var api *HTTPError
			if errors.As(err, &api) && api.Status == 404 && api.Code == "job_unknown" && receipt != nil && initial == nil {
				// The accepted keyed receipt is evidence that this job exists.
				// Retry visibility until the owner deadline, without resubmission.
			} else if !RetryStatusRead(err) {
				return last, err
			} else {
				// Retry spacing is learned below.
			}
		} else {
			changed := last == nil || last.State != j.State || last.Reason != j.Reason
			pace.Observe(clock.Now().Sub(started), changed)
			last = j
			if j.Spec.Deadline != nil {
				jobDeadline = j.Spec.Deadline
			}
			if initial != nil && initial.StartedAt == nil && j.State == "running" {
				now := clock.Now()
				initial.StartedAt = &now
			}
			if initial == nil {
				initial = j
			}
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

		}
		if err != nil {
			pace.Observe(clock.Now().Sub(started), false)
		}
		delay := pace.Delay(clock.Now(), ClientJobEvent(initial, clock.Now()), err, c.Jitter)
		if err == nil && held {
			// The negotiated hold paces reads and wakes on changes. Sleeping
			// between holds would leave completion unobserved as pacing grows.
			delay = 0
		}
		if jobDeadline != nil {
			delay = min(delay, max(0, jobDeadline.Sub(clock.Now())))
		}
		if err := c.pause(ctx, delay); err != nil {
			return last, waitError(err)
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
	return c.downloadResultFile(ctx, ref, dest)
}

func (c Client) downloadResultFile(ctx context.Context, ref FileRef, dest string) error {
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
	ctx = WithFileStoreSubmission(ctx, Spec{Project: options.Project, Class: options.Class})
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
		if err = c.UploadJobInput(ctx, source, ref); err != nil {
			return nil, err
		}
		files[name] = ref
	}
	j, err := c.SubmitAndWait(ctx, Spec{AuthoringOrigin: OriginExtension, Name: extension, Project: options.Project, Class: options.Class, Extension: extension, SubmissionKey: options.Key, Files: files, After: options.After})
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

type RunOptions struct {
	Key, Project, Class string
	// After selects one accepted piece, or the whole fan-out parent when Piece is empty.
	After *PieceDependency
}

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
