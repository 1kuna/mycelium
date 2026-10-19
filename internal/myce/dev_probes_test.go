package myce

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These compose existing component tests and the real dev runtime. There is no
// second scheduler, service harness or consumer database in the probe suite.
func devProbe(t *testing.T) (*Dev, DevConfig, context.Context) {
	t.Helper()
	root := newTestStateRoot(t)
	c := DevConfig{Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "probe-token", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}
	t.Setenv("MYCE_TOKEN", c.Token)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	d, err := StartDev(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, c, ctx
}

func probeWait(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal("probe effect deadline:", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestDevProbeQueueRestart(t *testing.T) {
	d, c, ctx := devProbe(t)
	marker := filepath.Join(filepath.Dir(c.Database), "once")
	s := spec("experiment")
	s.Resumable, s.SubmissionKey = false, "lost-post"
	s.Run.Command = []string{"sh", "-c", `printf x >> "$1"`, "fixture", marker}
	var accepted Job
	// Lose the HTTP response after the real server commits the submission.
	lost := d.Client()
	lost.HTTP = &http.Client{Transport: lostDevSubmitResponse{http.DefaultTransport}}
	if err := lost.Call(ctx, "POST", "/api/jobs", s, nil); err == nil {
		t.Fatal("lost POST returned a response")
	}
	committed, err := d.Queue.Snapshot()
	if err != nil || len(committed.Jobs) != 1 || committed.Jobs[0].Spec.SubmissionKey != s.SubmissionKey {
		t.Fatal("lost response did not leave exactly one committed job", err)
	}
	if err := d.Client().Call(ctx, "POST", "/api/jobs", s, &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.ID != committed.Jobs[0].ID {
		t.Fatal("lost response replay changed job identity")
	}
	probeWait(t, ctx, func() bool {
		snapshot, err := d.Queue.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, job := range snapshot.Jobs {
			if job.ID == accepted.ID {
				return job.State == "succeeded" && job.ReleaseConfirmed
			}
		}
		return false
	})
	consumer := probeConsumer{project: s.Project, key: "reserved-offline", execution: Execution{ID: accepted.ID, Attempt: 1}}
	consumer.cancelPending = true
	d.stopQueue()
	if err := d.server.Close(); err != nil {
		t.Fatal(err)
	}
	d.wg.Wait()
	if err := d.Client().Call(ctx, "GET", "/api/status", nil, nil); err == nil {
		t.Fatal("queue outage not exercised")
	}
	if err := d.Client().Call(ctx, "POST", "/api/hosts/dev-host/maintenance", HostMaintenanceRequest{Action: "disable", Stop: true}, nil); err == nil {
		t.Fatal("offline maintenance succeeded")
	}
	clock := d.Queue.clock
	if err = d.Queue.Close(); err != nil {
		t.Fatal(err)
	}
	q, err := Open(c.Database, c.Checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	d.Queue = q
	listener, err := net.Listen("tcp", strings.TrimPrefix(d.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	d.server = &http.Server{Handler: API{Queue: q, Token: c.Token}.Handler(), ReadHeaderTimeout: time.Second}
	queueCtx, stop := context.WithCancel(ctx)
	d.stopQueue = stop
	d.wg.Add(2)
	go func() { defer d.wg.Done(); _ = d.server.Serve(listener) }()
	go func() { defer d.wg.Done(); _ = q.Run(queueCtx) }()
	if _, err := q.ChangeHostMaintenance("dev-host", HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	ack, err := d.Client().CancelSubmission(ctx, consumer.project, consumer.key)
	if err != nil || !consumer.completeCancellation(ack) || ack.State != "absent" {
		t.Fatal("reservation acknowledgement", ack, err)
	}
	reserved := s
	reserved.SubmissionKey = consumer.key
	var cancelled Job
	if err = d.Client().Call(ctx, "POST", "/api/jobs", reserved, &cancelled); err != nil || cancelled.State != "cancelled" || cancelled.Attempt != 0 {
		t.Fatal("reservation lost precedence", cancelled, err)
	}
	// Exercise both serializations and a real concurrent API race while drained.
	for _, order := range []string{"cancel-first", "submit-first", "concurrent"} {
		raced := s
		raced.SubmissionKey = order
		submit := func() error { return d.Client().Call(ctx, "POST", "/api/jobs", raced, nil) }
		cancel := func() error {
			_, err := d.Client().CancelSubmission(ctx, raced.Project, raced.SubmissionKey)
			return err
		}
		if order == "concurrent" {
			start, done := make(chan struct{}), make(chan error, 2)
			go func() { <-start; done <- submit() }()
			go func() { <-start; done <- cancel() }()
			close(start)
			for range 2 {
				if err := <-done; err != nil {
					t.Fatal("submit/cancel race", err)
				}
			}
		} else {
			first, second := cancel, submit
			if order == "submit-first" {
				first, second = submit, cancel
			}
			if err := first(); err != nil {
				t.Fatal(err)
			}
			if err := second(); err != nil {
				t.Fatal(err)
			}
		}
		var replay Job
		if err := d.Client().Call(ctx, "POST", "/api/jobs", raced, &replay); err != nil || replay.State != "cancelled" || replay.Attempt != 0 {
			t.Fatal("submit/cancel admitted work", order, replay, err)
		}
	}
	var replay Job
	if err = d.Client().Call(ctx, "POST", "/api/jobs", s, &replay); err != nil || replay.ID != accepted.ID || replay.State != "succeeded" || replay.Attempt != 1 {
		t.Fatal("lost POST replay reran", replay, err)
	}
	conflict := s
	conflict.Name = "changed-body"
	err = d.Client().Call(ctx, "POST", "/api/jobs", conflict, nil)
	var apiError *HTTPError
	if !errors.As(err, &apiError) || apiError.Status != 409 || apiError.Code != "submission_key_conflict" {
		t.Fatal("changed body did not conflict", err)
	}
	if consumer.publish(consumer.execution) {
		t.Fatal("cancelled consumer published")
	}
	if _, err := q.ChangeHostMaintenance("dev-host", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool { st, err := q.HostMaintenanceStatus("dev-host"); return err == nil && st.Healthy })
	if effect, err := os.ReadFile(marker); err != nil || string(effect) != "x" {
		t.Fatal("completed command duplicated or missing", err)
	}
	// Expire only an unbound reservation; retained cancelled jobs remain bound.
	if ack, err = d.Client().CancelSubmission(ctx, s.Project, "expires"); err != nil || ack.State != "absent" {
		t.Fatal(ack, err)
	}
	if err = d.Client().Call(ctx, "POST", "/api/dev/clock/advance", map[string]float64{"seconds": (30*24*time.Hour + time.Second).Seconds()}, nil); err != nil {
		t.Fatal(err)
	}
	expired := s
	expired.SubmissionKey = "expires"
	err = d.Client().Call(ctx, "POST", "/api/jobs", expired, nil)
	if !errors.As(err, &apiError) || apiError.Status != 410 || apiError.Code != "submission_key_expired" {
		t.Fatal("reservation did not expire", err)
	}
	if ack, err = d.Client().CancelSubmission(ctx, s.Project, "expires"); err != nil || ack.State != "unknown" || ack.Release != nil {
		t.Fatal(ack, err)
	}
	if ack, err = d.Client().CancelSubmission(ctx, s.Project, "unseen"); err != nil || ack.State != "absent" {
		t.Fatal(ack, err)
	}
	if err = d.Client().Call(ctx, "POST", "/api/jobs", reserved, &replay); err != nil || replay.ID != cancelled.ID || replay.State != "cancelled" {
		t.Fatal("retained cancellation expired", replay, err)
	}
	t.Log("PASS: real queue stop/reopen, lost POST replay once, 409, cancellation reservation, consumer gate, 30d reservation expiry")
}

type lostDevSubmitResponse struct{ base http.RoundTripper }

func (x lostDevSubmitResponse) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := x.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return nil, errors.New("fixture lost committed POST response")
}

type probeConsumer struct {
	project, key             string
	execution                Execution
	cancelPending, cancelled bool
}

func (c *probeConsumer) completeCancellation(ack Cancellation) bool {
	if ack.Project != c.project || ack.SubmissionKey == nil || *ack.SubmissionKey != c.key || !ack.WillNotRun || ack.Release == nil || (*ack.Release != "confirmed" && *ack.Release != "not_applicable") {
		return false
	}
	c.cancelPending, c.cancelled = false, true
	return true
}
func (c probeConsumer) publish(execution Execution) bool {
	return !c.cancelPending && !c.cancelled && execution == c.execution
}

// A process restart keeps durable queue admission disabled. Fresh reports from
// the restarted worker restore health without a local registry or rest proof.
func TestDevProbeRebootResume(t *testing.T) {
	root := newTestStateRoot(t)
	c := DevConfig{Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "reboot-fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	d, err := StartDev(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	probeWait(t, ctx, func() bool { s, err := d.Queue.HostMaintenanceStatus("dev-host"); return err == nil && s.Healthy })
	before, err := d.Queue.HostMaintenanceStatus("dev-host")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Queue.ChangeHostMaintenance("dev-host", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	// Inspect the durable record before any restarted worker can check in.
	q, err := Open(c.Database, c.Checkpoints, RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	saved, err := q.HostMaintenanceStatus("dev-host")
	closeErr := q.Close()
	if err != nil || closeErr != nil || saved.Policy.Mode != "disabled" || saved.Healthy || saved.Quiescent {
		t.Fatal("restart inferred healthy release", saved, err, closeErr)
	}
	d, err = StartDev(c)
	if err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		s, err := d.Queue.HostMaintenanceStatus("dev-host")
		return err == nil && s.Healthy && s.Quiescent
	})
	after, err := d.Queue.HostMaintenanceStatus("dev-host")
	if err != nil || after.Policy.Mode != "disabled" || len(after.Workers) != 1 || !after.Workers[0].Fresh || after.Workers[0].Machine.Session == before.Workers[0].Machine.Session {
		t.Fatal("fresh session did not restore disabled host", after, err)
	}
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", "exit 0"}
	var submitted Job
	if err = d.Client().Call(ctx, "POST", "/api/jobs", s, &submitted); err != nil {
		t.Fatal(err)
	}
	current := job(t, d.Queue, submitted.ID)
	if current.Attempt != 0 || current.State != "queued" {
		t.Fatal("disabled restart admitted work", current)
	}
	if _, err = d.Queue.ChangeHostMaintenance("dev-host", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool { j := job(t, d.Queue, submitted.ID); return j.State == "succeeded" && j.ReleaseConfirmed })
}
