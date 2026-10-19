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
	c := DevConfig{Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), HostStateDir: filepath.Join(root, "host"), Address: "127.0.0.1:0", Token: "probe-token", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}
	t.Setenv("MYCE_TOKEN", c.Token) // HostSnapshot's authenticated local queue view.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	d, err := StartDev(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(filepath.Join(c.HostStateDir, hostStateFile)); err == nil {
			report, err := (HostControl{Dir: c.HostStateDir, Clock: d.Queue.clock, Service: &fakeHostService{}, Observe: testHostExclusion}).Disable(ctx, "probe finally", 0)
			if err != nil || !report.Confirmed {
				t.Errorf("finally local close: %v, confirmed=%v", err, report.Confirmed)
			}
		}
		if err := d.Close(); err != nil {
			t.Error("dev teardown:", err)
		}
		state, err := ReadHostIntent(c.HostStateDir)
		if err != nil || state.Desired != "closed" {
			t.Error("finally did not fence host", err)
		}
		if ctx.Err() != nil {
			t.Error("probe exceeded its single 30s budget including teardown")
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

func TestDevProbeInstallUpgrade(t *testing.T) {
	// The same executable really launches and serves the worker-child protocol.
	// Filesystem/config/legacy-install cases live in the CLI half of this probe.
	v, _ := realHostChildFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := v.Mutate(ctx, HostMutation{Operation: "close"}, nil, nil)
	if err != nil || result.Revision != 2 {
		t.Fatal("same-binary child close", result, err)
	}
	before := snapshotHostSecurityFiles(t, v.StateDir)
	result, err = v.Mutate(ctx, HostMutation{Operation: "status"}, nil, nil)
	if err != nil || result.Envelope == nil || result.Envelope.Desired.State == nil || *result.Envelope.Desired.State != "closed" {
		t.Fatal("child did not finish CLOSED", result, err)
	}
	after := snapshotHostSecurityFiles(t, v.StateDir)
	if len(before) != len(after) {
		t.Fatal("child status wrote state")
	}
	for path, value := range before {
		if after[path] != value {
			t.Fatal("child status changed a file")
		}
	}
	t.Log("PASS: same-binary child protocol, durable CLOSED, read-only status")
}

func TestDevProbeOwnerSwitch(t *testing.T) {
	d, c, ctx := devProbe(t)
	root := filepath.Dir(c.Database)
	marker := filepath.Join(root, "short-effect")
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", `test -z "$MYCE_TOKEN" || exit 99; printf done > "$1"`, "fixture", marker}
	var job Job
	if err := d.Client().Call(ctx, "POST", "/api/jobs", s, &job); err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		snapshot, err := d.Queue.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range snapshot.Jobs {
			if j.ID == job.ID {
				return j.State == "succeeded" && j.ReleaseConfirmed
			}
		}
		return false
	})
	if effect, err := os.ReadFile(marker); err != nil || string(effect) != "done" {
		t.Fatal("short command effect", err)
	}
	report, err := (HostControl{Dir: c.HostStateDir, Clock: d.Queue.clock, Service: &fakeHostService{}, Observe: testHostExclusion}).Disable(ctx, "external owner", 0)
	if err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	probeWait(t, ctx, func() bool {
		_ = d.Queue.Tick()
		state, _ := ReadHostIntent(c.HostStateDir)
		return state.HostSettlement() == "confirmed"
	})
	closed, _ := ReadHostIntent(c.HostStateDir)
	// Hold the existing reconcile lock while a fake external owner is active.
	// A concurrent open cannot pass that owner; unconditional close still fences.
	unlock, err := hostOperationLock(c.HostStateDir, "reconcile.lock", true)
	if err != nil {
		t.Fatal(err)
	}
	openCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- (HostControl{Dir: c.HostStateDir, Clock: d.Queue.clock, Expected: closed.HostRevision}).Enable(openCtx)
	}()
	newClose, err := changeHostIntent(c.HostStateDir, d.Queue.clock, hostIntentChange{Desired: "closed", FreshClose: true})
	cancel()
	openErr := <-done
	unlock()
	if err != nil || openErr == nil || newClose.Revision <= closed.Revision {
		t.Fatal("concurrent owner fence failed", err, openErr)
	}
	if _, err = hostLaunchGate(c.HostStateDir, closed.HostRevision); err == nil {
		t.Fatal("stale owner admitted")
	}
	t.Log("PASS: short command/release, external owner lock, concurrent close, stale launch refused")
}

func TestDevProbeOfflineRollback(t *testing.T) {
	if !t.Run("real-command-offline-close", TestHostEnvelopeDevLifecycle) {
		return
	}
	if !t.Run("incomplete-docker-disposition", TestExclusionPendingOperationNeedsPositiveDisposition) {
		return
	}
	if !t.Run("late-docker-disposition", TestExclusionLateTerminalStartAndNetworkAcceptance) {
		return
	}
	t.Log("PASS: offline 2s command fenced, consumer cancellation required, incomplete daemon evidence refused, late operation positively disposed")
}

func TestDevProbeRebootResume(t *testing.T) {
	root := newTestStateRoot(t)
	c := DevConfig{Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), HostStateDir: filepath.Join(root, "host"), Address: "127.0.0.1:0", Token: "probe-token", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	d, err := StartDev(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			r, err := (HostControl{Dir: c.HostStateDir, Clock: d.Queue.clock, Service: &fakeHostService{}, Observe: testHostExclusion}).Disable(ctx, "reboot finally", 0)
			if err != nil || !r.Confirmed {
				t.Error("reboot finally", err)
			}
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}
		if ctx.Err() != nil {
			t.Error("reboot exceeded single 30s deadline")
		}
	}()
	clock := d.Queue.clock
	closed, err := changeHostIntent(c.HostStateDir, clock, hostIntentChange{Desired: "closed", Grace: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	saved := *closed.StopDeadline
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	// Restart the actual dev queue/worker on the same SQLite and host state.
	d, err = StartDev(c)
	if err != nil {
		t.Fatal(err)
	}
	clock = d.Queue.clock
	stateAfterRestart, err := ReadHostIntent(c.HostStateDir)
	if err != nil || stateAfterRestart.HostRevision != closed.HostRevision || !stateAfterRestart.StopDeadline.Equal(saved) {
		t.Fatal("dev restart renewed close", err)
	}
	if err := PublishHostObservation(c.HostStateDir, closed.HostRevision, "previous-boot", "closed", clock); err != nil {
		t.Fatal(err)
	}
	observation, err := HostSnapshot(ctx, HostInvocation{StateDir: c.HostStateDir}, clock)
	if err != nil || observation.Observed.State == nil || *observation.Observed.State != "unknown" {
		t.Fatal("previous boot trusted", err)
	}
	advance := func(seconds float64) {
		t.Helper()
		if err := d.Client().Call(ctx, "POST", "/api/dev/clock/advance", map[string]float64{"seconds": seconds}, nil); err != nil {
			t.Fatal(err)
		}
	}
	advance(3)
	// A new controller reads the durable close, as after restart. Unknown native
	// evidence must alarm/refuse handoff even when service stop itself succeeded.
	service := &fakeHostService{}
	controller := HostControl{Dir: c.HostStateDir, Clock: clock, Service: service, Observe: testUnknownExclusion}
	r, err := controller.Disable(ctx, "restart inside deadline", time.Minute)
	if err == nil || r.Confirmed || service.grace <= 0 || service.grace > 7*time.Second {
		t.Fatal("restart did not use remaining saved grace", r, err, service.grace)
	}
	advance(8)
	controller = HostControl{Dir: c.HostStateDir, Clock: clock, Service: service, Observe: testUnknownExclusion}
	r, err = controller.Disable(ctx, "expired restart", time.Minute)
	if err == nil || r.Confirmed || service.grace != 0 {
		t.Fatal("expired close passed/renewed grace", r, err, service.grace)
	}
	t.Log("ALARM: exclusion remains UNKNOWN after deadline; automatic reopen refused")
	state, _ := ReadHostIntent(c.HostStateDir)
	if !state.StopDeadline.Equal(saved) {
		t.Fatal("restart renewed durable deadline")
	}
	if err = (HostControl{Dir: c.HostStateDir, Clock: clock, Expected: state.HostRevision}).Enable(ctx); err == nil {
		t.Fatal("unsettled restart reopened")
	}
	// Explicit retry has only the original total probe budget, with zero stop
	// grace. Positive fake hardware evidence now permits settlement, not open.
	r, err = (HostControl{Dir: c.HostStateDir, Clock: clock, Service: service, Observe: testHostExclusion}).Disable(ctx, "explicit bounded retry", time.Minute)
	if err != nil || !r.Confirmed || service.grace != 0 {
		t.Fatal("bounded retry", r, err)
	}
	state, _ = ReadHostIntent(c.HostStateDir)
	if state.Desired != "closed" || !state.StopDeadline.Equal(saved) {
		t.Fatal("retry changed deadline/fence")
	}
	t.Log("PASS: saved deadline survives controller restart, expiry alarms, explicit retry does not renew grace")
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
	report, err := (HostControl{Dir: c.HostStateDir, Clock: d.Queue.clock, Service: &fakeHostService{}, Observe: testHostExclusion}).Disable(ctx, "queue outage", 0)
	if err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	state, _ := ReadHostIntent(c.HostStateDir)
	if consumer.mayOpen(state) {
		t.Fatal("consumer reopened before cancellation")
	}
	if err = (HostControl{Dir: c.HostStateDir, Clock: d.Queue.clock, Expected: state.HostRevision}).Enable(ctx); err == nil {
		t.Fatal("offline host reopened")
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
	if err = q.ConfigureLocalHost(c.HostStateDir, d.URL); err != nil {
		t.Fatal(err)
	}
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
	clients := func(endpoint string) (Client, error) { return Client{URL: endpoint, Token: c.Token}, nil }
	if err = ReconcileHostLocal(ctx, c.HostStateDir, clock, clients); err != nil {
		t.Fatal(err)
	}
	state, _ = ReadHostIntent(c.HostStateDir)
	if state.HostSettlement() != "confirmed" || consumer.mayOpen(state) {
		t.Fatal("host settlement substituted for consumer cancellation")
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
	// Exercise both serializations and a real concurrent API race while CLOSED.
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
	if !consumer.mayOpen(state) || consumer.publish(consumer.execution) {
		t.Fatal("consumer cancellation/publication fence failed")
	}
	if err := (HostControl{Dir: c.HostStateDir, Clock: clock, Expected: state.HostRevision}).Enable(ctx); err != nil {
		t.Fatal("reconnect open", err)
	}
	probeWait(t, ctx, func() bool {
		e, err := HostSnapshot(ctx, HostInvocation{StateDir: c.HostStateDir, Config: ServiceConfig{RequiredWorkers: []string{"fake-1"}}}, clock)
		return err == nil && e.Ready != nil && *e.Ready
	})
	report, err = (HostControl{Dir: c.HostStateDir, Clock: clock, Service: &fakeHostService{}, Observe: testHostExclusion}).Disable(ctx, "reconnect finally", 0)
	if err != nil || !report.Confirmed {
		t.Fatal("reconnect final close", err)
	}
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

func TestDevProbeRestVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"closed", TestHostRestClosedMatrix}, {"ready", TestHostRestReadyMatrix},
		{"freshness-revision-boot", TestHostRestFreshnessAndRevision},
		{"placement-and-terminal-custody", TestHostRestPlacementObservation},
		{"mixed-fail-unknown", TestHostRestPartialSourceContraryMatrix},
		{"all-source-expiry", TestHostRestEvidenceNormalizationMatrix},
	} {
		if !t.Run(tc.name, tc.run) {
			return
		}
	}
	t.Log("PASS: complete closed/ready, freshness, boot/revision, placement, release, FAIL/UNKNOWN matrix")
}

// A deliberately tiny consumer fake: host settlement cannot complete its own
// cancellation obligation, and cancellation revokes publication immediately.
// The consumer's persistence/health implementation belongs to the consumer.
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
func (c probeConsumer) mayOpen(host HostIntent) bool {
	return host.Desired == "closed" && host.HostSettlement() == "confirmed" && !c.cancelPending && c.cancelled
}
func (c probeConsumer) publish(execution Execution) bool {
	return !c.cancelPending && !c.cancelled && execution == c.execution
}
