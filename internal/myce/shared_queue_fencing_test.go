package myce

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func proofCheckin(t *testing.T, c Client, m Machine) Reply {
	t.Helper()
	var reply Reply
	in := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}
	if err := c.Call(t.Context(), http.MethodPost, "/api/workers/checkin", in, &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func proofAssignment(t *testing.T, c Client, m Machine, job string) *Job {
	t.Helper()
	for _, directive := range proofCheckin(t, c, m).Attempts {
		if directive.Job != nil && directive.ID == job {
			return directive.Job
		}
	}
	t.Fatal("worker did not receive the exact assignment")
	return nil
}

// Hold the proposal before the worker checks its handover. For Started
// the first exchange returns a probe 204 without authorizing execution.
func TestSharedQueueMemberDelayedControl(t *testing.T) {
	for _, action := range []string{"grant", "start", "stop"} {
		t.Run(action, func(t *testing.T) {
			pg := startProofPostgres(t)
			a := pg.authority(t)
			dir := t.TempDir()
			old := startProofMember(t, dir, "old", "127.0.0.1:0", pg.dsn)
			first := proofLeader(t, a, old, 0)
			m := machine("fixture-worker", "cpu", "fixture-host")
			job := proofSubmit(t, old.client, proofSpec(action))
			req := Result{Machine: m.ID, Session: m.Session}
			if action != "grant" {
				assigned := proofAssignment(t, old.client, m, job.ID)
				req.Execution = Execution{ID: assigned.ID, Attempt: assigned.Attempt}
			}
			if action == "stop" {
				var started map[string]any
				if err := old.client.Call(t.Context(), http.MethodPost, "/api/workers/started", req, &started); err != nil {
					t.Fatal(err)
				}
				if _, err := old.client.Cancel(t.Context(), job.ID); err != nil {
					t.Fatal(err)
				}
			}

			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			captured, deliver := make(chan struct{}, 1), make(chan struct{})
			client := old.client
			// Route both messages through the stable worker-facing endpoint.
			// The fixture changes its backend only after the new turn is live.
			next := startProofMember(t, dir, "next", "127.0.0.1:0", pg.dsn)
			route := make(chan struct{})
			stopPath := "/api/workers/" + url.PathEscape(m.ID) + "/events"
			client.HTTP = &http.Client{Transport: proofRoundTrip(func(req *http.Request) (*http.Response, error) {
				select {
				case <-route:
					req = req.Clone(req.Context())
					upstream, _ := url.Parse(next.client.URL)
					req.URL.Host, req.Host = upstream.Host, upstream.Host
				default:
				}
				res, err := http.DefaultTransport.RoundTrip(req)
				if err != nil {
					return nil, err
				}

				proposal := action == "grant" && req.URL.Path == "/api/workers/checkin" &&
					res.Header.Get(sharedTurnHeader) != "" && res.StatusCode == http.StatusOK ||
					action == "start" && req.URL.Path == "/api/workers/started" && res.StatusCode == http.StatusNoContent ||
					action == "stop" && req.URL.Path == stopPath && res.StatusCode == http.StatusOK
				if !proposal {
					return res, nil
				}
				body, err := io.ReadAll(res.Body)
				res.Body.Close()
				if err != nil {
					return nil, err
				}
				res.Body = io.NopCloser(bytes.NewReader(body))
				captured <- struct{}{}
				select {
				case <-deliver:
					return res, nil
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			})}
			done := make(chan error, 1)
			var reply Reply
			var started map[string]any
			var event WorkerEvent
			go func() {
				switch action {
				case "grant":
					done <- client.Call(ctx, http.MethodPost, "/api/workers/checkin",
						Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}, &reply)
				case "start":
					done <- client.Call(ctx, http.MethodPost, "/api/workers/started", req, &started)
				case "stop":
					done <- client.Call(ctx, http.MethodGet, stopPath+"?session="+url.QueryEscape(m.Session), nil, &event)
				}
			}()
			select {
			case <-captured:
			case err := <-done:
				t.Fatalf("control ended before the reply was held: %v", err)
			case <-ctx.Done():
				t.Fatal("control reply was not produced")
			}
			old.stop(t, syscall.SIGTERM)
			proofLeader(t, a, next, first.number)
			close(route)
			close(deliver)
			err := <-done
			if err == nil {
				t.Fatal("delayed control reply conferred authority after handover")
			}
			if len(reply.Attempts) != 0 || started != nil || event.Cursor != "" {
				t.Fatal("refused control reply reached the worker's destination")
			}
			if action == "start" {
				j, err := next.client.GetJob(ctx, job.ID)
				if err != nil || j.StartedAt != nil {
					t.Fatalf("delayed start was committed: job=%v err=%v", j, err)
				}
			}
			if action == "stop" {
				if err := next.client.Call(ctx, http.MethodGet, stopPath+"?session="+url.QueryEscape(m.Session), nil, &event); err != nil {
					t.Fatal(err)
				}
				if len(event.Events) != 1 || event.Events[0].ID != req.ID ||
					event.Events[0].Attempt != req.Attempt || event.Events[0].Session != req.Session {
					t.Fatal("successor did not issue the committed stop for the exact attempt")
				}
			}
		})
	}
}

func TestSharedQueueMemberHungStorePromotion(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	old := startProofMember(t, dir, "old", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, old, 0)
	extension := fakeExtension(t)
	path := filepath.Join(dir, "files", "blobs", extension.Digest)
	proofEventually(t, "initial file store", func() bool {
		return old.client.Call(t.Context(), http.MethodPost, "/api/extensions", extension, nil) == nil
	})
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	old.stop(t, syscall.SIGTERM)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	// Reopening the retained bundle blocks in the real filesystem reader.
	survivor := startProofMember(t, dir, "survivor", "127.0.0.1:0", pg.dsn)
	var fifo *os.File
	proofEventually(t, "retained bundle verification to reach the blocked read", func() bool {
		fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0600)
		if err != nil {
			return false
		}
		fifo = os.NewFile(uintptr(fd), "fixture-bundle")
		return true
	})
	defer fifo.Close()
	second := proofLeader(t, a, survivor, first.number)
	plain := proofSubmit(t, survivor.client, proofSpec("while-store-blocked"))
	backedSpec := proofSpec("requires-store")
	backedSpec.Resumable = true
	backed := proofSubmit(t, survivor.client, backedSpec)
	m := machine("plain-worker", "cpu", "plain-host")
	assigned := proofAssignment(t, survivor.client, m, plain.ID)
	execution := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}}
	if err := survivor.client.Call(t.Context(), http.MethodPost, "/api/workers/started", execution, nil); err != nil {
		t.Fatal(err)
	}
	execution.Success = true
	if err := survivor.client.Call(t.Context(), http.MethodPost, "/api/workers/complete", execution, nil); err != nil {
		t.Fatal(err)
	}
	reply := proofCheckin(t, survivor.client, machine("backed-worker", "cpu", "backed-host"))
	for _, d := range reply.Attempts {
		if d.ID == backed.ID && d.Job != nil {
			t.Fatal("assigned file-dependent work before the backing was ready")
		}
	}
	proofUnavailable(t, survivor.client.Call(t.Context(), http.MethodGet, "/api/files/blobs/"+extension.Digest, nil, nil))

	// Verify renewal over the lease window while still blocking verification.
	proofEventually(t, "renewal during the blocked read", func() bool {
		turn, err := a.current(t.Context())
		return err == nil && sameSharedTurn(turn, second) && turn.ready && turn.expires.Sub(second.expires) > proofMemberLease
	})
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := fifo.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := fifo.Close(); err != nil {
		t.Fatal(err)
	}
	proofEventually(t, "file-dependent work to resume", func() bool {
		var reply Reply
		m := machine("backed-worker", "cpu", "backed-host")
		err := survivor.client.Call(t.Context(), http.MethodPost, "/api/workers/checkin",
			Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}, &reply)
		if err != nil {
			return false
		}
		for _, d := range reply.Attempts {
			if d.ID == backed.ID && d.Job != nil {
				return true
			}
		}
		return false
	})
}

func TestSharedQueuePostgresStartupErrorSanitized(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const role, database, password = "fixture_role", "fixture_database", "fixture_password"
	target := t.TempDir() // no PostgreSQL socket exists here
	dsn := "host=" + strconv.Quote(target) + " dbname=" + database + " user=" + role + " password=" + password + " sslmode=disable"
	// A quoted Unix-socket path in the pgx keyword format uses single quotes.
	dsn = strings.ReplaceAll(dsn, `"`, "'")
	_, err := openSharedQueueAuthority(ctx, dsn)
	for _, detail := range []string{target, database, role, password} {
		if strings.Contains(fmt.Sprint(err), detail) {
			t.Fatal("PostgreSQL startup error contains connection details")
		}
	}
	if err == nil || err.Error() != "shared queue PostgreSQL authority could not be opened" {
		t.Fatal("startup did not return the sanitized PostgreSQL error")
	}
}

func TestSharedQueuePostgresHoldsFrozenEnvironment(t *testing.T) {
	for _, state := range []string{"missing", "faulted"} {
		t.Run(state, func(t *testing.T) {
			pg := startProofPostgres(t)
			a := pg.authority(t)
			q, turn := proofOpenReclaimQueue(t, a, t.TempDir(), state, proofClock())
			defer func() { _ = a.renew(t.Context(), turn, 0, false) }()
			s := proofSpec("locked-environment")
			s.Environment = Environment{Backend: "uv", LockFile: "uv.lock", Python: "3.12", Platforms: []string{"cpu"},
				Files: map[string][]byte{"uv.lock": []byte("version = 1\n"), "pyproject.toml": []byte("[project]\nname = 'fixture'\n")}}
			j, err := q.Submit(s)
			if err != nil {
				t.Fatal(err)
			}
			ref := q.s.Jobs[j.ID].EnvironmentRecipes["cpu"].FilesRef
			if ref == nil {
				t.Fatal("submission did not freeze its environment")
			}
			m := machine("fixture-worker", "cpu", "fixture-host")
			m.EnvironmentBuilder = true
			checkin := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}
			reply, err := q.CheckinKeyed(checkin)
			if err != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch == nil {
				t.Fatalf("healthy queue did not offer preparation: %v", err)
			}
			plan := reply.EnvironmentPlan[0]
			if err := q.AuthorizeLaunch(*plan.Launch); err != nil {
				t.Fatal(err)
			}
			checkin.Machine.EnvironmentReports = []EnvironmentReport{{Key: plan.Key, State: "ready", Attempt: plan.Launch.Attempt}}
			checkin.Machine.Environments = []string{plan.Key}
			store := q.files
			if state == "missing" {
				q.files = nil
			} else {
				q.sharedWriter.storageFault.Store(true)
			}

			// A blocked collector must not gate control while storage is unavailable.
			q.fileLifecycle.Lock()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				reply, err := q.CheckinKeyedContext(ctx, checkin)
				if err == nil && (len(reply.Attempts) != 0 || len(reply.EnvironmentPlan) != 0 || len(reply.ModelPlan) != 0) {
					err = fmt.Errorf("unavailable backing conferred a job or plan")
				}
				done <- err
			}()
			select {
			case err = <-done:
				q.fileLifecycle.Unlock()
			case <-ctx.Done():
				q.fileLifecycle.Unlock()
				<-done
				t.Fatal("check-in waited on a blocked file lifecycle")
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(q.s.Machines[m.ID].Environments, plan.Key) {
				t.Fatal("fixture did not report the cached environment as ready")
			}
			builder := machine("other-builder", "cpu", "other-host")
			builder.EnvironmentBuilder = true
			reply, err = q.CheckinKeyed(Checkin{Machine: builder, AttemptProtocol: keyedAttemptProtocol, period: time.Second})
			if err != nil || len(reply.EnvironmentPlan) != 0 || len(reply.Attempts) != 0 {
				t.Fatalf("unavailable backing offered preparation: plan=%v err=%v", len(reply.EnvironmentPlan), err)
			}
			if j := q.s.Jobs[j.ID]; j.State != "queued" || j.Attempt != 0 {
				t.Fatalf("file-dependent job was assigned: %s attempt %d", j.State, j.Attempt)
			}
			if _, err := q.Submit(s); err != nil {
				t.Fatalf("durable submission replay during storage outage: %v", err)
			}
			s.SubmissionKey = "new-lockfile"
			_, err = q.Submit(s)
			proofUnavailable(t, err)
			q.files = store
			q.sharedWriter.storageFault.Store(false)
			reply, err = q.CheckinKeyed(checkin)
			if err != nil || !slices.ContainsFunc(reply.Attempts, func(d AttemptDirective) bool {
				return d.ID == j.ID && d.Job != nil && len(d.Job.EnvironmentRecipes["cpu"].Files) != 0
			}) {
				t.Fatalf("healthy backing did not deliver the original hydrated assignment: %v", err)
			}
			if _, err := q.Submit(s); err != nil {
				t.Fatalf("retry after backing recovered: %v", err)
			}
		})
	}
}

func TestSharedQueueMemberRepromotesWithBlockedRequest(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	member := startProofMember(t, dir, "survivor", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, member, 0)
	extension := fakeExtension(t)
	proofEventually(t, "initial file store", func() bool {
		return member.client.Call(t.Context(), http.MethodPost, "/api/extensions", extension, nil) == nil
	})
	path := filepath.Join(dir, "files", "blobs", extension.Digest)
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	// The real extension request owns its old queue admission until the
	// retained bundle read finishes, even after its socket is closed.
	client := member.client
	client.HTTP = http.DefaultClient
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- client.Call(ctx, http.MethodGet, "/api/extensions/"+extension.Digest, nil, nil)
	}()
	var fifo *os.File
	proofEventually(t, "old request to open blocked bundle", func() bool {
		fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0600)
		if err != nil {
			return false
		}
		fifo = os.NewFile(uintptr(fd), "blocked-store-read")
		return true
	})
	defer func() {
		_ = os.Remove(path)
		_ = os.WriteFile(path, payload, 0600)
		_ = fifo.Close()
	}()
	select {
	case err := <-done:
		t.Fatalf("old bundle request did not stay blocked: %v", err)
	default:
	}
	if err := a.renew(t.Context(), first, 0, false); err != nil {
		t.Fatal(err)
	}
	proofLeader(t, a, member, first.number)
	job := proofSubmit(t, member.client, proofSpec("plain-job-after-repromotion"))
	if job.ID == "" {
		t.Fatal("the new turn did not accept a submission")
	}
}

func TestSharedQueueMemberControlCheckUsesUnlockedPostgresRead(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	member := startProofMember(t, dir, "first", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, member, 0)
	confirm := func(ctx context.Context, c Client, turn sharedQueueTurn) int {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+sharedTurnPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)
		req.Header.Set(sharedVerifyHeader, sharedControlProtocol)
		req.Header.Set(sharedTurnHeader, strconv.FormatInt(turn.number, 10))
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		return res.StatusCode
	}
	// A plain PostgreSQL read can complete while the coordinator row is locked.
	tx, err := a.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, _, err := lockedSharedTurn(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if status := confirm(t.Context(), member.client, first); status != http.StatusNoContent {
		t.Errorf("current-turn confirmation under a locked row returned %d", status)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	member.stop(t, syscall.SIGTERM)
	next := startProofMember(t, dir, "next", "127.0.0.1:0", pg.dsn)
	second := proofLeader(t, a, next, first.number)
	if status := confirm(t.Context(), next.client, first); status != http.StatusServiceUnavailable {
		t.Errorf("stale turn confirmation returned %d", status)
	}
	if status := confirm(t.Context(), next.client, second); status != http.StatusNoContent {
		t.Errorf("current successor confirmation returned %d", status)
	}
}

func TestSharedQueueMemberRetriesLaunchAcrossHandover(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	old := startProofMember(t, dir, "first", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, old, 0)
	m := machine("fixture-worker", "cpu", "fixture-host")
	job := proofSubmit(t, old.client, proofSpec("launch-on-successor"))
	j := proofAssignment(t, old.client, m, job.ID)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	captured, deliver, awaitingCheckin := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	const launchPath = "/api/workers/launch"
	c := old.client
	var nextURL *url.URL // Published to the observer by closing deliver.
	var successorTurn string
	c.HTTP = &http.Client{Transport: proofRoundTrip(func(req *http.Request) (*http.Response, error) {
		select {
		case <-deliver:
			req = req.Clone(req.Context())
			req.URL.Host, req.Host = nextURL.Host, nextURL.Host
			res, err := http.DefaultTransport.RoundTrip(req)
			if err == nil && req.Header.Get(sharedTurnHeader) == successorTurn && res.StatusCode == http.StatusServiceUnavailable {
				select {
				case awaitingCheckin <- struct{}{}:
				default:
				}
			}
			return res, err
		default:
		}
		res, err := http.DefaultTransport.RoundTrip(req)
		if err != nil || req.URL.Path != launchPath || res.StatusCode != http.StatusNoContent {
			return res, err
		}
		defer res.Body.Close()
		close(captured)
		select {
		case <-deliver:
			res.Body = http.NoBody
			return res, nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})}
	w := Worker{Client: c, Interval: 25 * time.Millisecond, launchCtx: ctx}
	execution := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	launch := workerLaunchRequest{HostLaunch: HostLaunch{Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt), Machine: m.ID, Session: m.Session}}
	done := make(chan error, 1)
	go func() {
		var result struct{ OK bool }
		_, err := w.controlHandshake(launchPath, launch, &result)
		if err == nil && !result.OK {
			err = fmt.Errorf("launch lacked positive authorization")
		}
		done <- err
	}()
	select {
	case <-captured:
	case err := <-done:
		t.Fatalf("launch probe did not reach first turn: %v", err)
	case <-ctx.Done():
		t.Fatal("launch probe was not produced")
	}
	old.stop(t, syscall.SIGTERM)
	next := startProofMember(t, dir, "next", "127.0.0.1:0", pg.dsn)
	second := proofLeader(t, a, next, first.number)
	nextURL, _ = url.Parse(next.client.URL)
	successorTurn = strconv.FormatInt(second.number, 10)
	close(deliver)
	select {
	case <-awaitingCheckin:
	case err := <-done:
		t.Fatalf("worker lost its launch attempt before reporting to the successor: %v", err)
	case <-ctx.Done():
		t.Fatal("launch never reached the successor")
	}
	proofCheckin(t, next.client, m)
	if err := <-done; err != nil {
		t.Fatalf("worker lost its launch attempt on handover: %v", err)
	}
	if err := next.client.Call(ctx, http.MethodPost, "/api/workers/started", execution, nil); err != nil {
		t.Fatal(err)
	}
	got, err := next.client.GetJob(ctx, j.ID)
	if err != nil || got.Attempt != j.Attempt || got.StartedAt == nil || got.State != "running" {
		t.Fatalf("successor did not start the original attempt: %v", err)
	}
}
