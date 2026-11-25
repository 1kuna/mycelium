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

// On the reviewed protocol, hold the successful GET 204 itself after the
// original action. On the current protocol, hold the proposal before its
// durable acceptance. For Started the proposal is still a 204.
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
					res.Header.Get("X-Myce-Shared-Control-Receipt") != "" && res.StatusCode == http.StatusOK ||
					action == "start" && req.URL.Path == "/api/workers/started" && res.StatusCode == http.StatusNoContent ||
					action == "stop" && req.URL.Path == stopPath && res.StatusCode == http.StatusOK
				oldVerify := action != "stop" && req.URL.Path == sharedTurnPath &&
					req.Method == http.MethodGet && res.StatusCode == http.StatusNoContent
				if !proposal && !oldVerify {
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
