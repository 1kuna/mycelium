package myce

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type clientTestTransport func(*http.Request) (*http.Response, error)

func (f clientTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientJobIdentityEcho(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	ctx := context.Background()
	s := spec("experiment")
	s.SubmissionKey = "Exact.Key_182:Go-01"
	first, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	assertIdentity := func(j *Job, err error) {
		t.Helper()
		if err != nil {
			t.Error(err)
			return
		}
		if j.Spec.Project != s.Project || j.Spec.SubmissionKey != s.SubmissionKey {
			t.Errorf("identity = (%q, %q), want (%q, %q)", j.Spec.Project, j.Spec.SubmissionKey, s.Project, s.SubmissionKey)
		}
	}
	assertIdentity(first, nil)
	assertIdentity(c.Submit(ctx, s))
	assertIdentity(c.GetJob(ctx, first.ID))
	var wire Job
	err = c.Call(ctx, "GET", "/api/jobs/"+first.ID, nil, &wire)
	assertIdentity(&wire, err)
	if _, err := c.Cancel(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	assertIdentity(c.Wait(ctx, first.ID))
	var status Snapshot
	if err := c.Call(ctx, "GET", "/api/status", nil, &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Jobs) != 1 || status.Jobs[0].Spec.SubmissionKey != "" {
		t.Fatal("status list exposed submission key")
	}
}

func TestClientKeyedRetryAfterLostSubmitResponse(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := d.Client()
	var lost atomic.Bool
	c.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && r.Method == "POST" && r.URL.Path == "/api/jobs" && !lost.Swap(true) {
			response.Body.Close()
			return nil, errors.New("synthetic lost accepted response")
		}
		return response, err
	})}
	spec := Spec{Name: "retry", Project: "demo", SubmissionKey: "business-revision", Run: RunSpec{Command: []string{"sh", "-c", "exit 0"}}, Needs: ResourceNeeds{MemoryGB: 1, Platform: Platform{Any: true}}}
	job, err := c.SubmitAndWait(ctx, spec)
	if err != nil || job.State != "succeeded" || !lost.Load() {
		t.Fatal(job, err)
	}
	replay, err := c.SubmitAndWait(ctx, spec)
	if err != nil || replay.ID != job.ID {
		t.Fatal("keyed replay", err)
	}
	snap, err := d.Queue.Snapshot()
	if err != nil || len(snap.Jobs) != 1 {
		t.Fatal("duplicate keyed execution", snap, err)
	}
	if err = c.DownloadResult(ctx, job.ID, "missing", filepath.Join(root, "unused")); err == nil {
		t.Fatal("missing accepted artifact allowed")
	}
	if _, err = c.SubmitAndWait(ctx, Spec{}); err == nil {
		t.Fatal("unkeyed helper allowed")
	}
}

func TestClientWaitTimeoutReportsActualQueuedReason(t *testing.T) {
	q, _ := fixture(t)
	e := compositionExtension(t, false)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, Spec{Name: "waiting", Project: "demo", Extension: extensionKey(e), SubmissionKey: "timeout"})
	m := machine("unqualified", "fake")
	check(t, q, m, nil)
	server := httptest.NewServer(API{q, "fixture"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.Wait(ctx, j.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	for _, want := range []string{j.ID, "still queued", "no hypha has a qualified extension"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("missing %s: %v", want, err)
		}
	}
	if got := job(t, q, j.ID); got.State != "queued" {
		t.Fatal("timeout cancelled job", got)
	}
}

// A request timeout is an observation failure, not the overall wait deadline.
func TestClientWaitRetriesTransientStatusWithinContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		posts, reads := 0, 0
		c := Client{URL: "http://fixture", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			body := `{"id":"keyed-job","state":"queued"}`
			if r.Method == "POST" && r.URL.Path == "/api/jobs" {
				posts++
			} else if r.Method == "GET" && r.URL.Path == "/api/jobs/keyed-job" {
				reads++
				if reads == 2 {
					return nil, context.DeadlineExceeded
				}
				state := "queued"
				if reads >= 3 {
					state = "succeeded"
				}
				body = `{"id":"keyed-job","state":"` + state + `"}`
			} else {
				t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		start := time.Now()
		j, err := c.SubmitAndWait(ctx, Spec{SubmissionKey: "one-check"})
		if err != nil || j == nil || j.State != "succeeded" || posts != 1 || reads != 3 {
			t.Fatalf("job=%+v error=%v posts=%d reads=%d", j, err, posts, reads)
		}
		if time.Since(start) < 2*time.Second {
			t.Fatal("rapid status polling")
		}
		// Repeated failures preserve the last observation and stop at caller deadline.
		outageReads := 0
		c.HTTP.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
			outageReads++
			if outageReads == 1 {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"keyed-job","state":"queued","reason":"preparing"}`))}, nil
			}
			return nil, context.DeadlineExceeded
		})
		ctx2, cancel2 := context.WithTimeout(context.Background(), 7*time.Second)
		defer cancel2()
		last, err := c.Wait(ctx2, j.ID)
		if !errors.Is(err, context.DeadlineExceeded) || ctx2.Err() == nil || last == nil || last.State != "queued" || !strings.Contains(err.Error(), "preparing") || outageReads < 3 {
			t.Fatalf("deadline/last observation lost: job=%+v error=%v reads=%d", last, err, outageReads)
		}
		ctx3, cancel3 := context.WithCancel(context.Background())
		cancel3()
		if _, err = c.Wait(ctx3, j.ID); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestClientWaitSubmittedUnknownVisibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		recover  bool
		deadline time.Duration
		reads    int
		elapsed  time.Duration
	}{
		{name: "visible on retry", status: 404, recover: true, reads: 2, elapsed: time.Second},
		{name: "missing receipt is bounded", status: 404, reads: 6, elapsed: 5 * time.Second},
		{name: "authentication fails immediately", status: 401, reads: 1},
		{name: "caller deadline wins", status: 404, deadline: 500 * time.Millisecond, reads: 1, elapsed: 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				posts, reads := 0, 0
				c := Client{URL: "http://fixture", Token: "test-only", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
					if r.Header.Get("Authorization") != "Bearer test-only" {
						t.Fatal("missing authentication")
					}
					status, body := 200, `{"id":"accepted-job","state":"queued"}`
					switch {
					case r.Method == "POST" && r.URL.Path == "/api/jobs":
						posts++
					case r.Method == "GET" && r.URL.Path == "/api/jobs/accepted-job":
						reads++
						status, body = tc.status, `{"code":"job_unknown","error":"job is unknown"}`
						if tc.recover && reads > 1 {
							status, body = 200, `{"id":"accepted-job","state":"succeeded"}`
						}
					default:
						t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
					}
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				})}}
				ctx := context.Background()
				if tc.deadline > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.deadline)
					defer cancel()
				}
				start := time.Now()
				j, err := c.SubmitAndWait(ctx, Spec{SubmissionKey: "one-submission"})
				var api *HTTPError
				switch {
				case tc.recover:
					if err != nil || j == nil || j.State != "succeeded" {
						t.Fatal(j, err)
					}
				case tc.deadline > 0:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				default:
					if !errors.As(err, &api) || api.Status != tc.status {
						t.Fatal(err)
					}
				}
				if posts != 1 || reads != tc.reads || time.Since(start) != tc.elapsed {
					t.Fatalf("posts=%d reads=%d elapsed=%v", posts, reads, time.Since(start))
				}
			})
		})
	}
}

func TestClientWaitUpdatesRetryLogRead(t *testing.T) {
	updates := 0
	c := Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"updated-job","state":"succeeded"}`))}, nil
	})}}
	j, err := c.Wait(context.Background(), "updated-job", func(j *Job) error {
		updates++
		if updates == 1 {
			return &HTTPError{Status: 503}
		}
		return nil
	})
	if err != nil || j.State != "succeeded" || updates != 2 {
		t.Fatalf("job=%+v err=%v updates=%d", j, err, updates)
	}
	// A local output failure must return immediately, rather than being retried
	// forever as though it were a transient queue read.
	failure := errors.New("output closed")
	_, err = c.Wait(context.Background(), "updated-job", func(*Job) error { return failure })
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestClientJobReplyTruncated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reads := 0
		c := Client{URL: "http://fixture", HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
			reads++
			body := `{"id":"fixture","state":"succeeded"}`
			if reads == 1 {
				body = `{"id":"fixture","state":`
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		job, err := c.Wait(ctx, "fixture")
		if err != nil || job == nil || job.State != "succeeded" || reads != 2 {
			t.Fatalf("truncated reply must retry: job=%v err=%v reads=%d", job, err, reads)
		}
	})
}

func TestClientJobReplyInvalid(t *testing.T) {
	for _, body := range []string{
		`{"jobs":[{"id":"fixture","state":"succeeded"}]}`,
		`{"id":"","state":"succeeded"}`,
		`{"id":"fixture","state":""}`,
		`<html>wrong endpoint</html>`,
		``,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, body)
			}))
			defer server.Close()
			c := Client{URL: server.URL}
			for _, read := range []func(context.Context, string) (*Job, error){c.GetJob, func(ctx context.Context, id string) (*Job, error) { return c.Wait(ctx, id) }} {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				job, err := read(ctx, "fixture")
				contextErr := ctx.Err()
				cancel()
				var api *HTTPError
				if job != nil || !errors.As(err, &api) || api.Code != "job_reply_invalid" || contextErr != nil || RetryStatusRead(err) {
					t.Errorf("invalid reply must fail without polling: job=%v err=%v context=%v", job, err, contextErr)
				}
			}
		})
	}
}

func TestClientJobReplyOldQueue(t *testing.T) {
	for _, body := range []string{"404 page not found", `{"code":"job_unknown","error":"job is unknown"}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(404)
				io.WriteString(w, body)
			}))
			defer server.Close()
			c := Client{URL: server.URL}
			for _, read := range []func(context.Context, string) (*Job, error){c.GetJob, func(ctx context.Context, id string) (*Job, error) { return c.Wait(ctx, id) }} {
				_, err := read(context.Background(), "job")
				var api *HTTPError
				if !errors.As(err, &api) || api.Status != 404 {
					t.Fatalf("expected typed 404, got %v", err)
				}
				if strings.Contains(body, "job_unknown") {
					if api.Code != "job_unknown" {
						t.Fatalf("lost missing-job error: %v", err)
					}
				} else if api.Code != "queue_upgrade_required" || !strings.Contains(err.Error(), "queue too old for this client; upgrade the queue first") {
					t.Fatalf("unclear old queue error: %v", err)
				}
			}
		})
	}
}
