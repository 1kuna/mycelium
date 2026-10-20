package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type clientTestTransport func(*http.Request) (*http.Response, error)

func (f clientTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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
