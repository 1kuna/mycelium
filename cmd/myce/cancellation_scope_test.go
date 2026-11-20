package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// myce cancel exits 0 only for confirmed cancellation; an acknowledged cancel
// with release pending exits 75, and --wait observes the stored
// acknowledgement until release is confirmed (#1341).
func TestCancelExitsZeroOnlyWhenConfirmed(t *testing.T) {
	t.Parallel()
	id := "job-1"
	ack := func(release *string) myce.Cancellation {
		return myce.Cancellation{JobID: &id, Project: "test", State: "stopping", WillNotRun: true, Release: release}
	}
	pending, confirmed, notApplicable := "pending", "confirmed", "not_applicable"
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		release := &pending
		if reads >= 2 {
			release = &confirmed
		}
		a := ack(release)
		_ = json.NewEncoder(w).Encode(myce.Job{ID: id, State: "cancelled", Cancellation: &a})
	}))
	defer server.Close()
	client := myce.Client{URL: server.URL, Token: "test-only"}
	pauses := 0
	pause := func(context.Context, time.Duration) error { pauses++; return nil }
	send := func(release *string) func(context.Context) (myce.Cancellation, error) {
		return func(context.Context) (myce.Cancellation, error) { return ack(release), nil }
	}
	run := func(release *string, wait bool) (myce.Cancellation, int, string) {
		t.Helper()
		var out bytes.Buffer
		a, err := cancelAndConfirm(context.Background(), client, send(release), wait, pause)
		err = printCancelAck(&out, a, err)
		return a, exitCode(err), out.String()
	}

	if _, code, out := run(&notApplicable, false); code != 0 || out == "" {
		t.Fatalf("cancelled before start: exit %d %q", code, out)
	}
	if _, code, out := run(&pending, false); code != cancelUnconfirmedExit || out == "" || reads != 0 {
		t.Fatalf("release pending: exit %d %q reads=%d", code, out, reads)
	}
	if a, code, _ := run(&pending, true); code != 0 || *a.Release != "confirmed" || reads != 2 || pauses != 2 {
		t.Fatalf("--wait: %+v exit %d reads=%d pauses=%d", a, code, reads, pauses)
	}
	// Expired coverage can never be confirmed: --wait returns at once.
	if _, code, _ := run(nil, true); code != cancelUnconfirmedExit || pauses != 2 {
		t.Fatalf("unknown release: exit %d pauses=%d", code, pauses)
	}
	ctx, stop := context.WithCancel(context.Background())
	stop()
	interrupted := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	if _, err := cancelAndConfirm(ctx, client, send(&pending), true, interrupted); exitCode(err) != cancelUnconfirmedExit {
		t.Fatalf("interrupted --wait: %v", err)
	}

	expired, stop := context.WithTimeout(context.Background(), 0)
	defer stop()
	var out bytes.Buffer
	a, err := cancelAndConfirm(expired, client, send(&pending), true, interrupted)
	err = printCancelAck(&out, a, err)
	if exitCode(err) != cancelUnconfirmedExit || !strings.Contains(err.Error(), "timed out") || !strings.Contains(out.String(), `"release":"pending"`) {
		t.Fatalf("timed out after acknowledgement: exit %d, %v, receipt=%s", exitCode(err), err, &out)
	}
	out.Reset()
	a, err = cancelAndConfirm(expired, client, func(ctx context.Context) (myce.Cancellation, error) {
		return myce.Cancellation{}, ctx.Err()
	}, true, interrupted)
	if err = printCancelAck(&out, a, err); !errors.Is(err, context.DeadlineExceeded) || out.Len() != 0 {
		t.Fatalf("timed out before acknowledgement: %v, receipt=%s", err, &out)
	}
}
