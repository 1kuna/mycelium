package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// #205: the retained declaration, not accepted results or dispatched children,
// defines the abort scope. Both routes expose the same exact release contract.
func TestCancellationScopeFanOut(t *testing.T) {
	for _, route := range []string{"id", "key"} {
		for _, stage := range []string{"preparing", "running", "retried"} {
			t.Run(route+"/"+stage, func(t *testing.T) {
				q, clock, m, s := fanOutFixture(t, 7)
				s.SubmissionKey = "abort-scope"
				s.FanOut.MaxConcurrency = 2
				s.Outputs = map[string]string{"out": "results"}
				s.AllowedMachines = []string{m.ID}
				parent := submit(t, q, s)
				first := check(t, q, m, nil).Job
				if first == nil || len(first.Batch.Pieces) != 2 {
					t.Fatal("missing first two-piece wave")
				}
				finish(t, q, first, true)
				readySpec := pieceConsumer(s, parent.ID, "item-1")
				readySpec.SubmissionKey, readySpec.AllowedMachines = "ready", []string{"unjoined"}
				ready := submit(t, q, readySpec)
				blockedSpec := pieceConsumer(s, parent.ID, "item-6")
				blockedSpec.SubmissionKey = "blocked"
				blocked := submit(t, q, blockedSpec)
				consumerSpec := pieceConsumer(s, parent.ID, "item-0")
				consumerSpec.SubmissionKey = "running-consumer"
				consumerSpec.AllowedMachines = []string{"consumer"}
				consumer := submit(t, q, consumerSpec)
				cm := cloneMachine(&m)
				cm.ID, cm.HostID, cm.Session = "consumer", "consumer", "consumer-session"
				e := registeredExtension(t, q, s.Extension)
				report := newExtensionReport(e, &cm, s.workType())
				report.Setup, report.Qualified, report.CheckID = true, true, "consumer-check"
				cm.ExtensionReports = []ExtensionReport{report}
				consumer = check(t, q, cm, nil).Job
				if consumer == nil || consumer.Spec.After == nil {
					t.Fatalf("missing running consumer: %s", q.waitReason(q.s.Jobs[consumerSpec.After.Job]))
				}
				if err := q.Started(result(consumer, true)); err != nil {
					t.Fatal(err)
				}
				wave := check(t, q, m, nil).Job
				if wave == nil || len(wave.Batch.Pieces) != 2 {
					t.Fatalf("missing second two-piece wave: %+v", wave.Batch)
				}
				if stage == "running" {
					if err := q.Started(result(wave, true)); err != nil {
						t.Fatal(err)
					}
				}
				if stage == "retried" {
					r := result(wave, false)
					r.Interrupted, r.ReleasePending = true, true
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					if job(t, q, wave.ID).State != "queued" {
						t.Fatal("wave did not requeue")
					}
				}
				server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
				client := Client{URL: server.URL, Token: "test"}
				ctx := context.Background()
				var ack Cancellation
				var err error
				if route == "id" {
					ack, err = client.Cancel(ctx, parent.ID)
				} else {
					ack, err = client.CancelSubmission(ctx, s.Project, s.SubmissionKey)
				}
				if err != nil || ackRelease(ack) != "pending" {
					t.Fatalf("cancel: %+v %v", ack, err)
				}
				got, err := client.GetJob(ctx, parent.ID)
				if err != nil {
					t.Fatal(err)
				}
				assertCancellationScope(t, got, first, wave, ready, blocked, consumer, "pending")
				var status Snapshot
				if err := client.Call(ctx, "GET", "/api/status", nil, &status); err != nil {
					t.Fatal(err)
				}
				statusJSON, _ := json.Marshal(status)
				if strings.Contains(string(statusJSON), s.SubmissionKey) {
					t.Fatal("status disclosed submission key")
				}
				if got.Cancellation.SubmissionKey == nil || *got.Cancellation.SubmissionKey != s.SubmissionKey {
					t.Fatal("GET lost cancellation key echo")
				}
				for _, id := range []string{ready.ID, blocked.ID} {
					member, err := client.GetJob(ctx, id)
					if err != nil || member.State != "cancelled" || member.Cancellation == nil || ackRelease(*member.Cancellation) != "not_applicable" || member.ReleaseConfirmed {
						t.Fatalf("never assigned GET: %+v %v", member, err)
					}
				}
				before := got.CancellationScope
				server.Close()
				q = reopenCancellationQueue(t, q, clock)
				server = httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
				defer server.Close()
				client.URL = server.URL
				got, err = client.GetJob(ctx, parent.ID)
				if err != nil || !reflect.DeepEqual(before, got.CancellationScope) {
					t.Fatalf("scope changed on reopen: %+v %v", got, err)
				}
				futureSpec := pieceConsumer(s, parent.ID, "item-6")
				futureSpec.SubmissionKey = "future"
				future := submit(t, q, futureSpec)
				if future.State != "cancelled" || future.Attempt != 0 {
					t.Fatal("future dependent escaped fence", future)
				}
				// An unrelated session cannot discharge either descendant.
				r := result(wave, false)
				r.ReleaseOnly = true
				r.Session = "foreign"
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got, _ = client.GetJob(ctx, parent.ID)
				if got.Cancellation == nil || ackRelease(*got.Cancellation) != "pending" {
					t.Fatal("foreign release accepted")
				}
				r = result(wave, false)
				r.ReleaseOnly = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got, _ = client.GetJob(ctx, parent.ID)
				if ackRelease(*got.Cancellation) != "pending" {
					t.Fatal("running consumer forgotten")
				}
				finish(t, q, consumer, true)
				got, err = client.GetJob(ctx, parent.ID)
				if err != nil || ackRelease(*got.Cancellation) != "confirmed" || got.ReleaseConfirmed {
					t.Fatalf("settled aggregate: %+v %v", got, err)
				}
				if len(got.FanOut.Children) != 2 || len(got.FanOut.Results) != 2 || !got.CancellationScope.Pieces[0].Accepted {
					t.Fatal("terminal results or dispatch fence lost")
				}
				if got.CancellationScope.Pieces[6].Materialized || got.CancellationScope.Pieces[6].Release != "not_applicable" {
					t.Fatal("invented undispatched execution")
				}
				if len(got.CancellationScope.Pieces[6].Dependents) != 2 {
					t.Fatal("future dependent omitted")
				}
				ack, err = client.Cancel(ctx, parent.ID)
				if err != nil || ackRelease(ack) != "confirmed" {
					t.Fatal("cancel/GET disagreement", ack, err)
				}
				if err := q.Started(result(wave, true)); !errors.Is(err, ErrStale) {
					t.Fatal("cancelled attempt authorized", err)
				}
				if next := check(t, q, m, nil).Job; next != nil {
					t.Fatal("undispatched work materialized", next)
				}
			})
		}
	}
}

func assertCancellationScope(t *testing.T, got, first, wave, ready, blocked, consumer *Job, release string) {
	t.Helper()
	if got.Cancellation == nil || ackRelease(*got.Cancellation) != release || !got.Cancellation.WillNotRun {
		t.Fatalf("missing GET cancellation: %+v", got)
	}
	scope := got.CancellationScope
	if scope == nil || len(scope.Pieces) != 7 || len(scope.Members) != 5 {
		t.Fatalf("incomplete scope: %+v", scope)
	}
	members := map[string]CancellationMember{}
	for _, m := range scope.Members {
		members[m.JobID] = m
		if !m.WillNotRun {
			t.Fatal("member not fenced", m)
		}
	}
	for id, want := range map[string]string{first.ID: "confirmed", wave.ID: "pending", ready.ID: "not_applicable", blocked.ID: "not_applicable", consumer.ID: "pending"} {
		if members[id].Release != want {
			t.Fatalf("member %s release=%s want=%s", id, members[id].Release, want)
		}
	}
	if p := scope.Pieces[6]; p.Key != "item-6" || p.Materialized || p.Accepted || p.WaveID != "" || !p.WillNotRun || p.Release != "not_applicable" || !reflect.DeepEqual(p.Dependents, []string{blocked.ID}) {
		t.Fatalf("undispatched piece: %+v", p)
	}
	if p := scope.Pieces[2]; !p.Materialized || p.Accepted || p.WaveID != wave.ID || p.Release != "pending" {
		t.Fatalf("unaccepted piece: %+v", p)
	}
	data, _ := json.Marshal(scope.Pieces[6])
	var wire map[string]any
	_ = json.Unmarshal(data, &wire)
	if wire["materialized"] != false || wire["accepted"] != false || wire["release"] != "not_applicable" {
		t.Fatalf("ambiguous wire: %s", data)
	}
}

func TestCancellationScopeNeverDispatched(t *testing.T) {
	q, clock, _, s := fanOutFixture(t, 2)
	parent := submit(t, q, s)
	cancelAck(t, q, parent.ID)
	clock.Advance(time.Second)
	q = reopenCancellationQueue(t, q, clock)
	got, err := q.jobSnapshot(parent.ID)
	if err != nil || got.Cancellation == nil || ackRelease(*got.Cancellation) != "not_applicable" || len(got.CancellationScope.Pieces) != 2 || len(got.CancellationScope.Members) != 0 {
		t.Fatalf("unassigned parent: %+v %v", got, err)
	}
}

// A settled replacement cannot hide an unreleased lost consumer execution.
func TestCancellationScopeHistoricalCopies(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 1)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	wave := check(t, q, m, nil).Job
	finish(t, q, wave, true)
	c := pieceConsumer(cpuSpec(1, 1), parent.ID, "item-0")
	c.Project = s.Project
	c.AllowedMachines = []string{"one", "two"}
	consumer := submit(t, q, c)
	one := cpuMachine("one", "one", "cpu", 4, 16)
	two := cpuMachine("two", "two", "cpu", 4, 16)
	first := check(t, q, one, nil).Job
	if first == nil || first.ID != consumer.ID {
		t.Fatal("missing first consumer attempt")
	}
	clock.Advance(LossAfter)
	check(t, q, observer(), nil)
	second := check(t, q, two, nil).Job
	if second == nil || second.ID != consumer.ID || second.Attempt != first.Attempt+1 {
		t.Fatal("missing replacement consumer")
	}
	ack := cancelAck(t, q, parent.ID)
	if ackRelease(ack) != "pending" {
		t.Fatal("lost copy not pending")
	}
	finish(t, q, second, true)
	got, err := q.jobSnapshot(parent.ID)
	if err != nil || ackRelease(*got.Cancellation) != "pending" {
		t.Fatal("old execution disappeared", err)
	}
	q = reopenCancellationQueue(t, q, clock)
	r := result(first, false)
	r.ReleaseOnly = true
	r.Session = "wrong"
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got, err = q.jobSnapshot(parent.ID)
	if err != nil || ackRelease(*got.Cancellation) != "pending" {
		t.Fatal("wrong session released lost copy")
	}
	r.Session = first.Session
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got, err = q.jobSnapshot(parent.ID)
	if err != nil || ackRelease(*got.Cancellation) != "confirmed" || got.State != "succeeded" || len(got.FanOut.Results) != 1 {
		t.Fatal("settlement changed terminal producer", err)
	}
}
