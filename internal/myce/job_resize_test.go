package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
	"time"
)

func resizeRequest(jobs ...*Job) ResizeJobsRequest {
	req := ResizeJobsRequest{Project: jobs[0].Spec.Project, HostMemoryGB: 11, RequestedBy: "operator", Reason: "measured worker use"}
	for _, j := range jobs {
		req.Jobs = append(req.Jobs, j.ID)
	}
	return req
}

func requireResizeError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var api *HTTPError
	if !errors.As(err, &api) || api.Status != status || api.Code != code {
		t.Fatalf("wanted %d %s, got %v", status, code, err)
	}
}

func TestQueuedJobResizeKeyedReplayAfterRestart(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "same-submission"
	s.Needs.HostMemoryGB = 16.38
	before := submit(t, q, s)
	req := resizeRequest(before)
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "unauthorized"}
	request := func(req ResizeJobsRequest, out *ResizeJobsReceipt) error {
		return client.Call(t.Context(), http.MethodPost, "/api/jobs/resize", req, out)
	}
	requireResizeError(t, request(req, nil), 401, "authentication_required")
	client.Token = "fixture"
	wrongProject := req
	wrongProject.Project = "another-project"
	requireResizeError(t, request(wrongProject, nil), 403, "job_project_mismatch")
	invalid := req
	invalid.HostMemoryGB = 0
	requireResizeError(t, request(invalid, nil), 400, "job_resize_invalid")
	var receipt ResizeJobsReceipt
	for range 2 {
		if err := request(req, &receipt); err != nil || receipt.HostMemoryGB != 11 || !slices.Equal(receipt.Jobs, req.Jobs) {
			t.Fatalf("resize receipt = %+v, %v", receipt, err)
		}
	}

	server.Close()
	q = reopenCancellationQueue(t, q, clock)
	replay := submit(t, q, s)
	if replay.ID != before.ID || !replay.SubmissionReused || replay.Spec.Needs.HostMemoryGB != 11 || memoryValue(replay.Spec.MemoryLimitGB) != 11 {
		t.Fatalf("original specification did not replay the resized job: %+v", replay)
	}
	expected := before.Spec
	expected.Needs.HostMemoryGB, expected.MemoryLimitGB = 11, &req.HostMemoryGB
	if !reflect.DeepEqual(replay.Spec, expected) || replay.State != "queued" || replay.Front != before.Front ||
		replay.QueueOrder != before.QueueOrder || replay.Queued != before.Queued || replay.WaitingSince != before.WaitingSince ||
		replay.Attempt != before.Attempt || !reflect.DeepEqual(replay.Dependency, before.Dependency) {
		t.Fatal("resize changed submission or queue identity")
	}
	changed := s
	changed.Needs.HostMemoryGB = 11
	_, err := q.Submit(changed)
	requireResizeError(t, err, 409, "submission_key_conflict")
	page, err := q.JobLedger(before.ID, 0, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	resizes := 0
	for _, entry := range page.Entries {
		if entry.Kind == "host_memory_resized" {
			resizes++
			want := JobResizeAudit{RequestedBy: req.RequestedBy, OldMemoryGB: 16.38, NewMemoryGB: 11}
			if entry.Resize == nil || *entry.Resize != want || entry.Reason != req.Reason || !entry.At.Equal(clock.Now()) {
				t.Fatalf("unexpected audit entry: %+v, %+v", entry, entry.Resize)
			}
		}
	}
	if resizes != 1 {
		t.Fatalf("wanted one durable resize entry after an identical retry, got %d", resizes)
	}
}

func TestQueuedJobResizeRefusesAttemptCustodyAtomically(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	offered := submit(t, q, s)
	attempt := check(t, q, machine("worker", "fake"), nil).Job
	if attempt == nil || attempt.ID != offered.ID {
		t.Fatal("missing assigned attempt")
	}
	waiting := submit(t, q, s)
	rejectBoth := func() {
		t.Helper()
		_, err := q.ResizeJobs(t.Context(), resizeRequest(waiting, offered))
		requireResizeError(t, err, 409, "job_resize_attempt_live")
		got := job(t, q, waiting.ID)
		if got.HostMemoryOverrideGB != 0 || got.Spec.Needs.HostMemoryGB != waiting.Spec.Needs.HostMemoryGB || got.Ledger.HighWater != waiting.Ledger.HighWater {
			t.Fatal("refused batch committed an earlier job")
		}
	}
	rejectBoth() // Assigned, before Started.
	if err := q.Started(result(attempt, false)); err != nil {
		t.Fatal(err)
	}
	rejectBoth() // Running.
	if err := q.update(func() error {
		q.requeue(q.s.Jobs[offered.ID], "lost worker", false)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, offered.ID); got.State != "queued" || !attemptReleasePending(got) {
		t.Fatal("fixture did not retain pending attempt custody")
	}
	rejectBoth() // Queued again, with physical release pending.
}

func TestQueuedJobResizeMemoryAdmission(t *testing.T) {
	for _, tc := range []struct {
		name           string
		budget, avail  float64
		resizeFirst    bool
		wantSecondCard bool
	}{
		{name: "incident estimate stays held", budget: 16.5, avail: 13.8},
		{name: "incident with two resized jobs still waits", budget: 16.5, avail: 13.8, resizeFirst: true},
		{name: "two resized jobs fit on separate cards", budget: 22, avail: 13.8, resizeFirst: true, wantSecondCard: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = tc.budget, tc.budget
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
			for i := range m.Devices {
				m.Devices[i].MemoryMeasured = true
			}
			s := spec("production")
			s.Name, s.Class = "separation", "production"
			s.Needs.Memory, s.Needs.MemoryGB = "auto", 0
			s.Needs.Platform = Platform{Required: []string{"fake"}}
			seedPoolReplay(q, &Job{Spec: s}, &m, "ram", 9.8, 16.38)
			machineCheck(t, q, m)

			resize := func(j *Job) {
				t.Helper()
				if _, err := q.ResizeJobs(t.Context(), resizeRequest(j)); err != nil {
					t.Fatal(err)
				}
			}
			first := submit(t, q, s)
			if tc.resizeFirst {
				resize(first)
			}
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != first.ID || len(run.AllocatedDevices) != 1 {
				t.Fatalf("first job not assigned to a card: %+v", run)
			}
			wantCharge := 16.38
			if tc.resizeFirst {
				wantCharge = 11
			}
			if run.Memory.RAM.ChargeGB != wantCharge {
				t.Fatalf("first charge = %g, want %g", run.Memory.RAM.ChargeGB, wantCharge)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}

			second := submit(t, q, s)
			resize(second)
			clock.Advance(time.Second)
			m.HostAvailableMemoryGB = tc.avail
			footprint := 9.8
			report := AttemptReport{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: run.Session, Started: true,
				CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: footprint, FootprintGB: &footprint}}
			reply, err := q.Checkin(Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
			if err != nil {
				t.Fatal(err)
			}
			next := assignedJob(reply)
			if !tc.wantSecondCard {
				if next != nil || job(t, q, second.ID).State != "queued" {
					t.Fatal("admitted over the physical allowance", next)
				}
				return
			}
			if next == nil || next.ID != second.ID || len(next.AllocatedDevices) != 1 || next.AllocatedDevices[0] == run.AllocatedDevices[0] ||
				next.Memory.RAM.ChargeGB != 11 || next.Memory.RAM.TypicalGB != 11 {
				t.Fatalf("resized job did not use 11 GiB on the idle card: %+v, wait=%s", next, job(t, q, second.ID).Reason)
			}
			worker := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return tc.avail, nil }}
			worker.hostMemorySamples = updateHostMemorySamples([]AttemptReport{report}, clock.Now(), nil)
			if err := localAttemptAdmission(worker, []Job{*run}, *next, nil); err != nil {
				t.Fatal("worker rejected the second card assignment", err)
			}
			if limit, err := worker.jobCeilingBytes(*next); err != nil || limit != 11<<30 {
				t.Fatalf("worker limit = %d, %v", limit, err)
			}
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
