package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func completionTerminal(t *testing.T, q *Queue, j *Job) {
	t.Helper()
	if err := q.update(func() error {
		j := q.touchJob(j.ID)
		j.State = "succeeded"
		now := q.clock.Now()
		j.FinishedAt = &now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func completionChange(t *testing.T, q *Queue, id, operation, delivery string) CompletionReply {
	t.Helper()
	reply, err := q.ChangeCompletion(context.Background(), id, CompletionRequest{Operation: operation, DeliveryID: delivery})
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestCompletionSuppressionDurable(t *testing.T) {
	for _, mode := range []string{"cancel_id", "cancel_key", "supersede", "foreground"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			s := spec("experiment")
			s.SubmissionKey = "original"
			j := submit(t, q, s)
			completionTerminal(t, q, j)
			switch mode {
			case "cancel_id":
				if _, err := q.CancelJob(j.ID); err != nil {
					t.Fatal(err)
				}
			case "cancel_key":
				if _, err := q.CancelSubmission(s.Project, s.SubmissionKey); err != nil {
					t.Fatal(err)
				}
			case "supersede":
				replacement := s
				replacement.SubmissionKey = "replacement"
				replacement.Supersedes = j.ID
				submit(t, q, replacement)
			case "foreground":
				completionChange(t, q, j.ID, "collect", "")
			}
			q = reopenCancellationQueue(t, q, clock)
			if got := completionChange(t, q, j.ID, "claim", "delivery"); got.Allowed {
				t.Fatal("suppressed completion was delivered after restart")
			}
		})
	}
}

func TestCompletionUnobservedAndInFlightIdentity(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	completionTerminal(t, q, j)
	first := completionChange(t, q, j.ID, "claim", "first-delivery")
	if !first.Allowed || first.DeliveryID != "first-delivery" {
		t.Fatal(first)
	}
	q = reopenCancellationQueue(t, q, clock)
	retry := completionChange(t, q, j.ID, "claim", "different-local-receipt")
	if !retry.Allowed || retry.DeliveryID != first.DeliveryID {
		t.Fatal("in-flight replay changed delivery_id", retry)
	}
	completionChange(t, q, j.ID, "delivered", retry.DeliveryID)
	q = reopenCancellationQueue(t, q, clock)
	if completionChange(t, q, j.ID, "claim", "again").Allowed {
		t.Fatal("acknowledged completion delivered twice")
	}
	j = submit(t, q, spec("experiment"))
	completionTerminal(t, q, j)
	first = completionChange(t, q, j.ID, "claim", "in-flight")
	completionChange(t, q, j.ID, "collect", "")
	if got := completionChange(t, q, j.ID, "claim", "after-collection"); got.Allowed || got.DeliveryID != first.DeliveryID {
		t.Fatal("collection changed in-flight identity or allowed another wake", got)
	}
	// The already-running receiver may finish; its acknowledgement stays valid.
	completionChange(t, q, j.ID, "delivered", first.DeliveryID)
}

func TestCompletionSupersedesRequiresExplicitSameProjectLink(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "original"
	old := submit(t, q, s)
	completionTerminal(t, q, old)
	// Merely resubmitting under a different key/name does not replace anything.
	s.SubmissionKey = "unrelated"
	submit(t, q, s)
	if !completionChange(t, q, old.ID, "claim", "original-delivery").Allowed {
		t.Fatal("guessed supersession")
	}
	s.Project, s.SubmissionKey, s.Supersedes = "other", "replacement", old.ID
	_, err := q.Submit(s)
	submissionError(t, err, 409, "job_supersedes_invalid")
	if q.s.Jobs[old.ID].Completion.SupersededBy != "" {
		t.Fatal("cross-project suppression persisted")
	}
	s.Project = old.Spec.Project
	replacement := submit(t, q, s)
	if replay := submit(t, q, s); replay.ID != replacement.ID {
		t.Fatal("replacement replay created another job")
	}
	if completionChange(t, q, old.ID, "claim", "late").Allowed {
		t.Fatal("explicit replacement did not suppress")
	}
}

func TestCompletionForegroundBatchAndReadOnlyObserver(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	completionTerminal(t, q, j)
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture", Clock: clock}
	ctx := context.Background()
	if _, err := c.WaitBatch(ctx, []string{j.ID}, nil, 0); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID].Completion != nil {
		t.Fatal("observer collected its own result")
	}
	if _, err := c.WaitBatchCollected(ctx, []string{j.ID}, nil, 0); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, clock)
	if completionChange(t, q, j.ID, "claim", "after-wait").Allowed {
		t.Fatal("foreground batch did not persist collection")
	}
}

func TestCompletionCollectionRollbackAndPendingWait(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	if err := q.collectCompletions(context.Background(), []WaitState{{ID: j.ID, State: "queued"}}); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID].Completion != nil {
		t.Fatal("pending wait disarmed completion")
	}
	completionTerminal(t, q, j)
	// One bad row rolls back all collection, leaving real completions armed.
	if err := q.collectCompletions(context.Background(), []WaitState{{ID: j.ID, State: "succeeded"}, {ID: "missing", State: "succeeded"}}); err == nil {
		t.Fatal("expected failed transaction")
	}
	clock.Advance(time.Second)
	if !completionChange(t, q, j.ID, "claim", "after-rollback").Allowed {
		t.Fatal("failed collection lost completion")
	}
}

func TestCompletionForegroundWaitKeepsPreviousWire(t *testing.T) {
	// Previous queues reject unknown JSON fields. Collection opts in through
	// a header they can ignore, without an extra request or per-job read loop.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var legacy struct {
			IDs     []string          `json:"ids"`
			After   map[string]uint64 `json:"after,omitempty"`
			Seconds int               `json:"seconds,omitempty"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&legacy); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if r.Header.Get(completionCollectHeader) != "1" {
			t.Error("foreground collection header missing")
		}
		_ = json.NewEncoder(w).Encode(WaitReply{Jobs: []WaitState{{ID: legacy.IDs[0], State: "succeeded", Revision: 1}}})
	}))
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	if _, err := c.WaitBatchCollected(context.Background(), []string{"job"}, nil, 0); err != nil {
		t.Fatal(err)
	}
}
