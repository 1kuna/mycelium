package myce

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func deliveryJob(t *testing.T, q *Queue) (*Job, EnvironmentRecipe) {
	t.Helper()
	r := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "tools"
	j := submit(t, q, s)
	return j, r
}

func TestBlobDeliveryAssignmentFailureUsesExactRelease(t *testing.T) {
	for _, protocol := range []string{"", keyedAttemptProtocol} {
		t.Run("protocol="+protocol, func(t *testing.T) {
			q, _ := fixture(t)
			j, r := deliveryJob(t, q)
			ref := q.s.Jobs[j.ID].EnvironmentRecipes["fake"].FilesRef
			path := q.files.blob(*ref)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			m := machine("delivery", "fake")
			m.Environments = []string{r.Key}
			c := Checkin{Machine: m, AttemptProtocol: protocol}
			reply, err := q.Checkin(c)
			if err != nil {
				t.Fatal("committed check-in turned into HTTP error", err)
			}
			current := job(t, q, j.ID)
			if current.State != "stopping" || current.ReleaseConfirmed {
				t.Fatalf("assignment stranded or prematurely released: %+v", current)
			}
			if reply.Job != nil {
				t.Fatal("unreadable assignment delivered")
			}
			if protocol == "" {
				if !reply.Stop {
					t.Fatal("legacy stop lost")
				}
			} else if len(reply.Attempts) != 1 || !reply.Attempts[0].Stop || reply.Attempts[0].Job != nil || reply.Attempts[0].ID != current.ID || reply.Attempts[0].Attempt != current.Attempt || reply.Attempts[0].Session != current.Session {
				t.Fatalf("exact stop lost: %+v", reply)
			}
			old := Result{Execution: Execution{ID: current.ID, Attempt: current.Attempt}, Machine: m.ID, Session: m.Session}
			if err := q.Started(old); !errors.Is(err, ErrStale) {
				t.Fatal("failed assignment still authorizes start", err)
			}
			old.ReleaseOnly = true
			if err := q.Complete(old); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			reply, err = q.Checkin(c)
			if err != nil {
				t.Fatal(err)
			}
			if protocol != "" {
				reply.Job = reply.Attempts[0].Job
			}
			if reply.Job == nil || reply.Job.Attempt <= current.Attempt || len(reply.Job.EnvironmentRecipes["fake"].Files) == 0 {
				t.Fatal("assignment did not recover after exact release", reply)
			}
		})
	}
}

func TestBlobDeliveryPreparationFailurePreservesControl(t *testing.T) {
	q, _ := fixture(t)
	j, _ := deliveryJob(t, q)
	ref := q.s.Jobs[j.ID].EnvironmentRecipes["fake"].FilesRef
	path := q.files.blob(*ref)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	m := machine("builder", "fake")
	m.EnvironmentBuilder = true
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: "stale", Attempt: 1}, Session: m.Session}}}
	reply, err := q.Checkin(c)
	if err != nil {
		t.Fatal("preparation swallowed control delivery", err)
	}
	if len(reply.EnvironmentPlan) != 0 || len(reply.Attempts) != 1 || !reply.Attempts[0].Stop || !reply.Attempts[0].Kill {
		t.Fatalf("bad preparation/control reply: %+v", reply)
	}
	if len(q.s.PreparationPlans) != 0 {
		t.Fatal("undelivered preparation still owns custody")
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	c.Attempts = nil
	reply, err = q.Checkin(c)
	if err != nil || len(reply.EnvironmentPlan) != 1 || len(reply.EnvironmentPlan[0].Files) == 0 {
		t.Fatal("preparation did not recover", reply, err)
	}
}

func TestBlobDeliveryReadOutsideQueueAndReplacementFence(t *testing.T) {
	q, _ := fixture(t)
	j, r := deliveryJob(t, q)
	m := machine("delivery", "fake")
	m.Environments = []string{r.Key}
	reply := check(t, q, m, nil)
	// Read the durable reference rather than the already hydrated transport.
	offer := cloneJob(q.s.Jobs[j.ID])
	reply.Job = &offer
	path := q.files.blob(*offer.EnvironmentRecipes["fake"].FilesRef)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	writer := make(chan *os.File, 1)
	go func() { f, _ := os.OpenFile(path, os.O_WRONLY, 0); writer <- f }()
	done := make(chan error, 1)
	go func() { done <- q.hydrateReply(&reply) }()
	f := submitVerificationWait(t, "blob read", writer)
	if f == nil {
		t.Fatal("blob reader did not reach FIFO")
	}
	defer f.Close()
	submitVerificationProgress(t, "replacement transaction during blob read", func() error {
		return q.update(func() error {
			current := q.touchJob(j.ID)
			current.Attempt++
			current.Session = "replacement"
			return nil
		})
	})
	// Empty bytes fail digest verification only after the newer attempt commits.
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := submitVerificationWait(t, "failed delivery", done); err != nil {
		t.Fatal(err)
	}
	if current := job(t, q, j.ID); current.State != "running" || current.Session != "replacement" {
		t.Fatal("delivery failure changed replacement", current)
	}
}
