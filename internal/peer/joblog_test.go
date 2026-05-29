package peer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"mycelium/test/fixtures"
)

func TestJobLogPutAndReadClonesPayload(t *testing.T) {
	log := NewJobLog()
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	payload := []byte(`{"model":"a"}`)
	if err := log.PutJob(context.Background(), job, payload); err != nil {
		t.Fatalf("PutJob: %v", err)
	}
	payload[0] = '['

	got, gotPayload, err := log.Job(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("Job: %v", err)
	}
	if got.ID != job.ID || string(gotPayload) != `{"model":"a"}` {
		t.Fatalf("job=%+v payload=%s", got, gotPayload)
	}
	gotPayload[0] = '['
	_, reread, err := log.Job(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("Job reread: %v", err)
	}
	if string(reread) != `{"model":"a"}` {
		t.Fatalf("payload was not cloned: %s", reread)
	}
}

func TestJobLogValidation(t *testing.T) {
	log := NewJobLog()
	if err := log.PutJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("")), []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("empty job err = %v", err)
	}
	if err := log.PutJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), nil); err == nil || !strings.Contains(err.Error(), "rescue payload") {
		t.Fatalf("empty payload err = %v", err)
	}
	if _, _, err := log.Job(context.Background(), "missing"); err == nil || !strings.Contains(err.Error(), "not in the local job log") {
		t.Fatalf("missing job err = %v", err)
	}
	if _, _, err := log.Job(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("empty read err = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := log.PutJob(canceled, fixtures.MakeJob(fixtures.WithJobID("job-canceled")), []byte(`{}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled put err = %v", err)
	}
	if _, _, err := log.Job(canceled, "job-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read err = %v", err)
	}
	var zero JobLog
	if err := zero.PutJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-zero")), []byte(`{"job":"zero"}`)); err != nil {
		t.Fatalf("zero PutJob: %v", err)
	}
	if _, payload, err := zero.Job(context.Background(), "job-zero"); err != nil || string(payload) != `{"job":"zero"}` {
		t.Fatalf("zero Job payload=%s err=%v", payload, err)
	}
}
