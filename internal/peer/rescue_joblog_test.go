package peer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
)

func TestRescueJobLogUsesLocalThenRegistry(t *testing.T) {
	ctx := context.Background()
	local := NewJobLog()
	localJob := fixtures.MakeJob(fixtures.WithJobID("local-job"))
	if err := local.PutJob(ctx, localJob, []byte(`{"job":"local"}`)); err != nil {
		t.Fatalf("PutJob local: %v", err)
	}
	registry := NewJobRegistry()
	registryJob := fixtures.MakeJob(fixtures.WithJobID("registry-job"))
	registryPayload, err := EncodeRescuePayload(registryJob, []byte(`{"job":"registry"}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	if err := registry.Put(ctx, domain.JobRecord{
		JobID:       registryJob.ID,
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     registryPayload,
		UpdatedAt:   time.Unix(1, 0).UTC(),
	}); err != nil {
		t.Fatalf("registry Put: %v", err)
	}
	log := NewRescueJobLog(local, registry, nil)
	wrappedJob := fixtures.MakeJob(fixtures.WithJobID("wrapped-job"))
	if err := log.PutJob(ctx, wrappedJob, []byte(`{"job":"wrapped"}`)); err != nil {
		t.Fatalf("PutJob wrapped: %v", err)
	}

	got, payload, err := log.Job(ctx, wrappedJob.ID)
	if err != nil || got.ID != wrappedJob.ID || string(payload) != `{"job":"wrapped"}` {
		t.Fatalf("wrapped job=%+v payload=%s err=%v", got, payload, err)
	}

	got, payload, err = log.Job(ctx, localJob.ID)
	if err != nil || got.ID != localJob.ID || string(payload) != `{"job":"local"}` {
		t.Fatalf("local job=%+v payload=%s err=%v", got, payload, err)
	}
	payload[0] = '['
	_, payload, err = log.Job(ctx, localJob.ID)
	if err != nil || string(payload) != `{"job":"local"}` {
		t.Fatalf("local payload was not cloned: %s err=%v", payload, err)
	}

	got, payload, err = log.Job(ctx, registryJob.ID)
	if err != nil || got.ID != registryJob.ID || string(payload) != `{"job":"registry"}` {
		t.Fatalf("registry job=%+v payload=%s err=%v", got, payload, err)
	}
	payload[0] = '['
	_, payload, err = log.Job(ctx, registryJob.ID)
	if err != nil || string(payload) != `{"job":"registry"}` {
		t.Fatalf("registry payload was not cloned: %s err=%v", payload, err)
	}
}

func TestRescueJobLogEncryptsPrivateRegistryFallback(t *testing.T) {
	ctx := context.Background()
	key := []byte("0123456789abcdef0123456789abcdef")
	job := fixtures.MakeJob(fixtures.WithJobID("private-job"))
	job.Handling = domain.HandlingPrivate
	body := []byte(`{"secret":true}`)
	encoded, err := EncodeRescuePayloadWithKey(job, body, key)
	if err != nil {
		t.Fatalf("EncodeRescuePayloadWithKey: %v", err)
	}
	registry := NewJobRegistry()
	if err := registry.Put(ctx, domain.JobRecord{
		JobID:       job.ID,
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     encoded,
		Handling:    domain.HandlingPrivate,
		UpdatedAt:   time.Unix(2, 0).UTC(),
	}); err != nil {
		t.Fatalf("registry Put: %v", err)
	}

	got, payload, err := NewRescueJobLog(NewJobLog(), registry, key).Job(ctx, job.ID)
	if err != nil || got.ID != job.ID || got.Handling != domain.HandlingPrivate || string(payload) != string(body) {
		t.Fatalf("private job=%+v payload=%s err=%v", got, payload, err)
	}
	if _, _, err := NewRescueJobLog(NewJobLog(), registry, nil).Job(ctx, job.ID); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("missing key err = %v", err)
	}
}

func TestRescueJobLogErrors(t *testing.T) {
	ctx := context.Background()
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	payload, err := EncodeRescuePayload(fixtures.MakeJob(fixtures.WithJobID("other")), []byte(`{}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	registry := NewJobRegistry()
	if err := registry.Put(ctx, domain.JobRecord{
		JobID:       job.ID,
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     payload,
		UpdatedAt:   time.Unix(3, 0).UTC(),
	}); err != nil {
		t.Fatalf("registry Put mismatch: %v", err)
	}
	redacted := NewJobRegistry()
	if err := redacted.Put(ctx, domain.JobRecord{
		JobID:           "redacted",
		Coordinator:     "peer-a",
		Status:          domain.JobRunning,
		PayloadRedacted: true,
		Handling:        domain.HandlingPrivate,
		UpdatedAt:       time.Unix(4, 0).UTC(),
	}); err != nil {
		t.Fatalf("registry Put redacted: %v", err)
	}
	unrelated := NewJobRegistry()
	unrelatedPayload, err := EncodeRescuePayload(fixtures.MakeJob(fixtures.WithJobID("other-job")), []byte(`{}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload unrelated: %v", err)
	}
	if err := unrelated.Put(ctx, domain.JobRecord{
		JobID:       "other-job",
		Coordinator: "peer-a",
		Status:      domain.JobRunning,
		Request:     unrelatedPayload,
		UpdatedAt:   time.Unix(5, 0).UTC(),
	}); err != nil {
		t.Fatalf("registry Put unrelated: %v", err)
	}
	snapshotErr := errors.New("snapshot")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, tt := range []struct {
		name string
		log  *RescueJobLog
		ctx  context.Context
		id   string
		want string
	}{
		{name: "missing local on put", log: NewRescueJobLog(nil, nil, nil), ctx: ctx, id: "", want: "local job log"},
		{name: "canceled read", log: NewRescueJobLog(NewJobLog(), nil, nil), ctx: canceled, id: "job-a", want: "canceled"},
		{name: "local miss no registry", log: NewRescueJobLog(NewJobLog(), nil, nil), ctx: ctx, id: "missing", want: "local job log"},
		{name: "no source", log: NewRescueJobLog(nil, nil, nil), ctx: ctx, id: "missing", want: "not configured"},
		{name: "snapshot", log: NewRescueJobLog(NewJobLog(), failingJobRegistry{err: snapshotErr}, nil), ctx: ctx, id: "job-a", want: "snapshot"},
		{name: "mismatch", log: NewRescueJobLog(NewJobLog(), registry, nil), ctx: ctx, id: job.ID, want: "does not match"},
		{name: "redacted", log: NewRescueJobLog(NewJobLog(), redacted, nil), ctx: ctx, id: "redacted", want: "redacted"},
		{name: "unrelated registry preserves local miss", log: NewRescueJobLog(NewJobLog(), unrelated, nil), ctx: ctx, id: "missing", want: "local job log"},
		{name: "registry miss", log: NewRescueJobLog(nil, NewJobRegistry(), nil), ctx: ctx, id: "missing", want: "job registry"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.id == "" {
				err = tt.log.PutJob(tt.ctx, job, []byte(`{}`))
			} else {
				_, _, err = tt.log.Job(tt.ctx, tt.id)
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

type failingJobRegistry struct {
	err error
}

func (r failingJobRegistry) Put(context.Context, domain.JobRecord) error {
	return nil
}

func (r failingJobRegistry) Watch(context.Context, string) (<-chan domain.JobRecord, error) {
	ch := make(chan domain.JobRecord)
	close(ch)
	return ch, nil
}

func (r failingJobRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	return nil, r.err
}
