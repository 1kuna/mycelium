package peer

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type RescueJobLog struct {
	local             *JobLog
	registry          ports.JobRegistry
	privatePayloadKey []byte
}

func NewRescueJobLog(local *JobLog, registry ports.JobRegistry, privatePayloadKey []byte) *RescueJobLog {
	return &RescueJobLog{
		local:             local,
		registry:          registry,
		privatePayloadKey: append([]byte(nil), privatePayloadKey...),
	}
}

func (l *RescueJobLog) PutJob(ctx context.Context, job domain.Job, payload []byte) error {
	if l.local == nil {
		return fmt.Errorf("local job log is required")
	}
	return l.local.PutJob(ctx, job, payload)
}

func (l *RescueJobLog) Job(ctx context.Context, jobID string) (domain.Job, []byte, error) {
	if err := ctx.Err(); err != nil {
		return domain.Job{}, nil, err
	}
	var localErr error
	if l.local != nil {
		job, payload, err := l.local.Job(ctx, jobID)
		if err == nil {
			return job, payload, nil
		}
		localErr = err
	}
	if l.registry == nil {
		if localErr != nil {
			return domain.Job{}, nil, localErr
		}
		return domain.Job{}, nil, fmt.Errorf("job registry is not configured")
	}
	records, err := l.registry.Snapshot(ctx)
	if err != nil {
		return domain.Job{}, nil, err
	}
	for _, rec := range records {
		if rec.JobID != jobID {
			continue
		}
		return l.decodeRecord(rec)
	}
	if localErr != nil {
		return domain.Job{}, nil, localErr
	}
	return domain.Job{}, nil, fmt.Errorf("job %q is not in the job registry", jobID)
}

func (l *RescueJobLog) decodeRecord(rec domain.JobRecord) (domain.Job, []byte, error) {
	if rec.PayloadRedacted {
		return domain.Job{}, nil, fmt.Errorf("job %q has redacted rescue payload", rec.JobID)
	}
	job, payload, err := DecodeRescuePayloadWithKey(rec.Request, l.privatePayloadKey)
	if err != nil {
		return domain.Job{}, nil, err
	}
	if job.ID != rec.JobID {
		return domain.Job{}, nil, fmt.Errorf("rescue payload job %q does not match registry job %q", job.ID, rec.JobID)
	}
	return job, payload, nil
}

var _ JobSource = (*RescueJobLog)(nil)
