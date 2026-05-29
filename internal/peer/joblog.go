package peer

import (
	"context"
	"fmt"
	"sync"

	"mycelium/internal/domain"
)

type JobLog struct {
	mu      sync.Mutex
	jobs    map[string]domain.Job
	payload map[string][]byte
}

func NewJobLog() *JobLog {
	return &JobLog{
		jobs:    map[string]domain.Job{},
		payload: map[string][]byte{},
	}
}

func (l *JobLog) PutJob(ctx context.Context, job domain.Job, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.ID == "" {
		return fmt.Errorf("job id is required")
	}
	if len(payload) == 0 {
		return fmt.Errorf("job %q has no rescue payload", job.ID)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.jobs == nil {
		l.jobs = map[string]domain.Job{}
	}
	if l.payload == nil {
		l.payload = map[string][]byte{}
	}
	l.jobs[job.ID] = job
	l.payload[job.ID] = append([]byte(nil), payload...)
	return nil
}

func (l *JobLog) Job(ctx context.Context, jobID string) (domain.Job, []byte, error) {
	if err := ctx.Err(); err != nil {
		return domain.Job{}, nil, err
	}
	if jobID == "" {
		return domain.Job{}, nil, fmt.Errorf("job id is required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	job, ok := l.jobs[jobID]
	if !ok {
		return domain.Job{}, nil, fmt.Errorf("job %q is not in the local job log", jobID)
	}
	return job, append([]byte(nil), l.payload[jobID]...), nil
}

var _ JobSource = (*JobLog)(nil)
