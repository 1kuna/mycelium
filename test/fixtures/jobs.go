package fixtures

import "mycelium/internal/domain"

func MakeJob(opts ...func(*domain.Job)) domain.Job {
	j := domain.Job{
		ID:         "job_test",
		TaskType:   "chat",
		Model:      "qwen2.5-9b-instruct",
		Project:    "project-test",
		Priority:   domain.PriorityNormal,
		SpeedPref:  domain.SpeedThroughput,
		Preemption: domain.PreemptInherit,
		Streaming:  true,
		Status:     domain.JobQueued,
	}
	for _, opt := range opts {
		opt(&j)
	}
	return j
}

func WithJobID(id string) func(*domain.Job) {
	return func(j *domain.Job) { j.ID = id }
}

func Interactive(j *domain.Job) {
	j.Priority = domain.PriorityInteractive
}

func Background(j *domain.Job) {
	j.Priority = domain.PriorityBackground
}

func Latency(j *domain.Job) {
	j.SpeedPref = domain.SpeedLatency
}

func Auto(j *domain.Job) {
	j.SpeedPref = domain.SpeedAuto
}

func HardForInteractive(j *domain.Job) {
	j.Preemption = domain.PreemptHardForInteractive
}

func Hard(j *domain.Job) {
	j.Preemption = domain.PreemptHard
}

func WithContext(n int) func(*domain.Job) {
	return func(j *domain.Job) { j.ContextRequest = n }
}

func WithPreset(id string) func(*domain.Job) {
	return func(j *domain.Job) { j.PresetID = id }
}
