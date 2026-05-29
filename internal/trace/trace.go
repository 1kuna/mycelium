package trace

import "time"

type Step struct {
	Operation  string         `json:"operation"`
	Input      map[string]any `json:"input,omitempty"`
	Status     string         `json:"status"`
	Error      string         `json:"error,omitempty"`
	DurationMS float64        `json:"duration_ms"`
}

type Trace struct {
	Steps []Step `json:"steps"`
	clock func() time.Time
}

func New(now func() time.Time) *Trace {
	return &Trace{clock: now}
}

func (t *Trace) Do(op string, input map[string]any, fn func() error) error {
	step := Step{Operation: op, Input: input, Status: "pending"}
	start := t.clock()
	err := fn()
	step.DurationMS = float64(t.clock().Sub(start).Microseconds()) / 1000.0
	if err != nil {
		step.Status = "error"
		step.Error = err.Error()
	} else {
		step.Status = "success"
	}
	t.Steps = append(t.Steps, step)
	return err
}
