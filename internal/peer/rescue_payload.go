package peer

import (
	"encoding/json"
	"fmt"

	"mycelium/internal/domain"
)

type RescuePayload struct {
	Job  domain.Job `json:"job"`
	Body []byte     `json:"body"`
}

func EncodeRescuePayload(job domain.Job, body []byte) ([]byte, error) {
	if job.ID == "" {
		return nil, fmt.Errorf("rescue payload job id is required")
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("rescue payload body is required")
	}
	data, _ := json.Marshal(RescuePayload{Job: job, Body: append([]byte(nil), body...)})
	return data, nil
}

func DecodeRescuePayload(data []byte) (domain.Job, []byte, error) {
	if len(data) == 0 {
		return domain.Job{}, nil, fmt.Errorf("rescue payload is required")
	}
	var payload RescuePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return domain.Job{}, nil, err
	}
	if payload.Job.ID == "" {
		return domain.Job{}, nil, fmt.Errorf("rescue payload job id is required")
	}
	if len(payload.Body) == 0 {
		return domain.Job{}, nil, fmt.Errorf("rescue payload body is required")
	}
	return payload.Job, append([]byte(nil), payload.Body...), nil
}
