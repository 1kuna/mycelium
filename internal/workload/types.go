package workload

import (
	"encoding/json"

	"mycelium/internal/domain"
)

type AdmissionRequest struct {
	Job     domain.Job      `json:"job"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type AdmissionResponse struct {
	Job           domain.Job               `json:"job"`
	Decision      domain.PlacementDecision `json:"decision"`
	Instance      domain.ModelInstance     `json:"instance,omitempty"`
	Lease         domain.Lease             `json:"lease,omitempty"`
	Result        *domain.JobResult        `json:"result,omitempty"`
	Queued        bool                     `json:"queued"`
	PayloadBytes  int                      `json:"payload_bytes,omitempty"`
	PayloadStored bool                     `json:"payload_stored,omitempty"`
}

type StatusResponse struct {
	Job    domain.Job        `json:"job"`
	Result *domain.JobResult `json:"result,omitempty"`
}

type DrainResponse struct {
	Results []AdmissionResponse `json:"results"`
}
