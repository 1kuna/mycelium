package workload

import (
	"encoding/json"
	"fmt"

	"mycelium/internal/domain"
)

type AdmissionRequest struct {
	Job       domain.Job      `json:"job"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	QueueOnly bool            `json:"queue_only,omitempty"`
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
	Payload       PayloadEvidence          `json:"payload_evidence,omitempty"`
	Execution     ExecutionEvidence        `json:"execution_evidence,omitempty"`
}

type StatusResponse struct {
	Job       domain.Job        `json:"job"`
	Result    *domain.JobResult `json:"result,omitempty"`
	Payload   PayloadEvidence   `json:"payload_evidence,omitempty"`
	Execution ExecutionEvidence `json:"execution_evidence,omitempty"`
}

type PayloadEvidence struct {
	State      string `json:"state,omitempty"`
	Replayable bool   `json:"replayable,omitempty"`
	Redacted   bool   `json:"redacted,omitempty"`
	Bytes      int    `json:"bytes,omitempty"`
	Error      string `json:"error,omitempty"`
}

type ExecutionEvidence struct {
	State                          string `json:"state,omitempty"`
	AutomaticProviderOutputCapture bool   `json:"automatic_provider_output_capture"`
	PayloadContract                string `json:"payload_contract,omitempty"`
	PayloadKind                    string `json:"payload_kind,omitempty"`
	PayloadExecutable              bool   `json:"payload_executable,omitempty"`
	ResultReplayable               bool   `json:"result_replayable,omitempty"`
	ResultBodyStored               bool   `json:"result_body_stored,omitempty"`
	ResultArtifact                 bool   `json:"result_artifact,omitempty"`
	ResultBytes                    int    `json:"result_bytes,omitempty"`
	ResultSHA256                   bool   `json:"result_sha256,omitempty"`
	Error                          string `json:"error,omitempty"`
}

const (
	ExecutablePayloadKindOpenAIChat        = "openai_chat"
	ExecutablePayloadKindOpenAICompletion  = "openai_completion"
	ExecutablePayloadKindAnthropicMessages = "anthropic_messages"
)

type ExecutablePayload struct {
	Kind string          `json:"kind"`
	Body json.RawMessage `json:"body"`
}

func DecodeExecutablePayload(payload []byte) (ExecutablePayload, error) {
	var out ExecutablePayload
	if len(payload) == 0 {
		return out, fmt.Errorf("payload is empty")
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, err
	}
	switch out.Kind {
	case ExecutablePayloadKindOpenAIChat, ExecutablePayloadKindOpenAICompletion, ExecutablePayloadKindAnthropicMessages:
	default:
		return out, fmt.Errorf("unsupported executable payload kind %q", out.Kind)
	}
	if len(out.Body) == 0 {
		return out, fmt.Errorf("executable payload body is required")
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(out.Body, &body); err != nil {
		return out, fmt.Errorf("executable payload body must be a JSON object: %w", err)
	}
	return out, nil
}

type FinishRequest struct {
	Error           string `json:"error,omitempty"`
	ResultArtifact  string `json:"result_artifact,omitempty"`
	ResultMediaType string `json:"result_media_type,omitempty"`
	ResultBytes     int    `json:"result_bytes,omitempty"`
	ResultSHA256    string `json:"result_sha256,omitempty"`
	ResultBody      []byte `json:"result_body,omitempty"`
}

type CancelRequest struct {
	Reason string `json:"reason,omitempty"`
}

type FinishResponse struct {
	Job       domain.Job        `json:"job"`
	Result    *domain.JobResult `json:"result,omitempty"`
	Execution ExecutionEvidence `json:"execution_evidence,omitempty"`
}

type DrainResponse struct {
	Results []AdmissionResponse `json:"results"`
}
