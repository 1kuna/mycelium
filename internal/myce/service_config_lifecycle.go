package myce

import (
	"bytes"
	"encoding/json"
)

// Absence lets older installers retain their supported default rather than
// rejecting the queue's adaptive sentinel. Explicit budgets, including zero,
// remain saved intent.
func (c ServiceConfig) MarshalJSON() ([]byte, error) {
	type fields ServiceConfig
	var budget *int
	if c.ProductionInfrastructureRetries != DefaultProductionInfrastructureRetries {
		value := c.ProductionInfrastructureRetries
		budget = &value
	}
	return json.Marshal(struct {
		*fields
		ProductionInfrastructureRetries *int `json:"production_infrastructure_retries,omitempty"`
	}{(*fields)(&c), budget})
}

// Apply the absent default at every decode boundary, including nested migration
// journals. Keep strict config decoding and the known retired overlay fields.
func (c *ServiceConfig) UnmarshalJSON(data []byte) error {
	type fields ServiceConfig
	next := fields(*c)
	next.ProductionInfrastructureRetries = DefaultProductionInfrastructureRetries
	legacy := struct {
		*fields
		HostStateDir    json.RawMessage `json:"host_state_dir"`
		StartDisabled   json.RawMessage `json:"start_disabled"`
		RequiredWorkers json.RawMessage `json:"required_workers"`
	}{fields: &next}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&legacy); err != nil {
		return err
	}
	*c = ServiceConfig(next)
	return nil
}
