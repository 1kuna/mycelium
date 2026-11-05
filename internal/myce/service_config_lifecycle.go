package myce

import "encoding/json"

// The adaptive default is represented by absence, just like ReadServiceConfig's
// other defaulted settings. Persist explicit budgets, including zero, without
// turning a queue-only default into a mandatory worker configuration value.
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
