package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestServiceConfigLifecycleDefaultAndExplicitBudgets(t *testing.T) {
	for _, budget := range []int{-1, 0, 7} {
		c := ServiceConfig{ProductionInfrastructureRetries: budget}
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		_, present := raw["production_infrastructure_retries"]
		if present != (budget >= 0) {
			t.Fatal("default/explicit intent lost", budget, present)
		}
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		got, err := ReadServiceConfig(path)
		if err != nil || got.ProductionInfrastructureRetries != budget {
			t.Fatal(budget, got.ProductionInfrastructureRetries, err)
		}
	}
}
