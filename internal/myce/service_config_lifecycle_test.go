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
		if !present {
			t.Fatal("default/explicit intent lost", budget, present)
		}
		// Migration journals decode nested configs without ReadServiceConfig.
		// Absence would turn adaptive -1 into the explicit zero-retry budget.
		var nested struct{ Config ServiceConfig }
		wrapped, err := json.Marshal(struct{ Config ServiceConfig }{c})
		if err != nil || json.Unmarshal(wrapped, &nested) != nil || nested.Config.ProductionInfrastructureRetries != budget {
			t.Fatal("nested retry budget changed", budget, nested.Config.ProductionInfrastructureRetries, err)
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
