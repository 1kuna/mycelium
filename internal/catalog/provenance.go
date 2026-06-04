package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"

	"mycelium/internal/safeid"
)

func ReadProvenance(storeDir, presetID string) (Provenance, error) {
	if err := safeid.Validate("preset id", presetID); err != nil {
		return Provenance{}, err
	}
	data, err := os.ReadFile(filepath.Join(storeDir, "provenance", presetID+".json"))
	if err != nil {
		return Provenance{}, err
	}
	var prov Provenance
	if err := json.Unmarshal(data, &prov); err != nil {
		return Provenance{}, err
	}
	return prov, nil
}
