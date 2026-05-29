package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func ReadProvenance(storeDir, presetID string) (Provenance, error) {
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
