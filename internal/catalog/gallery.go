package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"

	"mycelium/internal/domain"
	"mycelium/internal/safeid"
)

func ReadPreset(storeDir, presetID string) (domain.Preset, error) {
	if err := safeid.Validate("preset id", presetID); err != nil {
		return domain.Preset{}, err
	}
	data, err := os.ReadFile(filepath.Join(storeDir, "presets", presetID+".json"))
	if err != nil {
		return domain.Preset{}, err
	}
	var preset domain.Preset
	if err := json.Unmarshal(data, &preset); err != nil {
		return domain.Preset{}, err
	}
	return preset, nil
}
