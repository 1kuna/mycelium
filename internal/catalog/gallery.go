package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"

	"mycelium/internal/domain"
)

func ReadPreset(storeDir, presetID string) (domain.Preset, error) {
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
