package catalog

import (
	"time"

	"mycelium/internal/domain"
)

type InstallRequest struct {
	Source        string
	ID            string
	Model         string
	ContextLength int
	Quant         string
	Backend       domain.Backend
}

type InstallResult struct {
	Preset     domain.Preset
	Provenance Provenance
	Progress   []ProgressEvent
}

type Provenance struct {
	PresetID         string    `json:"preset_id"`
	Source           string    `json:"source"`
	Importer         string    `json:"importer"`
	MaterializedPath string    `json:"materialized_path"`
	InstalledAt      time.Time `json:"installed_at"`
}

type ProgressEvent struct {
	JobID   string    `json:"job_id"`
	Stage   string    `json:"stage"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}
