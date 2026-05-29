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

type InstallState struct {
	JobID          string          `json:"job_id"`
	Source         string          `json:"source"`
	PresetID       string          `json:"preset_id,omitempty"`
	Status         string          `json:"status"`
	DraftName      string          `json:"draft_name,omitempty"`
	DraftImporter  string          `json:"draft_importer,omitempty"`
	DraftSize      int64           `json:"draft_size,omitempty"`
	ModelPath      string          `json:"model_path,omitempty"`
	PresetPath     string          `json:"preset_path,omitempty"`
	ProvenancePath string          `json:"provenance_path,omitempty"`
	Progress       []ProgressEvent `json:"progress,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at"`
}
