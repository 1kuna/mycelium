package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

// EnvironmentEntry exposes immutable input identities, never recipes or file paths.
type EnvironmentEntry struct {
	Library        string                `json:"library"`
	ReadingSide    string                `json:"reading_side"`
	ResolutionBase string                `json:"resolution_base"`
	Name           string                `json:"name"`
	Platforms      []EnvironmentPlatform `json:"platforms"`
}

type EnvironmentReadObservation struct {
	Worker     string          `json:"worker"`
	State      string          `json:"state"`
	Diagnostic *PathDiagnostic `json:"path_diagnostic,omitempty"`
}

type EnvironmentPlatform struct {
	WorkerReads  []EnvironmentReadObservation `json:"worker_reads,omitempty"`
	Platform     string                       `json:"platform"`
	Backend      string                       `json:"backend"`
	Key          string                       `json:"key"`
	InputDigests map[string]string            `json:"input_digests"`
}

func (q *Queue) EnvironmentEntries() []EnvironmentEntry {
	// The configured catalog is immutable. Hash its detached snapshot outside q.mu.
	q.mu.Lock()
	catalog := q.environments
	// Capture only actual reported read failures. Ready cache evidence says
	// nothing about whether a worker can still read the original library.
	observations := map[string][]EnvironmentReadObservation{}
	ids := make([]string, 0, len(q.s.Machines))
	for id := range q.s.Machines {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for i, id := range ids {
		m := q.s.Machines[id]
		for _, report := range m.EnvironmentReports {
			if report.State != "failed" {
				continue
			}
			d := safeWorkerPathDiagnostic(report.PathDiagnostic)
			if d == nil {
				continue
			}
			observation := EnvironmentReadObservation{Worker: fmt.Sprintf("worker-%d", i+1), State: "unknown"}
			if !m.Gone && !m.LastSeen.IsZero() && !q.clock.Now().Before(m.LastSeen) && q.clock.Now().Sub(m.LastSeen) <= q.machineLossTimeout(id) {
				observation.State, observation.Diagnostic = "failed", d
			}
			observations[report.Key] = append(observations[report.Key], observation)
		}
	}
	q.mu.Unlock()
	entries := make([]EnvironmentEntry, 0, len(catalog.Environments))
	for name, recipes := range catalog.Environments {
		entry := EnvironmentEntry{Library: "environment-library", ReadingSide: "queue", ResolutionBase: "environment-library", Name: name, Platforms: []EnvironmentPlatform{}}
		for platform, recipe := range recipes {
			backend := recipe.Backend
			if backend == "" {
				if recipe.LockFile != "" {
					backend = "uv"
				} else {
					backend = "binary"
				}
			}
			p := EnvironmentPlatform{WorkerReads: observations[recipe.Key], Platform: platform, Backend: backend, Key: recipe.Key, InputDigests: map[string]string{}}
			// Only canonical input labels are public; arbitrary source names stay private.
			for _, name := range []string{"requirements.txt", "uv.lock", "pyproject.toml", "pixi.lock", "pixi.toml", "binary.sha256"} {
				if data, ok := recipe.Files[name]; ok {
					sum := sha256.Sum256(data)
					p.InputDigests[name] = hex.EncodeToString(sum[:])
				}
			}
			entry.Platforms = append(entry.Platforms, p)
		}
		sort.Slice(entry.Platforms, func(i, j int) bool { return entry.Platforms[i].Platform < entry.Platforms[j].Platform })
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}
