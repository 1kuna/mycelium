package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// EnvironmentEntry exposes immutable input identities, never recipes or file paths.
type EnvironmentEntry struct {
	Name      string                `json:"name"`
	Platforms []EnvironmentPlatform `json:"platforms"`
}

type EnvironmentPlatform struct {
	Platform     string            `json:"platform"`
	Backend      string            `json:"backend"`
	Key          string            `json:"key"`
	InputDigests map[string]string `json:"input_digests"`
}

func (q *Queue) EnvironmentEntries() []EnvironmentEntry {
	// The configured catalog is immutable. Hash its detached snapshot outside q.mu.
	q.mu.Lock()
	catalog := q.environments
	q.mu.Unlock()
	entries := make([]EnvironmentEntry, 0, len(catalog.Environments))
	for name, recipes := range catalog.Environments {
		entry := EnvironmentEntry{Name: name, Platforms: []EnvironmentPlatform{}}
		for platform, recipe := range recipes {
			backend := recipe.Backend
			if backend == "" {
				if recipe.LockFile != "" {
					backend = "uv"
				} else {
					backend = "binary"
				}
			}
			p := EnvironmentPlatform{Platform: platform, Backend: backend, Key: recipe.Key, InputDigests: map[string]string{}}
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
