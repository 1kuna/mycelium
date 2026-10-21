package myce

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Receipts are written before the corresponding rename. Keep intermediate
// publications too: a crash can leave either side of any rename on disk.
type migrationPublication struct {
	Path    string
	SHA256  string
	Mode    os.FileMode
	Removed bool
}

func migrationDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func (j *workerMigration) publishing(files installFiles) installFiles {
	files.publication = func(path string, data []byte, mode os.FileMode, removed bool) error {
		j.Publications = append(j.Publications, migrationPublication{path, migrationDigest(data), mode, removed})
		return saveMigration(machineJournal(j.Config), *j)
	}
	return files
}

// Validate every changed artifact before stopping units or restoring anything.
// Unchanged artifacts (notably an already converted shared catalog) are not ours
// to restore. Errors contain only basenames and difference kinds, never values.
func checkedJournalTransaction(j workerMigration, command func(context.Context, string, ...string) ([]byte, error)) (*installTransaction, error) {
	tx := journalTransaction(j, command)
	tx.published = nil
	var differences []string
	for _, f := range j.Files {
		writes := []migrationPublication{}
		for _, p := range j.Publications {
			if p.Path == f.Path {
				writes = append(writes, p)
			}
		}
		if len(writes) == 0 || tx.fileMatches(f.Path) {
			continue
		}
		st, err := os.Lstat(f.Path)
		missing := errors.Is(err, os.ErrNotExist)
		var data []byte
		if err == nil && st.Mode().IsRegular() {
			data, err = os.ReadFile(f.Path)
		}
		owned := slices.ContainsFunc(writes, func(p migrationPublication) bool {
			if p.Removed {
				return missing
			}
			return err == nil && st.Mode().IsRegular() && st.Mode().Perm() == p.Mode.Perm() && migrationDigest(data) == p.SHA256
		})
		if !owned {
			differences = append(differences, filepath.Base(f.Path)+": content/mode differs from conversion")
			continue
		}
		tx.published = append(tx.published, tx.originals[f.Path])
	}
	if len(differences) > 0 {
		return nil, migrationError("machine_migration_conflict", strings.Join(differences, "; "))
	}
	return tx, nil
}

// Decode only the retired registry identity; do not revive the old host-control
// subsystem. Source config explicitly selects each record we may retire.
func legacyMigrationRegistries(sources []ServiceConfig) ([]string, error) {
	dirs := map[string]bool{}
	for _, s := range sources {
		data, err := os.ReadFile(s.Config)
		if err != nil {
			return nil, err
		}
		var old struct {
			HostStateDir string `json:"host_state_dir"`
		}
		if err = json.Unmarshal(data, &old); err != nil {
			return nil, err
		}
		dir := old.HostStateDir
		if dir == "" {
			dir = filepath.Join(s.WorkerHome, ".local", "share", "myce")
		}
		if !filepath.IsAbs(dir) {
			return nil, migrationError("machine_migration_conflict", "legacy registry root must be absolute")
		}
		dirs[dir] = true
	}
	var paths []string
	for dir := range dirs {
		found, err := filepath.Glob(filepath.Join(dir, "worker-*.json"))
		if err != nil {
			return nil, err
		}
		for _, path := range found {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			var r struct {
				Config string `json:"config"`
				Unit   string `json:"unit"`
				State  string `json:"state_dir"`
				Host   string `json:"host_id"`
				Name   string `json:"name"`
				Queue  string `json:"queue"`
			}
			if err = json.Unmarshal(data, &r); err != nil {
				return nil, migrationError("machine_migration_conflict", "invalid legacy worker registry")
			}
			selected := slices.ContainsFunc(sources, func(s ServiceConfig) bool {
				return r.Config == s.Config && r.Unit == filepath.Base(s.Service) && r.State == s.State && r.Host == s.HostID && r.Name == s.Name && r.Queue == s.Queue
			})
			if !selected {
				return nil, migrationError("machine_migration_incomplete", "unselected or conflicting legacy worker registry "+filepath.Base(path))
			}
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	return paths, nil
}
