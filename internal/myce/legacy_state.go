package myce

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const persistedSchema = 3

var ErrLegacyState = errors.New("legacy_state_present")

// LegacyStateError identifies operator-owned archive paths. Detection never
// imports custody, repairs state, or archives anything on the operator's behalf.
type LegacyStateError struct{ Paths []string }

func (e *LegacyStateError) Error() string {
	return fmt.Sprintf("%s: stop the old services, archive %s, then do a fresh install (preserve config and secret references)", ErrLegacyState, strings.Join(e.Paths, ", "))
}
func (e *LegacyStateError) Unwrap() error { return ErrLegacyState }
func legacyState(paths ...string) error   { return &LegacyStateError{Paths: paths} }

// Current worker and registry writers put schema_version first. Inspect only
// that small marker, never deserialize an old execution/report payload.
func requireStateMarker(path, archive string) error {
	f, err := openHostRegular(path, uint32(os.Geteuid()), 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 256))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return legacyState(archive)
	}
	key, err := d.Token()
	if err != nil || key != "schema_version" {
		return legacyState(archive)
	}
	var version int
	if d.Decode(&version) != nil || version != persistedSchema {
		return legacyState(archive)
	}
	return nil
}

// Use SQLite's read-only connection and scalar JSON marker query. In particular,
// no lock file, PRAGMA change, DDL, or queue decoder runs on the refused path.
func refuseLegacyQueue(path string) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	u := url.URL{Scheme: "file", Path: absolute, RawQuery: "mode=ro&immutable=1"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	var exists int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='queue'").Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		// A WAL-only RC database may have no main-file table yet. New writers
		// commit this header marker before enabling WAL, so it survives crashes.
		if info, e := os.Stat(path + "-wal"); e == nil && info.Size() > 0 {
			var version int
			if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				return err
			}
			if version != persistedSchema {
				return legacyState(path, filepath.Dir(path))
			}
		} else if e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		return nil
	}
	var version sql.NullInt64
	err = db.QueryRow("SELECT json_extract(data, '$.schema_version') FROM queue WHERE id=1").Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return legacyState(path, filepath.Dir(path))
	}
	if err != nil {
		return err
	}
	if !version.Valid || version.Int64 != persistedSchema {
		return legacyState(path, filepath.Dir(path))
	}
	return nil
}

func refuseLegacyWorker(dir, environment string) error {
	for _, pattern := range []string{"state.json", "operator-stop.json", "engine-*.json", "attempt-*.json"} {
		paths, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := requireStateMarker(path, dir); err != nil {
				return err
			}
		}
	}
	if environment == "" {
		environment = filepath.Join(dir, "environment-cache")
	}
	paths, err := filepath.Glob(filepath.Join(environment, "*", "build-state.json"))
	if err != nil {
		return err
	}
	paths = append(paths, filepath.Join(environment, "build-state.json"))
	for _, path := range paths {
		if err := requireStateMarker(path, environment); err != nil {
			return err
		}
	}
	return nil
}

func refuseLegacyHost(dir string) error {
	paths, err := legacyHostPaths(dir)
	if err != nil {
		return err
	}
	if len(paths) > 0 {
		return errors.Join(ErrHostState, legacyState(dir))
	}
	commands, err := filepath.Glob(filepath.Join(dir, "attempt-command-*.json"))
	if err != nil {
		return err
	}
	for _, path := range commands {
		if err := requireStateMarker(path, dir); err != nil {
			return errors.Join(ErrHostState, err)
		}
	}
	registries, err := filepath.Glob(filepath.Join(dir, "worker-*.json"))
	if err != nil {
		return err
	}
	for _, path := range registries {
		if err = requireStateMarker(path, dir); err != nil {
			return errors.Join(ErrHostState, err)
		}
	}
	return nil
}

func legacyUnitParent(dir string) string {
	if filepath.Base(filepath.Dir(dir)) == "units" && ValidWorkerUnit(filepath.Base(dir)) {
		return filepath.Dir(filepath.Dir(dir))
	}
	return ""
}
