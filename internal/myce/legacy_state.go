package myce

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const persistedSchema = 3
const FormatMarker = ".myce-format"
const stateFormat = "v3\n"

var ErrLegacyState = errors.New("legacy_state_present")

// LegacyStateError identifies operator-owned archive roots. Refusal never
// decodes, imports, repairs, or archives their contents.
type LegacyStateError struct{ Paths []string }

func (e *LegacyStateError) Error() string {
	return fmt.Sprintf("%s: stop the old services, archive %s, then do a fresh install (preserve config and secret references)", ErrLegacyState, strings.Join(e.Paths, ", "))
}
func (e *LegacyStateError) Unwrap() error { return ErrLegacyState }
func legacyState(paths ...string) error   { return &LegacyStateError{Paths: paths} }

// CheckStateRoots is the single read-only startup gate. All roots are checked
// before PrepareStateRoots creates anything, including fresh sibling roots.
// An empty path means that optional root is not used by this invocation.
func CheckStateRoots(roots ...string) error {
	for _, root := range roots {
		if root == "" {
			continue
		}
		marker := filepath.Join(root, FormatMarker)
		st, err := os.Lstat(marker)
		if errors.Is(err, os.ErrNotExist) {
			entries, err := os.ReadDir(root)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			for _, entry := range entries {
				// Interrupted fresh marker writes have only private temporary
				// names. Never decode them or mistake them for durable state.
				suffix, own := strings.CutPrefix(entry.Name(), ".myce-write-")
				if !own || suffix == "" || strings.Trim(suffix, "0123456789") != "" || !entry.Type().IsRegular() {
					return legacyState(root)
				}
			}
			continue
		}
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() || st.Size() != int64(len(stateFormat)) {
			return legacyState(root)
		}
		data, err := os.ReadFile(marker)
		if err != nil {
			return err
		}
		if string(data) != stateFormat {
			return legacyState(root)
		}
	}
	return nil
}

// PrepareStateRoots writes and fsyncs the format marker before any other state
// file. The directory sync makes the marker durable before startup proceeds.
func PrepareStateRoots(roots ...string) error {
	return prepareStateRoots(durableFileOps{}, roots...)
}

func prepareStateRoots(ops durableFileOps, roots ...string) error {
	if err := CheckStateRoots(roots...); err != nil {
		return err
	}
	for _, root := range roots {
		if root == "" {
			continue
		}
		if err := ops.mkdir(root); err != nil {
			return err
		}
		marker := filepath.Join(root, FormatMarker)
		if _, err := os.Lstat(marker); os.IsNotExist(err) {
			if err = ops.bytes(marker, []byte(stateFormat)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if err = CheckStateRoots(root); err != nil {
			return err
		}
		// An earlier attempt may have renamed successfully and then failed its
		// directory barrier. Verify and repeat the barrier before any state I/O.
		f, err := os.Open(marker)
		if err != nil {
			return err
		}
		if err = errors.Join(ops.flush(f), f.Close()); err != nil {
			return err
		}
		if err = ops.directory(root); err != nil {
			return err
		}
	}
	return nil
}
