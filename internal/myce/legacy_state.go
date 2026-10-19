package myce

import (
	"errors"
	"fmt"
	"io"
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
		f, err := os.Open(root)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		_, err = f.Readdirnames(1)
		f.Close()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		marker := filepath.Join(root, FormatMarker)
		st, err := os.Lstat(marker)
		if errors.Is(err, os.ErrNotExist) {
			return legacyState(root)
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
	if err := CheckStateRoots(roots...); err != nil {
		return err
	}
	for _, root := range roots {
		if root == "" {
			continue
		}
		if err := os.MkdirAll(root, 0700); err != nil {
			return err
		}
		marker := filepath.Join(root, FormatMarker)
		f, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(err, os.ErrExist) {
			if err = CheckStateRoots(root); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		_, err = f.WriteString(stateFormat)
		if err == nil {
			err = f.Sync()
		}
		err = errors.Join(err, f.Close())
		if err != nil {
			return err
		}
		if err = syncWorkerDir(root); err != nil {
			return err
		}
	}
	return nil
}
