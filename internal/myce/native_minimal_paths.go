package myce

import (
	"errors"
	"path/filepath"
	"strings"
)

// A link is recreated in the empty filesystem, never followed to import host
// content. Even dangling links outside the read layout are refused: a later
// writable overlay must not turn an innocuous alias into a control endpoint.
func nativeMinimalLink(path, target string, read, write []string) error {
	if target == "" || strings.ContainsAny(target, "\x00\r\n") {
		return errors.New("minimal-root invalid symlink")
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(path), target)
	}
	target = filepath.Clean(target)
	contains := func(root, path string) bool {
		rel, err := filepath.Rel(root, path)
		return err == nil && (rel == "." || filepath.IsLocal(rel))
	}
	for _, root := range write {
		if contains(root, target) || contains(target, root) {
			return errors.New("minimal-root symlink aliases writable content")
		}
	}
	for _, root := range read {
		if contains(root, target) {
			return nil
		}
	}
	return errors.New("minimal-root symlink escapes read layout")
}

func nativeMinimalDestination(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.ContainsAny(path, "\x00\r\n") {
		return errors.New("minimal-root requires a clean exact absolute path")
	}
	return nil
}

func (w Worker) requireNativeMinimalRoot() error {
	if !w.NativeMinimalRoot {
		return nil
	}
	for _, capability := range w.nativeIsolationCapabilities() {
		if capability == "native_isolation_minimal_root" {
			return nil
		}
	}
	return errors.New("native_minimal_root_unavailable: service-context capability probe refused; check operator runtime layout and host policy (no fallback selected)")
}
