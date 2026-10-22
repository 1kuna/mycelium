package myce

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

func init() {
	publishJobOutputs = func(j Job, root string) error { return publishDarwinOutputs(j, root, os.Rename) }
}

// SMB supports ordinary directory rename, but neither hard links nor
// RENAME_EXCL. Publish complete attempt directories: rename refuses an existing
// nonempty directory, so no existing result can be overwritten. Replacing an
// empty directory publishes new results without exposing a partial file.
func publishDarwinOutputs(j Job, root string, renameDirectory func(string, string) error) error {
	groups := map[string][]string{}
	for name, prefix := range j.Spec.Outputs {
		groups[prefix] = append(groups[prefix], name)
	}
	for prefix, names := range groups {
		dest := filepath.Join(root, prefix, j.ID, strconv.Itoa(j.Attempt))
		// Apply the same ancestry confinement as file publication, including the
		// destination attempt directory; never follow a pre-existing symlink.
		for p := dest; p != root; p = filepath.Dir(p) {
			rel, err := filepath.Rel(root, p)
			if err != nil || !filepath.IsLocal(rel) {
				return errors.New("output escapes root")
			}
			if info, e := os.Lstat(p); e == nil && !info.IsDir() {
				return errors.New("output parent is not a directory")
			} else if e != nil && !os.IsNotExist(e) {
				return e
			}
		}
		parent := filepath.Dir(dest)
		if err := os.MkdirAll(parent, 0700); err != nil {
			return err
		}
		staged, err := os.MkdirTemp(parent, ".output-")
		if err != nil {
			return err
		}
		for _, name := range names {
			var source string
			source, err = confinedPath(j.Spec.Run.Workdir, name)
			if err == nil {
				err = copyJobFile(source, filepath.Join(staged, name))
			}
			if err != nil {
				break
			}
		}
		if err == nil {
			err = renameDirectory(staged, dest)
		}
		_ = os.RemoveAll(staged)
		if err != nil {
			return fmt.Errorf("attempt output publication failed: %w", err)
		}
	}
	return nil
}
