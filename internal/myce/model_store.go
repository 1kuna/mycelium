package myce

import (
	"context"
	"os"
	"path/filepath"
)

// Keep model admission, loaded/pinned protection and pre-copy plans in the
// existing ModelCache; only its source changes from NAS to authenticated HTTP.
func (c *ModelCache) copyStoredModel(ctx context.Context, temp string, m Model, r *CopyReport, copied *int64) error {
	before := r.Bytes
	if len(m.Files) == 0 {
		if err := c.FileClient.DownloadFile(ctx, FileRef{m.SHA256, m.SizeBytes}, temp); err != nil {
			return err
		}
	} else {
		if err := os.MkdirAll(temp, 0700); err != nil {
			return err
		}
		for _, file := range m.Files {
			dest := filepath.Join(temp, filepath.FromSlash(file.Path))
			if err := noModelSymlinks(dest, true); err != nil {
				return err
			}
			if err := c.FileClient.DownloadFile(ctx, FileRef{file.SHA256, file.SizeBytes}, dest); err != nil {
				return err
			}
		}
		if err := verifyModelFiles(ctx, temp, m); err != nil {
			return err
		}
		if err := syncModelDirectory(temp); err != nil {
			return err
		}
	}
	r.Bytes = m.SizeBytes
	*copied += max(0, m.SizeBytes-before)
	return verifyModelContext(ctx, temp, m)
}
