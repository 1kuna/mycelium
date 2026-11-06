package myce

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// modelSnapshotSource handles real links and Samba/CIFS mfsymlinks (XSym).
// Without this step an XSym regular file would declare its OWN hash, allowing
// the shared-input verifier to faithfully copy a stub instead of model bytes.
func modelSnapshotSource(ctx context.Context, file, blobs string) (string, error) {
	source, err := filepath.EvalSymlinks(file)
	if err != nil {
		return "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	linked := source != file
	if linked && !insideLocalRoot(blobs, source) {
		return "", modelSnapshotError("model_snapshot_escape", "permission_denied", nil)
	}
	if info, err := os.Stat(source); err != nil || !info.Mode().IsRegular() {
		return "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	f, err := os.Open(source)
	if err != nil {
		return "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	// mfsymlinks have a fixed 1067-byte format, including a 1024-byte target
	// region. Read at most that plus one byte; never scan weights to detect it.
	header := make([]byte, 8)
	n, err := io.ReadFull(contextReader{ctx, f}, header)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	if strings.HasPrefix(string(header[:n]), "XSym") {
		rest, err := io.ReadAll(io.LimitReader(contextReader{ctx, f}, 1068-int64(n)))
		data := append(header[:n], rest...)
		if err != nil || info.Size() != 1067 || len(data) != 1067 || string(data[:5]) != "XSym\n" || data[9] != '\n' || data[42] != '\n' {
			return "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
		}
		size, err := strconv.Atoi(string(data[5:9]))
		if err != nil || size <= 0 || size > 1024 {
			return "", modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
		}
		target := string(data[43 : 43+size])
		sum := md5.Sum([]byte(target)) // Format checksum only; blob identity is SHA-256/Git SHA-1.
		if hex.EncodeToString(sum[:]) != string(data[10:42]) || strings.ContainsAny(target, "\x00\r\n") {
			return "", modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(source), filepath.FromSlash(target))
		}
		// Refuse escape before any read of the target, including via real links.
		if !insideLocalRoot(blobs, target) {
			return "", modelSnapshotError("model_snapshot_escape", "permission_denied", nil)
		}
		source, err = filepath.EvalSymlinks(target)
		if err != nil {
			return "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
		}
		linked = true
	} else if string(header[:n]) == "IntxLNK\x01" || strings.HasPrefix(string(header[:n]), "../") {
		// Other filesystem/checkout link encodings are not qualified. Never
		// manufacture an identity from their bytes and present them as weights.
		return "", modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
	}
	if linked {
		if !insideLocalRoot(blobs, source) {
			return "", modelSnapshotError("model_snapshot_escape", "permission_denied", nil)
		}
		name := filepath.Base(source)
		if !sha256Name.MatchString(name) && !modelCommitPattern.MatchString(name) {
			return "", modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
		}
	}
	return source, nil
}
