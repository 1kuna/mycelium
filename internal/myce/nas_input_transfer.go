package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type nasInputReader struct {
	file *os.File
	read func(*os.File, []byte) (int, error)
}

func (r nasInputReader) Read(p []byte) (int, error) {
	if r.read != nil {
		return r.read(r.file, p)
	}
	return r.file.Read(p)
}

// One sequential source read, with local checksum verification before use.
// A completed transient read resumes from the local prefix in this preparation
// attempt. Never retry local write/sync failures as remote-read recovery.
// Legacy path inputs lack an authored checksum; freeze their source metadata,
// hash the bytes as copied, then verify that digest on the local destination.
func copyNASInput(ctx context.Context, client Client, source, target string, expected *FileRef, read func(*os.File, []byte) (int, error)) (stageErr error) {
	defer func() {
		observeStorageContext(ctx, stageErr)
		stageErr = tracedSharedError(ctx, stageErr)
	}()
	traceSharedInput(ctx, "resolve", "source")
	if err := sharedInputProgress(ctx); err != nil {
		return err
	}
	if err := noModelSymlinks(source, false); err != nil {
		return err
	}
	// A completed tree member/archive in a retained stage needs no NAS read.
	if expected != nil {
		if st, err := os.Lstat(target); err == nil && st.Mode().IsRegular() && st.Size() == expected.SizeBytes {
			traceSharedInput(ctx, "verify", "cache")
			return verifyFile(ctx, target, *expected)
		}
	}
	traceSharedInput(ctx, "stat", "source")
	original, err := os.Stat(source)
	if err != nil {
		return err
	}
	if !original.Mode().IsRegular() {
		return errors.New("NAS input must be a regular file")
	}
	ref := FileRef{SizeBytes: original.Size()}
	if expected != nil {
		ref = *expected
		if original.Size() != ref.SizeBytes {
			return errors.New("shared NAS file size/type differs from manifest")
		}
	}
	traceSharedFile(ctx, ref)
	traceSharedInput(ctx, "prepare", "cache")
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	return client.recoverInputTransfer(ctx, ref, "nas_read", func(Client) error {
		if err := sharedInputProgress(ctx); err != nil {
			return err
		}
		traceSharedInput(ctx, "open", "source")
		in, err := os.Open(source)
		if err != nil {
			return err
		}
		defer in.Close()
		traceSharedInput(ctx, "stat", "source")
		info, err := in.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != original.Size() || !info.ModTime().Equal(original.ModTime()) || !os.SameFile(info, original) {
			return errors.New("NAS input changed during staging")
		}
		traceSharedInput(ctx, "open", "cache")
		var offset int64
		if st, err := os.Lstat(target); err == nil {
			if !st.Mode().IsRegular() || st.Size() > ref.SizeBytes {
				return errors.New("invalid NAS input partial")
			}
			offset = st.Size()
		} else if !os.IsNotExist(err) {
			return err
		}
		_, free, err := inputCacheBudget(filepath.Dir(target), 0, 0, nil)
		if err != nil {
			return err
		}
		if need := ref.SizeBytes - offset; need > free {
			return &stagingCapacityError{required: need, available: free, capacity: "disk_free_floor"}
		}
		if offset == 0 && expected != nil && read == nil {
			traceSharedInput(ctx, "clone", "cache")
			if cloneSharedFile(in, target) {
				if t, ok := ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace); ok {
					t.mu.Lock()
					t.detail.BytesCopied = ref.SizeBytes
					t.mu.Unlock()
				}
				traceSharedInput(ctx, "verify", "cache")
				if err := verifyFile(ctx, target, ref); err != nil {
					return err
				}
				return os.Chmod(target, 0400|original.Mode().Perm()&0111)
			}
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return errors.Join(errFileTransferLocal, tracedSharedError(ctx, err))
		}
		hash := sha256.New()
		// Only local bytes are re-read on recovery. The NAS resumes at offset.
		traceSharedInput(ctx, "verify", "cache")
		_, err = io.CopyN(hash, contextReader{ctx, out}, offset)
		if err == nil {
			_, err = in.Seek(offset, io.SeekStart)
		}
		if t, ok := ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace); ok {
			t.mu.Lock()
			t.detail.BytesCopied = offset
			t.mu.Unlock()
		}
		if err == nil {
			_, err = io.CopyN(io.MultiWriter(fileDownloadWriter{sharedInputWriter{ctx, out}}, hash), contextReader{ctx, sharedInputReader{ctx, nasInputReader{in, read}, "source"}}, ref.SizeBytes-offset)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				observeStorageContext(ctx, &os.PathError{Op: "read", Path: source, Err: io.ErrUnexpectedEOF})
			}
			err = tracedSharedCopyError(ctx, err)
		}
		traceSharedInput(ctx, "sync", "cache")
		if syncErr := out.Sync(); syncErr != nil {
			err = errors.Join(errFileTransferLocal, tracedSharedError(ctx, syncErr))
		}
		traceSharedInput(ctx, "close", "cache")
		if closeErr := out.Close(); closeErr != nil {
			err = errors.Join(errFileTransferLocal, tracedSharedError(ctx, closeErr))
		}
		if err != nil {
			return err
		}
		traceSharedInput(ctx, "stat", "source")
		info, err = in.Stat()
		if err != nil {
			return err
		}
		if info.Size() != original.Size() || !info.ModTime().Equal(original.ModTime()) {
			return errors.New("NAS input changed during staging")
		}
		digest := hex.EncodeToString(hash.Sum(nil))
		if expected != nil && digest != expected.SHA256 {
			return errors.New("NAS input SHA-256 mismatch")
		}
		ref.SHA256 = digest
		traceSharedInput(ctx, "verify", "cache")
		if err := verifyFile(ctx, target, ref); err != nil {
			return err
		}
		traceSharedInput(ctx, "permissions", "cache")
		mode := os.FileMode(0600)
		if expected != nil {
			mode = 0400 | original.Mode().Perm()&0111
		}
		return os.Chmod(target, mode)
	})
}
