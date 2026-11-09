package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	tus "github.com/tus/tusd/v2/pkg/handler"
)

// Immutable PATCH chunks precede a checksummed atomic progress manifest. The
// conventional data name is only an allocation marker/final assembly target;
// stat.Size never grants resume authority. FileInfo keeps existing field names.
type durableUploadChunk struct {
	Offset int64   `json:"offset"`
	Ref    FileRef `json:"file"`
}

type durableUploadRecord struct {
	tus.FileInfo
	Version  int                  `json:"myce_version"`
	Chunks   []durableUploadChunk `json:"myce_chunks"`
	Checksum string               `json:"myce_metadata_sha256,omitempty"`
}

func (r durableUploadRecord) digest() (string, error) {
	r.Checksum = ""
	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (s *FileStore) uploadOps() durableFileOps {
	ops := s.durable
	ops.syncDir = s.syncDir
	return ops
}
func (s *FileStore) uploadPath(id string) string { return filepath.Join(s.root, "uploads", id) }

// Inventory needs declared allocations, not bulk payload reads. Resume/read
// authority additionally verifies the prefix through verifiedUpload below.
func (s *FileStore) uploadRecord(id string) (durableUploadRecord, error) {
	var record durableUploadRecord
	if err := (FileRef{SHA256: id}).validate(); err != nil {
		return record, err
	}
	path := s.uploadPath(id) + ".info"
	if err := noModelSymlinks(path, false); err != nil {
		if os.IsNotExist(err) {
			return record, tus.ErrNotFound
		}
		return record, err
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return record, tus.ErrNotFound
	}
	if err != nil {
		return record, err
	}
	if err = json.Unmarshal(data, &record); err != nil {
		return record, uploadIntegrity("upload metadata is corrupt")
	}
	if record.Version != 1 || record.Checksum == "" {
		return record, uploadIntegrity("upload has no verified durable progress; preserve it for operator recovery")
	}
	checksum, err := record.digest()
	if err != nil || checksum != record.Checksum || record.ID != id || record.MetaData["sha256"] != id || record.SizeIsDeferred || record.IsPartial || record.IsFinal || record.Offset < 0 || record.Size < record.Offset {
		return record, uploadIntegrity("upload metadata integrity mismatch")
	}
	var offset int64
	for _, chunk := range record.Chunks {
		if err := chunk.Ref.validate(); err != nil || chunk.Ref.SizeBytes <= 0 || chunk.Offset != offset || chunk.Ref.SizeBytes > record.Size-offset {
			return record, uploadIntegrity("invalid upload chunk generation")
		}
		offset += chunk.Ref.SizeBytes
	}
	if offset != record.Offset {
		return record, uploadIntegrity("upload chunk generation is incomplete")
	}
	return record, nil
}

func (s *FileStore) verifiedUpload(ctx context.Context, id string) (*durableUpload, error) {
	record, err := s.uploadRecord(id)
	if err != nil {
		return nil, err
	}
	// Publication can outlive cleanup. A verified final blob supersedes chunks,
	// including a crash between removing metadata and removing their directory.
	if record.Offset == record.Size && s.verify(ctx, FileRef{SHA256: id, SizeBytes: record.Size}) == nil {
		return &durableUpload{store: s, record: record}, nil
	}
	for _, chunk := range record.Chunks {
		if err := verifyFile(ctx, s.uploadChunkPath(id, chunk.Offset), chunk.Ref); err != nil {
			return nil, uploadIntegrity("upload durable chunk integrity mismatch")
		}
	}
	return &durableUpload{store: s, record: record}, nil
}

func (s *FileStore) uploadChunkPath(id string, offset int64) string {
	return filepath.Join(s.uploadPath(id)+".parts", strconv.FormatInt(offset, 10))
}

// Allocation metadata is a hint, not a certificate of content or resume offset.
// Accept previous filestore size hints so legacy partials do not block unrelated
// uploads; only verifiedUpload can make their bytes authoritative (and refuses).
func (s *FileStore) uploadAllocation(id string) (tus.FileInfo, error) {
	var info tus.FileInfo
	if err := noModelSymlinks(s.uploadPath(id)+".info", false); err != nil {
		if os.IsNotExist(err) {
			return info, tus.ErrNotFound
		}
		return info, err
	}
	data, err := os.ReadFile(s.uploadPath(id) + ".info")
	if os.IsNotExist(err) {
		return info, tus.ErrNotFound
	}
	if err != nil {
		return info, err
	}
	if err = json.Unmarshal(data, &info); err != nil || info.ID != id || info.Size < 0 || info.Offset < 0 || info.Offset > info.Size {
		return info, errors.New("upload allocation metadata unavailable")
	}
	return info, nil
}

// Integrity failures need explicit recovery, not an endless transient retry.
func uploadIntegrity(message string) error {
	return tus.NewError("ERR_UPLOAD_INTEGRITY", message, http.StatusUnprocessableEntity)
}

func uploadHTTPError(w http.ResponseWriter, err error) {
	var refusal tus.Error
	if errors.As(err, &refusal) {
		http.Error(w, refusal.Error(), refusal.HTTPResponse.StatusCode)
		return
	}
	http.Error(w, "upload metadata unavailable", http.StatusServiceUnavailable)
}

type recoverUploadKey struct{}

type durableUploadStore struct{ store *FileStore }

func (d durableUploadStore) NewUpload(ctx context.Context, info tus.FileInfo) (tus.Upload, error) {
	ref := FileRef{SHA256: info.ID, SizeBytes: info.Size}
	if err := ref.validate(); err != nil || info.MetaData["sha256"] != info.ID || info.SizeIsDeferred || info.IsPartial || info.IsFinal {
		return nil, errors.New("invalid durable upload identity")
	}
	path := d.store.uploadPath(info.ID)
	if _, err := os.Lstat(path + ".info"); !os.IsNotExist(err) {
		return nil, errors.New("upload metadata already exists or is unavailable")
	}
	if st, err := os.Lstat(path); err == nil {
		// Only an empty pre-metadata creation is provably disposable. Unknown
		// nonempty data is never silently truncated to invent a new upload.
		if !st.Mode().IsRegular() || st.Size() != 0 {
			return nil, uploadIntegrity("unindexed upload requires operator recovery")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	entries, err := os.ReadDir(path + ".parts")
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if len(entries) != 0 {
		return nil, uploadIntegrity("unindexed upload chunks require operator recovery")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.store.uploadOps().mkdir(path + ".parts"); err != nil {
		return nil, err
	}
	if err := d.store.uploadOps().bytes(path, nil); err != nil {
		return nil, err
	}
	info.Offset, info.Storage = 0, nil // Never follow metadata-supplied paths.
	upload := &durableUpload{store: d.store, record: durableUploadRecord{FileInfo: info, Version: 1}}
	if err := upload.commit(upload.record); err != nil {
		return nil, err
	}
	return upload, nil
}
func (d durableUploadStore) GetUpload(ctx context.Context, id string) (tus.Upload, error) {
	return d.store.verifiedUpload(ctx, id)
}

type durableUpload struct {
	store  *FileStore
	record durableUploadRecord
}

func (u *durableUpload) commit(record durableUploadRecord) error {
	checksum, err := record.digest()
	if err != nil {
		return err
	}
	record.Checksum = checksum
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err = u.store.uploadOps().bytes(u.store.uploadPath(record.ID)+".info", data); err != nil {
		return err
	}
	if err = u.store.refreshUpload(record.ID); err != nil {
		return err
	}
	u.record = record
	return nil
}
func (u *durableUpload) GetInfo(ctx context.Context) (tus.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return tus.FileInfo{}, err
	}
	// tus skips FinishUpload for a reopened complete offset. Recover before
	// returning that offset, under its identity lock and the checked claim.
	if recover, _ := ctx.Value(recoverUploadKey{}).(bool); recover && u.record.Offset == u.record.Size {
		if err := u.FinishUpload(ctx); err != nil {
			return tus.FileInfo{}, err
		}
		if err := u.store.completeUpload(creationContext(ctx), u.record.FileInfo); err != nil {
			return tus.FileInfo{}, err
		}
	} else {
		// Referenced chunks already passed file and name barriers before this
		// manifest was written. GetUpload reverified their bytes; only progress
		// metadata/freshness barriers can still be uncertain here.
		if err := u.store.refreshUpload(u.record.ID); err != nil {
			return tus.FileInfo{}, err
		}
	}
	return u.record.FileInfo, nil
}
func (u *durableUpload) WriteChunk(ctx context.Context, offset int64, src io.Reader) (int64, error) {
	if offset != u.record.Offset {
		return 0, errors.New("upload offset changed")
	}
	if offset < 0 || offset > u.record.Size {
		return 0, errors.New("invalid upload offset")
	}
	var n int64
	h := sha256.New()
	path := u.store.uploadChunkPath(u.record.ID, offset)
	err := u.store.uploadOps().replace(path, func(f *os.File) error {
		var err error
		remaining := u.record.Size - offset
		n, err = io.Copy(io.MultiWriter(f, h), contextReader{ctx, io.LimitReader(src, remaining)})
		if err != nil {
			return err
		}
		if n == remaining {
			var extra [1]byte
			read, readErr := io.ReadFull(contextReader{ctx, src}, extra[:])
			if read != 0 {
				return errors.New("upload exceeds declared length")
			}
			if readErr != io.EOF {
				return readErr
			}
		}
		if n == 0 {
			return io.EOF // No chunk name or manifest is changed for an empty PATCH.
		}
		return ctx.Err()
	})
	if errors.Is(err, io.EOF) && n == 0 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	next := u.record
	next.Chunks = append(append([]durableUploadChunk(nil), u.record.Chunks...), durableUploadChunk{Offset: offset, Ref: FileRef{SHA256: hex.EncodeToString(h.Sum(nil)), SizeBytes: n}})
	next.Offset += n
	if err = u.commit(next); err != nil {
		return 0, err
	}
	return n, nil
}
func (u *durableUpload) GetReader(ctx context.Context) (io.ReadCloser, error) {
	return &uploadChunkReader{ctx: ctx, store: u.store, record: u.record}, nil
}
func (u *durableUpload) FinishUpload(ctx context.Context) error {
	if u.record.Offset != u.record.Size {
		return errors.New("upload generation is incomplete")
	}
	ref := FileRef{SHA256: u.record.ID, SizeBytes: u.record.Size}
	if u.store.verify(ctx, ref) == nil {
		return nil // Rename completed; publication barriers/cleanup can be retried.
	}
	if verifyFile(ctx, u.store.uploadPath(u.record.ID), ref) == nil {
		return nil // A previous finish assembled the same verified bytes.
	}
	// Immutable chunks remain authoritative until assembly and publication
	// succeed. Reserve no fictional free space: refuse on measured headroom,
	// and preserve every chunk if concurrent writers still cause ENOSPC.
	probe := u.store.diskProbe
	if probe == nil {
		probe = workerDiskProbe
	}
	disk, err := probe(u.store.root)
	if err != nil {
		return err
	}
	floor := diskFreeFloor(disk.TotalBytes)
	available := disk.FreeBytes - min(disk.FreeBytes, floor)
	if disk.TotalBytes == 0 || uint64(u.record.Size) > available {
		return errors.New("upload assembly needs additional local disk headroom; durable chunks retained")
	}
	reader, err := u.GetReader(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	// Assemble only once, after all chunks are durable. The aggregate remains
	// unpublished if any referenced chunk or final identity fails verification.
	return u.store.uploadOps().replace(u.store.uploadPath(u.record.ID), func(f *os.File) error {
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(f, h), reader)
		if err != nil {
			return err
		}
		if n != u.record.Size || hex.EncodeToString(h.Sum(nil)) != u.record.ID {
			return uploadIntegrity("uploaded file size or SHA-256 mismatch")
		}
		return nil
	})
}

// Verify each chunk through its open descriptor before yielding it; only one
// descriptor is retained at a time, independent of the upload's chunk count.
type uploadChunkReader struct {
	ctx    context.Context
	store  *FileStore
	record durableUploadRecord
	index  int
	file   *os.File
}

func (r *uploadChunkReader) Read(p []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if r.file == nil {
			if r.index == len(r.record.Chunks) {
				return 0, io.EOF
			}
			chunk := r.record.Chunks[r.index]
			path := r.store.uploadChunkPath(r.record.ID, chunk.Offset)
			if err := noModelSymlinks(path, false); err != nil {
				return 0, err
			}
			f, err := os.Open(path)
			if err != nil {
				return 0, err
			}
			ref, err := IdentifyOpenFile(r.ctx, f)
			if err != nil || ref != chunk.Ref {
				f.Close()
				return 0, uploadIntegrity("upload chunk integrity mismatch")
			}
			if _, err = f.Seek(0, io.SeekStart); err != nil {
				f.Close()
				return 0, err
			}
			r.file = f
			r.index++
		}
		n, err := r.file.Read(p)
		if err != io.EOF {
			return n, err
		}
		closeErr := r.file.Close()
		r.file = nil
		if n != 0 || closeErr != nil {
			return n, closeErr
		}
	}
}
func (r *uploadChunkReader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}
