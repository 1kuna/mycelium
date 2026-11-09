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

// Each immutable PATCH has a constant-size checksummed descriptor linked to
// its predecessor. The atomic .info record names only the tail and count. A
// live process remembers verified immutable chunks; restart verifies the chain
// and bytes once. Neither payload nor metadata work rescans a live prefix.
type durableUploadChunk struct {
	Offset           int64   `json:"offset"`
	Ref              FileRef `json:"file"`
	Previous         int64   `json:"previous"`
	PreviousChecksum string  `json:"previous_sha256,omitempty"`
	Checksum         string  `json:"sha256,omitempty"`
}

func (c durableUploadChunk) digest() (string, error) {
	c.Checksum = ""
	data, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type durableUploadRecord struct {
	tus.FileInfo
	Version    int                  `json:"myce_version"`
	Count      int64                `json:"myce_chunk_count"`
	TailOffset int64                `json:"myce_tail_offset"`
	Tail       string               `json:"myce_tail_sha256,omitempty"`
	Checksum   string               `json:"myce_metadata_sha256,omitempty"`
	Chunks     []durableUploadChunk `json:"-"` // Process-local verified prefix only.
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

func (s *FileStore) noteUploadWork(kind string, bytes int64) {
	if s.uploadWork != nil {
		s.uploadWork(kind, bytes)
	}
}
func (s *FileStore) rememberUpload(record durableUploadRecord) {
	s.uploadCacheMu.Lock()
	defer s.uploadCacheMu.Unlock()
	if s.uploadCache == nil {
		s.uploadCache = map[string]durableUploadRecord{}
	}
	s.uploadCache[record.ID] = record
}
func (s *FileStore) forgetUpload(id string) {
	s.uploadCacheMu.Lock()
	defer s.uploadCacheMu.Unlock()
	delete(s.uploadCache, id)
}

func (s *FileStore) uploadOps() durableFileOps {
	ops := s.durable
	ops.syncDir = s.syncDir
	return ops
}
func (s *FileStore) uploadPath(id string) string { return filepath.Join(s.root, "uploads", id) }

// This reads constant-size progress metadata. Legacy FileInfo is only an
// allocation/identity hint until adoption under the upload lock below.
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
	s.noteUploadWork("metadata", int64(len(data)))
	if err = json.Unmarshal(data, &record); err != nil {
		return record, uploadIntegrity("upload metadata is corrupt")
	}
	if record.ID != id || record.MetaData["sha256"] != id || record.SizeIsDeferred || record.IsPartial || record.IsFinal || record.Offset < 0 || record.Size < record.Offset {
		return record, uploadIntegrity("upload metadata identity mismatch")
	}
	if record.Version == 0 && record.Checksum == "" && record.Count == 0 && record.Tail == "" && record.TailOffset == 0 {
		return record, nil // Upstream tus metadata; never trust its Offset/Storage.
	}
	checksum, err := record.digest()
	if err != nil || record.Version != 2 || checksum != record.Checksum || record.Count < 0 || record.Count > record.Offset ||
		(record.Count == 0 && (record.Offset != 0 || record.Tail != "" || record.TailOffset != 0)) ||
		(record.Count > 0 && (record.Tail == "" || record.TailOffset < 0 || record.TailOffset >= record.Offset)) {
		return record, uploadIntegrity("upload metadata integrity mismatch")
	}
	return record, nil
}

func (s *FileStore) verifiedUpload(ctx context.Context, id string) (*durableUpload, error) {
	record, err := s.uploadRecord(id)
	if err != nil {
		return nil, err
	}
	if record.Version == 0 {
		return s.adoptLegacyUpload(ctx, record)
	}
	s.uploadCacheMu.Lock()
	cached, ok := s.uploadCache[id]
	s.uploadCacheMu.Unlock()
	if ok && cached.Checksum == record.Checksum {
		return &durableUpload{store: s, record: cached}, nil
	}
	// A verified final blob supersedes chunks during interrupted cleanup.
	if record.Offset == record.Size && s.verify(ctx, FileRef{SHA256: id, SizeBytes: record.Size}) == nil {
		return &durableUpload{store: s, record: record}, nil
	}
	end, offset, checksum := record.Offset, record.TailOffset, record.Tail
	for count := record.Count; count > 0; count-- {
		path := s.uploadChunkPath(id, offset) + ".json"
		if err := noModelSymlinks(path, false); err != nil {
			return nil, uploadIntegrity("upload chunk descriptor unavailable")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		s.noteUploadWork("metadata", int64(len(data)))
		var chunk durableUploadChunk
		if err = json.Unmarshal(data, &chunk); err != nil {
			return nil, uploadIntegrity("upload chunk descriptor is corrupt")
		}
		digest, err := chunk.digest()
		if err != nil || digest != checksum || chunk.Checksum != checksum || chunk.Offset != offset || chunk.Ref.validate() != nil || chunk.Ref.SizeBytes <= 0 || chunk.Ref.SizeBytes > end || chunk.Offset != end-chunk.Ref.SizeBytes ||
			(count == 1 && (chunk.Offset != 0 || chunk.Previous != -1 || chunk.PreviousChecksum != "")) ||
			(count > 1 && (chunk.Previous < 0 || chunk.Previous >= offset || chunk.PreviousChecksum == "")) {
			return nil, uploadIntegrity("upload chunk chain integrity mismatch")
		}
		if err := s.verifyUploadChunk(ctx, s.uploadChunkPath(id, offset), chunk.Ref); err != nil {
			return nil, err
		}
		record.Chunks = append(record.Chunks, chunk)
		end, offset, checksum = chunk.Offset, chunk.Previous, chunk.PreviousChecksum
	}
	for left, right := 0, len(record.Chunks)-1; left < right; left, right = left+1, right-1 {
		record.Chunks[left], record.Chunks[right] = record.Chunks[right], record.Chunks[left]
	}
	s.rememberUpload(record)
	return &durableUpload{store: s, record: record}, nil
}

func (s *FileStore) verifyUploadChunk(ctx context.Context, path string, ref FileRef) error {
	if err := noModelSymlinks(path, false); err != nil {
		return uploadIntegrity("upload durable chunk unavailable")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() != ref.SizeBytes {
		return uploadIntegrity("upload durable chunk size mismatch")
	}
	h := sha256.New()
	n, err := io.Copy(h, contextReader{ctx, f})
	s.noteUploadWork("verify", n)
	if err != nil {
		return err
	}
	if n != ref.SizeBytes || hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return uploadIntegrity("upload durable chunk integrity mismatch")
	}
	return nil
}

// The old store has no prefix digest. Snapshot and hash its actual regular-file
// bytes into an immutable chunk, then atomically commit new progress. Preserve
// the old payload until that commit succeeds. The final whole-file SHA-256 is
// still required; adoption never invents proof of the original sender's bytes.
// Caller owns tus's identity lock (or the isolated fixture's sole writer).
func (s *FileStore) adoptLegacyUpload(ctx context.Context, record durableUploadRecord) (*durableUpload, error) {
	path := s.uploadPath(record.ID)
	if err := noModelSymlinks(path, false); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > record.Size {
		return nil, uploadIntegrity("invalid legacy upload payload")
	}
	if err := s.uploadOps().mkdir(path + ".parts"); err != nil {
		return nil, err
	}
	info := record.FileInfo
	info.Offset, info.Storage = 0, nil
	record = durableUploadRecord{FileInfo: info, Version: 2}
	u := &durableUpload{store: s, record: record}
	if st.Size() > 0 {
		n, err := u.WriteChunk(ctx, 0, io.LimitReader(f, st.Size()))
		if err != nil || n != st.Size() {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	} else if err := u.commit(record); err != nil {
		return nil, err
	}
	// The new chunk owns this prefix durably. Reclaim only the redundant old
	// copy; a failed cleanup can be retried or left for final publication.
	if st.Size() < record.Size {
		if err := s.uploadOps().bytes(path, nil); err != nil {
			return nil, err
		}
	}
	return u, nil
}

func (s *FileStore) uploadChunkPath(id string, offset int64) string {
	return filepath.Join(s.uploadPath(id)+".parts", strconv.FormatInt(offset, 10))
}

// Allocation metadata is a hint, not a certificate of content or resume offset.
// Accept previous filestore size hints so legacy partials do not block unrelated
// uploads; only verifiedUpload can make their bytes authoritative.
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
	upload := &durableUpload{store: d.store, record: durableUploadRecord{FileInfo: info, Version: 2}}
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
	u.store.noteUploadWork("metadata", int64(len(data)))
	if err = u.store.uploadOps().bytes(u.store.uploadPath(record.ID)+".info", data); err != nil {
		return err
	}
	if err = u.store.refreshUpload(record.ID); err != nil {
		return err
	}
	u.record = record
	u.store.rememberUpload(record)
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
		// manifest was written. GetUpload loaded verified immutable evidence;
		// it rehashes the prefix once after a restart. Only progress
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
	u.store.noteUploadWork("accept", n)
	chunk := durableUploadChunk{Offset: offset, Ref: FileRef{SHA256: hex.EncodeToString(h.Sum(nil)), SizeBytes: n}, Previous: -1}
	if u.record.Count > 0 {
		chunk.Previous, chunk.PreviousChecksum = u.record.TailOffset, u.record.Tail
	}
	chunk.Checksum, err = chunk.digest()
	if err != nil {
		return 0, err
	}
	data, err := json.Marshal(chunk)
	if err != nil {
		return 0, err
	}
	u.store.noteUploadWork("metadata", int64(len(data)))
	if err := u.store.uploadOps().bytes(path+".json", data); err != nil {
		return 0, err
	}
	next := u.record
	next.Chunks = append(next.Chunks, chunk)
	next.Count++
	next.TailOffset, next.Tail = offset, chunk.Checksum
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
		u.store.noteUploadWork("assemble", n)
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
			r.store.noteUploadWork("verify", ref.SizeBytes)
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
