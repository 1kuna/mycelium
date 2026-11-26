package myce

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
)

// Shared queue members mount one store at different local paths. tusd records
// the creating host's absolute data path in each .info file and opens it
// verbatim, so another member, or the same member after a remount, finds no
// upload. The data file always sits beside its .info under this store's
// uploads directory; resolve it there and ignore the recorded path (#2144).
type rootedUploads struct{ filestore.FileStore }

func (s *FileStore) uploads() rootedUploads {
	fs := filestore.New(filepath.Join(s.root, "uploads"))
	fs.DirModePerm, fs.FileModePerm = 0700, 0600
	return rootedUploads{fs}
}

func (s *FileStore) getUpload(ctx context.Context, id string) (tus.Upload, error) {
	return s.uploads().GetUpload(ctx, id)
}

func (s rootedUploads) GetUpload(ctx context.Context, id string) (tus.Upload, error) {
	infoPath := filepath.Join(s.Path, id+".info")
	data, err := os.ReadFile(infoPath)
	if os.IsNotExist(err) {
		return nil, tus.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var info tus.FileInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	path := filepath.Join(s.Path, id)
	if recorded := info.Storage[filestore.StorageKeyPath]; recorded == "" || recorded == path {
		// Written on this mount: tusd's own upload, exactly as before.
		return s.FileStore.GetUpload(ctx, id)
	}
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, tus.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	info.Offset = st.Size()
	info.Storage = map[string]string{"Type": "filestore", filestore.StorageKeyPath: path, filestore.StorageKeyInfoPath: infoPath}
	return &rootedUpload{info: info, path: path, perm: s.FileModePerm}, nil
}

// The tus core operations of tusd's filestore upload, on the local path. The
// handler disables termination, concatenation and downloads, and creation
// refuses deferred lengths, so no extension is ever asked of these uploads.
type rootedUpload struct {
	info tus.FileInfo
	path string
	perm fs.FileMode
}

func (u *rootedUpload) GetInfo(context.Context) (tus.FileInfo, error) { return u.info, nil }

func (u *rootedUpload) WriteChunk(_ context.Context, _ int64, src io.Reader) (int64, error) {
	f, err := os.OpenFile(u.path, os.O_WRONLY|os.O_APPEND, u.perm)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(f, src)
	u.info.Offset += n
	if err != nil {
		f.Close()
		return n, err
	}
	return n, f.Close()
}

func (u *rootedUpload) GetReader(context.Context) (io.ReadCloser, error) { return os.Open(u.path) }
func (u *rootedUpload) FinishUpload(context.Context) error               { return nil }

// tus HEAD takes the per-upload lock and fails with 500 after 20 seconds when
// a PATCH holds it, for example while publication verifies a large file on a
// network share. The client then restarts the transfer. Report the durable
// length instead: bytes are only appended in stream order, so any length is a
// valid resume point, and the PATCH that resumes it takes the lock.
func serveUploadHead(w http.ResponseWriter, info tus.FileInfo) {
	h := w.Header()
	h.Set("Tus-Resumable", "1.0.0")
	h.Set("Cache-Control", "no-store")
	h.Set("Upload-Offset", strconv.FormatInt(info.Offset, 10))
	h.Set("Upload-Length", strconv.FormatInt(info.Size, 10))
	if len(info.MetaData) != 0 {
		h.Set("Upload-Metadata", tus.SerializeMetadataHeader(info.MetaData))
	}
	w.WriteHeader(http.StatusOK)
}
