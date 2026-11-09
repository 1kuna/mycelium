package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	tus "github.com/tus/tusd/v2/pkg/handler"
)

const fileFaultContent = "prefix-suffix"

func fileFaultStore(t *testing.T, root string) *FileStore {
	t.Helper()
	if err := prepareFileStoreRoot(root, durableFileOps{}); err != nil {
		t.Fatal(err)
	}
	return &FileStore{root: root, clock: RealClock{}, syncDir: syncLocalDirectory}
}

func fileFaultCreate(t *testing.T, s *FileStore) *durableUpload {
	t.Helper()
	ref := creationRef(fileFaultContent)
	u, err := (durableUploadStore{s}).NewUpload(context.Background(), tus.FileInfo{ID: ref.SHA256, Size: ref.SizeBytes, MetaData: tus.MetaData{"sha256": ref.SHA256}})
	if err != nil {
		t.Fatal(err)
	}
	return u.(*durableUpload)
}

func fileFaultWrite(t *testing.T, u *durableUpload, data string) {
	t.Helper()
	n, err := u.WriteChunk(context.Background(), u.record.Offset, strings.NewReader(data))
	if err != nil || n != int64(len(data)) {
		t.Fatalf("write: %d %v", n, err)
	}
}

// These are actual publication primitives, not application-level kill points.
// Death is real SIGKILL. The torn-file case modifies an unflushed temporary
// descriptor first; name-loss modeling is covered separately below.
func TestFileStoreCrashHelper(t *testing.T) {
	root, point := os.Getenv("MYCE_FILE_FAULT_ROOT"), os.Getenv("MYCE_FILE_FAULT_POINT")
	boundary := strings.TrimPrefix(point, "legacy-")
	if root == "" {
		return
	}
	kill := func() {
		fmt.Fprintln(os.Stdout, "file fault reached:", point)
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		os.Exit(99)
	}
	ops := durableFileOps{}
	blobRenamed := false
	ops.syncFile = func(f *os.File) error {
		parent := filepath.Base(filepath.Dir(f.Name()))
		match := (boundary == "chunk-file" && strings.HasSuffix(parent, ".parts")) ||
			(boundary == "manifest-file" && parent == "uploads") ||
			(boundary == "assembly-file" && parent == "uploads") ||
			(boundary == "marker-file" && filepath.Dir(f.Name()) == root)
		if match {
			if err := f.Truncate(1); err != nil {
				t.Fatal(err)
			}
			kill()
		}
		return syncLocalFile(f)
	}
	ops.rename = func(from, to string) error {
		base, dir := filepath.Base(to), filepath.Base(filepath.Dir(to))
		kind := ""
		switch {
		case base == FormatMarker:
			kind = "marker"
		case strings.HasSuffix(dir, ".parts"):
			kind = "chunk"
		case strings.HasSuffix(base, ".info"):
			kind = "manifest"
		case dir == "blobs":
			kind = "blob"
		case dir == "uploads":
			kind = "assembly"
		}
		if boundary == kind+"-before-rename" {
			kill()
		}
		if err := os.Rename(from, to); err != nil {
			return err
		}
		if kind == "blob" {
			blobRenamed = true
		}
		if boundary == kind+"-after-rename" {
			kill()
		}
		return nil
	}
	ops.syncDir = func(path string) error {
		base := filepath.Base(path)
		if (boundary == "chunk-dir" && strings.HasSuffix(base, ".parts")) ||
			((boundary == "manifest-dir" || boundary == "assembly-dir") && base == "uploads") ||
			(boundary == "blob-dir" && base == "blobs") ||
			(boundary == "source-dir" && base == "uploads" && blobRenamed) ||
			(boundary == "marker-dir" && path == root) ||
			(boundary == "child-dir" && base == "blobs") {
			kill()
		}
		return syncLocalDirectory(path)
	}
	if strings.HasPrefix(point, "marker-") || boundary == "child-dir" {
		if err := prepareFileStoreRoot(root, ops); err != nil {
			t.Fatal(err)
		}
	} else {
		s := fileFaultStore(t, root)
		if strings.HasPrefix(point, "legacy-") {
			s.durable, s.syncDir = ops, ops.syncDir
		}
		u, err := s.verifiedUpload(context.Background(), creationRef(fileFaultContent).SHA256)
		if err != nil {
			t.Fatal(err)
		}
		// Installation happens after reopen/verification so the selected barrier
		// belongs to the mutation under test, not fixture initialization.
		s.durable, s.syncDir = ops, ops.syncDir
		if strings.HasPrefix(point, "chunk-") || strings.HasPrefix(point, "manifest-") {
			fileFaultWrite(t, u, "-suffix")
		} else {
			if err := u.FinishUpload(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.publish(context.Background(), creationRef(fileFaultContent)); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Fatal("fault boundary was not reached")
}

func fileFaultKill(t *testing.T, root, point string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestFileStoreCrashHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "MYCE_FILE_FAULT_ROOT="+root, "MYCE_FILE_FAULT_POINT="+point, "TMPDIR="+t.TempDir())
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("helper did not die: %v %s", err, output)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL || !strings.Contains(string(output), "file fault reached: "+point) {
		t.Fatalf("wrong helper exit: %v %s", err, output)
	}
}

func TestFileStoreCrashProgressAndPublication(t *testing.T) {
	for _, point := range []string{"chunk-file", "chunk-before-rename", "chunk-after-rename", "chunk-dir", "manifest-file", "manifest-before-rename", "manifest-after-rename", "manifest-dir", "assembly-file", "assembly-before-rename", "assembly-after-rename", "assembly-dir", "blob-before-rename", "blob-after-rename", "blob-dir", "source-dir"} {
		t.Run(point, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			s := fileFaultStore(t, root)
			u := fileFaultCreate(t, s)
			fileFaultWrite(t, u, "prefix") // This offset was acknowledged before death.
			progress := strings.HasPrefix(point, "chunk-") || strings.HasPrefix(point, "manifest-")
			if !progress {
				fileFaultWrite(t, u, "-suffix")
			}
			fileFaultKill(t, root, point)
			s = fileFaultStore(t, root)
			ref := creationRef(fileFaultContent)
			if s.verify(context.Background(), ref) != nil {
				u, err := s.verifiedUpload(context.Background(), ref.SHA256)
				if err != nil {
					t.Fatal("acknowledged generation unreadable", err)
				}
				if u.record.Offset != 6 && u.record.Offset != ref.SizeBytes {
					t.Fatal("acknowledged prefix lost or invented offset", u.record.Offset)
				}
				reader, _ := u.GetReader(context.Background())
				data, err := io.ReadAll(reader)
				reader.Close()
				if err != nil || string(data) != fileFaultContent[:int(u.record.Offset)] {
					t.Fatalf("invalid prefix %q %v", data, err)
				}
				if u.record.Offset == 6 {
					fileFaultWrite(t, u, "-suffix")
				}
			}
			// Exercise real tus recovery of a complete offset: HEAD must finish
			// before certifying it, despite tus's complete-PATCH short circuit.
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(root, 1<<20, false); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(q.files.uploadPath(ref.SHA256) + ".info"); err == nil {
				w := fileFaultRequest(q.files, http.MethodHead, ref, 0, "")
				if w.Code != 200 || w.Header().Get("Upload-Offset") != strconv.FormatInt(ref.SizeBytes, 10) {
					t.Fatalf("complete offset recovery: %d %s", w.Code, w.Body.String())
				}
			}
			if err := q.files.verify(context.Background(), ref); err != nil {
				t.Fatal("published bytes invalid", err)
			}
			// Later writes still work on the same reopened store.
			later := creationRef("later")
			w := httptest.NewRecorder()
			q.files.serveUpload(w, creationRequest(context.Background(), later))
			if w.Code != 201 || fileFaultRequest(q.files, http.MethodPatch, later, 0, "later").Code != 204 {
				t.Fatal("subsequent upload failed")
			}
		})
	}
}

func fileFaultRequest(s *FileStore, method string, ref FileRef, offset int64, data string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/api/files/uploads/"+ref.SHA256, strings.NewReader(data))
	r.Header.Set("Tus-Resumable", "1.0.0")
	r.Header.Set("Content-Type", "application/offset+octet-stream")
	r.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	w := httptest.NewRecorder()
	s.serveUpload(w, r)
	return w
}

func TestFileStoreCrashInitialization(t *testing.T) {
	for _, point := range []string{"marker-file", "marker-before-rename", "marker-after-rename", "marker-dir", "child-dir"} {
		t.Run(point, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "ancestor", "store")
			fileFaultKill(t, root, point)
			s := fileFaultStore(t, root)
			data, err := os.ReadFile(filepath.Join(root, FormatMarker))
			if err != nil || string(data) != stateFormat {
				t.Fatal("initialization did not recover", err)
			}
			u := fileFaultCreate(t, s)
			fileFaultWrite(t, u, fileFaultContent)
			if err := u.FinishUpload(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.publish(context.Background(), creationRef(fileFaultContent)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFileStoreDurabilityFailuresPrecedeHTTPAck(t *testing.T) {
	for _, operation := range []string{"create-file", "create-rename", "create-dir", "chunk-file", "chunk-rename", "chunk-dir", "manifest-file", "manifest-rename", "manifest-dir", "head-file", "head-dir", "blob-rename", "blob-dir", "assembly-space"} {
		t.Run(operation, func(t *testing.T) {
			_, s := creationStore(t, 1<<20)
			ref := creationRef(fileFaultContent)
			create := strings.HasPrefix(operation, "create-")
			if !create {
				w := httptest.NewRecorder()
				s.serveUpload(w, creationRequest(context.Background(), ref))
				if w.Code != 201 {
					t.Fatal("fixture create", w.Code, w.Body.String())
				}
			}
			if strings.HasPrefix(operation, "head-") {
				if w := fileFaultRequest(s, http.MethodPatch, ref, 0, "prefix"); w.Code != 204 {
					t.Fatal("fixture prefix", w.Code, w.Body.String())
				}
			}
			injected, chunkMoved, manifestMoved := false, false, false
			fault := errors.New("fixture durability barrier failed")
			fail := func() error { injected = true; return fault }
			s.durable.syncFile = func(f *os.File) error {
				part := strings.HasSuffix(filepath.Dir(f.Name()), ".parts")
				temp := strings.HasPrefix(filepath.Base(f.Name()), ".myce-write-")
				if (operation == "create-file" && temp) || (operation == "chunk-file" && part) || (operation == "manifest-file" && temp && !part) || operation == "head-file" {
					return fail()
				}
				return syncLocalFile(f)
			}
			s.durable.rename = func(from, to string) error {
				part := strings.HasSuffix(filepath.Dir(to), ".parts")
				if operation == "create-rename" || (operation == "chunk-rename" && part) || (operation == "manifest-rename" && strings.HasSuffix(to, ".info")) || (operation == "blob-rename" && filepath.Base(filepath.Dir(to)) == "blobs") {
					return fail()
				}
				if err := os.Rename(from, to); err != nil {
					return err
				}
				chunkMoved = chunkMoved || part
				manifestMoved = manifestMoved || strings.HasSuffix(to, ".info")
				return nil
			}
			s.syncDir = func(path string) error {
				part := strings.HasSuffix(path, ".parts")
				if operation == "create-dir" || (operation == "chunk-dir" && part && chunkMoved) || (operation == "manifest-dir" && filepath.Base(path) == "uploads" && manifestMoved) || operation == "head-dir" || (operation == "blob-dir" && filepath.Base(path) == "blobs") {
					return fail()
				}
				return syncLocalDirectory(path)
			}
			if operation == "assembly-space" {
				s.diskProbe = func(string) (DiskReading, error) {
					injected = true
					return DiskReading{TotalBytes: 1 << 30, FreeBytes: 1}, nil
				}
			}
			var w *httptest.ResponseRecorder
			if create {
				w = httptest.NewRecorder()
				s.serveUpload(w, creationRequest(context.Background(), ref))
			} else if strings.HasPrefix(operation, "head-") {
				w = fileFaultRequest(s, http.MethodHead, ref, 0, "")
			} else {
				// Only publication cases finish; other cases test partial progress.
				data := "prefix"
				if strings.HasPrefix(operation, "blob-") || operation == "assembly-space" {
					data = fileFaultContent
				}
				w = fileFaultRequest(s, http.MethodPatch, ref, 0, data)
			}
			if !injected || w.Code < 400 {
				t.Fatalf("barrier did not prevent acknowledgement: injected=%v status=%d %s", injected, w.Code, w.Body.String())
			}
			// Clearing the fault either exposes the old offset or a complete,
			// verified newer generation; it must not require manual repair.
			s.durable, s.syncDir, s.diskProbe = durableFileOps{}, syncLocalDirectory, nil
			if create {
				w = httptest.NewRecorder()
				s.serveUpload(w, creationRequest(context.Background(), ref))
				if w.Code != 201 {
					t.Fatalf("create retry: %d %s", w.Code, w.Body.String())
				}
			}
			w = fileFaultRequest(s, http.MethodHead, ref, 0, "")
			if w.Code != 200 {
				t.Fatalf("retry HEAD: %d %s", w.Code, w.Body.String())
			}
			offset, err := strconv.ParseInt(w.Header().Get("Upload-Offset"), 10, 64)
			if err != nil || offset < 0 || offset > ref.SizeBytes {
				t.Fatal("invalid resumed offset", offset, err)
			}
			if offset < ref.SizeBytes {
				w = fileFaultRequest(s, http.MethodPatch, ref, offset, fileFaultContent[int(offset):])
				if w.Code != 204 {
					t.Fatalf("retry PATCH: %d %s", w.Code, w.Body.String())
				}
			}
			if err := s.verify(context.Background(), ref); err != nil {
				t.Fatal("recovered content", err)
			}
		})
	}
}

func TestFileStoreLostNamesAndCorruptGenerations(t *testing.T) {
	for _, fault := range []string{"lost-chunk-name", "lost-manifest-name", "torn-chunk", "torn-manifest", "torn-descriptor"} {
		t.Run(fault, func(t *testing.T) {
			s := fileFaultStore(t, filepath.Join(t.TempDir(), "store"))
			u := fileFaultCreate(t, s)
			fileFaultWrite(t, u, "prefix")
			ref := creationRef(fileFaultContent)
			meta := s.uploadPath(ref.SHA256) + ".info"
			old, err := os.ReadFile(meta)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "lost-chunk-name", "lost-manifest-name":
				s.syncDir = func(path string) error {
					if (fault == "lost-chunk-name" && strings.HasSuffix(path, ".parts")) || (fault == "lost-manifest-name" && filepath.Base(path) == "uploads") {
						return errors.New("fixture name barrier lost")
					}
					return syncLocalDirectory(path)
				}
				if _, err := u.WriteChunk(context.Background(), 6, strings.NewReader("-suffix")); err == nil {
					t.Fatal("failed barrier acknowledged")
				}
				// Model a directory's pre-barrier image after reboot. Chunk bytes
				// were ordered before metadata; the acknowledged manifest survives.
				if fault == "lost-chunk-name" {
					err = os.Remove(s.uploadChunkPath(ref.SHA256, 6))
				} else {
					err = os.WriteFile(meta, old, 0600)
				}
			case "torn-chunk":
				err = os.WriteFile(s.uploadChunkPath(ref.SHA256, 0), []byte("torn"), 0600)
			case "torn-manifest":
				// Valid JSON with an altered acknowledged offset must still fail.
				err = os.WriteFile(meta, []byte(strings.Replace(string(old), `"Offset":6`, `"Offset":5`, 1)), 0600)
			case "torn-descriptor":
				err = os.WriteFile(s.uploadChunkPath(ref.SHA256, 0)+".json", []byte(`{"offset":0}`), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			s = fileFaultStore(t, s.root)
			reopened, err := s.verifiedUpload(context.Background(), ref.SHA256)
			if strings.HasPrefix(fault, "lost-") {
				if err != nil || reopened.record.Offset != 6 {
					t.Fatal("acknowledged generation did not survive name loss", err)
				}
				fileFaultWrite(t, reopened, "-suffix")
				if err := reopened.FinishUpload(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := s.publish(context.Background(), ref); err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatal("corrupt/unverified generation certified as progress")
				}
				q, _ := fixture(t)
				if err := q.ConfigureFileStore(s.root, 1<<20, false); err != nil {
					t.Fatal(err)
				}
				for _, method := range []string{http.MethodPost, http.MethodHead} {
					w := httptest.NewRecorder()
					if method == http.MethodPost {
						q.files.serveUpload(w, creationRequest(context.Background(), ref))
					} else {
						w = fileFaultRequest(q.files, method, ref, 0, "")
					}
					if w.Code != 422 || !strings.Contains(w.Body.String(), "ERR_UPLOAD_INTEGRITY") || w.Header().Get("Upload-Offset") != "" {
						t.Fatalf("bad generation was not a permanent refusal: %d %s", w.Code, w.Body.String())
					}
				}
			}
		})
	}
}

func TestFileStoreInitializationRejectsUnknownState(t *testing.T) {
	for _, name := range []string{FormatMarker, "old-state", ".myce-write-not-private"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, name)
			if err := os.WriteFile(path, []byte("torn"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := prepareFileStoreRoot(root, durableFileOps{}); !errors.Is(err, ErrLegacyState) {
				t.Fatal("unknown state was accepted", err)
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != "torn" {
				t.Fatal("refusal mutated state", err)
			}
		})
	}
}
