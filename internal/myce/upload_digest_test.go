package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

func digestContent(size int) ([]byte, FileRef) {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	sum := sha256.Sum256(b)
	return b, FileRef{SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(size)}
}

// digestPatch sends one tus PATCH and counts the file reads it made on the
// request path: every store read goes through contextReader, which reports
// each read to the request's progress callback.
func digestPatch(t *testing.T, s *FileStore, ref FileRef, offset int64, body []byte) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	var reads atomic.Int64
	ctx := context.WithValue(context.Background(), ioProgressKey{}, func() { reads.Add(1) })
	r := httptest.NewRequest("PATCH", "/api/files/uploads/"+ref.SHA256, bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set("Tus-Resumable", "1.0.0")
	r.Header.Set("Content-Type", "application/offset+octet-stream")
	r.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	w := httptest.NewRecorder()
	s.serveUpload(w, r)
	return w, reads.Load()
}

func digestCreate(t *testing.T, s *FileStore, ref FileRef) {
	t.Helper()
	w := httptest.NewRecorder()
	s.serveUpload(w, creationRequest(context.Background(), ref))
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
}

// digestSend PATCHes data[from:to] in chunks and requires that none of them
// read the file back.
func digestSend(t *testing.T, s *FileStore, ref FileRef, data []byte, from, to, chunk int) {
	t.Helper()
	for at := from; at < to; at += chunk {
		end := min(at+chunk, to)
		w, reads := digestPatch(t, s, ref, int64(at), data[at:end])
		if w.Code != 204 {
			t.Fatalf("PATCH at %d: %d %s", at, w.Code, w.Body.String())
		}
		if reads != 0 {
			t.Fatalf("PATCH at %d read the upload back %d times", at, reads)
		}
	}
}

func digestPublished(t *testing.T, s *FileStore, ref FileRef, data []byte) {
	t.Helper()
	if err := verifyFile(context.Background(), s.blob(ref), ref); err != nil {
		t.Fatal("blob does not hold the uploaded bytes:", err)
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "uploads"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ref.SHA256) || strings.HasPrefix(e.Name(), ".") {
			t.Fatal("publication left an upload path behind:", e.Name())
		}
	}
}

func digestUnpublished(t *testing.T, s *FileStore, ref FileRef) {
	t.Helper()
	if _, err := os.Lstat(s.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("refused bytes are visible under their digest:", err)
	}
}

// A large upload's final PATCH publishes without reading the file back: the
// digest was hashed as each chunk arrived (#2155). On a network share those
// reads (one verify of the upload, one of the blob, or a copy where hard
// links are refused) outlasted the client's response window.
func TestUploadPublicationReadsNothingBack(t *testing.T) {
	for _, mode := range []string{"hard link", "links refused"} {
		t.Run(mode, func(t *testing.T) {
			_, s := creationStore(t, 1<<20)
			if mode == "links refused" {
				s.link = func(old, new string) error {
					return &os.LinkError{Op: "link", Old: old, New: new, Err: syscall.EOPNOTSUPP}
				}
			}
			const chunk = 64 << 10
			data, ref := digestContent(3*chunk + 17)
			digestCreate(t, s, ref)
			digestSend(t, s, ref, data, 0, len(data)-chunk, chunk)
			upload, err := os.Stat(filepath.Join(s.root, "uploads", ref.SHA256))
			if err != nil {
				t.Fatal(err)
			}
			w, reads := digestPatch(t, s, ref, int64(len(data)-chunk), data[len(data)-chunk:])
			if w.Code != 204 {
				t.Fatal(w.Code, w.Body.String())
			}
			if reads != 0 {
				t.Fatalf("final PATCH read the upload back %d times", reads)
			}
			blob, err := os.Stat(s.blob(ref))
			if err != nil || !os.SameFile(upload, blob) {
				t.Fatal("blob is not the uploaded file itself (copied?)", err)
			}
			digestPublished(t, s, ref, data)
		})
	}
}

// The record vouches only for bytes its writers saw. Wrong bytes, a rewrite
// of recorded bytes by a former writer, or a damaged record all fall back to
// reading the file at publication, which refuses what does not match.
func TestUploadDigestRefusesWhatItDidNotSee(t *testing.T) {
	const chunk = 64 << 10
	for _, mode := range []string{"wrong bytes", "rewritten wrong", "rewritten same", "damaged record", "partial"} {
		t.Run(mode, func(t *testing.T) {
			_, s := creationStore(t, 1<<20)
			data, ref := digestContent(3 * chunk)
			sent := bytes.Clone(data)
			if mode == "wrong bytes" {
				sent[len(sent)-1] ^= 1
			}
			digestCreate(t, s, ref)
			digestSend(t, s, ref, sent, 0, 2*chunk, chunk)
			record := filepath.Join(s.root, "uploads", ref.SHA256+uploadDigestSuffix)
			switch mode {
			case "rewritten wrong", "rewritten same":
				// A former coordinator's late write lands behind the record.
				late := data[:chunk/2]
				if mode == "rewritten wrong" {
					late = bytes.Repeat([]byte{0xee}, chunk/2)
				}
				upload, err := s.getUpload(context.Background(), ref.SHA256)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := upload.WriteChunk(context.Background(), 0, bytes.NewReader(late)); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Lstat(record); !os.IsNotExist(err) {
					t.Fatal("a rewrite behind the record left it in place:", err)
				}
			case "damaged record":
				b, err := os.ReadFile(record)
				if err != nil {
					t.Fatal(err)
				}
				b[len(b)/2] ^= 1
				if err := os.WriteFile(record, b, 0600); err != nil {
					t.Fatal(err)
				}
			case "partial":
				w, _ := digestPatch(t, s, ref, 2*chunk, data[2*chunk:len(data)-1])
				if w.Code != 204 {
					t.Fatal(w.Code, w.Body.String())
				}
				digestUnpublished(t, s, ref)
				return
			}
			w, reads := digestPatch(t, s, ref, 2*chunk, sent[2*chunk:])
			switch mode {
			case "rewritten same", "damaged record":
				if w.Code != 204 || reads == 0 {
					t.Fatalf("publication without a usable record: %d after %d reads; want 204 after verifying from disk", w.Code, reads)
				}
				digestPublished(t, s, ref, data)
			default:
				assertUploadError(t, w, 500, "file_upload_unavailable")
				digestUnpublished(t, s, ref)
			}
		})
	}
}

// The record lives beside the upload in the shared store, so a successor,
// mounting the store at another path, publishes without reading it back.
// A writer that died after its bytes landed but before its record costs the
// successor only that gap. An upload with no usable record and a large gap,
// such as one in flight across this upgrade, is verified from disk.
func TestUploadDigestSurvivesHandover(t *testing.T) {
	const chunk = 64 << 10
	for _, mode := range []string{"record current", "writer died before its record", "no record"} {
		t.Run(mode, func(t *testing.T) {
			size, sent := 4*chunk, 2*chunk
			if mode == "no record" {
				// Past what a successor would read to rejoin a record.
				size, sent = uploadDigestCatchUp+4*chunk, uploadDigestCatchUp+2*chunk
			}
			q, s := creationStore(t, 64<<20)
			data, ref := digestContent(size)
			digestCreate(t, s, ref)
			digestSend(t, s, ref, data, 0, sent, max(chunk, sent/8))
			path := filepath.Join(s.root, "uploads", ref.SHA256)
			switch mode {
			case "writer died before its record":
				f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write(data[sent : sent+chunk]); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				sent += chunk
			case "no record":
				if err := os.Remove(path + uploadDigestSuffix); err != nil {
					t.Fatal(err)
				}
			}

			// The successor mounts the store somewhere else (#2144).
			moved := filepath.Join(t.TempDir(), "elsewhere")
			if err := os.Rename(s.root, moved); err != nil {
				t.Fatal(err)
			}
			if err := q.ConfigureFileStore(moved, 64<<20, false); err != nil {
				t.Fatal(err)
			}
			successor := q.files
			if successor == s {
				t.Fatal("no new store")
			}
			if offset := successor.mustUploadOffset(t, ref); offset != int64(sent) {
				t.Fatalf("successor offset %d, want %d", offset, sent)
			}
			w, reads := digestPatch(t, successor, ref, int64(sent), data[sent:len(data)-chunk])
			if w.Code != 204 {
				t.Fatal(w.Code, w.Body.String())
			}
			if mode == "record current" && reads != 0 {
				t.Fatalf("successor read the upload back %d times", reads)
			}
			if mode == "writer died before its record" && reads == 0 {
				t.Fatal("successor did not rejoin the record from the unrecorded chunk")
			}
			w, reads = digestPatch(t, successor, ref, int64(len(data)-chunk), data[len(data)-chunk:])
			if w.Code != 204 {
				t.Fatal(w.Code, w.Body.String())
			}
			if mode == "no record" {
				if reads == 0 {
					t.Fatal("published an upload no record vouched for without reading it")
				}
			} else if reads != 0 {
				t.Fatalf("final PATCH after handover read the upload back %d times", reads)
			}
			digestPublished(t, successor, ref, data)
		})
	}
}

func (s *FileStore) mustUploadOffset(t *testing.T, ref FileRef) int64 {
	t.Helper()
	upload, err := s.getUpload(context.Background(), ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	info, err := upload.GetInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.Offset
}
