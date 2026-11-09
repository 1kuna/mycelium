package myce

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
)

func TestFileStoreLinearUploadWork(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	s := fileFaultStore(t, root)
	const chunks, chunkSize = 64, 4096
	body := strings.Repeat("x", chunks*chunkSize)
	ref := creationRef(body)
	work := map[string]int64{}
	observe := func(kind string, n int64) { work[kind] += n }
	s.uploadWork = observe
	_, err := (durableUploadStore{s}).NewUpload(ctx, tus.FileInfo{ID: ref.SHA256, Size: ref.SizeBytes, MetaData: tus.MetaData{"sha256": ref.SHA256}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < chunks; i++ {
		before := work["metadata"]
		// Model both the route's identity lookup and tus's locked loader/HEAD.
		if _, err := s.uploadRecord(ref.SHA256); err != nil {
			t.Fatal(err)
		}
		u, err := s.verifiedUpload(ctx, ref.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := u.GetInfo(ctx); err != nil {
			t.Fatal(err)
		}
		fileFaultWrite(t, u, body[i*chunkSize:(i+1)*chunkSize])
		// Fixed-width integer fields and one fixed-size descriptor replace the
		// prefix-sized manifest. This bound applies to EVERY request, not just
		// an average that could hide growth in later PATCHes.
		if delta := work["metadata"] - before; delta > 4096 {
			t.Fatalf("PATCH %d metadata grew with prefix: %d bytes", i, delta)
		}
		if work["verify"] != 0 {
			t.Fatalf("PATCH %d rehashed accepted prefix: %d bytes", i, work["verify"])
		}
	}
	if work["accept"] != ref.SizeBytes {
		t.Fatal("incoming hashing was not linear", work)
	}
	liveMetadata := work["metadata"]
	// A new store has no process-local evidence. It must verify all accepted
	// bytes once, then resume the constant-work path for later requests.
	s = fileFaultStore(t, root)
	s.uploadWork = observe
	u, err := s.verifiedUpload(ctx, ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if work["verify"] != ref.SizeBytes {
		t.Fatal("restart did not verify exactly one prefix", work)
	}
	if work["metadata"]-liveMetadata > chunks*1024+4096 {
		t.Fatal("restart metadata work exceeded one chain traversal", work)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.verifiedUpload(ctx, ref.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	if work["verify"] != ref.SizeBytes {
		t.Fatal("resumed process repeatedly verified the same prefix", work)
	}
	if err := u.FinishUpload(ctx); err != nil {
		t.Fatal(err)
	}
	if work["verify"] != 2*ref.SizeBytes || work["assemble"] != ref.SizeBytes {
		t.Fatal("completion work was not linear", work)
	}
	if err := s.publish(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := s.verify(ctx, ref); err != nil {
		t.Fatal("final SHA-256 mismatch", err)
	}
	t.Logf("payload=%d accepted-hash=%d restart/completion-verify=%d assembly-hash=%d live-metadata=%d total-metadata=%d", ref.SizeBytes, work["accept"], work["verify"], work["assemble"], liveMetadata, work["metadata"])
}

// Use upstream filestore itself, including its actual Storage and Offset
// fields; a synthetic v2 record with its version removed is not an upgrade.
func seedLegacyUpload(t *testing.T, root, prefix string) FileRef {
	t.Helper()
	fileFaultStore(t, root)
	ref := creationRef(fileFaultContent)
	u, err := filestore.New(filepath.Join(root, "uploads")).NewUpload(context.Background(), tus.FileInfo{ID: ref.SHA256, Size: ref.SizeBytes, MetaData: tus.MetaData{"sha256": ref.SHA256}})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := u.WriteChunk(context.Background(), 0, strings.NewReader(prefix)); err != nil || n != int64(len(prefix)) {
		t.Fatalf("legacy fixture: %d %v", n, err)
	}
	return ref
}

func TestFileStoreLegacyUpgradeRestartsFromZero(t *testing.T) {
	for _, fault := range []string{"upgrade", "torn-prefix", "retire-file", "retire-after-rename", "retire-cleanup-dir"} {
		t.Run(fault, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			prefix := "prefix"
			if fault == "torn-prefix" {
				prefix = "broken" // Same surviving length; no sender-prefix evidence.
			}
			ref := seedLegacyUpload(t, root, prefix)
			if strings.HasPrefix(fault, "retire-") {
				fileFaultKill(t, root, fault)
			}
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(root, 1<<20, false); err != nil {
				t.Fatal(err)
			}
			type patch struct{ offset, body string }
			patches := make(chan patch, 2)
			api := API{Queue: q, Token: "fixture"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPatch && r.Header.Get("Upload-Offset") == "0" {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, "fixture body read", 500)
						return
					}
					r.Body.Close()
					r.Body = io.NopCloser(strings.NewReader(string(body)))
					select {
					case patches <- patch{r.Header.Get("Upload-Offset"), string(body)}:
					default:
						t.Error("unexpected repeated restart PATCHes")
					}
				}
				api.ServeHTTP(w, r)
			}))
			defer server.Close()
			client := Client{URL: server.URL, Token: "fixture"}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// Requests from a pre-upgrade session see missing session, never a
			// certified surviving prefix or a terminal integrity error.
			for _, method := range []string{http.MethodHead, http.MethodPatch} {
				res, err := client.fileRequest(ctx, method, "/api/files/uploads/"+ref.SHA256, strings.NewReader("-suffix"), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "6", "Content-Type": "application/offset+octet-stream"}, ref.SizeBytes)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode != 404 || res.Header.Get("Upload-Offset") != "" {
					t.Fatalf("legacy session was not missing: %s %d", method, res.StatusCode)
				}
			}
			source := filepath.Join(t.TempDir(), "source")
			if err := os.WriteFile(source, []byte(fileFaultContent), 0600); err != nil {
				t.Fatal(err)
			}
			// Production client code is unchanged. Its normal create/reconcile
			// path must replace the missing legacy session and resend ALL bytes.
			if err := client.UploadFile(ctx, source, ref); err != nil {
				t.Fatal("unchanged client exposed a terminal upgrade error", err)
			}
			select {
			case got := <-patches:
				if got.offset != "0" || got.body != fileFaultContent {
					t.Fatal("legacy bytes were reused instead of resent", got)
				}
			default:
				t.Fatal("client did not restart the upload")
			}
			if err := q.files.verify(ctx, ref); err != nil {
				t.Fatal("restarted upload final SHA-256 mismatch", err)
			}
			for _, suffix := range []string{"", ".info", ".parts", ".retired"} {
				if _, err := os.Lstat(q.files.uploadPath(ref.SHA256) + suffix); !os.IsNotExist(err) {
					t.Fatal("legacy/session cleanup incomplete", suffix, err)
				}
			}
		})
	}
}
