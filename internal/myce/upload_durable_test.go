package myce

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

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

func TestFileStoreLegacyUpgradeContinues(t *testing.T) {
	for _, route := range []string{"client", "inflight-patch", "legacy-chunk-file", "legacy-manifest-before-rename", "legacy-manifest-after-rename"} {
		t.Run(route, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			ref := seedLegacyUpload(t, root, "prefix")
			if strings.HasPrefix(route, "legacy-") {
				fileFaultKill(t, root, route)
			}
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(root, 1<<20, false); err != nil {
				t.Fatal(err)
			}
			if route == "inflight-patch" {
				w := fileFaultRequest(q.files, http.MethodPatch, ref, 6, "-suffix")
				if w.Code != 204 {
					t.Fatalf("in-flight legacy PATCH needs no manual step: %d %s", w.Code, w.Body.String())
				}
			} else {
				api := API{Queue: q, Token: "fixture"}.Handler()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPatch && r.Header.Get("Upload-Offset") != strconv.Itoa(len("prefix")) {
						t.Errorf("legacy prefix was discarded: offset %q", r.Header.Get("Upload-Offset"))
					}
					api.ServeHTTP(w, r)
				}))
				defer server.Close()
				source := filepath.Join(t.TempDir(), "source")
				if err := os.WriteFile(source, []byte(fileFaultContent), 0600); err != nil {
					t.Fatal(err)
				}
				client := Client{URL: server.URL, Token: "fixture", singleAttempt: true}
				if err := client.UploadFile(context.Background(), source, ref); err != nil {
					t.Fatal("unchanged client failed transparent upgrade", err)
				}
			}
			if err := q.files.verify(context.Background(), ref); err != nil {
				t.Fatal("legacy completion did not verify final SHA-256", err)
			}
		})
	}
}

func TestFileStoreLegacyAdoptionDoesNotCertifyCorruptFinalBytes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	ref := seedLegacyUpload(t, root, "broken")
	s := fileFaultStore(t, root)
	u, err := s.verifiedUpload(context.Background(), ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := u.GetReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(got) != "broken" {
		t.Fatal("adoption did not preserve the actual legacy bytes", err)
	}
	fileFaultWrite(t, u, "-suffix")
	if err := u.FinishUpload(context.Background()); err == nil {
		t.Fatal("legacy corruption bypassed final SHA-256 check")
	}
	if err := s.verify(context.Background(), ref); err == nil {
		t.Fatal("corrupt legacy bytes published")
	}
}
