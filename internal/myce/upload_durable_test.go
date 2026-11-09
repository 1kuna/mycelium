package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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
	if err := writeLegacyUpload(root, ref, prefix); err != nil {
		t.Fatal(err)
	}
	return ref
}

func writeLegacyUpload(root string, ref FileRef, prefix string) error {
	u, err := filestore.New(filepath.Join(root, "uploads")).NewUpload(context.Background(), tus.FileInfo{ID: ref.SHA256, Size: ref.SizeBytes, MetaData: tus.MetaData{"sha256": ref.SHA256, "purpose": "job-input"}})
	if err != nil {
		return err
	}
	if n, err := u.WriteChunk(context.Background(), 0, strings.NewReader(prefix)); err != nil || n != int64(len(prefix)) {
		return fmt.Errorf("legacy fixture: %d %v", n, err)
	}
	return nil
}

// Each unchanged public client is called ONCE. The server changes to real
// upstream metadata while that call is active, including after an acknowledged
// first chunk, or another caller overtakes a pending PATCH after reset. No
// test-side retry or preliminary failed session probe can hide
// a terminal error in either deployed client's retry/reconciliation path.
func TestFileStoreLegacyUpgradeRestartsFromZero(t *testing.T) {
	for _, language := range []string{"Go", "Python"} {
		for _, upgrade := range []string{"before-head", "between-chunks", "overtaken-patch"} {
			t.Run(language+"/"+upgrade, func(t *testing.T) {
				chunkSize := 4 << 20
				if language == "Python" {
					chunkSize = 1 << 20
				}
				content := strings.Repeat("s", chunkSize) + "tail"
				ref := creationRef(content)
				root := filepath.Join(t.TempDir(), "store")
				q, _ := fixture(t)
				if err := q.ConfigureFileStore(root, 32<<20, false); err != nil {
					t.Fatal(err)
				}
				api := API{Queue: q, Token: "fixture"}.Handler()
				endpoint := "/api/files/uploads/" + ref.SHA256
				var mu sync.Mutex
				upgraded, acceptedFirst, overtook := false, false, false
				zeroHeads, advancedHeads, staleRetries, zeroPatches := 0, 0, 0, 0
				var resent int64
				digest := sha256.New()
				var serverErr error
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					fail := func(err error) {
						serverErr = err
						http.Error(w, "upgrade fixture failed", 500)
					}
					if strings.HasPrefix(r.URL.Path, "/api/files/uploads/") && r.URL.Path != "/api/files/uploads/" && r.URL.Path != endpoint {
						fail(fmt.Errorf("session identity changed: %s", r.URL.Path))
						return
					}
					inject := !upgraded && r.URL.Path == endpoint &&
						((upgrade == "before-head" || upgrade == "overtaken-patch") && r.Method == http.MethodHead ||
							upgrade == "between-chunks" && acceptedFirst && r.Method == http.MethodPatch)
					if inject {
						// Requests are serialized in this fixture. Replace modern
						// progress with actual old filestore state between requests.
						if err := os.RemoveAll(q.files.uploadPath(ref.SHA256) + ".parts"); err != nil {
							fail(err)
							return
						}
						// A torn, equal-length prefix must never be certified.
						if err := writeLegacyUpload(root, ref, strings.Repeat("?", chunkSize)); err != nil {
							fail(err)
							return
						}
						upgraded = true
					}
					var body []byte
					if r.Method == http.MethodPatch {
						var err error
						body, err = io.ReadAll(r.Body)
						r.Body.Close()
						if err != nil {
							fail(err)
							return
						}
						r.Body = io.NopCloser(strings.NewReader(string(body)))
					}
					if upgrade == "overtaken-patch" && upgraded && !overtook && r.Method == http.MethodPatch {
						// The active client has already read HEAD offset zero and
						// sent PATCH zero. Let a second caller durably accept the
						// prefix through the real API before releasing that PATCH.
						if r.Header.Get("Upload-Offset") != "0" {
							fail(fmt.Errorf("pending PATCH did not use HEAD zero: %q", r.Header.Get("Upload-Offset")))
							return
						}
						peer := r.Clone(r.Context())
						prefix := content[:chunkSize]
						peer.Body = io.NopCloser(strings.NewReader(prefix))
						peer.ContentLength = int64(len(prefix))
						peer.Header.Set("Upload-Offset", "0")
						response := httptest.NewRecorder()
						api.ServeHTTP(response, peer)
						if response.Code != 204 || response.Header().Get("Upload-Offset") != fmt.Sprint(chunkSize) {
							fail(fmt.Errorf("overtaking PATCH failed: %d %s", response.Code, response.Body.String()))
							return
						}
						overtook = true
						zeroPatches++
						resent += int64(len(prefix))
						digest.Write([]byte(prefix))
					}
					response := &uploadResponse{ResponseWriter: w}
					api.ServeHTTP(response, r)
					if r.Method == http.MethodPost && response.status == 201 {
						location, err := url.Parse(w.Header().Get("Location"))
						if err != nil || location.Path != endpoint {
							serverErr = fmt.Errorf("POST changed session: %q", w.Header().Get("Location"))
						}
					}
					if r.URL.Path != endpoint {
						return
					}
					if response.status == 404 || response.status == 422 || response.status == 409 {
						serverErr = fmt.Errorf("terminal legacy session response: %s %d", r.Method, response.status)
					}
					if r.Method == http.MethodHead && upgraded && response.status == 200 && w.Header().Get("Upload-Offset") == "0" {
						zeroHeads++
					}
					if r.Method == http.MethodHead && overtook && response.status == 200 && w.Header().Get("Upload-Offset") == fmt.Sprint(chunkSize) {
						advancedHeads++
					}
					if r.Method == http.MethodPatch {
						if upgraded && response.status == 503 {
							staleRetries++
						}
						if response.status == 204 {
							if r.Header.Get("Upload-Offset") == "0" {
								zeroPatches++
								acceptedFirst = true
							}
							if upgraded {
								resent += int64(len(body))
								digest.Write(body)
							}
						}
					}
				}))
				defer server.Close()
				source := filepath.Join(t.TempDir(), "source")
				if err := os.WriteFile(source, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if language == "Go" {
					client := Client{URL: server.URL, Token: "fixture"}
					if err := client.UploadFile(ctx, source, ref); err != nil {
						t.Fatal("single active UploadFile surfaced upgrade error", err)
					}
				} else {
					cmd := exec.CommandContext(ctx, "python3.11", "-c", `import json, sys
from mycelium import Client
print(json.dumps(Client(sys.argv[1], "fixture", transfer_timeout=25).upload_file(sys.argv[2])))`, server.URL, source)
					cmd.Env = append(os.Environ(), "PYTHONPATH=../../clients/python/src", "PYTHONDONTWRITEBYTECODE=1")
					out, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("single active Python upload_file surfaced upgrade error: %v: %s", err, out)
					}
					var got FileRef
					if err := json.Unmarshal(out, &got); err != nil || got != ref {
						t.Fatalf("Python returned wrong digest: %s (%v)", out, err)
					}
				}
				server.Close()
				mu.Lock()
				defer mu.Unlock()
				if serverErr != nil {
					t.Fatal(serverErr)
				}
				if !upgraded || zeroHeads != 1 || resent != ref.SizeBytes || hex.EncodeToString(digest.Sum(nil)) != ref.SHA256 {
					t.Fatalf("no full verified resend: upgraded=%v zeroHEADs=%d resent=%d digest=%x", upgraded, zeroHeads, resent, digest.Sum(nil))
				}
				wantZero, wantRetry := 1, 0
				if upgrade == "between-chunks" {
					wantZero, wantRetry = 2, 1
				}
				if upgrade == "overtaken-patch" {
					wantRetry = 1
					if !overtook || advancedHeads != 1 {
						t.Fatalf("client did not reconcile overtaking progress: overtook=%v advancedHEADs=%d", overtook, advancedHeads)
					}
				}
				if zeroPatches != wantZero || staleRetries != wantRetry {
					t.Fatalf("wrong resend/retry path: zeroPATCHes=%d staleRetries=%d", zeroPatches, staleRetries)
				}
				if err := q.files.verify(ctx, ref); err != nil {
					t.Fatal("restarted upload final SHA-256 mismatch", err)
				}
				assertUploadCleaned(t, q.files, ref)
			})
		}
	}
}

func assertUploadCleaned(t *testing.T, s *FileStore, ref FileRef) {
	t.Helper()
	for _, suffix := range []string{"", ".info", ".parts", ".retired"} {
		if _, err := os.Lstat(s.uploadPath(ref.SHA256) + suffix); !os.IsNotExist(err) {
			t.Fatal("legacy/session cleanup incomplete", suffix, err)
		}
	}
}

func TestFileStoreLegacyResetCrashRecovery(t *testing.T) {
	for _, point := range []string{"retire-file", "retire-after-rename", "retire-cleanup-dir", "retire-payload-dir", "retire-replacement-info", "retire-replacement-dir"} {
		t.Run(point, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			ref := seedLegacyUpload(t, root, "broken")
			fileFaultKill(t, root, point)
			payloads, err := fileFaultStore(t, root).inventory(context.Background())
			if err != nil || payloads[ref.SHA256].length != uint64(ref.SizeBytes) {
				t.Fatal("interrupted reset lost its capacity reservation", payloads, err)
			}
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(root, 1<<20, false); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: "fixture"}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			offset, err := client.uploadOffset(ctx, "/api/files/uploads/"+ref.SHA256, ref.SizeBytes)
			if err != nil || offset != 0 {
				t.Fatalf("interrupted reset did not recover same-ID HEAD: %d %v", offset, err)
			}
			// Even a stale PATCH after the resetting process died must be
			// transient, allowing old clients to reconcile the zero offset.
			res, err := client.fileRequest(ctx, http.MethodPatch, "/api/files/uploads/"+ref.SHA256, strings.NewReader("-suffix"), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "6", "Content-Type": "application/offset+octet-stream"}, ref.SizeBytes)
			if err != nil {
				t.Fatal(err)
			}
			if err := fileResponse(res, 204); !retryTransient(err) {
				t.Fatal("stale PATCH was not retryable", err)
			}
			source := filepath.Join(t.TempDir(), "source")
			if err := os.WriteFile(source, []byte(fileFaultContent), 0600); err != nil {
				t.Fatal(err)
			}
			if err := client.UploadFile(ctx, source, ref); err != nil {
				t.Fatal(err)
			}
			if err := q.files.verify(ctx, ref); err != nil {
				t.Fatal(err)
			}
			assertUploadCleaned(t, q.files, ref)
		})
	}
}

// A reset intent left beside a valid replacement must not authorize deleting
// any of that replacement's accepted chunks, even on a delayed cleanup retry.
func TestFileStoreLegacyResetPreservesReplacement(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	ref := seedLegacyUpload(t, root, "broken")
	s := fileFaultStore(t, root)
	ctx := context.Background()
	if _, err := s.resetLegacyUpload(ctx, ref.SHA256); err != nil {
		t.Fatal(err)
	}
	u, err := s.verifiedUpload(ctx, ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	fileFaultWrite(t, u, "prefix")
	intent := durableUploadRecord{FileInfo: tus.FileInfo{ID: ref.SHA256, Size: ref.SizeBytes, MetaData: tus.MetaData{"sha256": ref.SHA256}}, Version: 2, Restarted: true}
	intent.Checksum, err = intent.digest()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.uploadOps().bytes(s.uploadPath(ref.SHA256)+".retired", data); err != nil {
		t.Fatal(err)
	}
	if record, err := s.resetLegacyUpload(ctx, ref.SHA256); err != nil || record.Offset != 6 {
		t.Fatal("delayed reset changed replacement progress", record.Offset, err)
	}
	s = fileFaultStore(t, root) // Verify from disk, without cached evidence.
	u, err = s.verifiedUpload(ctx, ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	fileFaultWrite(t, u, "-suffix")
	if err := u.FinishUpload(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.publish(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := s.verify(ctx, ref); err != nil {
		t.Fatal(err)
	}
	assertUploadCleaned(t, s, ref)
}
