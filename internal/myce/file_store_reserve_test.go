package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func policyInput(t *testing.T, content string) (string, FileRef) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, ref
}
func assertFilePolicyRefusal(t *testing.T, err error, reason string) {
	t.Helper()
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != http.StatusInsufficientStorage || he.Code != "file_store_capacity" || !strings.Contains(he.Message, reason) {
		t.Fatalf("wanted capacity reason %q; got %v", reason, err)
	}
}

// Reproduce the review against deployed input/native-result framing: neither
// legacy path supplies project/class or execution identity on the wire.
func TestFileStoreReserveLegacyProductionWrites(t *testing.T) {
	for _, reserve := range []int64{0, 6} {
		for _, native := range []bool{false, true} {
			t.Run(fmt.Sprintf("reserve=%d/native=%t", reserve, native), func(t *testing.T) {
				q, _ := fixture(t)
				c := retentionClient(t, q)
				q.files.limit = 12
				_, input := retentionUpload(t, c, "123456")
				q.fileStoreProductionReserve, q.files.productionReserve = reserve, uint64(reserve)
				if !native {
					path, ref := policyInput(t, "next")
					if err := c.PreflightFileStore(context.Background(), FileManifest{"input": ref}); err != nil {
						t.Fatal("legacy production input preflight", err)
					}
					if err := c.UploadJobInput(context.Background(), path, ref); err != nil {
						t.Fatal("legacy production input", err)
					}
					if err := c.UploadJobInput(context.Background(), path, ref); err != nil {
						t.Fatal("legacy input replay", err)
					}
				} else {
					s := spec("production")
					s.Resumable = false
					s.Files = FileManifest{"input": input}
					s.Outputs = map[string]string{"out.txt": "results"}
					m := machine("legacy", "fake")
					check(t, q, m, nil)
					submitted := submit(t, q, s)
					assigned := check(t, q, m, nil).Job
					if assigned == nil || assigned.ID != submitted.ID {
						t.Fatal("not assigned")
					}
					assigned.Spec.Run.Workdir = t.TempDir()
					if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("next"), 0600); err != nil {
						t.Fatal(err)
					}
					c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
						r.Header.Del("X-Myce-File-Attempt")
						return http.DefaultTransport.RoundTrip(r)
					})}
					r, err := (Worker{Client: c}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
					if err != nil {
						t.Fatal("legacy production native result", err)
					}
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}

				}
				status := q.fileStoreStatus(context.Background())
				expected := uint64(0)
				if reserve > 0 {
					expected = 4
				}
				if status.UnidentifiedReserveAdmittedBytes != expected || status.UnidentifiedWritesMayUseReserve != (reserve > 0) {
					t.Fatalf("legacy reserve accounting: %+v", status)
				}
			})
		}
	}
}

func TestFileStoreReserveWorkerAttemptTier(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, _ := fixture(t)
			c := retentionClient(t, q)
			q.files.limit = 12
			_, input := retentionUpload(t, c, "123456")
			q.fileStoreProductionReserve, q.files.productionReserve = 6, 6
			s := spec(priority)
			s.Resumable = false
			s.Files = FileManifest{"input": input}
			s.Outputs = map[string]string{"out.txt": "results"}
			m := machine("publisher", "fake")
			check(t, q, m, nil)
			submitted := submit(t, q, s)
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != submitted.ID {
				t.Fatal("not assigned")
			}
			assigned.Spec.Run.Workdir = t.TempDir()
			if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("next"), 0600); err != nil {
				t.Fatal(err)
			}
			seenAttempt := false
			c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasPrefix(r.URL.Path, "/api/files/") {
					seenAttempt = seenAttempt || r.Header.Get("X-Myce-File-Attempt") != ""
					r.Header.Set("X-Myce-Project", "different-project")
					claimed := "production"
					if priority == "production" {
						claimed = "experiment"
					}
					r.Header.Set("X-Myce-Class", claimed)
				}
				return http.DefaultTransport.RoundTrip(r)
			})}
			r, err := (Worker{Client: c}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
			if !seenAttempt {
				t.Fatal("worker omitted execution identity")
			}
			if priority == "production" {
				if err != nil {
					t.Fatal("production native result", err)
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			} else {
				assertFilePolicyRefusal(t, err, "production reserve=6 bytes")
			}
			stale := *assigned
			stale.Attempt++
			path, ref := policyInput(t, "stale")
			err = c.UploadJobInput(withFileStoreAttempt(context.Background(), stale), path, ref)
			var he *HTTPError
			if !errors.As(err, &he) || he.Status != http.StatusConflict {
				t.Fatalf("stale upload identity accepted: %v", err)
			}
			if u := q.fileStoreStatus(context.Background()).UnidentifiedReserveAdmittedBytes; u != 0 {
				t.Fatalf("identified write counted as fallback: %d", u)
			}
		})
	}
}

func TestFileStoreReserveInputHintsCannotPromote(t *testing.T) {
	for _, tc := range []struct {
		project, class string
		allow          bool
	}{

		{"service", "experiment", false},
		{"research", "production", false},
	} {
		t.Run(tc.project+"/"+tc.class, func(t *testing.T) {
			q, _ := fixture(t)
			c := retentionClient(t, q)
			q.files.limit = 12
			retentionUpload(t, c, "123456")
			q.fileStoreProductionReserve, q.files.productionReserve = 6, 6
			if err := q.ConfigureProjects(map[string]ProjectPolicy{"service": {Weight: 1, DefaultCallPriority: "production"}, "research": {Weight: 1, DefaultCallPriority: "experiment"}}); err != nil {
				t.Fatal(err)
			}
			// Preserve absent class, as a project-only HTTP hint would.
			ctx := context.WithValue(context.Background(), fileStoreSubmissionKey{}, Spec{Project: tc.project, Class: tc.class})
			path, ref := policyInput(t, "next")
			for _, err := range []error{c.PreflightFileStore(ctx, FileManifest{"input": ref}), c.UploadJobInput(ctx, path, ref)} {
				if tc.allow {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					assertFilePolicyRefusal(t, err, "production reserve=6 bytes")
				}
			}
			if u := q.fileStoreStatus(context.Background()).UnidentifiedReserveAdmittedBytes; u != 0 {
				t.Fatalf("project-policy admission counted as fallback: %d", u)
			}
		})
	}
}
