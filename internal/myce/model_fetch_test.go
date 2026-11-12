package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

const modelFetchURL = "https://models.example/repo/resolve/revision/file"

type interruptedModelFetch struct{}

func (interruptedModelFetch) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func modelFetchResponse(data string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(data))}
}

func modelFetchTransport(t *testing.T, transport fetchTransport) {
	t.Helper()
	old := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: transport}
	t.Cleanup(func() { http.DefaultClient = old })
}

func modelFetchRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func modelFetchHash(data string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(data))) }

func modelFetchPartial(t *testing.T, root string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, ".myce-fetch-*", "*.partial"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("staging partials: %v, %v", paths, err)
	}
	return paths[0]
}

func assertModelFetchBytes(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Fatalf("file %s = %q, %v; want %q", path, got, err, want)
	}
}

func assertModelFetchUnpublished(t *testing.T, root, relative string) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(root, relative)); !os.IsNotExist(err) {
		t.Fatalf("unpublished destination exists: %v", err)
	}
}

func TestModelFetchBundleResumeAndVerify(t *testing.T) {
	root := modelFetchRoot(t)
	attempt := 0
	modelFetchTransport(t, func(r *http.Request) (*http.Response, error) {
		attempt++
		res := modelFetchResponse("weights")
		switch attempt {
		case 1:
			res.Body = io.NopCloser(io.MultiReader(strings.NewReader("wei"), interruptedModelFetch{}))
		case 2:
			if got := r.Header.Get("Range"); got != "bytes=3-" {
				t.Fatalf("resume range = %q", got)
			}
			res.StatusCode = http.StatusPartialContent
			res.Header.Set("Content-Range", "bytes 3-6/7")
			res.Body = io.NopCloser(strings.NewReader("ghts"))
		case 3:
			res = modelFetchResponse("{}")
		default:
			t.Fatalf("unexpected request %d", attempt)
		}
		return res, nil
	})
	relative := "bundle/weights/model.bin"
	if err := FetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights")); err == nil {
		t.Fatal("interruption accepted")
	}
	assertModelFetchUnpublished(t, root, relative)
	assertModelFetchBytes(t, modelFetchPartial(t, root), "wei")
	files := []ModelFile{
		{Path: "weights/model.bin", SizeBytes: 7, SHA256: modelFetchHash("weights")},
		{Path: "config.json", SizeBytes: 2, SHA256: modelFetchHash("{}")},
	}
	for _, f := range files {
		if err := FetchModel(context.Background(), modelFetchURL, root, "bundle/"+f.Path, f.SizeBytes, f.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	size, sum, err := ModelFilesIdentity(files)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyModelFiles(context.Background(), filepath.Join(root, "bundle"), Model{Name: "bundle", Files: files, SizeBytes: size, SHA256: sum}); err != nil {
		t.Fatal("fetched bundle must verify without any unlisted files:", err)
	}
	// Repeating after publication (including a crash after rename) is idempotent.
	if err := FetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights")); err != nil {
		t.Fatal(err)
	}
	if attempt != 3 {
		t.Fatalf("requests = %d", attempt)
	}
}

func TestModelFetchServerIgnoresRange(t *testing.T) {
	root := modelFetchRoot(t)
	attempt := 0
	modelFetchTransport(t, func(r *http.Request) (*http.Response, error) {
		attempt++
		res := modelFetchResponse("weights")
		if attempt == 1 {
			res.Body = io.NopCloser(io.MultiReader(strings.NewReader("wei"), interruptedModelFetch{}))
		} else if r.Header.Get("Range") != "bytes=3-" {
			t.Fatalf("resume range = %q", r.Header.Get("Range"))
		}
		return res, nil // Deliberately 200 with the entire file, even for Range.
	})
	relative := "bundle/model.bin"
	if err := FetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights")); err == nil {
		t.Fatal("interruption accepted")
	}
	// The prefix is untrusted: ignoring Range must not destroy corrupt bytes.
	partial := modelFetchPartial(t, root)
	if err := os.WriteFile(partial, []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	if err := fetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights"), syncWorkerDir, &report); err != nil {
		t.Fatal(err)
	}
	assertModelFetchBytes(t, filepath.Join(root, relative), "weights")
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(partial), "quarantine", "*"))
	if err != nil || len(paths) != 1 || !strings.Contains(report.String(), paths[0]) {
		t.Fatalf("quarantine = %v, %v; report = %q", paths, err, report.String())
	}
	assertModelFetchBytes(t, paths[0], "bad")
	if attempt != 2 {
		t.Fatalf("requests = %d; should reuse full response", attempt)
	}
}

func TestModelFetchCorruptPartialQuarantined(t *testing.T) {
	for _, corrupt := range []string{"bad", "corrupt", "oversized!"} {
		t.Run(corrupt, func(t *testing.T) {
			root := modelFetchRoot(t)
			attempt := 0
			modelFetchTransport(t, func(r *http.Request) (*http.Response, error) {
				attempt++
				res := modelFetchResponse("weights")
				if attempt == 1 {
					res.Body = io.NopCloser(io.MultiReader(strings.NewReader("wei"), interruptedModelFetch{}))
				} else if r.Header.Get("Range") != "" {
					res.StatusCode = http.StatusPartialContent
					res.Header.Set("Content-Range", "bytes 3-6/7")
					res.Body = io.NopCloser(strings.NewReader("ghts"))
				}
				return res, nil
			})
			relative := "bundle/model.bin"
			if err := FetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights")); err == nil {
				t.Fatal("interruption accepted")
			}
			partial := modelFetchPartial(t, root)
			if err := os.WriteFile(partial, []byte(corrupt), 0600); err != nil {
				t.Fatal(err)
			}
			var report bytes.Buffer
			if err := fetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights"), syncWorkerDir, &report); err != nil {
				t.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(filepath.Dir(partial), "quarantine", "*"))
			if err != nil || len(paths) != 1 || !strings.Contains(report.String(), paths[0]) {
				t.Fatalf("quarantine = %v, %v; report = %q", paths, err, report.String())
			}
			want := corrupt
			if corrupt == "bad" {
				want += "ghts" // Includes every byte of the corrupt resumed prefix.
			}
			assertModelFetchBytes(t, paths[0], want)
			assertModelFetchBytes(t, filepath.Join(root, relative), "weights")
		})
	}
}

func TestModelFetchDirectorySyncFailureRetry(t *testing.T) {
	for _, failure := range []string{"root-create", "staging-create", "bundle-create", "before-rename", "destination-after-rename", "staging-after-rename"} {
		t.Run(failure, func(t *testing.T) {
			existing := modelFetchRoot(t)
			root := filepath.Join(existing, "models")
			relative := "bundle/nested/model.bin"
			dest := filepath.Join(root, relative)
			requests := 0
			modelFetchTransport(t, func(*http.Request) (*http.Response, error) {
				requests++
				return modelFetchResponse("weights"), nil
			})
			injected := errors.New("injected directory sync failure")
			failed := false
			syncDir := func(dir string) error {
				stages, _ := filepath.Glob(filepath.Join(root, ".myce-fetch-*"))
				partials, _ := filepath.Glob(filepath.Join(root, ".myce-fetch-*", "*.partial"))
				_, published := os.Stat(dest)
				_, bundle := os.Stat(filepath.Dir(dest))
				// Existing ancestors may be unreadable inside a job sandbox (#1471).
				if rel, err := filepath.Rel(existing, dir); err != nil || strings.HasPrefix(rel, "..") {
					t.Errorf("synced existing ancestor %s above the created model root", dir)
				}
				match := false
				switch failure {
				case "root-create":
					match = dir == existing
				case "staging-create":
					match = len(stages) == 1 && dir == root
				case "bundle-create":
					match = bundle == nil && dir == filepath.Dir(dest)
				case "before-rename":
					match = len(partials) == 1 && published != nil && dir == stages[0]
				case "destination-after-rename":
					match = published == nil && dir == filepath.Dir(dest)
				case "staging-after-rename":
					match = published == nil && dir == stages[0]
				}
				if !failed && match {
					failed = true
					return injected
				}
				return syncWorkerDir(dir)
			}
			if err := fetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights"), syncDir, io.Discard); !errors.Is(err, injected) {
				t.Fatalf("directory sync error = %v", err)
			}
			assertModelFetchUnpublished(t, root, relative)
			if _, err := os.Stat(root); failure == "root-create" && !os.IsNotExist(err) {
				t.Fatalf("root whose parent sync failed was kept: %v", err)
			}
			if requests > 0 {
				assertModelFetchBytes(t, modelFetchPartial(t, root), "weights")
			}
			if err := fetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights"), syncDir, io.Discard); err != nil {
				t.Fatal("retry:", err)
			}
			assertModelFetchBytes(t, dest, "weights")
			if requests != 1 {
				t.Fatalf("requests = %d; a verified staged file must not redownload", requests)
			}
		})
	}
}

func TestModelFetchQuarantineSyncFailureRetry(t *testing.T) {
	for _, directory := range []string{"quarantine", "staging"} {
		t.Run(directory, func(t *testing.T) {
			root := modelFetchRoot(t)
			relative := "bundle/model.bin"
			requests := 0
			modelFetchTransport(t, func(*http.Request) (*http.Response, error) {
				requests++
				if requests == 1 {
					return modelFetchResponse("corrupt"), nil
				}
				return modelFetchResponse("weights"), nil
			})
			injected := errors.New("injected quarantine sync failure")
			allow := false
			syncDir := func(dir string) error {
				paths, _ := filepath.Glob(filepath.Join(root, ".myce-fetch-*", "quarantine", "*"))
				if len(paths) > 0 {
					q := filepath.Dir(paths[0])
					target := q
					if directory == "staging" {
						target = filepath.Dir(q)
					}
					if dir == target && !allow {
						return injected
					}
				}
				return syncWorkerDir(dir)
			}
			var report bytes.Buffer
			for i := 0; i < 2; i++ {
				if err := fetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights"), syncDir, &report); !errors.Is(err, injected) {
					t.Fatalf("sync failure = %v", err)
				}
				assertModelFetchUnpublished(t, root, relative)
				if requests != 1 {
					t.Fatal("partial path reused before quarantine became durable")
				}
			}
			paths, _ := filepath.Glob(filepath.Join(root, ".myce-fetch-*", "quarantine", "*"))
			if len(paths) != 1 || !strings.Contains(report.String(), paths[0]) {
				t.Fatalf("quarantine %v, report %q", paths, report.String())
			}
			assertModelFetchBytes(t, paths[0], "corrupt")
			allow = true
			if err := fetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights"), syncDir, &report); err != nil {
				t.Fatal(err)
			}
			assertModelFetchBytes(t, paths[0], "corrupt")
			assertModelFetchBytes(t, filepath.Join(root, relative), "weights")
		})
	}
}

func TestModelFetchConcurrentBundleCreation(t *testing.T) {
	root := modelFetchRoot(t)
	started, release := make(chan struct{}), make(chan struct{})
	modelFetchTransport(t, func(*http.Request) (*http.Response, error) {
		close(started)
		<-release
		return modelFetchResponse("weights"), nil
	})
	done := make(chan error, 1)
	go func() {
		done <- FetchModel(context.Background(), modelFetchURL, root, "bundle/nested/model.bin", 7, modelFetchHash("weights"))
	}()
	<-started // The first process holds the bundle lock before partial creation.
	// A regular-file contender (the locking interface used by NAS clients)
	// must contend on the same live lock, not an unrelated directory inode.
	stages, err := filepath.Glob(filepath.Join(root, ".myce-fetch-*"))
	if err != nil || len(stages) != 1 {
		close(release)
		<-done
		t.Fatalf("staging = %v, %v", stages, err)
	}
	contender, err := os.OpenFile(filepath.Join(stages[0], "fetch.lock"), os.O_RDWR, 0)
	if err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	err = syscall.Flock(int(contender.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	contender.Close()
	if !errors.Is(err, syscall.EWOULDBLOCK) {
		close(release)
		<-done
		t.Fatalf("regular-file lock contender = %v", err)
	}
	// Another fetch of the same file or another file in the bundle must refuse.
	for _, relative := range []string{"bundle/nested/model.bin", "bundle/config.json"} {
		err := FetchModel(context.Background(), modelFetchURL, root, relative, 7, modelFetchHash("weights"))
		if err == nil || !strings.Contains(err.Error(), "in use") {
			close(release)
			<-done
			t.Fatalf("concurrent bundle fetch = %v", err)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertModelFetchBytes(t, filepath.Join(root, "bundle/nested/model.bin"), "weights")
	assertModelFetchUnpublished(t, root, "bundle/config.json")
}

func TestModelFetchRequiresHTTPS(t *testing.T) {
	modelFetchTransport(t, func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid model URL reached the network")
		return nil, nil
	})
	for _, raw := range []string{"", "http://models.example/model", "file:///model", "https:model", "https:///model"} {
		root := filepath.Join(t.TempDir(), "model-root")
		if err := FetchModel(context.Background(), raw, root, "model.bin", 7, modelFetchHash("weights")); err == nil || !strings.Contains(err.Error(), "HTTPS file URL") {
			t.Fatalf("invalid URL %q: %v", raw, err)
		}
		if _, err := os.Stat(root); !os.IsNotExist(err) {
			t.Fatalf("invalid URL created model storage: %v", err)
		}
	}
}
