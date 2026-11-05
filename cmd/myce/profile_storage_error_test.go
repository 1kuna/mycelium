package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProfileStorageFailureCause(t *testing.T) {
	for _, adapter := range []bool{false, true} {
		for _, operation := range []string{"preflight", "upload"} {
			for _, failure := range []string{"503", "429", "401", "timeout", "refused", "reset"} {
				t.Run(strings.Join([]string{map[bool]string{false: "profile", true: "entry"}[adapter], operation, failure}, "/"), func(t *testing.T) {
					root := t.TempDir()
					source := filepath.Join(root, "source")
					if err := os.Mkdir(source, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("true"), 0600); err != nil {
						t.Fatal(err)
					}
					a, err := decodeAdapter([]byte(cpuAdapterYAML("source")))
					if err != nil {
						t.Fatal(err)
					}
					p, _, err := adapterProfile(a, "work")
					if err != nil {
						t.Fatal(err)
					}
					p.Adapter = adapter
					const secret = "fixture-secret-never-print"
					var reached atomic.Bool
					release := make(chan struct{})
					var server *httptest.Server
					server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if operation == "upload" && r.URL.Path == "/api/files/preflight" {
							w.WriteHeader(200)
							return
						}
						reached.Store(true)
						switch failure {
						case "timeout":
							select {
							case <-r.Context().Done():
							case <-release:
							}
						case "refused":
							// Stop accepting after preflight so the next upload connection is refused.
							if operation == "upload" {
								t.Error("refused fixture must close before upload")
							}
						case "reset":
							conn, _, e := w.(http.Hijacker).Hijack()
							if e != nil {
								t.Error(e)
								return
							}
							_ = conn.(*net.TCPConn).SetLinger(0)
							_ = conn.Close()
						default:
							status := 503
							if failure == "429" {
								status = 429
								w.Header().Set("Retry-After", "1")
							}
							if failure == "401" {
								status = 401
							}
							w.WriteHeader(status)
							_ = json.NewEncoder(w).Encode(map[string]string{"code": "request_failed", "error": secret + " https://private.invalid/path"})
						}
					}))
					defer server.Close()
					defer close(release)
					if failure == "refused" {
						if operation == "upload" {
							server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
								reached.Store(true)
								_ = server.Listener.Close()
								w.Header().Set("Connection", "close")
								w.WriteHeader(200)
							})
						} else {
							server.Close()
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
					defer cancel()
					var out bytes.Buffer
					err = runPreparedProfile(ctx, root, p, []byte("fixture"), nil, nil, "storage-cause", "", "", "", server.URL, secret, &out, io.Discard)
					if exitCode(err) != 69 {
						t.Fatalf("exit=%d err=%v", exitCode(err), err)
					}
					want := map[string]string{"503": "HTTP 503 code=request_failed", "429": "HTTP 429 code=request_failed", "401": "HTTP 401 code=request_failed", "timeout": "timeout", "refused": "ECONNREFUSED", "reset": "ECONNRESET"}[failure]
					for _, part := range []string{"operation=" + operation, `input="source"`, want, "intent retained", "rerunning the same command resumes from the receipt without duplicating anything"} {
						if !strings.Contains(err.Error(), part) {
							t.Errorf("missing %q: %v", part, err)
						}
					}
					for _, bad := range []string{secret, "private.invalid", root, server.URL} {
						if strings.Contains(err.Error(), bad) {
							t.Errorf("unsafe diagnostic: %v", err)
						}
					}
					var receipt profileReceipt
					if e := json.Unmarshal(out.Bytes(), &receipt); e != nil {
						t.Fatal(e)
					}
					saved := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{p.Project, "storage-cause"}), "receipt.json")
					var retained profileReceipt
					if e := readProfileJSON(saved, &retained); e != nil || retained.Key != receipt.Key || retained.Status != "intent" || len(retained.Uploads) == 0 {
						t.Fatalf("intent not retained: %v", e)
					}
					if failure != "refused" && !reached.Load() {
						t.Fatal("fake server not reached")
					}
				})
			}
		}
	}
}

func TestProfileStorageNamesAuthoredInput(t *testing.T) {
	for _, adapter := range []bool{false, true} {
		t.Run(map[bool]string{false: "profile", true: "entry"}[adapter], func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
				t.Fatal(err)
			}
			input := filepath.Join(root, "private-local-file")
			if err := os.WriteFile(input, []byte("fixture"), 0600); err != nil {
				t.Fatal(err)
			}
			a, err := decodeAdapter([]byte(cpuAdapterYAML("source")))
			if err != nil {
				t.Fatal(err)
			}
			p, _, err := adapterProfile(a, "work")
			if err != nil {
				t.Fatal(err)
			}
			p.Adapter = adapter
			// Supply per-command input separately: an explicit-key replay skips
			// preparation and must still recover the authored name from these flags.
			itemInputs := map[string]string{"dataset.json": input}
			var files map[string]struct {
				SHA256    string `json:"sha256"`
				SizeBytes int64  `json:"size_bytes"`
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/files/preflight":
					if err := json.NewDecoder(r.Body).Decode(&files); err != nil {
						t.Error(err)
					}
				case "/api/files/input-claims":
					_, _ = io.WriteString(w, `{"id":"fixture-claim"}`)
				default:
					for name, ref := range files {
						if strings.HasSuffix(r.URL.Path, ref.SHA256) {
							if name == "files/dataset.json" || strings.Contains(name, ".myce-inputs/") {
								w.WriteHeader(403)
								_, _ = io.WriteString(w, `{"code":"request_failed","error":"untrusted private detail"}`)
								return
							}
							w.Header().Set("ETag", `"`+ref.SHA256+`"`)
							w.Header().Set("Content-Length", fmt.Sprint(ref.SizeBytes))
							w.WriteHeader(200)
							return
						}
					}
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			for attempt := 0; attempt < 2; attempt++ {
				var out bytes.Buffer
				err = runPreparedProfile(context.Background(), root, p, []byte("fixture"), nil, itemInputs, "named-input", "", "", "", server.URL, "fixture-token", &out, io.Discard)
				if exitCode(err) != 69 || !strings.Contains(err.Error(), `operation=upload input="dataset.json" HTTP 403`) || strings.Contains(err.Error(), input) {
					t.Fatalf("invocation %d: wrong diagnostic: %v", attempt+1, err)
				}
			}
		})
	}
}
