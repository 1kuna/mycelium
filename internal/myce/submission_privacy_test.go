package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSubmissionFilesystemCausesStayLocal(t *testing.T) {
	for _, name := range []string{"missing lock_file", "directory lock_file", "files sync"} {
		t.Run(name, func(t *testing.T) {
			q, _ := fixture(t)
			s := spec("experiment")
			code := "job_environment_invalid"
			privateRoot := t.TempDir()
			if name == "files sync" {
				s = submitVerificationInput(t, q)
				code = "job_files_invalid"
				privateRoot = q.files.root
				q.files.syncDir = func(path string) error {
					return fmt.Errorf("sync input custody: %w", &os.PathError{Op: "fsync", Path: path, Err: os.ErrPermission})
				}
			} else {
				lock := filepath.Join(privateRoot, "uv.lock")
				if name == "directory lock_file" {
					if err := os.Mkdir(lock, 0700); err != nil {
						t.Fatal(err)
					}
				}
				s.Environment = Environment{LockFile: lock, Python: "3.12"}
			}
			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(data))
			req.Header.Set("Authorization", "Bearer test")
			response := httptest.NewRecorder()
			handler := API{Queue: q, Token: "test"}.Handler()
			handler.ServeHTTP(response, req)
			var body struct{ Code, Error string }
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if response.Code != http.StatusBadRequest || body.Code != code || body.Error != rejectionMessages[code] {
				t.Errorf("expected fixed %s rejection, got HTTP %d: %s", code, response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), privateRoot) {
				t.Error("HTTP response exposed queue-host path")
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			err = (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/jobs", s, nil)
			var apiError *HTTPError
			if !errors.As(err, &apiError) || apiError.Code != code || apiError.Message != rejectionMessages[code] {
				t.Errorf("Go client did not receive fixed %s rejection: %v", code, err)
			}
			if err != nil && strings.Contains(err.Error(), privateRoot) {
				t.Error("Go client error exposed queue-host path")
			}
		})
	}
}
