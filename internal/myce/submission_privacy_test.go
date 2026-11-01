package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSubmissionFilesystemCausesStayLocal(t *testing.T) {
	for _, name := range []string{"missing lock_file", "directory lock_file", "files sync"} {
		t.Run(name, func(t *testing.T) {
			q, _ := fixture(t)
			var logs bytes.Buffer
			old := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(old)
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
			var body struct {
				Code, Error    string
				PathDiagnostic *PathDiagnostic `json:"path_diagnostic"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			want := rejectionMessages[code]
			if name != "files sync" {
				if body.PathDiagnostic == nil {
					t.Fatal("missing path diagnostic", response.Body.String())
				}
				want = body.PathDiagnostic.String()
				if body.PathDiagnostic.Side != "queue" || body.PathDiagnostic.Path != s.Environment.LockFile || body.PathDiagnostic.Field != "environment.lock_file" {
					t.Fatal(body.PathDiagnostic)
				}
			}
			if response.Code != http.StatusBadRequest || body.Code != code || body.Error != want {
				t.Errorf("expected fixed %s rejection, got HTTP %d: %s", code, response.Code, response.Body.String())
			}
			if name == "files sync" && strings.Contains(response.Body.String(), privateRoot) {
				t.Error("HTTP response exposed queue-host path")
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			err = (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/jobs", s, nil)
			var apiError *HTTPError
			if !errors.As(err, &apiError) || apiError.Code != code || apiError.Message != want {
				t.Errorf("Go client did not receive fixed %s rejection: %v", code, err)
			}
			if apiError != nil && !reflect.DeepEqual(apiError.PathDiagnostic, body.PathDiagnostic) {
				t.Error("structured client/HTTP mismatch")
			}
			if strings.Contains(logs.String(), privateRoot) {
				t.Error("caller or hidden path leaked into rejection logs")
			}
			// Authentication must precede any path diagnosis.
			req = httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(data))
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != http.StatusUnauthorized || strings.Contains(response.Body.String(), "path_diagnostic") {
				t.Fatal(response.Body.String())
			}
			if name == "files sync" && err != nil && strings.Contains(err.Error(), privateRoot) {
				t.Error("Go client error exposed queue-host path")
			}
		})
	}
}

func TestEnvironmentPathDiagnosticStatusPrivacy(t *testing.T) {
	q, _ := fixture(t)
	m := machine("synthetic-worker", "cpu")
	m.EnvironmentReports = []EnvironmentReport{{
		Key: "tools", State: "failed",
		PathDiagnostic: &PathDiagnostic{
			Field: "environment.binary", Side: "worker", Path: "/synthetic/private/tool",
			ResolutionBase: "worker-environment-library", Cause: "missing",
		},
	}}
	q.s.Machines[m.ID] = &m
	h := API{Queue: q, Token: "test"}.Handler()
	for _, path := range []string{"/api/status", "/api/environments"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer test")
		response := httptest.NewRecorder()
		h.ServeHTTP(response, req)
		if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "/synthetic/private") {
			t.Fatalf("%s exposed worker diagnostic: %s", path, response.Body.String())
		}
	}
	// Redaction must operate on detached status data, preserving local evidence.
	if q.s.Machines[m.ID].EnvironmentReports[0].PathDiagnostic == nil {
		t.Fatal("status redaction changed queue evidence")
	}
}
