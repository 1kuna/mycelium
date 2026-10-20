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
	"strings"
	"testing"
)

func TestJobSpecInvalidHTTPAndQueueLog(t *testing.T) {
	for _, tc := range []struct {
		name, code, reason string
		prepare            func(*testing.T, *Queue, *Spec)
	}{
		{"logs", "job_logs_invalid", "logs must be keep or none", func(_ *testing.T, _ *Queue, s *Spec) { s.Logs = "bad" }},
		{"memory", "job_memory_invalid", "needs.memory_gb must be positive: CPU-only work stays outside Mycelium", func(_ *testing.T, _ *Queue, s *Spec) { s.Needs.MemoryGB = 0 }},
		{"docker", "job_docker_invalid", "main Docker memory must fit needs.host_memory_gb", func(_ *testing.T, _ *Queue, s *Spec) {
			s.Needs.HostMemoryGB = 1
			s.Run.Docker, s.Run.DockerNetwork, s.Run.DockerMemoryGB = true, "private", 2
		}},
		{"fields", "job_fields_required", "name, project and run.command are required", func(_ *testing.T, _ *Queue, s *Spec) { s.Name = "" }},
		{"deadline", "job_deadline_invalid", "deadline priority requires a deadline", func(_ *testing.T, _ *Queue, s *Spec) { s.Priority = "deadline" }},
		{"project", "job_project_invalid", "project cannot contain slash", func(_ *testing.T, _ *Queue, s *Spec) { s.Project = "bad/project" }},
		{"class", "job_class_invalid", "class cannot contain slash", func(_ *testing.T, _ *Queue, s *Spec) { s.Class = "bad/class" }},
		{"execution", "job_execution_invalid", "choose run or execution, not both", func(_ *testing.T, _ *Queue, s *Spec) { s.Execution = &ExecutionSpec{} }},
		{"placement", "job_placement_invalid", "project test allowed_machines has no overlap with job allowed_machines", func(t *testing.T, q *Queue, s *Spec) {
			if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{"allowed"}}}); err != nil {
				t.Fatal(err)
			}
			s.AllowedMachines = []string{"other"}
		}},
		{"capacity", "job_capacity_invalid", "no compatible machine in project test allowed_machines (last-known platform, capacity and environment)", func(t *testing.T, q *Queue, s *Spec) {
			check(t, q, machine("allowed", "fake"), nil)
			if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{"allowed"}}}); err != nil {
				t.Fatal(err)
			}
			s.Needs.Platform = Platform{Required: []string{"cuda"}}
		}},
		{"environment", "job_environment_invalid", `unknown environment "missing"`, func(_ *testing.T, q *Queue, s *Spec) {
			q.environments.Environments = map[string]map[string]EnvironmentRecipe{"known": {}}
			s.Environment.Name = "missing"
		}},
		{"sidecar environment", "job_environment_invalid", "sidecar helper requires a registered environment recipe", func(_ *testing.T, _ *Queue, s *Spec) {
			s.Needs.HostMemoryGB = 2
			s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"echo"}, MemoryGB: 1, Environment: "missing"}}
		}},
		{"extension", "job_program_invalid", "extension reference must resolve to one registered version; use name@version or extension_digest", func(_ *testing.T, _ *Queue, s *Spec) { s.Extension = "missing@1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			s := spec("experiment")
			tc.prepare(t, q, &s)
			s.Run.Env = map[string]string{"PRIVATE": "env-secret"}
			var logs bytes.Buffer
			old := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(old)
			// A direct queue rejection must log too, without depending on HTTP.
			_, err := q.Submit(s)
			var typed *HTTPError
			if !errors.As(err, &typed) || typed.Code != tc.code || typed.Message != tc.reason {
				t.Errorf("queue error: %v", err)
			}
			if !strings.Contains(logs.String(), "submission rejected code="+tc.code) || !strings.Contains(logs.String(), fmt.Sprintf("reason=%q", tc.reason)) || strings.Contains(logs.String(), "env-secret") {
				t.Errorf("queue log missing safe reason: %q", logs.String())
			}
			b, _ := json.Marshal(s)
			r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(b))
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
			var body struct{ Code, Error string }
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusBadRequest || body.Code != tc.code || body.Error != tc.reason {
				t.Errorf("response: %d %s", w.Code, w.Body.String())
			}
			validateWireValue(t, compileWireSchema(t, "error-v1.schema.json"), map[string]string{"code": body.Code, "error": body.Error})
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			err = (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/jobs", s, nil)
			if !errors.As(err, &typed) || typed.Code != tc.code || typed.Message != tc.reason {
				t.Fatalf("client lost reason: %v", err)
			}
		})
	}
}

func TestJobSpecInternalErrorStaysGeneric(t *testing.T) {
	for _, err := range []error{errors.New("private IO failure"), &HTTPError{Status: 500, Code: "internal", Message: "private state failure"}} {
		w := httptest.NewRecorder()
		respond(w, nil, err)
		var body struct{ Code, Error string }
		if e := json.Unmarshal(w.Body.Bytes(), &body); e != nil {
			t.Fatal(e)
		}
		if body.Error != "request failed" || body.Code != "request_failed" {
			t.Fatal(w.Body.String())
		}
	}
}
