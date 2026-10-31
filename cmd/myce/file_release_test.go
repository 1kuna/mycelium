package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mycelium/internal/myce"
)

func TestFileReleaseCommand(t *testing.T) {
	plan := myce.FileReleasePlan{ID: "reviewed", Selection: myce.FileReleaseSelection{Project: "example", Jobs: []string{"done"}}}
	var applied int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("missing authority")
		}
		switch r.URL.Path {
		case "/api/files/release/plan":
			var got myce.FileReleaseSelection
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil || got.Project != "example" || len(got.Jobs) != 1 || got.Jobs[0] != "done" {
				t.Error(got, err)
			}
			json.NewEncoder(w).Encode(plan)
		case "/api/files/release/apply":
			var got myce.FileReleaseApply
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil || !got.AcceptExpiry || got.PlanID != plan.ID || got.Selection.Project != "example" {
				t.Error(got, err)
			}
			applied++
			json.NewEncoder(w).Encode(myce.FileReleaseResult{PlanID: got.PlanID, Collection: "scheduled"})
		default:
			t.Error(r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "fixture")
	var out bytes.Buffer
	if err := fileReleaseCommand(context.Background(), []string{"plan", "--queue", server.URL, "--project", "example", "done"}, &out); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(file, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := fileReleaseCommand(context.Background(), []string{"apply", "--queue", server.URL, file}, &out); err == nil || applied != 0 {
		t.Fatal("missing explicit acceptance")
	}
	out.Reset()
	if err := fileReleaseCommand(context.Background(), []string{"apply", "--queue", server.URL, "--accept-expiry", file}, &out); err != nil {
		t.Fatal(err)
	}
	if applied != 1 || !bytes.Contains(out.Bytes(), []byte("scheduled")) {
		t.Fatal(applied, out.String())
	}
}
