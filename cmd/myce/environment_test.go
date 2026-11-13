package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mycelium/internal/myce"
)

func TestEnvironmentCLIReadback(t *testing.T) {
	entry := myce.EnvironmentEntry{Name: "tools", Platforms: []myce.EnvironmentPlatform{{Platform: "cpu", Backend: "uv", Key: "key", InputDigests: map[string]string{"requirements.txt": "digest"}}}}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("unexpected request: %s", r.Method)
		}
		switch r.URL.Path {
		case "/api/environments":
			json.NewEncoder(w).Encode([]myce.EnvironmentEntry{entry})
		case "/api/environments/tools":
			json.NewEncoder(w).Encode(entry)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	for _, command := range []string{"list", "get"} {
		args := []string{command, "--queue", server.URL, "--token", "fixture"}
		if command == "get" {
			args = append(args, "tools")
		}
		var out bytes.Buffer
		if err := environmentCommand(context.Background(), args, &out); err != nil {
			t.Fatal(err)
		}
		if !json.Valid(out.Bytes()) || !bytes.Contains(out.Bytes(), []byte("digest")) {
			t.Fatal(out.String())
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
	for _, args := range [][]string{{}, {"get"}, {"list", "extra"}, {"remove", "tools"}} {
		if err := environmentCommand(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatal("invalid arguments accepted", args)
		}
	}
}

func TestEnvironmentCLIRetry(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/api/environments/recipe-hash/retry" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("wrong retry request: %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Worker string               `json:"worker"`
			Target myce.ExecutionTarget `json:"target"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Worker != "worker" || body.Target != (myce.ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}) {
			t.Errorf("wrong retry scope: %+v", body)
		}
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := environmentCommand(context.Background(), []string{"retry", "--queue", server.URL, "--token", "fixture", "worker", "recipe-hash", "linux", "amd64", "cpu"}, &out); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !bytes.Contains(out.Bytes(), []byte(`"ok":true`)) {
		t.Fatal(calls, out.String())
	}
}
