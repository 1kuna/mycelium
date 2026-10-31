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
