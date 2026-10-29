package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestJobLedgerCLI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/job/ledger" || r.URL.Query().Get("after") != "2" || r.URL.Query().Get("high_water") != "5" || r.URL.Query().Get("limit") != "1" {
			t.Errorf("unexpected request %s", r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer synthetic" {
			t.Error("missing auth")
		}
		io.WriteString(w, `{"job_id":"job","complete":true,"high_water":5,"next":3,"has_more":true,"entries":[{"sequence":3}]}`)
	}))
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "synthetic")
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = old; reader.Close(); writer.Close() })
	err = run([]string{"ledger", "job", "--after", "2", "--high-water", "5", "--limit", "1"})
	writer.Close()
	os.Stdout = old
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		JobID   string `json:"job_id"`
		Next    int    `json:"next"`
		HasMore bool   `json:"has_more"`
	}
	if err = json.NewDecoder(reader).Decode(&page); err != nil || page.JobID != "job" || page.Next != 3 || !page.HasMore {
		t.Fatal("bad ledger output", page, err)
	}
}
