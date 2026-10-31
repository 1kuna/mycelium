package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// Following one job must use neither status nor unrelated retained history.
// A prefix resolves through job GET; the final log is published with completion.
func TestLogsFollowUsesOnlyJobReadsAndDrainsFinalLogs(t *testing.T) {
	for _, state := range []string{"succeeded", "failed", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			reads, logsReads := 0, 0
			const id = "1234567890abcdef"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/jobs/12345678", "/api/jobs/" + id:
					reads++
					got := myce.Job{ID: id, State: "running"}
					if reads > 1 {
						got.State = state
					}
					_ = json.NewEncoder(w).Encode(got)
				case "/api/jobs/" + id + "/logs":
					logsReads++
					if reads > 1 {
						if r.URL.Query().Get("after") != "1" {
							t.Error("lost log cursor")
						}
						fmt.Fprint(w, `[{"seq":2,"attempt":1,"text":"final\n"}]`)
					} else {
						fmt.Fprint(w, `[{"seq":1,"attempt":1,"text":"first\n"}]`)
					}
				default:
					t.Error("follow fetched fleet history", r.URL.Path)
					http.Error(w, "unexpected fleet read", 500)
				}
			}))
			defer server.Close()
			out := captureJoiningOutput(t, func() error {
				return logs(context.Background(), myce.Client{URL: server.URL, Token: "test"}, "12345678", true)
			})
			if reads != 2 || logsReads != 2 || !strings.Contains(out, "first\nfinal\n") {
				t.Fatalf("reads=%d logs=%d output=%q", reads, logsReads, out)
			}
		})
	}
}
