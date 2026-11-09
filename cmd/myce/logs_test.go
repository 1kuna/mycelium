package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

// Exercise the same Client.Call used by installed status/log reads: the server
// controls the retry interval and the caller controls when recovery stops.
func TestLogsShedRetryAndResume(t *testing.T) {
	var pageReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/job" {
			fmt.Fprint(w, `{"id":"job","state":"succeeded"}`)
			return
		}
		after := r.URL.Query().Get("after")
		if after == "0" {
			chunks := make([]myce.Log, 256)
			for i := range chunks {
				chunks[i] = myce.Log{Seq: int64(i + 1), Text: "prefix\n"}
			}
			_ = json.NewEncoder(w).Encode(chunks)
			return
		}
		if after != "256" {
			t.Errorf("lost cursor: %s", after)
		}
		read := pageReads.Add(1)
		if read == 1 {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(429)
			fmt.Fprint(w, `{"code":"client_rate_limited","error":"fleet-wide control pressure"}`)
			return
		}
		fmt.Fprint(w, `[{"seq":257,"text":"tail\n"}]`)
	}))
	defer server.Close()
	for _, interrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(interrupt), func(t *testing.T) {
			pageReads.Store(0)
			pauses := 0
			client := myce.Client{URL: server.URL, Token: "fixture", Jitter: func() float64 { return .5 }, Pause: func(_ context.Context, d time.Duration) error {
				pauses++
				if d < 10500*time.Millisecond {
					t.Errorf("retry before Retry-After plus jitter: %s", d)
				}
				if interrupt {
					return context.Canceled
				}
				return nil
			}}
			var out bytes.Buffer
			err := logsAfter(context.Background(), client, "job", false, 0, &out)
			if interrupt {
				if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "retry after 7s") || !strings.Contains(err.Error(), "myce logs job --after 256") {
					t.Fatalf("missing retry interval/cursor: %v", err)
				}
				// Run the supported command parser with the printed continuation.
				t.Setenv("MYCE_TOKEN", "fixture")
				resumed := captureJoiningOutput(t, func() error { return run([]string{"logs", "job", "--queue", server.URL, "--after", "256"}) })
				if resumed != "tail\n" {
					t.Fatalf("replayed prefix: %q", resumed)
				}
			} else if err != nil || out.String() != strings.Repeat("prefix\n", 256)+"tail\n" {
				t.Fatalf("retry lost output: %v %q", err, out.String())
			}
			if pauses != 1 {
				t.Fatalf("pauses=%d", pauses)
			}
		})
	}
}
