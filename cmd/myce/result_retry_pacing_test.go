package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

type resultPacingClock struct{ now time.Time }

func (c *resultPacingClock) Now() time.Time { return c.now }

type resultPacingTransport func(*http.Request) (*http.Response, error)

func (f resultPacingTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestResultRetryPacingCurrentPressure(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		for _, bounded := range []bool{false, true} {
			t.Run(fmt.Sprintf("recovery=%v/deadline=%v", recovery, bounded), func(t *testing.T) {
				clock := &resultPacingClock{now: time.Now()}
				start := clock.Now()
				ctx := context.Background()
				if bounded {
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, start.Add(17*time.Second))
					defer cancel()
				}
				data := []byte("verified accepted result\n")
				hash := sha256.Sum256(data)
				ref := myce.FileRef{SHA256: hex.EncodeToString(hash[:]), SizeBytes: int64(len(data))}
				job, err := json.Marshal(myce.Job{ID: "job", State: "succeeded", OutputRefs: myce.FileManifest{"result.txt": ref}})
				if err != nil {
					t.Fatal(err)
				}
				downloads, pauses := 0, 0
				var notBefore time.Time
				c := myce.Client{URL: "http://fixture.invalid", Clock: clock, Jitter: func() float64 { return .5 }, Pause: func(_ context.Context, delay time.Duration) error {
					pauses++
					want := time.Duration((downloads-1)%13+1) * time.Second * 3 / 2
					if end, ok := ctx.Deadline(); ok {
						want = min(want, max(0, end.Sub(clock.Now())))
					}
					if delay != want {
						t.Fatalf("download retry %d pause=%s want=%s", pauses, delay, want)
					}
					clock.now = clock.now.Add(delay)
					return nil
				}, HTTP: &http.Client{Transport: resultPacingTransport(func(r *http.Request) (*http.Response, error) {
					if clock.Now().Before(notBefore) || bounded && !clock.Now().Before(start.Add(17*time.Second)) {
						t.Fatal("request before server signal or after owner deadline")
					}
					header := make(http.Header)
					body, status := string(job), 200
					if strings.HasPrefix(r.URL.Path, "/api/files/") {
						downloads++
						clock.now = clock.now.Add(8 * time.Millisecond)
						if downloads <= 19 {
							signal := (downloads-1)%13 + 1
							notBefore = clock.Now().Add(time.Duration(signal) * time.Second)
							header.Set("Retry-After", fmt.Sprint(signal))
							status, body = 429, `{"code":"client_rate_limited"}`
						} else {
							body = string(data)
							header.Set("ETag", `"`+ref.SHA256+`"`)
						}
					}
					return &http.Response{StatusCode: status, Header: header, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body))}, nil
				})}}
				out := t.TempDir()
				if recovery {
					err = c.RetryTransient(ctx, func(ctx context.Context, one myce.Client) error {
						return fetchResult(ctx, one, "job", out, 0, "", io.Discard)
					}, nil)
				} else {
					err = c.DownloadResult(ctx, "job", "result.txt", filepath.Join(out, "result.txt"))
				}
				if bounded {
					if !errors.Is(err, context.DeadlineExceeded) || clock.Now().Sub(start) != 17*time.Second {
						t.Fatal("owner deadline lost", err, clock.Now().Sub(start))
					}
					return
				}
				got, readErr := os.ReadFile(filepath.Join(out, "result.txt"))
				if err != nil || readErr != nil || string(got) != string(data) || downloads != 20 || pauses != 19 {
					t.Fatal("accepted result lost", err, readErr, downloads, pauses)
				}
			})
		}
	}
}
