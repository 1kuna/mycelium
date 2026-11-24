package myce

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Model the measured ~50 ms CPU cost of a serialized durable transaction.
// A fake store queues twelve transactions, and synctest advances its CPU budget
// without wall-clock sleeps or real hardware. Native cgroup reproduction is
// separate; this regression binds installer defaults to the unchanged RPC gate.
func TestQueueDefaultProvisioningAllowsAuthorizationBurst(t *testing.T) {
	for _, path := range []string{"/api/workers/launch", "/api/workers/started"} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				home := t.TempDir()
				c, err := PlanInstall("queue", home, "", home, "http://queue.invalid", "queue", Detection{OS: "linux", Arch: "amd64"})
				if err != nil {
					t.Fatal(err)
				}
				cores := 1.0 // Conservatively model one CPU when the service has no quota.
				if c.CPUQuota != "" {
					v, err := strconv.ParseFloat(strings.TrimSuffix(c.CPUQuota, "%"), 64)
					if err != nil {
						t.Fatal(err)
					}
					cores = v / 100
				}
				store := &queuedControlStore{work: make(chan chan struct{}, 12), cost: time.Duration(float64(50*time.Millisecond) / cores)}
				done := make(chan struct{})
				exited := make(chan struct{})
				defer func() { close(done); <-exited }()
				go func() {
					defer close(exited)
					for {
						select {
						case reply := <-store.work:
							select {
							case <-time.After(store.cost):
								close(reply)
							case <-done:
								return
							}
						case <-done:
							return
						}
					}
				}()
				// Eleven prior heartbeat/status/submission transactions own their turn.
				for i := 0; i < 11; i++ {
					store.work <- make(chan struct{})
				}
				worker := Worker{Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: store}}}
				if path == "/api/workers/launch" {
					err = worker.authorizeLaunch(HostLaunch{})
				} else {
					err = worker.authorizeStart(Job{}, "session")
				}
				if err != nil {
					t.Fatalf("default queue provisioning starves authorization behind control burst: %v", err)
				}
			})
		})
	}
}

type queuedControlStore struct {
	work chan chan struct{}
	cost time.Duration
}

func (s *queuedControlStore) RoundTrip(r *http.Request) (*http.Response, error) {
	done := make(chan struct{})
	select {
	case s.work <- done:
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
	select {
	case <-done:
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"ok":true,"start_gate":%q}`, startGateProtocol))), Header: make(http.Header)}, nil
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
}
