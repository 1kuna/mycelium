package myce

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestQueueControlPlaneServicePolicy(t *testing.T) {
	for _, target := range []Detection{{OS: "linux", Arch: "amd64"}, {OS: "linux", Arch: "arm64"}, {OS: "darwin", Arch: "arm64"}} {
		t.Run(target.OS+"/"+target.Arch, func(t *testing.T) {
			home := t.TempDir()
			c, err := PlanInstall("queue", home, "", home, "http://queue.invalid", "queue", target)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.ValidateInstall(); err != nil {
				t.Fatal(err)
			}
			if c.Nice != 0 || c.CPUQuota != "" {
				t.Fatalf("control plane inherits execution throttle: nice=%d quota=%q", c.Nice, c.CPUQuota)
			}
			// Owner-use policy is for execution; it must not throttle the queue.
			c.DailyUse, c.Laptop = true, true
			text := c.ServiceText()
			for _, forbidden := range []string{"GOMAXPROCS=", "Nice=10", "CPUQuota=25%", "LowPriorityIO", "Background"} {
				if strings.Contains(text, forbidden) {
					t.Errorf("queue service contains %q", forbidden)
				}
			}
			if target.OS == "linux" && (!strings.Contains(text, "Nice=0\n") || !strings.Contains(text, "CPUQuota=\n")) {
				t.Fatal("queue does not reset default CPU limits")
			}
		})
	}
}

func TestQueueUpgradeRetiresLegacyDefaultThrottle(t *testing.T) {
	for _, target := range []Detection{{OS: "linux", Arch: "amd64"}, {OS: "linux", Arch: "arm64"}, {OS: "darwin", Arch: "arm64"}} {
		for _, role := range []string{"queue", "worker"} {
			t.Run(target.OS+"/"+target.Arch+"/"+role, func(t *testing.T) {
				home := t.TempDir()
				target.Platform, target.MemoryMode, target.MemoryGB = "fake", "fixed", 8
				c, err := PlanInstall(role, home, "", home, "http://queue.invalid", "fixture", target)
				if err != nil {
					t.Fatal(err)
				}
				c.Nice, c.CPUQuota = 10, "25%" // Saved pre-fix defaults, including upgrades from another architecture.
				if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(c)
				if err = os.WriteFile(c.Config, raw, 0600); err != nil {
					t.Fatal(err)
				}
				got, err := ReadServiceConfig(c.Config)
				if err != nil {
					t.Fatal(err)
				}
				if role == "queue" {
					if got.Nice != 0 || got.CPUQuota != "" {
						t.Fatal("saved upgrade reapplies legacy queue throttle")
					}
				} else if got.Nice != 10 || got.CPUQuota != "25%" {
					t.Fatal("worker configuration changed")
				}
				// A second read of the rewritten config is idempotent.
				raw, _ = json.Marshal(got)
				os.WriteFile(c.Config, raw, 0600)
				again, err := ReadServiceConfig(c.Config)
				if err != nil || again.Nice != got.Nice || again.CPUQuota != got.CPUQuota {
					t.Fatal("non-idempotent migration", err)
				}
			})
		}
	}
}

func TestQueueUpgradePreservesExplicitCPUSettings(t *testing.T) {
	for _, v := range []struct {
		nice  int
		quota string
	}{{0, "45%"}, {5, "25%"}, {10, "150%"}} {
		home := t.TempDir()
		c, err := PlanInstall("queue", home, "", home, "http://queue.invalid", "queue", Detection{OS: "linux", Arch: "amd64"})
		if err != nil {
			t.Fatal(err)
		}
		c.Nice, c.CPUQuota = v.nice, v.quota
		os.MkdirAll(filepath.Dir(c.Config), 0700)
		raw, _ := json.Marshal(c)
		os.WriteFile(c.Config, raw, 0600)
		got, err := ReadServiceConfig(c.Config)
		if err != nil || got.Nice != v.nice || got.CPUQuota != v.quota {
			t.Fatal("explicit operator CPU setting changed", err)
		}
	}
}

func TestManualQueueTemplatesDoNotThrottleControlPlane(t *testing.T) {
	for _, name := range []string{"myce-queue.service", "local.myce.queue.plist"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "deploy", name))
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range []string{"GOMAXPROCS", "CPUQuota=25%", "Nice=10", "LowPriorityIO", "Background"} {
			if strings.Contains(string(data), s) {
				t.Errorf("%s contains %s", name, s)
			}
		}
	}
}

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
