package myce

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQueueUpgradeRetiresLegacyDefaultThrottle(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("QueueUpgradePreservesExplicitCPUSettings", func(t *testing.T) {
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

	})
}

// Model the measured ~50 ms CPU cost of a serialized durable transaction.
// A fake store queues twelve transactions, and synctest advances its CPU budget
// without wall-clock sleeps or real hardware. Native cgroup reproduction is
// separate; this regression binds installer defaults to the unchanged RPC gate.

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
