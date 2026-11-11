package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The tools report includes the native PATH in every prepared environment.
// Release installations with a few dozen environments exceed the ordinary
// 1 MiB JSON budget; the old worker masks that 400 as a protocol-upgrade 426.
func TestAttemptProtocolReleaseWorkerJoins(t *testing.T) {
	const queueVersion = "v3.1.0-rc.20261110.s076r6.9"
	previousVersion := BuildVersion
	BuildVersion = queueVersion
	t.Cleanup(func() { BuildVersion = previousVersion })
	q, clock := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}

	tools := make([]string, 2400)
	for i := range tools {
		tools[i] = fmt.Sprintf("native-tool-%04d", i)
	}
	cache := map[string][]string{"": tools}
	var environments []string
	for i := range 40 {
		key := fmt.Sprintf("environment-%02d", i)
		environments = append(environments, key)
		cache[key] = tools
	}
	inventory := preparedToolInventory(cache, environments, "")
	for _, version := range []string{queueVersion, "v3.1.0-rc.20261110.s076r6.8"} {
		t.Run(version, func(t *testing.T) {
			m := machine("release-worker", "fake")
			m.Release = &RunningRelease{Version: version, Commit: "fixture"}
			m.Environments, m.ToolInventory = environments, inventory
			c := Checkin{Machine: m, Fresh: true}
			data, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			if m.ToolInventory != nil && len(data) <= 1<<20 {
				t.Fatal("fixture must reproduce the release's oversized inventory")
			}
			w := Worker{Machine: m, Clock: clock, Client: client}
			reply, err := w.fixtureCheckin(context.Background(), c)
			if err != nil {
				c.AttemptProtocol = keyedAttemptProtocol
				raw := client.Call(context.Background(), "POST", "/api/workers/checkin", c, nil)
				t.Fatalf("worker %s joining queue %s (%d bytes): %v; original queue response: %v", version, queueVersion, len(data), err, raw)
			}
			if reply.AttemptProtocol != keyedAttemptProtocol || len(reply.Attempts) != 0 {
				t.Fatalf("unexpected startup reply: %+v", reply)
			}
		})
	}
	t.Run("oversized inventory remains a size refusal", func(t *testing.T) {
		q.requestBudgets.memory = func(context.Context) (int64, error) { return 32 << 20, nil }
		q.requestBudgets.lanes = 1
		m := machine("oversized-worker", "fake")
		m.ToolInventory = map[string][]string{"": {strings.Repeat("x", 8<<20)}}
		w := Worker{Machine: m, Clock: clock, Client: client}
		_, err := w.fixtureCheckin(context.Background(), Checkin{Machine: m})
		var refusal *HTTPError
		if !errors.As(err, &refusal) || refusal.Status != http.StatusRequestEntityTooLarge || refusal.Code != "request_too_large" {
			t.Fatalf("size refusal masked as protocol incompatibility: %v", err)
		}
	})
}
