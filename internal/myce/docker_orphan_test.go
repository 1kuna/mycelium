package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDockerOrphanStopProtocol(t *testing.T) {
	for _, mode := range []string{"term", "refuse-kill", "delivered-error"} {
		t.Run(mode, func(t *testing.T) {
			_, s, clock, root := dockerFixture(t)
			for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "mode": mode} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			endpoint := "fake-" + ID()
			newOrphan := func() *dockerAttempt {
				d := &dockerAttempt{binary: filepath.Join(root, "docker"), clock: clock, timeout: time.Second, env: withoutToken(os.Environ()), state: dockerExecution{Identity: "owned", Endpoint: endpoint, Container: "id1"}}
				d.stop(clock.Now(), 5*time.Second, false)
				return d
			}
			d := newOrphan()
			// Reconstructing the cleanup object in the same process must not
			// reissue stop, even after uncertain delivery or a longer new grace.
			for i := 0; i < 3; i++ {
				_, _ = d.reconcile()
				if mode == "term" {
					// Even a restart of the same exact container must not cause
					// another graceful signal during this process lifetime.
					if err := os.WriteFile(filepath.Join(root, "running"), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				d = newOrphan()
			}

			calls := func() string {
				b, err := os.ReadFile(filepath.Join(root, "calls"))
				if err != nil {
					t.Fatal(err)
				}
				return string(b)
			}
			if strings.Count(calls(), "kill --signal=TERM id1\n") != 1 || strings.Contains(calls(), "--signal=KILL") {
				t.Fatalf("before grace: %s", calls())
			}
			clock.advance(5*time.Second - time.Nanosecond)
			_, _ = d.reconcile()
			if strings.Contains(calls(), "--signal=KILL") {
				t.Fatal("premature KILL")
			}
			clock.advance(time.Nanosecond)
			if mode != "term" {
				if err := os.WriteFile(filepath.Join(root, "mode"), []byte("refuse-kill"), 0600); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if _, err := d.reconcile(); err == nil {
						t.Fatal("missing KILL error")
					}
				}
				if strings.Count(calls(), "kill --signal=KILL id1\n") != 2 {
					t.Fatalf("KILL not retried: %s", calls())
				}
				if err := os.WriteFile(filepath.Join(root, "mode"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 3 && !d.state.Released; i++ {
				if _, err := d.reconcile(); err != nil {
					t.Fatal(err)
				}
			}
			if !d.state.Released || d.state.EvidenceSaved {
				t.Fatalf("untruthful orphan state: %+v", d.state)
			}
			if strings.Count(calls(), "kill --signal=TERM id1\n") != 1 || strings.Contains(calls(), "stop --time ") || strings.Contains(calls(), "logs ") {
				t.Fatalf("orphan signal/evidence calls: %s", calls())
			}
			for _, suffix := range []string{".docker.json", ".docker-evidence.json"} {
				if _, err := os.Stat(filepath.Join(s.dir, "owned"+suffix)); !os.IsNotExist(err) {
					t.Fatal("invented orphan attempt/evidence record", err)
				}
			}
		})
	}
}

func TestDockerEvidenceWithoutStore(t *testing.T) {
	d := &dockerAttempt{state: dockerExecution{Identity: "orphan"}}
	if err := d.retainEvidence(nil); err != nil {
		t.Fatal(err)
	}
	if d.state.EvidenceSaved {
		t.Fatal("evidence saved without a durable write")
	}
}
