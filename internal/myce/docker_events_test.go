package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #149: Docker can accept KILL while its CLI reply times out. Its event must
// survive independently of the successful-delivery receipt and daemon history.
func TestDockerRetainsEventsWhenKillReplyTimesOut(t *testing.T) {
	w, store, clock, root := dockerFixture(t)
	t.Setenv("MYCE_TEST_DOCKER_EVENTS", "1")
	j := dockerJob()
	j.Spec.Logs = "none"
	j.Checkpoint = t.TempDir()
	p, err := startDockerFixture(w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte("kill-timeout"), 0600); err != nil {
		t.Fatal(err)
	}
	p.stop(clock.Now(), 0, true)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "kill-sent")); return e == nil })
	var input strings.Builder
	for i := 0; i < 260; i++ {
		// The stream contains private labels; only lifecycle facts may persist.
		event := map[string]any{"Type": "container", "Action": "start", "timeNano": time.Now().UnixNano() + int64(i), "Actor": map[string]any{"ID": strings.Repeat("a", 64), "Attributes": map[string]string{"myce.attempt": "ATTEMPT", "private": "DO-NOT-RETAIN"}}}
		if i == 258 {
			event["Action"] = "kill"
			event["Actor"].(map[string]any)["Attributes"].(map[string]string)["signal"] = "9"
		}
		if i == 259 {
			event["Action"] = "die"
			event["Actor"].(map[string]any)["Attributes"].(map[string]string)["exitCode"] = "137"
		}
		data, _ := json.Marshal(event)
		input.Write(data)
		input.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(root, "events-input"), []byte(input.String()), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-p.done:
		if f.err != nil || f.result.CleanupCause == nil || f.result.CleanupCause.Operation != "kill" || f.result.CleanupCause.Class != "timeout" || f.result.Interrupted {
			t.Fatalf("event observation must not invent successful Mycelium delivery: %+v", f)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("attempt did not finish")
	}
	path := filepath.Join(store.dir, store.state.Identity+".docker-events.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lost Docker kill/exit events after timed-out reply: %v", err)
	}
	var archive struct {
		Events []struct {
			Action   string    `json:"action"`
			At       time.Time `json:"at"`
			Signal   string    `json:"signal"`
			ExitCode *int      `json:"exit_code"`
		}
		Dropped int `json:"dropped"`
	}
	if err := json.Unmarshal(data, &archive); err != nil || len(archive.Events) != 256 || archive.Dropped != 4 {
		t.Fatalf("bounded event archive: events=%d dropped=%d err=%v", len(archive.Events), archive.Dropped, err)
	}
	kill, exit := archive.Events[254], archive.Events[255]
	if kill.Action != "kill" || kill.Signal != "9" || kill.At.IsZero() || exit.Action != "die" || exit.ExitCode == nil || *exit.ExitCode != 137 {
		t.Fatalf("missing terminal timing: kill=%+v exit=%+v", kill, exit)
	}
	st, _ := os.Stat(path)
	if st.Mode().Perm() != 0600 || len(data) > 128*1024 || strings.Contains(string(data), "DO-NOT-RETAIN") {
		t.Fatal("event retention exposed labels or exceeded its bound")
	}
}
