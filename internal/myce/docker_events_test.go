package myce

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

// #149: Docker can accept KILL while its CLI reply times out. Its event must
// survive independently of the successful-delivery receipt and daemon history.
func TestDockerRetainsEventsWhenKillReplyTimesOut(t *testing.T) {
	w, store, clock, root := dockerFixture(t)
	t.Setenv("MYCE_TEST_DOCKER_EVENTS", "1")
	if err := syscall.Mkfifo(filepath.Join(root, "events-sent"), 0600); err != nil {
		t.Fatal(err)
	}
	j := dockerJob()
	j.Spec.Logs = "none"
	j.Checkpoint = t.TempDir()
	p, err := startDockerFixture(w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	input := dockerEventsInput(clock.Now())
	if err := os.WriteFile(filepath.Join(root, "events-pending"), []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "events-pending"), filepath.Join(root, "events-input")); err != nil {
		t.Fatal(err)
	}
	// The producer must finish emitting before attempt shutdown kills its CLI.
	// A FIFO provides a completion handshake without a timing assumption.
	if _, err := os.ReadFile(filepath.Join(root, "events-sent")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte("kill-timeout"), 0600); err != nil {
		t.Fatal(err)
	}
	p.stop(clock.Now(), 0, true)
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
	var archive dockerEventArchive
	if err := json.Unmarshal(data, &archive); err != nil || len(archive.Events) != 256 || archive.Dropped != 4 {
		t.Fatalf("bounded event archive: events=%d dropped=%d rejected=%d status=%s cause=%v err=%v", len(archive.Events), archive.Dropped, archive.Rejected, archive.Status, archive.Cause, err)
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

func dockerEventsInput(at time.Time) string {
	var input strings.Builder
	for i := 0; i < 260; i++ {
		// The stream contains private labels; only lifecycle facts may persist.
		event := map[string]any{"Type": "container", "Action": "start", "timeNano": at.UnixNano() + int64(i), "Actor": map[string]any{"ID": strings.Repeat("a", 64), "Attributes": map[string]string{"myce.attempt": "ATTEMPT", "private": "DO-NOT-RETAIN"}}}
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
	return input.String()
}

// #150: disk persistence must not backpressure event intake. No wall clock
// or host load is needed: hold the first save until every event has been read.
func TestDockerEventsSlowPersistence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader, producer := io.Pipe()
		defer reader.Close()
		defer producer.Close()
		w := &dockerEventWriter{identity: "ATTEMPT"}
		saving, releaseSave := make(chan struct{}), make(chan struct{})
		readDone, produced := make(chan struct{}), make(chan struct{})
		finished := make(chan error, 1)
		var persisted dockerEventArchive
		go func() {
			finished <- w.record(reader, readDone, func(archive dockerEventArchive) error {
				if persisted.Events == nil {
					close(saving)
					<-releaseSave
				}
				persisted = archive
				return nil
			})
		}()
		go func() {
			input := dockerEventsInput(time.Unix(1, 0))
			first := strings.IndexByte(input, '\n') + 1
			_, err := io.WriteString(producer, input[:first])
			<-saving
			if err == nil {
				_, err = io.WriteString(producer, input[first:])
			}
			_ = producer.CloseWithError(err)
			close(produced)
		}()
		<-saving
		synctest.Wait()
		select {
		case <-produced:
		default:
			t.Error("slow archive persistence blocked event intake; shutdown can kill unwritten events")
		}
		select {
		case <-readDone:
		default:
			t.Error("slow archive persistence blocked stream drain")
		}
		close(releaseSave)
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if len(persisted.Events) != 256 || persisted.Dropped != 4 {
			t.Fatalf("bounded event archive: events=%d dropped=%d", len(persisted.Events), persisted.Dropped)
		}
		if persisted.Events[254].Action != "kill" || persisted.Events[255].Action != "die" {
			t.Fatal("lost terminal events during slow persistence")
		}
	})
}
