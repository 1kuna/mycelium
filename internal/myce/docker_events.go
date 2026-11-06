package myce

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// Diagnostic observations never supply successful Mycelium signal receipts.
type dockerLifecycleEvent struct {
	Container string    `json:"container"`
	Action    string    `json:"action"`
	At        time.Time `json:"at"`
	Signal    string    `json:"signal,omitempty"`
	ExitCode  *int      `json:"exit_code,omitempty"`
}
type dockerEventArchive struct {
	Events    []dockerLifecycleEvent `json:"events"`
	Dropped   int                    `json:"dropped"`
	Rejected  int                    `json:"rejected"`
	Streams   int                    `json:"streams"`
	StartedAt time.Time              `json:"stream_started_at"`
	EndedAt   *time.Time             `json:"stream_ended_at,omitempty"`
	Status    string                 `json:"status"`
	Cause     *RuntimeCause          `json:"cause,omitempty"`
}
type dockerEventWriter struct {
	archive       dockerEventArchive
	identity, dir string
	capacity      int64
}

func (w *dockerEventWriter) save() error {
	return saveWorkerJSON(w.dir, w.identity+".docker-events.json", w.archive)
}
func (w *dockerEventWriter) event(line []byte) error {
	var raw struct {
		Type     string
		Action   string
		TimeNano int64 `json:"timeNano"`
		Actor    struct {
			ID         string
			Attributes map[string]string
		}
	}
	if json.Unmarshal(line, &raw) != nil {
		w.archive.Rejected++
		return nil
	}
	if raw.Type != "container" || raw.Actor.Attributes["myce.attempt"] != w.identity {
		w.archive.Rejected++
		return nil
	}
	switch raw.Action {
	case "create", "start", "kill", "die", "oom", "stop", "destroy":
	default:
		w.archive.Rejected++
		return nil
	}
	if _, err := hex.DecodeString(raw.Actor.ID); err != nil || len(raw.Actor.ID) != 64 || raw.TimeNano <= 0 {
		w.archive.Rejected++
		return nil
	}
	e := dockerLifecycleEvent{Container: raw.Actor.ID, Action: raw.Action, At: time.Unix(0, raw.TimeNano).UTC()}
	if signal, err := strconv.Atoi(raw.Actor.Attributes["signal"]); err == nil && signal > 0 && signal <= 64 {
		e.Signal = strconv.Itoa(signal)
	}
	if code, err := strconv.Atoi(raw.Actor.Attributes["exitCode"]); err == nil && code >= 0 && code <= 255 {
		e.ExitCode = &code
	}
	// Accepted events are unique attempt evidence. Exhaustion stops optional
	// capture, never discards earlier custody diagnostics to make room.
	if w.capacity > 0 && int64(len(w.archive.Events)+1)*int64(unsafe.Sizeof(e)+64) > w.capacity {
		return errors.New("Docker event evidence capacity exhausted")
	}
	w.archive.Events = append(w.archive.Events, e)
	return nil
}

// record keeps intake independent of disk latency. Notifications coalesce, not
// events: every accepted record enters the bounded archive or increments Dropped.
// Only the saver writes files; it joins intake and persists the final snapshot.
func (w *dockerEventWriter) record(stdout io.ReadCloser, readDone chan<- struct{}, save func(dockerEventArchive) error) error {
	var mu sync.Mutex
	dirty := make(chan struct{}, 1)
	finished := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		limit := w.capacity
		if limit <= 0 {
			limit = diagnosticCapacityBytes(w.dir)
		}
		scanner.Buffer(make([]byte, min(4096, limit)), int(max(1, limit)))
		var captureErr error
		for scanner.Scan() {
			mu.Lock()
			captureErr = w.event(scanner.Bytes())
			w.archive.Status = "recording"
			mu.Unlock()
			if captureErr != nil {
				break
			}
			select {
			case dirty <- struct{}{}:
			default:
			}
		}
		close(readDone)
		finished <- errors.Join(captureErr, scanner.Err())
	}()
	persist := func() error {
		mu.Lock()
		archive := w.archive
		archive.Events = slices.Clone(archive.Events)
		mu.Unlock()
		return save(archive)
	}
	for {
		select {
		case err := <-finished:
			return errors.Join(err, persist())
		case <-dirty:
			if err := persist(); err != nil {
				_ = stdout.Close()
				return errors.Join(err, <-finished)
			}
		}
	}
}

// One standard CLI subscription per attempt, independent of daemon control
// calls, heartbeat and custody locks. Only its own process group is cancelled.
func (d *dockerAttempt) observeDockerEvents() func() {
	d.mu.Lock()
	w := &dockerEventWriter{identity: d.state.Identity, dir: d.dir}
	capacity, releaseBudget := reserveDiagnosticBytes(w.dir, ^uint64(0))
	w.capacity = capacity
	if w.capacity == 0 {
		releaseBudget()
		d.mu.Unlock()
		log.Print("Docker event capacity unavailable; capture withheld")
		return func() {}
	}
	endpoint := d.state.Endpoint
	d.mu.Unlock()
	path := filepath.Join(d.dir, w.identity+".docker-events.json")
	if f, err := os.Open(path); err == nil {
		data, readErr := io.ReadAll(io.LimitReader(f, w.capacity+1))
		f.Close()
		if readErr != nil || int64(len(data)) > w.capacity || json.Unmarshal(data, &w.archive) != nil {
			// Preserve damaged evidence rather than overwriting it on recovery.
			releaseBudget()
			log.Print("Docker events archive unreadable; capture unavailable")
			return func() {}
		}
	} else if !os.IsNotExist(err) {
		releaseBudget()
		log.Print("Docker events archive unavailable")
		return func() {}
	}
	now := time.Now().UTC()
	w.archive.Streams++
	w.archive.StartedAt = now
	w.archive.EndedAt = nil
	w.archive.Status = "starting"
	w.archive.Cause = nil
	ctx, cancel := context.WithCancel(context.Background())
	args := []string{"--host", endpoint, "events", "--since", strconv.FormatInt(now.Unix()-1, 10), "--filter", "type=container", "--filter", "label=myce.attempt=" + w.identity, "--format", "{{json .}}"}
	for _, action := range []string{"create", "start", "kill", "die", "oom", "stop", "destroy"} {
		args = append(args, "--filter", "event="+action)
	}
	cmd := exec.CommandContext(ctx, d.binary, args...)
	cmd.Env = d.env
	cmd.Stderr = io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer releaseBudget()
		err := w.save()
		if err == nil {
			var stdout io.ReadCloser
			stdout, err = cmd.StdoutPipe()
			if err == nil {
				err = cmd.Start()
				if err == nil {
					readDone := make(chan struct{})
					readExpired := make(chan bool, 1)
					go func() {
						select {
						case <-ctx.Done():
							// KILL closes the CLI's writer. Let Scanner drain
							// buffered events before closing our reader.
							select {
							case <-readDone:
							case <-time.After(time.Second):
								_ = stdout.Close()
								readExpired <- true
								return
							}
						case <-readDone:
						}
						readExpired <- false
					}()
					err = w.record(stdout, readDone, func(archive dockerEventArchive) error {
						return saveWorkerJSON(w.dir, w.identity+".docker-events.json", archive)
					})
					if <-readExpired {
						w.archive.Cause = &RuntimeCause{Operation: "events", Class: "timeout"}
					}
					if err != nil {
						if w.archive.Cause == nil {
							w.archive.Cause = runtimeDiagnostic(runtimeErrorFor("events", err))
						}
						cancel()
					}
					err = errors.Join(err, cmd.Wait())
				}
				_ = stdout.Close()
			}
		}

		w.archive.Status = "ended"
		if ctx.Err() != nil && w.archive.Cause == nil {
			w.archive.Status = "stopped"
		} else if err != nil && w.archive.Cause == nil {
			w.archive.Cause = runtimeDiagnostic(runtimeErrorFor("events", err))
		}
		ended := time.Now().UTC()
		w.archive.EndedAt = &ended
		if err := w.save(); err != nil {
			log.Print("Docker events archive persistence failed")
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); <-done }) }
}
