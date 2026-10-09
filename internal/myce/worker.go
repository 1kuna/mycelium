package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// MemoryGB is the GPU memory budget made available to this worker. Running jobs
// reserve their declared needs from it; no job gets unreported memory.
type Worker struct {
	Client   Client
	Machine  Machine
	Interval time.Duration
}
type process struct {
	job          Job
	cmd          *exec.Cmd
	done         chan error
	logDone      chan error
	stopOnce     sync.Once
	killOnce     sync.Once
	stopTimer    *time.Timer
	result       *Result
	exited       bool
	exitErr      error
	logsFinished bool
}

func (p *process) stop(grace time.Duration, kill bool) {
	if p.cmd == nil {
		return
	}
	if kill {
		p.killOnce.Do(func() { _ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL) })
		return
	}
	p.stopOnce.Do(func() {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		p.stopTimer = time.AfterFunc(grace, func() { p.killOnce.Do(func() { _ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL) }) })
	})
}
func (w Worker) start(ctx context.Context, j Job) (*process, error) {
	if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
		return nil, err
	}
	cmd := exec.Command(j.Spec.Run.Command[0], j.Spec.Run.Command[1:]...)
	cmd.Dir = j.Spec.Run.Workdir
	env := map[string]string{}
	for _, v := range os.Environ() {
		k, v, ok := strings.Cut(v, "=")
		if ok {
			env[k] = v
		}
	}
	for k, v := range j.Spec.Run.Env {
		env[k] = v
	}
	env["MYCE_JOB_ID"] = j.ID
	env["MYCE_ATTEMPT"] = strconv.Itoa(j.Attempt)
	env["MYCE_CHECKPOINT_DIR"] = filepath.Clean(j.Checkpoint)
	// The fleet credential must not be inherited by job commands.
	delete(env, "MYCE_TOKEN")
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		cmd.Env = append(cmd.Env, k+"="+env[k])
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = writer
	cmd.Stderr = writer
	if err := cmd.Start(); err != nil {
		reader.Close()
		writer.Close()
		return nil, err
	}
	writer.Close()
	p := &process{job: j, cmd: cmd, done: make(chan error, 1), logDone: make(chan error, 1)}
	go func() {
		defer reader.Close()
		buf := make([]byte, 16384)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				l := LogChunk{Result: Result{Machine: w.Machine.ID, Session: w.Machine.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, Text: string(buf[:n])}
				for {
					sendErr := w.Client.Call(ctx, "POST", "/api/workers/log", l, nil)
					if sendErr == nil {
						break
					}
					if stale(sendErr) {
						p.logDone <- sendErr
						return
					}
					log.Printf("log upload for %s: %v", j.ID, sendErr)
					select {
					case <-ctx.Done():
						p.logDone <- ctx.Err()
						return
					case <-time.After(w.Interval):
					}
				}
			}
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				p.logDone <- err
				return
			}
		}
	}()
	go func() {
		err := cmd.Wait() // Kill leftover children so they cannot hold the log pipe open.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		p.done <- err
	}()
	return p, nil
}
func (w Worker) Run(stop context.Context) error {
	if w.Interval <= 0 {
		w.Interval = time.Second
	}
	if w.Machine.ID == "" || !validGPU(w.Machine.GPU) || w.Machine.FreeMemoryGB <= 0 {
		return errors.New("worker requires a name, GPU platform and positive free-memory budget")
	}
	if w.Machine.Session == "" {
		w.Machine.Session = ID()
	}
	// Graceful leave must still be able to upload checkpoints' logs and results.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticker := time.NewTicker(w.Interval)
	defer ticker.Stop()
	var p *process
	leaving := false
	leaveSent := false
	stopCh := stop.Done()
	base := w.Machine.FreeMemoryGB
	defer func() {
		if p != nil {
			p.stop(0, true)
			if p.stopTimer != nil {
				p.stopTimer.Stop()
			}
		}
	}()
	for {
		select {
		case <-stopCh:
			leaving = true
			stopCh = nil
			if p != nil && p.job.Spec.Resumable {
				p.stop(StopGrace, false)
			}
		default:
		}
		if leaving && !leaveSent {
			if err := w.Client.Call(ctx, "POST", "/api/workers/"+w.Machine.ID+"/leave", map[string]bool{"now": false}, nil); err != nil {
				log.Printf("planned leave: %v", err)
			} else {
				leaveSent = true
			}
		}
		if p != nil && p.result == nil {
			if !p.exited {
				select {
				case err := <-p.done:
					p.exited = true
					p.exitErr = err
					if p.stopTimer != nil {
						p.stopTimer.Stop()
					}
				default:
				}
			}
			if !p.logsFinished {
				select {
				case err := <-p.logDone:
					p.logsFinished = true
					if err != nil && !stale(err) {
						log.Printf("job log stream: %v", err)
					}
				default:
				}
			}
			if p.exited && p.logsFinished {
				r := Result{Machine: w.Machine.ID, Session: w.Machine.Session, Execution: Execution{ID: p.job.ID, Attempt: p.job.Attempt}, Success: p.exitErr == nil}
				if p.exitErr != nil {
					r.Error = p.exitErr.Error()
				}
				p.result = &r
			}
		}
		if p != nil && p.result != nil && (!leaving || leaveSent) {
			err := w.Client.Call(ctx, "POST", "/api/workers/complete", p.result, nil)
			if err == nil || stale(err) {
				p = nil
			} else {
				log.Printf("completion upload: %v", err)
			}
		}
		m := w.Machine
		m.Leaving = leaving
		m.FreeMemoryGB = base
		var running *Execution
		if p != nil {
			m.FreeMemoryGB = max(0, base-p.job.Spec.Needs.MemoryGB)
			running = &Execution{ID: p.job.ID, Attempt: p.job.Attempt}
		}
		var reply Reply
		err := w.Client.Call(ctx, "POST", "/api/workers/checkin", Checkin{Machine: m, Running: running}, &reply)
		if err != nil {
			var he *HTTPError
			if errors.As(err, &he) && (he.Status == 400 || he.Status == 401) {
				return err
			}
			log.Printf("worker %s reconnecting: %v", m.ID, err)
		} else {
			if reply.Leave {
				leaving = true
				leaveSent = true
			}
			if p != nil && reply.Stop {
				p.stop(time.Duration(max(0, reply.GraceSeconds)*float64(time.Second)), reply.Kill)
			}
			if reply.Job != nil {
				if stop.Err() != nil {
					leaving = true
					r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: reply.Job.ID, Attempt: reply.Job.Attempt}, Error: "worker leaving before command started"}
					p = &process{job: *reply.Job, result: &r}
					continue
				}
				var startErr error
				p, startErr = w.start(ctx, *reply.Job)
				if startErr != nil {
					r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: reply.Job.ID, Attempt: reply.Job.Attempt}, Error: startErr.Error()} // Keep failed starts until the queue acknowledges them.
					p = &process{job: *reply.Job, result: &r}
					log.Printf("start job: %v", startErr)
				}
			}
			if leaving && p == nil {
				return nil
			}
		}
		select {
		case <-stopCh:
			leaving = true
			stopCh = nil
			if p != nil && p.job.Spec.Resumable {
				p.stop(StopGrace, false)
			}
		case <-ticker.C:
		}
	}
}
func (w Worker) String() string {
	return fmt.Sprintf("%s (%s, %.1f GB)", w.Machine.ID, w.Machine.GPU, w.Machine.FreeMemoryGB)
}
