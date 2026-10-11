package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// StateDir holds the worker lock, execution identity, logs and pending results.
// An empty StateDir uses the current user's cache, keyed by queue and machine.
type Worker struct {
	control               *processControl
	DeviceBinding         string
	EnvironmentSourceRoot string
	EnvironmentRoot       string
	EnvironmentLimit      int64
	environmentCache      *EnvironmentCache
	environmentRun        func(context.Context, []string, string, []string, io.Writer) error
	// Test seams for deterministic filesystem faults and audited fake engines.
	beforeModelCopy             func(context.Context)
	modelEngine                 func(Model) Engine
	CopyParallel                int
	CacheRoot, ModelRoot        string
	CacheLimit                  int64
	ModelConfig                 ModelConfig
	ModelListen, ModelAdvertise string
	Client                      Client
	Machine                     Machine
	Interval                    time.Duration
	Clock                       Clock
	StateDir                    string
	CheckpointRoot              string
	HostMemorySource            MemorySource
	HostMemoryReserveGB         float64
	MemorySource                MemorySource
	MemoryMeasured              bool
	batchMemory                 *atomic.Uint64
}
type workerFinish struct {
	result Result
	err    error
}
type process struct {
	mu      sync.Mutex
	job     Job
	cmd     *exec.Cmd
	done    chan workerFinish
	stopAt  *time.Time
	stopped bool
	killed  bool
}

func (p *process) stop(now time.Time, grace time.Duration, kill bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopLocked(now, grace, kill)
}
func (p *process) enforceGrace(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopAt != nil && !now.Before(*p.stopAt) {
		p.stopLocked(now, 0, true)
	}
}
func (p *process) stopLocked(now time.Time, grace time.Duration, kill bool) {
	if p.cmd == nil {
		return
	}
	if kill {
		if !p.killed {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			p.killed = true
		}
		return
	}
	deadline := now.Add(grace)
	if p.stopAt != nil && deadline.Before(*p.stopAt) {
		p.stopAt = &deadline
	}
	if !p.stopped {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		deadline := now.Add(grace)
		p.stopAt = &deadline
		p.stopped = true
	}
}

var errWorkerMemory = errors.New("free memory unavailable or insufficient")

var errWorkerCheckpoint = errors.New("checkpoint storage unavailable")

func (w Worker) start(s *workerStore, j Job) (*process, error) {
	if j.Spec.Needs.HostMemoryGB > 0 {
		if w.HostMemorySource == nil {
			return nil, fmt.Errorf("%w: host RAM source unavailable", errWorkerMemory)
		}
		free, e := w.HostMemorySource(context.Background())
		need := hostNeed(&w.Machine, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
		if e != nil || !validMemoryValue(free) || free < need {
			return nil, fmt.Errorf("%w: host RAM need %.1f GiB, available %.1f GiB: %v", errWorkerMemory, need, free, e)
		}
	}
	if w.MemorySource != nil {
		free, e := w.MemorySource(context.Background())
		if e != nil || !validMemoryValue(free) || free < j.Spec.Needs.MemoryGB {
			return nil, fmt.Errorf("%w: need %.1f GB, available %.1f GB: %v", errWorkerMemory, j.Spec.Needs.MemoryGB, free, e)
		}
	}
	checkpoint, err := prepareCheckpoint(j)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errWorkerCheckpoint, err)
	}
	var environmentPath string
	var release func()
	environmentStarted := false
	defer func() {
		if !environmentStarted && release != nil {
			release()
		}
	}()
	if r, ok := j.EnvironmentRecipes[w.Machine.GPU]; ok {
		if r.BinaryRelative != "" && w.EnvironmentSourceRoot != "" {
			r.Binary = filepath.Join(w.EnvironmentSourceRoot, r.BinaryRelative)
		}
		if w.environmentCache == nil {
			return nil, errors.New("environment builder unavailable")
		}
		environmentPath, release, err = w.environmentCache.Acquire(context.Background(), r)
		if err != nil {
			return nil, err
		}
	}
	// Resolve before the supervisor so a nonexistent command remains a clear
	// start failure, rather than an unexplained shell exit status.
	command := j.Spec.Run.Command[0]
	if strings.Contains(command, "/") && !filepath.IsAbs(command) {
		command, err = filepath.Abs(filepath.Join(j.Spec.Run.Workdir, command))
		if err != nil {
			return nil, err
		}
	}
	if environmentPath != "" && !strings.Contains(command, "/") {
		for _, bin := range []string{filepath.Join(environmentPath, "venv", "bin"), filepath.Join(environmentPath, "bin")} {
			candidate := filepath.Join(bin, command)
			if st, e := os.Stat(candidate); e == nil && st.Mode()&0111 != 0 {
				command = candidate
				break
			}
		}
	}
	command, err = exec.LookPath(command)
	if err != nil {
		return nil, err
	}
	if strings.Contains(command, "/") && !filepath.IsAbs(command) {
		command, err = filepath.Abs(command)
		if err != nil {
			return nil, err
		}
	}
	identity := ID()
	args := []string{"-c", `read gate || exit 125; trap 'wait "$child"; exit $?' TERM; "$@" < /dev/null & child=$!; wait "$child"; exit $?`, "myce-supervisor-" + identity, command}
	args = append(args, j.Spec.Run.Command[1:]...)
	cmd := exec.Command("sh", args...)
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
	if environmentPath != "" {
		env["PATH"] = filepath.Join(environmentPath, "venv", "bin") + string(os.PathListSeparator) + filepath.Join(environmentPath, "bin") + string(os.PathListSeparator) + env["PATH"]
		env["VIRTUAL_ENV"] = filepath.Join(environmentPath, "venv")
		env["MYCE_ENVIRONMENT_DIR"] = environmentPath
	}
	if w.Machine.Device != "" {
		bound, e := WithModelDevice(Model{}, w.Machine.GPU, w.Machine.Device)
		if e != nil {
			return nil, e
		}
		for k, v := range bound.DeviceEnv {
			env[k] = v
		}
		if w.Machine.GPU == "cuda" && w.DeviceBinding != "" {
			env["CUDA_VISIBLE_DEVICES"] = w.DeviceBinding
		}
	}
	env["MYCE_PROCESS_ID"] = identity
	env["MYCE_JOB_ID"] = j.ID
	env["MYCE_ATTEMPT"] = strconv.Itoa(j.Attempt)
	env["MYCE_CHECKPOINT_DIR"] = checkpoint
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
	logName := identity + ".log"
	output, err := os.OpenFile(filepath.Join(s.dir, logName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		output.Close()
		return nil, err
	}
	defer gateReader.Close()
	defer gateWriter.Close()
	cmd.Stdin = gateReader
	cmd.Stdout = output
	cmd.Stderr = output
	// Record intent before creating even the gated process. It cannot execute
	// the command until its group identity has been durably recorded.
	s.state.Job = &j
	if w.batchMemory != nil {
		w.batchMemory.Store(math.Float64bits(j.Spec.Needs.MemoryGB))
	}
	s.state.Identity = identity
	s.state.Log = logName
	s.state.Offset = 0
	s.state.Pending = 0
	s.state.Result = nil
	s.state.PID = 0
	if err = s.save(); err != nil {
		output.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		output.Close()
		return nil, err
	}
	s.state.PID = cmd.Process.Pid
	if err = s.save(); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		output.Close()
		return nil, err
	}
	p := &process{job: j, cmd: cmd, done: make(chan workerFinish, 1)}
	if w.control != nil {
		w.control.set(p)
	}
	if _, err = gateWriter.Write([]byte("start\n")); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	result := Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	environmentStarted = true
	go func() {
		if release != nil {
			defer release()
		}
		waitErr := cmd.Wait()
		// Descendants must not outlive the command or append to delivered output.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		groupErr := waitWorkerGroup(cmd.Process.Pid, w.Clock)
		syncErr := output.Sync()
		closeErr := output.Close()
		result.Success = waitErr == nil && groupErr == nil && syncErr == nil && closeErr == nil
		if waitErr != nil {
			result.Error = waitErr.Error()
		} else if groupErr != nil {
			result.Error = groupErr.Error()
		} else if syncErr != nil {
			result.Error = syncErr.Error()
		} else if closeErr != nil {
			result.Error = closeErr.Error()
		}
		// Persist independently of the reconnect loop, which may be in a network
		// request when the command finishes. Restart replays this exact session.
		saveErr := saveWorkerJSON(s.dir, identity+".result.json", result)
		p.done <- workerFinish{result: result, err: saveErr}
	}()
	return p, nil
}

func (w Worker) replay(ctx context.Context, s *workerStore) error {
	if s.state.Log == "" {
		return nil
	}
	f, err := os.Open(filepath.Join(s.dir, s.state.Log))
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	// Persist each chunk boundary before upload, so lost acknowledgements
	// replay identical bytes at the same offset even if the output grows.
	limit := info.Size()
	buf := make([]byte, 16384)
	for s.state.Offset < limit {
		n := s.state.Pending
		if n == 0 {
			n = int(min(int64(len(buf)), limit-s.state.Offset))
			s.state.Pending = n
			if err = s.save(); err != nil {
				return err
			}
		}
		if _, err = f.ReadAt(buf[:n], s.state.Offset); err != nil {
			return err
		}
		r := Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}}
		chunk := LogChunk{Result: r, Offset: s.state.Offset + 1, Text: string(buf[:n])}
		if err = w.Client.Call(ctx, "POST", "/api/workers/log", chunk, nil); err != nil {
			return err
		}
		s.state.Offset += int64(n)
		s.state.Pending = 0
		if err = s.save(); err != nil {
			return err
		}
	}
	return nil
}
func (w Worker) Run(stop context.Context) error {
	if w.Interval <= 0 {
		w.Interval = time.Second
	}
	if w.Clock == nil {
		w.Clock = RealClock{}
	}
	if w.Machine.ID == "" || !validGPU(w.Machine.GPU) || (w.MemorySource == nil && (!validMemoryValue(w.Machine.FreeMemoryGB) || w.Machine.FreeMemoryGB <= 0)) {
		return errors.New("worker requires a name, GPU platform and positive free-memory budget")
	}
	s, err := w.openStore()
	if err != nil {
		return err
	}
	defer s.close()
	if w.EnvironmentRoot == "" {
		w.EnvironmentRoot = filepath.Join(s.dir, "environment-cache")
	}
	if w.EnvironmentLimit == 0 {
		w.EnvironmentLimit = 20 * 1024 * 1024 * 1024
	}
	w.environmentCache = &EnvironmentCache{Root: w.EnvironmentRoot, Limit: w.EnvironmentLimit, Clock: w.Clock, Run: w.environmentRun}
	if err = w.environmentCache.Cleanup(context.Background()); err != nil {
		return err
	}
	environments := &environmentRuntime{dir: s.dir, cache: w.environmentCache, reports: map[string]EnvironmentReport{}}
	if err = environments.restore(); err != nil {
		return err
	}
	defer environments.stop()
	if w.CheckpointRoot != "" {
		root, e := filepath.Abs(w.CheckpointRoot)
		if e != nil {
			return e
		}
		s.state.CheckpointRoot = root
	}
	if err = s.cleanupOrphan(w.Clock); err != nil {
		return err
	}
	recovering := false
	if s.state.Job != nil && s.state.Identity != "" {
		var r Result
		data, e := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".result.json"))
		if e == nil {
			if e = decodeWorkerJSON(data, &r); e != nil {
				return e
			}
			s.state.Result = &r
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	if s.state.Job != nil && s.state.Result == nil {
		r := Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Interrupted: true, Error: "worker restarted; previous process group cleaned up"}
		s.state.Result = &r
		s.state.PID = 0
	}
	if s.state.Job != nil && s.state.Result != nil {
		recovering = true
		w.Machine.Session = s.state.Session
	} else {
		root := s.state.CheckpointRoot
		if !s.state.Fresh || s.state.Session == "" {
			previous := s.state.Session
			s.state = workerState{Session: ID(), PreviousSession: previous, Fresh: true, CheckpointRoot: root}
		}
		w.Machine.Session = s.state.Session
	}
	if err = s.save(); err != nil {
		return err
	}
	if w.ModelConfig.Idle <= 0 {
		w.ModelConfig.Idle = 10 * time.Minute
	}
	if w.ModelConfig.Wait <= 0 {
		w.ModelConfig.Wait = 5 * time.Minute
	}
	if err = validateModels(w.ModelConfig); err != nil {
		return err
	}
	if w.MemorySource == nil {
		w.MemorySource, _ = NewMemorySource("fake", w.Machine.FreeMemoryGB, 0)
	}
	if w.Machine.HostID == "" {
		w.Machine.HostID, _ = os.Hostname()
		if w.Machine.HostID == "" {
			return errors.New("worker requires physical host identity")
		}
	}
	if w.HostMemorySource == nil {
		w.HostMemorySource, err = NewHostMemorySource(w.HostMemoryReserveGB)
		if err != nil {
			return err
		}
	}
	w.Machine.HostMemoryMeasured = true
	w.batchMemory = &atomic.Uint64{}
	if s.state.Job != nil {
		w.batchMemory.Store(math.Float64bits(s.state.Job.Spec.Needs.MemoryGB))
	}
	rt, err := startModels(w, s.dir)
	if err != nil {
		return err
	}
	defer func() {
		if e := rt.close(); e != nil {
			log.Printf("model engine cleanup: %v", e)
		}
	}()
	rawMemory := w.MemorySource
	// Native reports already include loaded allocations. Configured budgets need
	// our own resident-model reservations deducted immediately before a start.
	w.MemorySource = func(ctx context.Context) (float64, error) {
		free, e := rawMemory(ctx)
		reports, used := rt.report()
		if w.MemoryMeasured {
			used = 0
			for _, report := range reports {
				if report.State == "loading" {
					for _, model := range w.ModelConfig.Models {
						if model.Name == report.Name {
							used += model.MemoryGB
						}
					}
				}
			}
		}
		free = max(0, free-used)
		return free, e
	}
	ctx := context.Background()
	ticker := time.NewTicker(min(w.Interval, 100*time.Millisecond))
	defer ticker.Stop()
	var p *process
	leaving := stop.Err() != nil
	leaveSent := false
	stopCh := stop.Done()
	base := w.Machine.FreeMemoryGB
	fresh := !recovering && s.state.Fresh
	eventCtx, eventCancel := context.WithCancel(context.Background())
	defer eventCancel()
	events := make(chan WorkerEvent, 1)
	go w.watchStops(eventCtx, events)
	w.control = &processControl{clock: w.Clock, wake: make(chan struct{}, 1)}
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		w.control.run(eventCtx, stop, events)
	}()
	defer func() { eventCancel(); <-controlDone }()
	var retry workerRetry
	defer func() {
		if p != nil {
			p.stop(w.Clock.Now(), 0, true)
			finished := <-p.done
			if finished.err != nil {
				log.Printf("persist result: %v", finished.err)
			}
		}
	}()
	for {
		now := w.Clock.Now()
		batch := 0.0
		if s.state.Job != nil {
			batch = s.state.Job.Spec.Needs.MemoryGB
		}
		w.batchMemory.Store(math.Float64bits(batch))
		select {
		case <-stopCh:
			leaving = true
			stopCh = nil
			if p != nil && p.job.Spec.Resumable {
				p.stop(now, StopGrace, false)
			}
		default:
		}
		if p != nil {
			p.enforceGrace(now)
			select {
			case f := <-p.done:
				retry = workerRetry{}
				w.control.set(nil)
				p = nil
				if f.err != nil {
					return fmt.Errorf("persist command result: %w", f.err)
				}
				s.state.Result = &f.result
				s.state.PID = 0
				if err = s.save(); err != nil {
					return err
				}
			default:
			}
		}
		if retry.ready(now) {
			rt.mu.Lock()
			rt.session = w.Machine.Session
			rt.mu.Unlock()
			m := w.Machine
			m.EnvironmentBuilder = true
			if e := environments.upload(ctx, w.Client, m); e != nil && !stale(e) {
				log.Printf("environment log upload: %v", e)
			}
			ready, envReports, envTimes := environments.report()
			m.Environments = append(slices.Clone(w.Machine.Environments), ready...)
			m.EnvironmentReports = envReports
			m.EnvironmentBuildSeconds = envTimes
			if !recovering {
				m.ModelURL = rt.url
			}
			m.ModelConfigHash = modelConfigHash(w.ModelConfig)
			reports, _ := rt.report()
			if rt.cache != nil {
				var ce error
				m.CachedModels, m.CacheFreeBytes, ce = rt.cache.Report(w.ModelConfig.Models)
				m.CopyReports, m.CopyBytesPerSecond = rt.copyTelemetry()
				if ce != nil {
					m.CacheError = ce.Error()
				}
			}
			for _, r := range reports {
				if r.State == "failed" {
					m.CacheError = r.Error
				}
			}
			m.Leaving = leaving
			m.FreeMemoryGB = base
			m.TotalMemoryGB = base
			if w.MemorySource != nil {
				free, e := rawMemory(ctx)
				if e != nil || !validMemoryValue(free) {
					log.Printf("worker %s GPU memory probe failed: %v", m.ID, e)
					free = 0
					m.MemoryError = fmt.Sprintf("%v", e)
					if e == nil {
						m.MemoryError = "probe returned invalid memory"
					}
				}
				m.FreeMemoryGB = free
				m.MemoryMeasured = w.MemoryMeasured
			}
			hostFree, hostErr := w.HostMemorySource(ctx)
			m.HostAvailableMemoryGB = hostFree
			if hostErr != nil || !validMemoryValue(hostFree) {
				m.HostAvailableMemoryGB = 0
				m.HostMemoryError = fmt.Sprintf("host RAM probe failed: %v", hostErr)
			}
			m.CheckpointUnavailable = workerCheckpointAvailable(s.state.CheckpointRoot) != nil
			var running *Execution
			if s.state.Job != nil {
				if !w.MemoryMeasured {
					m.FreeMemoryGB = max(0, m.FreeMemoryGB-s.state.Job.Spec.Needs.MemoryGB)
				}
				progress, _ := checkpointProgress(attemptCheckpoint(*s.state.Job))
				running = &Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt, Progress: progress}
			}
			var reply Reply
			err = w.Client.Call(ctx, "POST", "/api/workers/checkin", Checkin{Started: p != nil, Machine: m, Running: running, Fresh: fresh, PreviousSession: s.state.PreviousSession, ModelReports: reports}, &reply)
			if err == nil {
				busy := reply.Job != nil || s.state.Job != nil || leaving || recovering
				for _, slot := range reply.ModelPlan {
					if slot.State == "loading" || slot.InFlight > 0 {
						busy = true
					}
				}
				for i := range reply.EnvironmentPlan {
					r := &reply.EnvironmentPlan[i]
					if r.BinaryRelative != "" && w.EnvironmentSourceRoot != "" {
						r.Binary = filepath.Join(w.EnvironmentSourceRoot, r.BinaryRelative)
					}
				}
				environments.sync(reply.EnvironmentPlan, busy)
				rt.syncCopies(reply.CopyPlan, reply.CacheUsage, reply.CachePins, busy)
				// syncCopies cancels immediately; do not block heartbeats behind an
				// outstanding filesystem syscall on an unavailable NAS. Loads share
				// the cache admission gate and wait for the reservation to release.
				rt.sync(reply.ModelPlan)
				fresh = false
				if s.state.Fresh {
					s.state.Fresh = false
					if e := s.save(); e != nil {
						return e
					}
				}
				if w.CheckpointRoot == "" && reply.CheckpointRoot != "" && reply.CheckpointRoot != s.state.CheckpointRoot {
					s.state.CheckpointRoot = reply.CheckpointRoot
					if e := s.save(); e != nil {
						return e
					}
				}
				if s.state.Job != nil && s.state.Result == nil && p == nil && !recovering {
					if reply.Stop {
						r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Interrupted: true, Error: "assignment withdrawn before checkpoint storage recovered"}
						s.state.Result = &r
						if e := s.save(); e != nil {
							return e
						}
					} else if workerCheckpointAvailable(s.state.CheckpointRoot) == nil {
						p, err = w.start(s, *s.state.Job)
						if err != nil {
							if errors.Is(err, errWorkerCheckpoint) || errors.Is(err, errWorkerMemory) {
								log.Printf("retry pending start: %v", err)
							} else {
								r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Error: err.Error()}
								s.state.Result = &r
								s.state.PID = 0
								err = s.save()
							}
						}
					}
				}
				if reply.Leave && !recovering {
					leaving = true
					leaveSent = true
				}
				if p != nil && reply.Stop {
					p.stop(now, time.Duration(max(0, reply.GraceSeconds)*float64(time.Second)), reply.Kill)
				}
				if leaving && !leaveSent {
					err = w.Client.Call(ctx, "POST", "/api/workers/"+m.ID+"/leave", map[string]bool{"now": false}, nil)
					if err == nil {
						leaveSent = true
					}
				}
				if err == nil && s.state.Job != nil && !reply.Hold {
					// A killed losing copy must never upload its result over the winner.
					if !(reply.Stop && reply.Kill) {
						err = w.replay(ctx, s)
					} else if s.state.Result != nil {
						err = &HTTPError{Status: 409}
					}
					if err == nil && s.state.Result != nil {
						err = w.Client.Call(ctx, "POST", "/api/workers/complete", s.state.Result, nil)
					}
					if s.state.Result != nil && (err == nil || stale(err)) {
						s.state.Job = nil
						s.state.Result = nil
						s.state.PID = 0
						s.state.Identity = ""
						s.state.Log = ""
						s.state.Offset = 0
						s.state.Pending = 0
						if recovering {
							recovering = false
							s.state.PreviousSession = s.state.Session
							s.state.Session = ID()
							s.state.Fresh = true
							w.Machine.Session = s.state.Session
							eventCancel()
							sessionCtx, sessionCancel := context.WithCancel(context.Background())
							defer sessionCancel()
							go w.watchStops(sessionCtx, events)
							fresh = true
						}
						if e := s.save(); e != nil {
							return e
						}
						err = nil
					} else if stale(err) && p != nil {
						p.stop(now, 0, true)
						err = nil
					}
				}
				if err == nil && reply.Job != nil {
					if s.state.Job != nil {
						return errors.New("queue assigned a job while an execution is still pending")
					}
					j := *reply.Job
					if !filepath.IsAbs(j.Checkpoint) {
						rel := filepath.Clean(j.Checkpoint)
						if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
							return errors.New("invalid relative checkpoint path")
						}
						j.Checkpoint = filepath.Join(s.state.CheckpointRoot, rel)
					}
					if leaving || recovering {
						r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: "worker leaving before command started"}
						s.state.Job = &j
						if w.batchMemory != nil {
							w.batchMemory.Store(math.Float64bits(j.Spec.Needs.MemoryGB))
						}
						s.state.Result = &r
						err = s.save()
					} else {
						s.state.Job = &j
						if w.batchMemory != nil {
							w.batchMemory.Store(math.Float64bits(j.Spec.Needs.MemoryGB))
						}
						if s.state.CheckpointRoot == "" {
							s.state.CheckpointRoot = filepath.Dir(j.Checkpoint)
						}
						if e := s.save(); e != nil {
							return e
						}
						if workerCheckpointAvailable(s.state.CheckpointRoot) != nil {
							err = nil
						} else {
							p, err = w.start(s, j)
							if err == nil && p != nil {
								startedCtx, startedCancel := context.WithTimeout(ctx, 2*time.Second)
								startedErr := w.Client.Call(startedCtx, "POST", "/api/workers/started", Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, nil)
								startedCancel()
								if startedErr != nil {
									log.Printf("worker startup acknowledgement: %v", startedErr)
								}
							}
						}
						if err != nil && !(errors.Is(err, errWorkerCheckpoint) || errors.Is(err, errWorkerMemory)) {
							r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: err.Error()}
							s.state.Job = &j
							if w.batchMemory != nil {
								w.batchMemory.Store(math.Float64bits(j.Spec.Needs.MemoryGB))
							}
							s.state.Result = &r
							s.state.PID = 0
							err = s.save()
						}
					}
				}
				if err == nil && leaving && s.state.Job == nil && !recovering && len(reply.ModelPlan) == 0 {
					return nil
				}
			}
			if err != nil {
				// Pause background I/O while disconnected: new priority decisions
				// cannot reach this worker. Preserve pins and durable copy bytes.
				environments.sync(nil, true)
				rt.cancelCopies()
				var he *HTTPError
				if errors.As(err, &he) && (he.Status == 400 || he.Status == 401) {
					return err
				}
				log.Printf("worker %s reconnecting: %v", m.ID, err)
				retry.failed(w.Clock.Now(), w.Interval)
			} else {
				nextInterval := w.Interval
				if running != nil && s.state.Job == nil {
					nextInterval = time.Millisecond
				}
				retry.succeeded(w.Clock.Now(), nextInterval)
			}
		}
		select {
		case <-stopCh:
			leaving = true
			stopCh = nil
			if p != nil && p.job.Spec.Resumable {
				p.stop(w.Clock.Now(), StopGrace, false)
			}
		case <-w.control.wake:
			retry = workerRetry{}
		case <-ticker.C:
		}
	}
}
func (w Worker) String() string {
	return fmt.Sprintf("%s (%s, %.1f GB)", w.Machine.ID, w.Machine.GPU, w.Machine.FreeMemoryGB)
}
