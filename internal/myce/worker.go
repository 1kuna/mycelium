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
	// Benchmark runs only when explicitly enabled; fake inventory never loads the host.
	OnboardingBenchmark   bool
	Benchmark             func(context.Context, BenchmarkConfig, *CapabilityProfile) error
	DailyUse              bool
	Laptop                bool
	Desktop               bool
	OwnerSignals          OwnerSignalSource
	availability          *availabilityMonitor
	FileCacheRoot         string
	FileCacheLimit        int64
	fileCache             *FileCache
	fileCheckpoints       bool
	ExtensionRoot         string
	JobFileRoot           string
	extensionFingerprint  string
	RequireMemoryCeilings bool
	memoryCeilings        *memoryCeilings
	MemorySwapMaxBytes    int64
	DockerEndpoint        string

	ContainerRetention *time.Duration
	LogRetention       time.Duration
	dockerCallTimeout  time.Duration
	Unit               string

	SuperviseProcess bool

	checkpointPrepare     func(Job) (string, error) // Filesystem fault seam.
	control               *processControl
	DeviceBinding         string
	EnvironmentSourceRoot string
	EnvironmentRoot       string
	EnvironmentLimit      int64
	environmentCache      *EnvironmentCache
	environmentRun        func(context.Context, []string, string, []string, io.Writer, io.Writer) error
	// Test seams for deterministic filesystem faults and audited fake engines.
	beforeModelCopy             func(context.Context)
	modelEngine                 func(Model) Engine
	CopyParallel                int
	CacheRoot, ModelRoot        string
	CacheLimit                  int64
	SecretRoot                  string
	ModelApprovalRoot           string
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
	FixedAcceleratorMemory      bool
	batchMemory                 *atomic.Uint64
}
type workerFinish struct {
	result Result
	err    error
}
type process struct {
	priorityStop *PriorityPreemption
	outcome      memberOutcome
	saveOutcome  func(memberOutcome) error

	machine string
	docker  *dockerAttempt
	native  *nativeGroup
	mu      sync.Mutex
	job     Job
	cmd     *exec.Cmd
	done    chan workerFinish
	stopAt  *time.Time
	stopped bool
	polite  bool
	killed  bool
}

// Save exact-attempt intent before delivery. Result goroutines read only the
// process copy under p.mu; restart recovery reads the durable worker/daemon copy.
func (p *process) recordPriorityStop(s *workerStore, stop *PriorityPreemption) error {
	if stop == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s.state.PriorityStop = stop
	if err := s.save(); err != nil {
		return err
	}
	if p.docker != nil {
		p.docker.mu.Lock()
		p.docker.state.PriorityStop = stop
		err := p.docker.saveLocked()
		p.docker.mu.Unlock()
		if err != nil {
			return err
		}
	}
	p.priorityStop = stop
	return nil
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
	if p.native != nil {
		p.native.stopLocked(now, grace, kill)
		return
	}
	if p.docker != nil {
		p.docker.stop(now, grace, kill)
		return
	}
	if p.cmd == nil {
		return
	}
	if kill {
		if !p.killed {
			p.signalLocked(syscall.SIGKILL)
			p.killed = true
		}
		return
	}
	deadline := now.Add(grace)
	if p.stopAt != nil && deadline.Before(*p.stopAt) {
		p.stopAt = &deadline
	}
	if !p.stopped {
		p.signalLocked(syscall.SIGTERM)
		deadline := now.Add(grace)
		p.stopAt = &deadline
		p.stopped = true
	}
}

// p.mu serializes delivery/persistence with exit observation. Capture the
// observation timestamp immediately after Wait, before acquiring this lock.
func (p *process) signalLocked(signal syscall.Signal) {
	if p.outcome.Exit != nil {
		return
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, signal); err != nil {
		return
	}
	at := time.Now().UTC()
	name := "TERM"
	if signal == syscall.SIGKILL {
		name = "KILL"
	}
	next := p.outcome
	next.Signals = append(append([]deliveredSignal(nil), next.Signals...), deliveredSignal{Signal: name, At: at})
	if p.saveOutcome != nil && p.saveOutcome(next) == nil {
		p.outcome = next
	}
}

var errWorkerMemory = errors.New("free memory unavailable or insufficient")

var errWorkerCheckpoint = errors.New("checkpoint storage unavailable")

var errWorkerReleasePending = errors.New("command release unconfirmed")

func (w Worker) start(s *workerStore, j Job) (*process, error) {
	if j.Spec.Needs.cpuOnly() {
		w.Machine.GPU, w.Machine.Device, w.DeviceBinding = "cpu", "0", ""
	}
	if !j.Spec.extensionJob() {
		if err := lowerExecution(&j.Spec); err != nil {
			return nil, err
		}
	}
	if err := w.checkAvailability(); err != nil {
		return nil, err
	}
	extensionEnv, extensionRelease, extensionErr := w.prepareExtensionExecution(&j)
	if extensionErr != nil {
		return nil, extensionErr
	}
	extensionStarted := false
	defer func() {
		if !extensionStarted && extensionRelease != nil {
			extensionRelease()
		}
	}()
	executionWorkdir := j.Spec.Run.Workdir
	if j.Spec.extensionJob() || len(j.Spec.Files) > 0 {
		value := j
		dir, err := s.extensionFiles.run(attemptCheckpoint(j), func() (string, error) {
			err := w.prepareExtensionFiles(&value, s.dir)
			return value.Spec.Run.Workdir, err
		})
		if err != nil {
			if strings.HasPrefix(err.Error(), "NAS unreachable:") {
				return nil, fmt.Errorf("%w: input copy pending", errWorkerCheckpoint)
			}
			return nil, err
		}
		j.Spec.Run.Workdir = dir
	}

	executionWork := j.Spec.Run.Workdir
	if executionWorkdir != "" {
		j.Spec.Run.Workdir = executionWorkdir
	}
	if j.Spec.Priority == "production" && j.Spec.StartBy == nil {
		return nil, errors.New("queue start-gate protocol mismatch: production assignment has no start_by; upgrade queue and workers together")
	}
	ceilingBytes, ceilingErr := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if ceilingErr != nil {
		return nil, ceilingErr
	}
	if err := w.checkHostRAM(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); err != nil {
		return nil, err
	}
	if w.MemorySource != nil {
		free, e := w.MemorySource(context.Background())
		if e != nil || !validMemoryValue(free) || free < j.Spec.Needs.MemoryGB {
			return nil, fmt.Errorf("%w: need %.1f GB, available %.1f GB: %v", errWorkerMemory, j.Spec.Needs.MemoryGB, free, e)
		}
	}
	prepare := prepareCheckpoint
	if j.FileTransport {
		prepare = func(value Job) (string, error) { return w.prepareFileCheckpoint(value, s.dir) }
	}
	if w.checkpointPrepare != nil {
		prepare = w.checkpointPrepare
	}
	var checkpoint string
	var err error
	if needsCheckpoint(&j) {
		checkpoint, err = s.nasPrepare.run(attemptCheckpoint(j), func() (string, error) { return prepare(j) })
		s.checkpointUnavailable = err != nil
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errWorkerCheckpoint, err)
		}
	} else {
		s.checkpointUnavailable = false
		checkpoint = filepath.Join(s.dir, "checkpoints", j.ID, fmt.Sprintf("attempt-%d", j.Attempt))
		if err = os.MkdirAll(checkpoint, 0700); err != nil {
			return nil, err
		}
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
			return nil, s.startFailure(j, environmentFailed, errors.New("environment builder unavailable"))
		}
		r.readyOnly = j.Spec.extensionJob()
		environmentPath, release, err = w.environmentCache.Acquire(withLaunchGate(context.Background(), func() error {
			return w.authorizeLaunch(HostLaunch{Machine: w.Machine.ID, Session: s.state.Session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)})
		}), r)
		if err != nil {
			return nil, s.startFailure(j, environmentFailed, err)
		}
	}
	expandExecution(&j.Spec, executionWork, environmentPath, machineTarget(&w.Machine))
	if err := w.checkHostRAM(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); err != nil {
		return nil, err
	}
	if err := w.checkAvailability(); err != nil {
		return nil, err
	}
	if j.Spec.Run.Docker {
		p, e := w.startDocker(s, j, checkpoint, release)
		environmentStarted = e == nil
		return p, e
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
		for _, bin := range environmentBins(environmentPath) {
			candidate := filepath.Join(bin, command)
			if st, e := os.Stat(candidate); e == nil && st.Mode()&0111 != 0 {
				command = candidate
				break
			}
		}
	}
	command, err = exec.LookPath(command)
	if err != nil {
		return nil, s.startFailure(j, launchFailed, err)
	}
	if strings.Contains(command, "/") && !filepath.IsAbs(command) {
		command, err = filepath.Abs(command)
		if err != nil {
			return nil, err
		}
	}
	if err := w.checkAvailability(); err != nil {
		return nil, err
	}
	if err := w.checkHostMemoryBudget(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); err != nil {
		return nil, err
	}
	identity := ID()
	memoryAttempt, err := w.memoryCeilings.create("attempt-"+identity, ceilingBytes, w.MemorySwapMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("memory ceiling setup: %w", err)
	}
	// Report the actual cgroup limit to the payload.
	if memoryAttempt != nil {
		ceilingBytes = memoryAttempt.bytes
	}
	started := false
	defer func() {
		if !started {
			_ = memoryAttempt.cleanup(w.Clock)
		}
	}()
	args := []string{"-c", `read gate || exit 125; trap 'wait "$child"; exit $?' TERM; "$@" < /dev/null & child=$!; exec 3>&-; wait "$child"; exit $?`, "myce-supervisor-" + identity, command}
	args = append(args, j.Spec.Run.Command[1:]...)
	var readyReader, readyWriter, approvalReader, approvalWriter *os.File
	if j.Spec.StartBy != nil {
		executable, e := os.Executable()
		if e != nil {
			return nil, e
		}
		readyReader, readyWriter, err = os.Pipe()
		if err != nil {
			return nil, err
		}
		defer readyReader.Close()
		defer readyWriter.Close()
		approvalReader, approvalWriter, err = os.Pipe()
		if err != nil {
			return nil, err
		}
		defer approvalReader.Close()
		defer approvalWriter.Close()
		args = append(args[:3], append([]string{executable, "_command-exec"}, args[3:]...)...)
	}
	cmd := exec.Command("sh", args...)
	if readyWriter != nil {
		cmd.ExtraFiles = []*os.File{readyWriter, approvalReader}
	}
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
	for k, v := range extensionEnv {
		env[k] = v
	}
	if environmentPath != "" {
		bindEnvironment(env, environmentPath)
	}
	placement := w.Machine
	if j.Spec.Needs.cpuOnly() {
		placement.GPU, placement.Device = "cpu", ""
	}
	if placement.Device != "" || placement.GPU == "cpu" {
		delete(env, "CUDA_VISIBLE_DEVICES")
		delete(env, "ONEAPI_DEVICE_SELECTOR")
		delete(env, "ZE_AFFINITY_MASK")
		bound, e := WithModelDevice(Model{}, placement.GPU, placement.Device)
		if e != nil {
			return nil, e
		}
		for k, v := range bound.DeviceEnv {
			env[k] = v
		}
		if placement.GPU == "cuda" && w.DeviceBinding != "" {
			env["CUDA_VISIBLE_DEVICES"] = w.DeviceBinding
		}
	}
	for k, v := range placementEnvironment(placement) {
		env[k] = v
	}
	if ceilingBytes > 0 {
		env["MYCE_MEMORY_LIMIT_BYTES"] = strconv.FormatInt(ceilingBytes, 10)
	} else {
		delete(env, "MYCE_MEMORY_LIMIT_BYTES")
	}
	env["MYCE_PROCESS_ID"] = identity
	env["MYCE_JOB_ID"] = j.ID
	env["MYCE_ATTEMPT"] = strconv.Itoa(j.Attempt)
	env["MYCE_CHECKPOINT_DIR"] = checkpoint
	if j.Spec.Program != nil {
		env["MYCE_USAGE_FILE"] = programUsageFile(s.dir, identity)
	} else {
		delete(env, "MYCE_USAGE_FILE")
	}
	delete(env, "MYCE_TOKEN")
	if len(j.Spec.Run.Sidecars) > 0 || j.Spec.Run.MemoryGB > 0 {
		if w.batchMemory != nil {
			w.batchMemory.Store(math.Float64bits(j.Spec.Needs.MemoryGB))
		}
		p, e := w.startNativeGroup(s, j, identity, memoryAttempt, env, environmentPath, extensionRelease, release)
		if e == nil {
			environmentStarted, extensionStarted, started = true, true, true
		}
		return p, e
	}
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
	output, err := openCommandOutput(s.dir, logName, j.Spec.Logs, os.O_EXCL)
	if err != nil {
		return nil, err
	}
	if j.Spec.Logs == "none" {
		logName = ""
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
	s.state.OperatorStopped = false
	s.state.PID = 0
	s.state.LaunchPending = true
	if err = s.save(); err != nil {
		output.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		output.Close()
		return nil, s.startFailure(j, launchFailed, err)
	}
	if readyWriter != nil {
		readyWriter.Close()
	}
	abort := func(cause error) error {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		output.Close()
		if releaseErr := waitWorkerGroup(cmd.Process.Pid, w.Clock); releaseErr != nil {
			return errors.Join(cause, fmt.Errorf("%w: %v", errWorkerReleasePending, releaseErr))
		}
		return cause
	}
	if err = memoryAttempt.attach(cmd.Process.Pid); err != nil {
		return nil, abort(fmt.Errorf("memory ceiling attach: %w", err))
	}
	s.state.PID = cmd.Process.Pid
	s.state.LaunchPending = false
	if err = saveWorkerJSON(s.dir, "attempt-"+identity+".json", workerState{PID: s.state.PID, Identity: identity}); err != nil {
		return nil, abort(err)
	}
	if err = s.save(); err != nil {
		return nil, abort(err)
	}
	p := &process{job: j, cmd: cmd, done: make(chan workerFinish, 1)}
	p.saveOutcome = func(facts memberOutcome) error { return saveWorkerJSON(s.dir, identity+".outcome.json", facts) }
	if w.control != nil {
		w.control.set(p)
	}
	if j.Spec.StartBy == nil {
		if err = w.authorizeStart(j, s.state.Session); err != nil {
			return nil, abort(s.startFailure(j, launchRefused, err))
		}
	}
	if _, err = gateWriter.Write([]byte("start\n")); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if readyReader != nil {
		// Bound receipt I/O; queue startup decisions still use its injected Clock.
		_ = readyReader.SetReadDeadline(time.Now().Add(2 * time.Second))
		var ready [1]byte
		if _, e := io.ReadFull(readyReader, ready[:]); e != nil || ready[0] != 'R' {
			return nil, abort(fmt.Errorf("command start gate unavailable: %v", e))
		}
		err = w.authorizeStart(j, s.state.Session)
		if err == nil {
			_, err = approvalWriter.Write([]byte("S"))
		}
		approvalWriter.Close()
		// A refused hand-off is an ordinary failed attempt; the payload remains gated.
		if err == nil {
			receipt, e := io.ReadAll(io.LimitReader(readyReader, 4096))
			if e != nil || len(receipt) > 0 {
				if e != nil {
					return nil, abort(s.startFailure(j, launchFailed, fmt.Errorf("command exec receipt: %w", e)))
				}
				return nil, abort(s.startFailure(j, launchFailed, errors.New(string(receipt))))
			}
		}
	}
	launchErr := err
	if launchErr != nil {
		launchErr = s.startFailure(j, launchRefused, launchErr)
		var recorded *recordedStartError
		if errors.As(launchErr, &recorded) {
			p.outcome.Infrastructure = recorded.facts.Infrastructure
		}
	}
	result := Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	environmentStarted = true
	extensionStarted = true
	started = true
	go func() {
		if extensionRelease != nil {
			defer extensionRelease()
		}
		if release != nil {
			defer release()
		}
		waitErr := cmd.Wait()
		observed := observedNativeExit(cmd, time.Now().UTC())
		p.mu.Lock()
		p.outcome.Exit = observed
		observationErr := p.saveOutcome(p.outcome)
		result.Forced = p.killed
		p.mu.Unlock()
		// Descendants must not outlive the command or append to delivered output.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		oomErr := memoryAttempt.failure()
		groupErr := errors.Join(waitWorkerGroup(cmd.Process.Pid, w.Clock), memoryAttempt.cleanup(w.Clock))
		result.ReleasePending = groupErr != nil
		syncErr := syncCommandOutput(output)
		closeErr := output.Close()
		result.Success = oomErr == nil && launchErr == nil && waitErr == nil && groupErr == nil && syncErr == nil && closeErr == nil
		if j.Spec.Program != nil {
			collectProgramUsage(&result, cmd.ProcessState, programUsageFile(s.dir, identity))
			result.OutOfMemory = result.OutOfMemory || (oomErr != nil && strings.HasPrefix(ceilingDiagnostic(oomErr.Error(), ""), "exceeded its "))
		}
		if oomErr != nil {
			result.ExecutionCause = &RuntimeCause{Operation: "command", Class: "resource_limit"}
		} else if launchErr != nil {
			result.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("launch", launchErr))
		} else if waitErr != nil {
			result.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("command", waitErr))
		}
		if groupErr != nil {
			result.CleanupCause = runtimeDiagnostic(runtimeErrorFor("process_group", groupErr))
		} else if syncErr != nil || closeErr != nil {
			result.CleanupCause = runtimeDiagnostic(runtimeErrorFor("persist", errors.Join(syncErr, closeErr)))
		}
		if oomErr != nil {
			result.Error = oomErr.Error()
		} else if launchErr != nil {
			result.Error = "command start hand-off: " + launchErr.Error()
		} else if waitErr != nil {
			result.Error = waitErr.Error()
		} else if groupErr != nil {
			result.Error = groupErr.Error()
		} else if syncErr != nil {
			result.Error = syncErr.Error()
		} else if closeErr != nil {
			result.Error = closeErr.Error()
		}
		p.mu.Lock()
		result.PoliteStop = p.polite
		facts := p.outcome
		if launchErr != nil {
			facts.Exit = &memberExit{Status: 1, ObservedAt: observed.ObservedAt}
			facts.Signals = nil
		}
		result = classifyAttemptOutcome(result, attemptCause{
			PriorityStop:  p.priorityStop,
			memberOutcome: facts,
			OutOfMemory:   result.OutOfMemory || oomErr != nil && strings.HasPrefix(ceilingDiagnostic(oomErr.Error(), ""), "exceeded its "),
		})
		p.mu.Unlock()
		if result.Success {
			if e := w.publishExtensionFiles(j); e != nil {
				result.Success = false
				result.Error = e.Error()
				result.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("outputs", e))
			}
		}
		// Persist independently of the reconnect loop, which may be in a network
		// request when the command finishes. Restart replays this exact session.
		saveErr := errors.Join(observationErr, saveWorkerJSON(s.dir, identity+".result.json", result))
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
	if w.Machine.MaxJobs < 0 {
		return errors.New("max-jobs must be nonnegative")
	}
	if w.Machine.ID == "" || !validGPU(w.Machine.GPU) || (w.Machine.GPU != "cpu" && w.MemorySource == nil && (!validMemoryValue(w.Machine.FreeMemoryGB) || w.Machine.FreeMemoryGB <= 0)) {
		return errors.New("worker requires a name, GPU platform and positive free-memory budget")
	}
	retention := DefaultContainerRetention
	if w.ContainerRetention != nil {
		retention = *w.ContainerRetention
	}
	if retention < 0 {
		return errors.New("container-retention must be nonnegative")
	}
	w.initMemoryCeilings()
	if err := w.requireMemoryCeilings(); err != nil {
		return err
	}
	s, err := w.openStore()
	if err != nil {
		return err
	}
	defer s.close()
	if err = w.initFileTransfers(stop, s); err != nil {
		return err
	}
	if w.Machine.CapabilityProfile == nil {
		probeCtx, cancel := context.WithTimeout(stop, 5*time.Second)
		profile := LocalProfileProbe().Detect(probeCtx, w.Machine, s.dir, w.Clock.Now())
		cancel()
		applyFixedAcceleratorMemory(&profile, w.Machine, w.FixedAcceleratorMemory)
		w.Machine.CapabilityProfile = &profile
	}

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
	environments := &environmentRuntime{authorize: w.authorizeLaunch, dir: s.dir, cache: w.environmentCache, reports: map[string]EnvironmentReport{}}
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
	var memoryFiles memoryFS = osMemoryFS{}
	if w.memoryCeilings != nil {
		memoryFiles = w.memoryCeilings.fs
	}
	if s.state.Job != nil && !s.state.Job.Spec.Run.Docker && s.state.Identity != "" {
		if err = s.recoverResult(w.Machine.ID); err != nil {
			return err
		}
		if err = s.recoverNativeCause(w.Machine.ID, memoryFiles); err != nil {
			return err
		}
	}
	if err = s.cleanupOrphan(w.Clock, memoryFiles); err != nil {
		return err
	}
	if s.state.Job != nil && !s.state.Job.Spec.Run.Docker && s.state.Result != nil {
		s.state.Result.ReleasePending = false
		if err = saveWorkerJSON(s.dir, s.state.Identity+".result.json", *s.state.Result); err != nil {
			return err
		}
	}
	var recoveringProcess *process
	if s.state.Job != nil && s.state.Job.Spec.Run.Docker && s.state.Identity != "" {
		recoveringProcess, err = w.restoreDocker(s)
		if err != nil {
			return err
		}
	}
	if recoveringProcess != nil {
		s.state.Result = nil
		defer recoveringProcess.docker.close()
	}
	recovering := recoveringProcess != nil
	if s.state.Job != nil && s.state.Identity != "" && recoveringProcess == nil {
		if err = s.recoverResult(w.Machine.ID); err != nil {
			return err
		}
	}
	if s.state.Job != nil && s.state.Result == nil && recoveringProcess == nil {
		facts := memberOutcome{ObservationLost: true}
		if err = saveWorkerJSON(s.dir, s.state.Identity+".observation-loss.json", facts); err != nil {
			return err
		}
		r := classifyAttemptOutcome(Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Error: "worker restarted; previous process group cleaned up"}, attemptCause{memberOutcome: facts, PriorityStop: s.state.PriorityStop})
		s.state.Result = &r
		s.state.PID = 0
	}
	if s.state.Result != nil && s.state.OperatorStopped {
		*s.state.Result = operatorOutcome(*s.state.Result, true)
	}
	if s.state.Job != nil && (s.state.Result != nil || recoveringProcess != nil) {
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
	// Startup may rotate an idle session after exact orphan recovery. The first
	// queue checkin binds the recovered session before admission.
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
	if w.Machine.GPU == "cpu" {
		w.Machine.HostMemoryMeasured = true
	}
	if w.HostMemorySource == nil {
		w.HostMemorySource, err = NewHostMemorySource(w.HostMemoryReserveGB)
		if err != nil {
			return err
		}
	}
	if _, err := MemoryCeilingConfig(w.Machine.HostMemoryBudgetGB, w.Machine.MemoryCeilingSlackGB, w.MemorySwapMaxBytes); err != nil {
		return err
	}
	log.Print(w.Machine.MemoryEnforcement)
	w.Machine.HostMemoryMeasured = true
	w.batchMemory = &atomic.Uint64{}
	if s.state.Job != nil {
		w.batchMemory.Store(math.Float64bits(s.state.Job.Spec.Needs.MemoryGB))
	}
	if w.LogRetention == 0 {
		w.LogRetention = DefaultLogRetention
	}
	if w.LogRetention < 0 {
		return errors.New("log-retention must be positive")
	}
	var nextLogCleanup time.Time
	retentionCtx, retentionCancel := context.WithCancel(context.Background())
	type retentionRequest struct {
		active string
		now    time.Time
	}
	retentionRequests := make(chan retentionRequest, 1)
	retentionDone := make(chan struct{})
	go func() {
		defer close(retentionDone)
		for {
			select {
			case <-retentionCtx.Done():
				return
			case request := <-retentionRequests:
				if e := pruneStoppedDocker(retentionCtx, s.dir, w.Machine.ID, request.active, request.now, retention, nil); e != nil && retentionCtx.Err() == nil {
					log.Printf("stopped-container retention (will retry): %v", e)
				}
			}
		}
	}()
	defer func() { retentionCancel(); <-retentionDone }()
	requestRetention := func(active string, now time.Time) {
		if w.DockerEndpoint == "" {
			return // Keep retained custody; disabling a runtime never retires it.
		}
		select {
		case retentionRequests <- retentionRequest{active: active, now: now}:
		default:
		}
	}

	availabilityCtx, availabilityCancel := context.WithCancel(context.Background())
	availabilityDone := make(chan struct{})
	availabilityWake := make(chan struct{}, 1)
	w.availability = newAvailabilityMonitor(w.Clock, w.DailyUse, w.Laptop, w.Desktop)
	if w.OwnerSignals == nil {
		w.OwnerSignals = LocalOwnerSignals
	}
	probeCtx, probeCancel := context.WithTimeout(availabilityCtx, 2*time.Second)
	w.availability.sample(w.OwnerSignals(probeCtx))
	probeCancel()
	go func() {
		defer close(availabilityDone)
		w.availability.run(availabilityCtx, w.OwnerSignals, availabilityWake)
	}()
	defer func() { availabilityCancel(); <-availabilityDone }()
	// Benchmark before any model load or admission, and never over a recovered job.
	// Fake inventory keeps dev/tests light; measured fake runners are tested separately.
	if w.OnboardingBenchmark && w.Benchmark != nil && w.Machine.GPU != "fake" && w.Machine.GPU != "cpu" && s.state.Job == nil {
		if err = w.runBenchmark(stop, s); err != nil {
			return err
		}
	} else if w.OnboardingBenchmark {
		if err = saveWorkerJSON(s.dir, "capability-profile.json", w.Machine.CapabilityProfile); err != nil {
			return err
		}
	}
	rt, err := startModels(w, s.dir)
	if err != nil {
		return err
	}
	rt.authorize = w.authorizeLaunch
	modelAvailabilityDone := make(chan struct{})
	go func() {
		defer close(modelAvailabilityDone)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-availabilityCtx.Done():
				return
			case <-tick.C:
				rt.withdrawLocalAvailability()
			}
		}
	}()
	defer func() {
		availabilityCancel()
		<-modelAvailabilityDone

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
	p := recoveringProcess
	leaving := stop.Err() != nil
	leaveSent := false
	stopCh := stop.Done()
	if w.ExtensionRoot == "" {
		w.ExtensionRoot = filepath.Join(s.dir, "extensions")
	}
	if w.ExtensionRoot != "" {
		probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		w.extensionFingerprint, _ = ExtensionFingerprint(probeCtx, w.Machine.GPU, w.Machine.Device)
		cancel()
	}
	w.Machine.ExtensionFingerprint = w.extensionFingerprint
	w.environmentCache.Fetch = w.fileCache.copyTo
	base := w.Machine.FreeMemoryGB
	fresh := !recovering && s.state.Fresh
	eventCtx, eventCancel := context.WithCancel(context.Background())
	defer eventCancel()
	events := make(chan WorkerEvent, 1)
	go w.watchStops(eventCtx, events)
	w.control = &processControl{clock: w.Clock, wake: make(chan struct{}, 1), availability: w.availability}
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		w.control.run(eventCtx, stop, events)
	}()
	defer func() { eventCancel(); <-controlDone }()
	var retry workerRetry
	probeUnavailable := false
	defer func() {
		if p != nil {
			p.stop(w.Clock.Now(), 0, true)
			if p.docker != nil {
				p.docker.close()
				return
			}
			finished := <-p.done
			if finished.err != nil {
				log.Printf("persist result: %v", finished.err)
			}
		}
	}()
	for {
		now := w.Clock.Now()
		if !now.Before(nextLogCleanup) {
			if err = pruneWorkerLogs(s.dir, s.state.Log, now, w.LogRetention); err != nil {
				return errors.New("worker log retention failed")
			}
			requestRetention(s.state.Identity, now)
			nextLogCleanup = now.Add(time.Minute)
		}
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
				if s.state.OperatorStopped {
					f.result = operatorOutcome(f.result, true)
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
			rt.memoryMachine = w.Machine
			rt.session = w.Machine.Session
			rt.mu.Unlock()
			m := w.Machine
			m.FileTransferError = s.fileError
			if w.ExtensionRoot != "" {
				m.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, m.GPU, w.extensionFingerprint, &m)
				w.Machine.ExtensionReports = m.ExtensionReports
			}
			if m.CapabilityProfile != nil {
				profile := *m.CapabilityProfile
				a := w.availability.snapshot()
				profile.Availability = &a
				m.CapabilityProfile = &profile
			}
			if p != nil && p.docker != nil {
				status := p.docker.status()
				p.docker.mu.Lock()
				m.DockerCause = safeRuntimeCause(p.docker.state.CleanupCause)
				p.docker.mu.Unlock()
				if status != "running" {
					m.DockerCleanup = status
				}
			}
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
			m.Capabilities = append(slices.DeleteFunc(slices.Clone(w.Machine.Capabilities), func(c string) bool { return c == "docker" }), "logs_none")
			if w.DockerEndpoint != "" {
				m.Capabilities = append(m.Capabilities, "docker")
			}
			m.ApprovedModels, m.ModelApprovalStates = rt.cache.modelApprovals(w.ModelConfig.Models)
			rt.setLocalApprovals(m.ApprovedModels, m.ModelApprovalStates)
			reports, _ := rt.report()
			if rt.cache != nil {
				var ce error
				m.CachedModels, m.CacheFreeBytes, ce = rt.cache.report(w.ModelConfig.Models, m.ApprovedModels)
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
			root := s.state.CheckpointRoot
			available, probeErr := s.nasProbe.run(root, func() (bool, error) { err := workerCheckpointAvailable(root); return err == nil, err })
			if !available != probeUnavailable {
				if available {
					log.Printf("worker %s NAS checkpoint probe recovered", m.ID)
				} else {
					log.Printf("worker %s NAS checkpoint probe failed: %v", m.ID, probeErr)
				}
				probeUnavailable = !available
			}
			m.CheckpointUnavailable = !available || s.checkpointUnavailable
			var running *Execution
			if s.state.Job != nil {
				if !w.MemoryMeasured {
					m.FreeMemoryGB = max(0, m.FreeMemoryGB-s.state.Job.Spec.Needs.MemoryGB)
				}
				var progress int64
				if needsCheckpoint(s.state.Job) && available {
					path := attemptCheckpoint(*s.state.Job)
					var progressErr error
					progress, progressErr = s.nasProgress.run(path, func() (int64, error) { return checkpointProgress(path) })
					if progressErr != nil && !errors.Is(progressErr, os.ErrNotExist) {
						m.CheckpointUnavailable = true
						log.Printf("NAS checkpoint progress unavailable: %v", progressErr)
					}
				}
				if s.state.Job.FileTransport && s.state.Result == nil && progress > s.fileGeneration {
					value := *s.state.Job
					transferWorker := w
					published, e := s.fileProgress.run(attemptCheckpoint(value), func() (int64, error) { return transferWorker.publishFileCheckpoint(value, s.dir) })
					if e == nil {
						s.fileGeneration = published
						s.fileError = ""
					} else {
						s.fileError = "checkpoint transfer pending; only published progress can resume elsewhere"
					}
				}
				running = &Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt, Progress: progress}
			}
			var reply Reply
			err = w.Client.Call(ctx, "POST", "/api/workers/checkin", Checkin{Started: p != nil && (p.docker == nil || p.docker.status() == "running") && (p.native == nil || p.native.started()), Machine: m, Running: running, Fresh: fresh, PreviousSession: s.state.PreviousSession, ModelReports: reports}, &reply)
			if err == nil {
				if reply.CPUBudgetCores != nil {
					w.Machine.CPUBudgetCores = *reply.CPUBudgetCores
				}
				if reply.HostMemoryBudget != nil {
					w.Machine.HostMemoryBudgetGB = reply.HostMemoryBudget.GiB
					rt.mu.Lock()
					rt.memoryMachine = w.Machine
					rt.mu.Unlock()
				}
				if s.state.Job != nil && s.state.Job.Spec.Run.Docker && s.state.Result != nil {
					ack := *s.state.Result
					ack.ReleaseOnly = true
					if ackErr := w.Client.Call(ctx, "POST", "/api/workers/complete", ack, nil); ackErr != nil && !stale(ackErr) {
						log.Printf("Docker release acknowledgement: %v", ackErr)
					}
				}
				busy := reply.Job != nil || s.state.Job != nil || leaving || recovering || w.checkAvailability() != nil
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
				keepPreparations := (reply.HostMaintenance == "drained" || reply.HostMaintenance == "disabled") && reply.PreparationStopAt == nil
				prepareWorker := w
				environments.mu.Lock()
				environments.prepare = prepareWorker.prepareRegisteredExtension
				environments.mu.Unlock()
				environments.sync(reply.EnvironmentPlan, busy, keepPreparations)
				rt.syncCopies(reply.CopyPlan, reply.CacheUsage, reply.CachePins, busy, keepPreparations)
				// syncCopies cancels immediately; do not block heartbeats behind an
				// outstanding filesystem syscall on an unavailable NAS. Loads share
				// the cache admission gate and wait for the reservation to release.
				rt.mu.Lock()
				rt.admissionBlocked = reply.AdmissionBlocked
				rt.mu.Unlock()
				rt.sync(reply.ModelPlan)
				fresh = false
				if s.state.Fresh {
					s.state.Fresh = false
					if e := s.save(); e != nil {
						return e
					}
				}
				if reply.FileCheckpoints && !w.fileCheckpoints {
					if w.CheckpointRoot != "" {
						return errors.New("NAS checkpoint-root cannot be combined with file-store checkpoints")
					}
					w.fileCheckpoints = true
					s.state.CheckpointRoot = filepath.Join(s.dir, "file-checkpoints")
					if e := os.MkdirAll(s.state.CheckpointRoot, 0700); e != nil {
						return e
					}
					if e := s.save(); e != nil {
						return e
					}
				}
				if !w.fileCheckpoints && w.CheckpointRoot == "" && reply.CheckpointRoot != "" && reply.CheckpointRoot != s.state.CheckpointRoot {
					s.state.CheckpointRoot = reply.CheckpointRoot
					if e := s.save(); e != nil {
						return e
					}
				}
				if s.state.Job != nil && s.state.Result == nil && p == nil && !recovering {
					if reply.Stop {
						cause := s.startFailure(*s.state.Job, launchRefused, errors.New("assignment withdrawn before checkpoint storage recovered"))
						r := startFailureOutcome(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Error: cause.Error()}, cause)
						s.state.Result = &r
						if e := s.save(); e != nil {
							return e
						}
					} else if !reply.AdmissionBlocked && (!needsCheckpoint(s.state.Job) || available) {
						p, err = w.start(s, *s.state.Job)
						if err != nil {
							if errors.Is(err, errWorkerCheckpoint) || errors.Is(err, errWorkerMemory) || errors.Is(err, errWorkerUnavailable) {
								log.Printf("retry pending start: %v", err)
							} else {
								r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Error: err.Error(), ExtensionUnavailable: errors.Is(err, errExtensionUnavailable), ReleasePending: errors.Is(err, errWorkerReleasePending)}
								r = startFailureOutcome(r, err)
								s.state.Result = &r
								if !r.ReleasePending {
									s.state.PID = 0
								}
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
					if e := p.recordPriorityStop(s, reply.PriorityStop); e != nil {
						return e
					}
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
					} else if s.state.Result != nil && !s.state.Job.Spec.Run.Docker {
						// Reject stale output, but deliver the exact attempt's cleanup status.
						released := *s.state.Result
						released.ReleaseOnly = !released.ReleasePending
						err = w.Client.Call(ctx, "POST", "/api/workers/complete", released, nil)
						if err == nil {
							err = &HTTPError{Status: 409}
						}
					}
					if err == nil && s.state.Result != nil && s.state.Job.FileTransport {
						if !s.state.Result.ReleasePending {
							released := *s.state.Result
							released.ReleaseOnly = true
							released.OutputRefs = nil
							if e := w.Client.Call(ctx, "POST", "/api/workers/complete", released, nil); e != nil && !stale(e) {
								err = e
							}
						}
						value := *s.state.Job
						result := *s.state.Result
						transferWorker := w
						finished, e := s.fileFinish.run(attemptCheckpoint(value), func() (Result, error) { return transferWorker.finishFileResult(value, result, s.dir) })
						if e != nil {
							err = e
							s.fileError = "file publication pending; local results retained"
						} else {
							s.fileError = ""
							s.state.Result = &finished
							err = s.save()
						}
					}
					if err == nil && s.state.Result != nil {
						err = w.Client.Call(ctx, "POST", "/api/workers/complete", s.state.Result, nil)
					}
					if s.state.Result != nil && err == nil && s.state.Job.Spec.Run.Docker && !s.state.Result.ReleasePending {
						if e := acknowledgeDocker(s, w.Machine.ID, now); e != nil {
							return e
						}
					}
					if s.state.Result != nil && (err == nil || stale(err)) {
						s.state.Job = nil
						s.state.PriorityStop = nil
						s.fileGeneration = 0
						s.state.OperatorStopped = false
						s.checkpointUnavailable = false
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
							if e := s.save(); e != nil {
								return e
							}
							eventCancel()
							sessionCtx, sessionCancel := context.WithCancel(context.Background())
							defer sessionCancel()
							go w.watchStops(sessionCtx, events)
							fresh = true
						}
						if e := s.save(); e != nil {
							return e
						}
						if retention == 0 {
							requestRetention("", now)
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
					if j.FileTransport {
						j.Checkpoint = filepath.Join(s.dir, "file-checkpoints", j.ID)
					} else if !filepath.IsAbs(j.Checkpoint) {
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
						if needsCheckpoint(&j) && !available {
							err = nil
						} else {
							p, err = w.start(s, j)
						}
						if err != nil && !(errors.Is(err, errWorkerCheckpoint) || errors.Is(err, errWorkerMemory) || errors.Is(err, errWorkerUnavailable)) {
							r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: err.Error(), ExtensionUnavailable: errors.Is(err, errExtensionUnavailable), ReleasePending: errors.Is(err, errWorkerReleasePending)}
							r = startFailureOutcome(r, err)
							s.state.Job = &j
							if w.batchMemory != nil {
								w.batchMemory.Store(math.Float64bits(j.Spec.Needs.MemoryGB))
							}
							s.state.Result = &r
							if !r.ReleasePending {
								s.state.PID = 0
							}
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
		case <-availabilityWake:
			retry = workerRetry{}
		case <-w.control.wake:
			retry = workerRetry{}
		case <-ticker.C:
		}
	}
}
func (w Worker) String() string {
	return fmt.Sprintf("%s (%s, %.1f GB)", w.Machine.ID, w.Machine.GPU, w.Machine.FreeMemoryGB)
}

func (w Worker) checkHostRAM(gpu, host float64) error {
	if err := w.checkHostMemoryBudget(gpu, host); err != nil {
		return err
	}
	if host == 0 {
		return nil
	}
	if w.HostMemorySource == nil {
		return fmt.Errorf("%w: host RAM source unavailable", errWorkerMemory)
	}
	free, err := w.HostMemorySource(context.Background())
	need := hostNeed(&w.Machine, gpu, host)
	if err != nil || !validMemoryValue(free) || free < need {
		return fmt.Errorf("%w: host RAM need %.1f GiB, available %.1f GiB: %v", errWorkerMemory, need, free, err)
	}
	return nil
}
