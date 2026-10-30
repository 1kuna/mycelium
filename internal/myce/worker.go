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

// Ordinary builds use the real NAS preparation; package fixtures replace it
// before workers start. A worker override still exercises storage faults.
var workerCheckpointPrepare = prepareCheckpoint

// StateDir holds the worker lock, execution identity, logs and pending results.
// An empty StateDir uses the current user's cache, keyed by queue and machine.
type Worker struct {
	hostMemorySamples    map[attemptKey]hostMemorySample
	DiskProbe            func(string) (DiskReading, error)
	diskSampler          *workerDiskSampler
	AcceleratorStopGrace time.Duration
	Enrollment           bool
	DeviceSettings       []DeviceInstall

	// Set only by the installed-service command path, never ordinary join.
	ServiceConfig *ServiceConfig
	fleet         *workerFleetRuntime

	DeviceFaultSource       DeviceFaultSource
	DeviceFaultMatchers     map[string]DeviceFaultMatcher
	faults                  *deviceQuarantine
	faultMonitorUnavailable bool
	InventoryProbe          func(context.Context) ([]MachineDevice, error)
	CPUIsolationProbe       func(context.Context) error
	cpuRuntimeProbe         *MachineProbe // Runtime/OS seam; tests still exercise real verification rules.
	selectedDevice          *MachineDevice
	cpuIsolation            bool
	inventoryAllowance      []string
	inventoryReason         string
	// Synthetic harness seam; nil keeps physical native GPU resolution.
	nativeGPUDeviceProbe func(Worker, map[string]string) ([]string, error)

	// Benchmark runs only when explicitly enabled; fake inventory never loads the host.
	CurveMaxConcurrency   int
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
	sharedInputs          *SharedInputCache
	fileCheckpoints       bool
	ExtensionRoot         string
	JobFileRoot           string
	extensionFingerprint  string
	RequireMemoryCeilings bool
	memoryCeilings        *memoryCeilings
	processMemory         *processMemoryProbe // Without cgroups only.
	MemorySwapMaxBytes    int64
	GPUTelemetryHelper    string // Operator-owned optional read-only telemetry executable.
	DockerEndpoint        string
	dockerEnvelope        bool
	nativeListenerProbe   func(port string, group int) error

	ContainerRetention *time.Duration
	LogRetention       time.Duration
	dockerCallTimeout  time.Duration
	dockerCommand      func(context.Context, ...string) ([]byte, error)
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
	beforeModelCopy      func(context.Context)
	modelEngine          func(Model) Engine
	CopyParallel         int
	CacheRoot, ModelRoot string
	// Operator-owned local runtime/model grants; never derived from run.env.
	NativeReadRoots             []string
	NativeExecuteRoots          []string
	nativeMetricCgroups         []string // Trusted attempt-member cgroups only.
	nativeSharedInputs          []string // Verified acquired payloads, never authored environment paths.
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
	hostRawMemorySource         MemorySource
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
	saveStopIntent   func(*time.Time, bool) error
	savePriorityStop func(*PriorityPreemption) error
	priorityStop     *PriorityPreemption
	outcome          memberOutcome
	saveOutcome      func(memberOutcome) error

	machine    string
	docker     *dockerAttempt
	native     *nativeGroup
	mu         sync.Mutex
	job        Job
	cmd        *exec.Cmd
	done       chan workerFinish
	stopAt     *time.Time
	stopped    bool
	polite     bool
	stopGroup  bool
	userCancel bool
	stopGrace  time.Duration
	killed     bool
}

// Save exact-attempt intent before delivery. Result goroutines read only the
// process copy under p.mu; restart recovery reads the durable worker/daemon copy.
func (p *process) recordPriorityStop(s *workerStore, stop *PriorityPreemption) error {
	if stop == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s != nil {
		s.state.PriorityStop = stop
		if err := s.save(); err != nil {
			return err
		}
	}
	if p.priorityStop != nil {
		return nil
	}
	if s == nil {
		if p.savePriorityStop == nil {
			return errors.New("priority stop receipt unavailable")
		}
		if err := p.savePriorityStop(stop); err != nil {
			return err
		}
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
	p.stopDirective(now, grace, kill, false)
}
func (p *process) stopDirective(now time.Time, grace time.Duration, kill, userCancel bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.userCancel = p.userCancel || userCancel
	p.stopGroup = p.job.ID != "" && !p.job.Spec.Needs.cpuOnly() && !p.userCancel
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
	if p.docker != nil {
		p.docker.mu.Lock()
		if p.userCancel && !p.docker.state.UserCancel {
			p.docker.state.UserCancel = true
			p.docker.dirty = true
		}
		p.docker.mu.Unlock()
	} else if p.stopGroup || p.native != nil && !p.job.Spec.Needs.cpuOnly() {
		if kill {
			grace = p.stopGrace
		}
		grace = min(acceleratorStopGrace(grace), acceleratorStopGrace(p.stopGrace))
		deadline := now.Add(grace)
		if p.stopped && p.stopAt != nil && p.stopAt.Before(deadline) {
			deadline = *p.stopAt
		}
		grace = max(0, deadline.Sub(now))
		kill = grace == 0
	}

	if p.saveStopIntent != nil && !p.job.Spec.Needs.cpuOnly() {
		deadline := now.Add(grace)
		if p.stopAt != nil && p.stopAt.Before(deadline) {
			deadline = *p.stopAt
		}
		_ = p.saveStopIntent(&deadline, p.userCancel)
	}
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

// A helper may exit before main (or main before a helper) after group TERM.
// Neither exit authorizes cleanup KILL before the shared injected-clock deadline.
func (p *process) waitStopGrace(clock Clock, group, anchor int) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		p.mu.Lock()
		waiting := p.stopGroup && p.stopped && !p.killed && p.stopAt != nil
		p.mu.Unlock()
		if !waiting {
			return
		}
		members, err := workerGroupMembers(group)
		populated, populationErr := false, error(nil)
		if p.native != nil {
			populated, populationErr = p.native.memory.outsideGroups(map[int]bool{group: true}, false)
		}
		if err == nil && populationErr == nil && !populated && (len(members) == 0 || len(members) == 1 && members[0] == anchor) {
			return
		}
		p.enforceGrace(clock.Now())
		<-tick.C
	}
}

// p.mu serializes delivery/persistence with exit observation. Capture the
// observation timestamp immediately after Wait, before acquiring this lock.
func (p *process) signalLocked(signal syscall.Signal) {
	exited := p.outcome.Exit != nil
	if exited && !p.stopGroup {
		return
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, signal); err != nil {
		return
	}
	if exited {
		return
	} // A descendant signal cannot rewrite the main's exit facts.
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

type stagedInputs struct {
	directory string
	at        time.Time
}

func (w Worker) start(s *workerStore, j Job) (*process, error) {
	return w.startContext(context.Background(), s, j)
}

func (w Worker) startContext(ctx context.Context, s *workerStore, j Job) (_ *process, startErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if j.Spec.Priority == "production" && j.Spec.StartBy == nil {
		return nil, errors.New("queue start-gate protocol mismatch: production assignment has no start_by; upgrade queue and workers together")
	}
	if err := w.verifyDevice(); err != nil {
		return nil, s.startFailure(j, launchRefused, err)
	}
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
	if err := w.checkHostRAM(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); err != nil {
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
	if err := s.warnNativeIsolation(&j); err != nil {
		return nil, err
	}
	sharedEnv, sharedRelease, sharedErr := w.acquireSharedInputs(ctx, j)
	if sharedErr != nil {
		if errors.Is(sharedErr, errCheckpointIOPending) {
			return nil, fmt.Errorf("%w: shared input copy pending", errWorkerCheckpoint)
		}
		return nil, runtimeErrorFor("input_staging", sharedErr)
	}
	if len(j.Spec.SharedInputs) > 0 {
		s.sharedRelease = sharedRelease
	}
	w.nativeSharedInputs = nil
	for _, path := range sharedEnv {
		w.nativeSharedInputs = append(w.nativeSharedInputs, path)
	}
	slices.Sort(w.nativeSharedInputs)
	sharedStarted := false
	defer func() {
		if !sharedStarted && !errors.Is(startErr, errWorkerReleasePending) {
			sharedRelease()
		}
	}()
	executionWorkdir := j.Spec.Run.Workdir
	if !j.Spec.Run.Docker || j.Spec.extensionJob() || len(jobInputFiles(&j))+len(jobInputPaths(&j))+len(j.Spec.SharedInputs) > 0 {
		value := j
		staged, err := s.extensionFiles.runBounded(ctx, attemptCheckpoint(j), func() (stagedInputs, error) {
			// Keep the slot until the filesystem call really returns, even after
			// this attempt has released. Two copies inside one stage remain allowed.
			if s.inputStaging != nil && !s.inputStaging.CompareAndSwap(false, true) {
				return stagedInputs{}, errCheckpointIOPending
			}
			if s.inputStaging != nil {
				defer s.inputStaging.Store(false)
			}
			return captureStagedInputs(w.Clock, func() (string, error) {
				err := w.prepareExtensionFilesContext(ctx, &value, s.dir)
				return value.Spec.Run.Workdir, err
			})
		}, false)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			if strings.HasPrefix(err.Error(), "NAS unreachable:") {
				return nil, fmt.Errorf("%w: input copy pending", errWorkerCheckpoint)
			}
			return nil, runtimeErrorFor("input_staging", err)
		}
		j.Spec.Run.Workdir = staged.directory
		// Timestamp the actual asynchronous completion, not the retry that
		// consumes it. Preserve the first completion across later start retries.
		if saved := s.state.Job; saved != nil && saved.ID == j.ID && saved.Attempt == j.Attempt && saved.Session == j.Session {
			retainInputsStagedAt(saved, &staged.at)
			j.InputsStagedAt = saved.InputsStagedAt
		} else {
			retainInputsStagedAt(&j, &staged.at)
		}
	}

	executionWork := j.Spec.Run.Workdir
	executionWorkdir, extensionErr = privateNativeExtension(&j, extensionEnv, executionWork, executionWorkdir)
	if extensionErr != nil {
		return nil, extensionErr
	}
	if executionWorkdir != "" {
		j.Spec.Run.Workdir = executionWorkdir
	}
	if err := os.MkdirAll(filepath.Dir(applicationProgressFile(s.dir, j)), 0700); err != nil {
		return nil, err
	}
	ceilingBytes, ceilingErr := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if ceilingErr != nil {
		return nil, ceilingErr
	}
	if w.MemorySource != nil {
		free, e := w.MemorySource(context.Background())
		if e != nil || !validMemoryValue(free) || free < j.Spec.Needs.MemoryGB {
			return nil, fmt.Errorf("%w: need %.1f GB, available %.1f GB: %v", errWorkerMemory, j.Spec.Needs.MemoryGB, free, e)
		}
	}
	prepare := workerCheckpointPrepare
	if j.FileTransport {
		prepare = func(value Job) (string, error) { return w.prepareFileCheckpoint(value, s.dir) }
	}
	if w.checkpointPrepare != nil {
		prepare = w.checkpointPrepare
	}
	var checkpoint string
	var err error
	if needsCheckpoint(&j) {
		checkpoint, err = s.nasPrepare.runBounded(ctx, attemptCheckpoint(j), func() (string, error) { return prepare(j) }, false)
		if err == nil {
			err = ctx.Err()
		}
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
		r.readyOnly = j.Spec.extensionJob() || w.Machine.SharedHost != nil
		buildCtx := context.WithValue(ctx, preparationCustodyKey{}, s.retainPreparation)
		environmentPath, release, err = w.environmentCache.Acquire(withLaunchGate(buildCtx, func() error {
			return w.authorizeLaunch(HostLaunch{Machine: w.Machine.ID, Session: s.state.Session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)})
		}), r)
		if err != nil {
			return nil, s.startFailure(j, environmentFailed, err)
		}
	}
	expandExecution(&j.Spec, executionWork, environmentPath, machineTarget(&w.Machine))
	if !j.Spec.Run.Docker && j.Spec.Run.Workdir == "" {
		j.Spec.Run.Workdir = executionWork
	}
	if err := w.checkHostRAM(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); err != nil {
		return nil, err
	}
	if err := w.checkAvailability(); err != nil {
		return nil, err
	}
	if j.Spec.Run.Docker {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
	if err := ctx.Err(); err != nil {
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
			if err := memoryAttempt.cleanup(w.Clock); err != nil {
				startErr = errors.Join(startErr, fmt.Errorf("%w: %v", errWorkerReleasePending, err))
			}
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
	cmd := exec.Command("/bin/sh", args...)
	if readyWriter != nil {
		cmd.ExtraFiles = []*os.File{readyWriter, approvalReader}
	}
	cmd.Dir = j.Spec.Run.Workdir
	env := jobBaseEnvironment()
	for k, v := range j.Spec.Run.Env {
		env[k] = v
	}
	for k, v := range sharedEnv {
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
	bound, e := w.deviceEnvironment()
	if e != nil {
		return nil, e
	}
	for k, v := range bound {
		env[k] = v
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
	env["MYCE_PROGRESS_FILE"] = applicationProgressFile(s.dir, j)
	if j.Spec.Program != nil {
		env["MYCE_USAGE_FILE"] = programUsageFile(s.dir, identity)
	} else {
		delete(env, "MYCE_USAGE_FILE")
	}
	delete(env, "MYCE_TOKEN")
	// TMPDIR is worker-owned, even when run.env or activation supplied one.
	// Keep it with the managed attempt files so existing custody/retention owns it.
	tmp := filepath.Join(executionWork, ".tmp")
	if err := os.MkdirAll(tmp, 0700); err != nil {
		return nil, err
	}
	env["TMPDIR"], env["TMP"], env["TEMP"] = tmp, tmp, tmp
	scratch := filepath.Join(executionWork, ".scratch")
	if err := os.MkdirAll(scratch, 0700); err != nil {
		return nil, err
	}
	env["MYCE_SCRATCH_DIR"] = scratch
	if len(j.Spec.Run.Sidecars) > 0 || j.Spec.Run.MemoryGB > 0 {
		p, e := w.startNativeGroup(s, j, identity, memoryAttempt, env, environmentPath, extensionRelease, release)
		if e == nil {
			environmentStarted, extensionStarted, sharedStarted, started = true, true, true, true
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
	cmd.Args = append(slices.Clone(cmd.Args[:4]), environmentCommand(cmd.Args[4:], cmd.Env)...)
	if err := w.isolateNativeAttempt(&j, nil, cmd, s.dir, executionWork, env, nil, true); err != nil {
		return nil, s.startFailure(j, launchRefused, reject("native_isolation", err))
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
	s.state.Identity = identity
	s.state.Log = logName
	s.state.Offset = 0
	s.state.Pending = 0
	s.state.Result = nil
	s.state.OperatorStopped = false
	s.state.PID = 0
	s.state.Cgroup = ""
	if memoryAttempt != nil {
		s.state.Cgroup = memoryAttempt.path
	}
	s.state.LaunchPending = true
	s.state.CustodyID = identity
	s.state.Accelerator = !j.Spec.Needs.cpuOnly()
	s.state.StopGrace = acceleratorStopGrace(w.AcceleratorStopGrace)
	s.state.StopAt, s.state.UserCancel = nil, false
	if err = s.save(); err != nil {
		output.Close()
		return nil, err
	}
	if err = w.verifyDevice(); err != nil {
		output.Close()
		return nil, s.startFailure(j, launchRefused, err)
	}
	if err = w.reportNativeCustody(j, s.state.Session, identity, identity, "reserved"); err != nil {
		output.Close()
		return nil, s.startFailure(j, launchRefused, err)
	}
	if err = cmd.Start(); err != nil {
		output.Close()
		return nil, s.startFailure(j, launchFailed, err)
	}
	if readyWriter != nil {
		readyWriter.Close()
	}
	payloadPossible := false
	abort := func(cause error) error {
		if payloadPossible && !j.Spec.Needs.cpuOnly() {
			var deadline *time.Time
			if stopErr := gracefulRecoveredGroup(cmd.Process.Pid, w.Clock, &deadline, w.AcceleratorStopGrace, func() error {
				return saveWorkerJSON(s.dir, identity+".stop.json", workerState{Identity: identity, Accelerator: !j.Spec.Needs.cpuOnly(), StopAt: deadline})
			}); stopErr != nil {
				return errors.Join(cause, fmt.Errorf("%w: %v", errWorkerReleasePending, stopErr))
			}
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		output.Close()
		if releaseErr := waitWorkerGroup(cmd.Process.Pid, w.Clock); releaseErr != nil {
			return errors.Join(cause, fmt.Errorf("%w: %v", errWorkerReleasePending, releaseErr))
		}
		return cause
	}
	if err = applySharedHostPriority(cmd.Process.Pid, w.Machine.SharedHost); err != nil {
		return nil, abort(err)
	}
	if err = memoryAttempt.attach(cmd.Process.Pid); err != nil {
		return nil, abort(fmt.Errorf("memory ceiling attach: %w", err))
	}
	s.state.PID = cmd.Process.Pid
	s.state.LaunchPending = false
	custody := workerState{Accelerator: !j.Spec.Needs.cpuOnly(), CustodyID: identity, PID: s.state.PID, Identity: identity, StopGrace: s.state.StopGrace}
	if len(j.Spec.SharedInputs) > 0 {
		// Shared readers retain group custody after the main PID exits.
		birth, e := processBirth(cmd.Process.Pid)
		if e != nil {
			return nil, abort(e)
		}
		custody.Groups = map[int]map[int]string{cmd.Process.Pid: {cmd.Process.Pid: birth}}
		if memoryAttempt != nil {
			custody.Cgroup = memoryAttempt.path
		}
	}
	if err = saveWorkerJSON(s.dir, "attempt-"+identity+".json", custody); err != nil {
		return nil, abort(err)
	}
	if err = s.save(); err != nil {
		return nil, abort(err)
	}
	if err = w.reportNativeCustody(j, s.state.Session, identity, identity, "started", cmd.Process.Pid); err != nil {
		return nil, abort(s.startFailure(j, launchRefused, err))
	}
	p := &process{job: j, stopGrace: w.AcceleratorStopGrace, cmd: cmd, done: make(chan workerFinish, 1), savePriorityStop: s.priorityStopRecorder()}
	var savedStopAt *time.Time
	var savedUserCancel bool
	p.saveStopIntent = func(deadline *time.Time, userCancel bool) error {
		if savedStopAt != nil && deadline.Equal(*savedStopAt) && savedUserCancel == userCancel {
			return nil
		}
		if err := saveWorkerJSON(s.dir, identity+".stop.json", workerState{Identity: identity, Accelerator: !j.Spec.Needs.cpuOnly(), StopAt: deadline, UserCancel: userCancel}); err != nil {
			return err
		}
		at := *deadline
		savedStopAt, savedUserCancel = &at, userCancel
		return nil
	}
	p.saveOutcome = func(facts memberOutcome) error { return saveWorkerJSON(s.dir, identity+".outcome.json", facts) }
	if w.control != nil {
		w.control.set(p)
	}
	if j.Spec.StartBy == nil {
		if err = w.authorizeStart(j, s.state.Session); err != nil {
			return nil, abort(s.startFailure(j, launchRefused, err))
		}
	}
	payloadPossible = true
	if _, err = gateWriter.Write([]byte("start\n")); err != nil {
		return nil, abort(err)
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
			// Queue waiting must not consume the local exec-receipt I/O budget.
			_ = readyReader.SetReadDeadline(time.Now().Add(2 * time.Second))
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
	result := Result{InputsStagedAt: j.InputsStagedAt, Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	environmentStarted = true
	extensionStarted = true
	sharedStarted = true
	started = true
	finishMemory := w.observeAttemptMemory(memoryAttempt, j, func() []int { return []int{cmd.Process.Pid} })
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
		p.mu.Unlock()
		if !j.Spec.Needs.cpuOnly() {
			p.stop(w.Clock.Now(), acceleratorStopGrace(w.AcceleratorStopGrace), false)
		}
		p.waitStopGrace(w.Clock, cmd.Process.Pid, 0)
		p.mu.Lock()
		result.Forced = p.killed
		p.mu.Unlock()
		// Descendants must not outlive the command or append to delivered output.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		groupErr := waitWorkerGroup(cmd.Process.Pid, w.Clock)
		peak := withExitMaxRSS(finishMemory(), cmd.ProcessState)
		oomErr := memoryAttempt.failure()
		groupErr = errors.Join(groupErr, memoryAttempt.cleanup(w.Clock))
		result.MemoryPeak = peak
		result.ReleasePending = groupErr != nil
		if !result.ReleasePending {
			s.releaseSharedInputs()
		}
		syncErr := syncCommandOutput(output)
		closeErr := output.Close()
		// Payload success is independent of physical release. Uncertain cleanup
		// retains custody while normal output checks and publication proceed.
		result.Success = oomErr == nil && launchErr == nil && waitErr == nil && syncErr == nil && closeErr == nil
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
			result.Error = nativeAttemptError(j, waitErr.Error())
		} else if groupErr != nil {
			result.Error = "cleanup uncertain: " + groupErr.Error()
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
			if e := w.publishExtensionFiles(j, executionWork); e != nil {
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
	if err := ValidateDeviceSettings(w.DeviceSettings); err != nil {
		return err
	}
	if _, err := deviceAllowance(w.Machine.Device); err != nil {
		return err
	}
	if _, err := deviceAllowance(w.DeviceBinding); err != nil {
		return err
	}
	if w.Interval <= 0 {
		w.Interval = time.Second
	}
	if w.Clock == nil {
		w.Clock = RealClock{}
	}
	if w.Machine.MaxJobs < 0 {
		return errors.New("max-jobs must be nonnegative")
	}
	if w.Machine.ID == "" || !validGPU(w.Machine.GPU) || !validMemoryValue(w.Machine.FreeMemoryGB) {
		return errors.New("worker requires a name, GPU platform and finite nonnegative memory budget")
	}
	retention := DefaultContainerRetention
	if w.ContainerRetention != nil {
		retention = *w.ContainerRetention
	}
	if retention < 0 {
		return errors.New("container-retention must be nonnegative")
	}
	w.initMemoryCeilings()
	w.dockerEnvelope = w.dockerEnvelopeSupported()
	if err := w.requireMemoryCeilings(); err != nil {
		return err
	}
	w.checkNativeSeqpacketStartup()
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
		profile := workerProfileProbe().Detect(probeCtx, w.Machine, s.dir, w.Clock.Now())
		cancel()
		applyFixedAcceleratorMemory(&profile, w.Machine, w.FixedAcceleratorMemory)
		w.Machine.CapabilityProfile = &profile
	}

	if w.EnvironmentRoot == "" {
		w.EnvironmentRoot = filepath.Join(s.dir, "environment-cache")
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
	stores, err := s.attemptStores()
	if err != nil {
		return err
	}
	if len(stores) > 0 {
		w.Machine.Session = s.state.Session
	} else {
		if !s.state.Fresh || s.state.Session == "" {
			s.state = workerState{Session: ID(), PreviousSession: s.state.Session, Fresh: true, CheckpointRoot: s.state.CheckpointRoot}
		}
		w.Machine.Session = s.state.Session
	}
	if err = s.save(); err != nil {
		return err
	}
	if err = w.initInventory(stop, s); err != nil {
		return err
	}
	if err = w.initDeviceFaults(stop, s); err != nil {
		return err
	}
	if err = w.prepareEnrollment(stop, s); err != nil {
		return err
	}
	w.fleet = w.newFleetRuntime()
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
	if !validMemoryValue(w.HostMemoryReserveGB) {
		return errors.New("host memory reserve must be finite and nonnegative")
	}
	if w.HostMemorySource == nil {
		w.hostRawMemorySource, err = workerHostMemorySource(0)
		if err != nil {
			return err
		}
		raw, reserve := w.hostRawMemorySource, w.HostMemoryReserveGB
		w.HostMemorySource = func(ctx context.Context) (float64, error) { n, e := raw(ctx); return max(0, n-reserve), e }
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
	retentionCtx, retentionCancel := context.WithCancel(context.Background())
	type retentionRequest struct {
		active string
		now    time.Time
	}
	retentionRequests := make(chan retentionRequest, 1)
	retentionDone := make(chan struct{})
	go func() {
		defer close(retentionDone)
		var lastRetentionErr string
		for {
			select {
			case <-retentionCtx.Done():
				return
			case request := <-retentionRequests:
				// Repeat a persistent error only when it changes, not every minute.
				if e := workerRetention(s.dir, request.now, w.LogRetention); e == nil {
					lastRetentionErr = ""
				} else if e.Error() != lastRetentionErr {
					lastRetentionErr = e.Error()
					log.Printf("worker retention: %v", e)
				}
				if w.DockerEndpoint == "" {
					continue
				}
				if e := pruneStoppedDocker(retentionCtx, s.dir, w.Machine.ID, request.active, request.now, retention, nil); e != nil && retentionCtx.Err() == nil {
					log.Printf("stopped-container retention (will retry): %v", e)
				}
			}
		}
	}()
	defer func() { retentionCancel(); <-retentionDone }()
	requestRetention := func(active string, now time.Time) {
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
		w.OwnerSignals = workerOwnerSignals
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
	if w.OnboardingBenchmark && w.Benchmark != nil && (w.Machine.GPU != "fake" || w.CurveMaxConcurrency > 0) && len(stores) == 0 {
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
	// Payload policy protects the actual listener port, including a configured
	// non-loopback alias and an ephemeral ModelListen port.
	w.ModelListen = strings.TrimPrefix(rt.url, "http://")
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
	if w.ExtensionRoot == "" {
		w.ExtensionRoot = filepath.Join(s.dir, "extensions")
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	w.extensionFingerprint, _ = ExtensionFingerprint(probeCtx, w.Machine.GPU, w.Machine.Device)
	cancel()
	w.Machine.ExtensionFingerprint = w.extensionFingerprint
	probeCtx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	w.Machine.CPUExtensionFingerprint, _ = ExtensionFingerprint(probeCtx, "cpu", "")
	cancel()
	w.environmentCache.Fetch = w.fileCache.copyTo
	return w.runAttempts(stop, s, stores, rt, environments, rawMemory, requestRetention)
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
	free, err := w.sharedHostLiveMemory(context.Background())
	need := hostNeed(&w.Machine, gpu, host)
	if err != nil || !hostMemoryAvailableFits(&w.Machine, gpu, host, free) {
		return fmt.Errorf("%w: host RAM need %g GiB, available %g GiB: %v", errWorkerMemory, need, free, err)
	}
	return nil
}
