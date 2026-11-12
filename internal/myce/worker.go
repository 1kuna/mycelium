package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
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
	storageHealth        *hostHealthMonitor
	hygieneCollector     *workerHygieneCollector
	HostHealthSource     func(context.Context) (HostHealthReading, error)
	diskBudgetWire       *workerDiskBudgetWire
	checkpointExpiry     *workerCheckpointExpiry
	toolReport           *workerToolReport // Serial check-in negotiation, shared by value copies.
	nasInspection        *workerNASInspection
	storageObserver      *storageObserver
	hostMemorySamples    map[attemptKey]hostMemorySample
	DiskProbe            func(string) (DiskReading, error)
	diskSampler          *workerDiskSampler
	diskBudget           *workerDiskBudget
	compilerCache        *workerCompilerCache
	rootMonitor          *workerRootMonitor
	MountRoots           func(context.Context) ([]string, error)
	AcceleratorStopGrace time.Duration
	Enrollment           bool
	DeviceSettings       []DeviceInstall

	// Set only by the installed-service command path, never ordinary join.
	ServiceConfig *ServiceConfig
	fleet         *workerFleetRuntime

	FaultProcessSource      func(map[int]string) ([]int, error)
	FaultHostSource         func() (map[string]string, []string)
	SignalEvidenceSource    func(SignalCapture) *SignalEvidence
	DeviceFaultSource       DeviceFaultSource
	DeviceHealthProbe       DeviceHealthProbe
	DeviceHealthReportProbe DeviceHealthReportProbe
	DeviceFaultMatchers     map[string]DeviceFaultMatcher
	faults                  *deviceQuarantine
	faultSince              time.Time       // Kernel records before worker start are not read (#595).
	faultPending            []KernelLogLine // Matched faults too recent to rule out teardown.
	faultTeardowns          map[string][]time.Time
	faultRecovery           map[string]deviceRecoveryObservation
	faultHealth             map[string]deviceHealthObservation
	faultRejected           map[string]deviceHealthObservation
	faultHealthAfter        time.Time // A monitoring gap invalidates older in-flight health readings.
	faultHealthDue          map[string]deviceHealthDue
	InventoryProbe          func(context.Context) ([]MachineDevice, error)
	CPUIsolationProbe       func(context.Context) error
	cpuRuntimeProbe         *MachineProbe // Runtime/OS seam; tests still exercise real verification rules.
	selectedDevice          *MachineDevice
	selectedDevices         []MachineDevice // Every card of a multi-card attempt.
	cpuIsolation            bool
	inventoryAllowance      []string
	inventoryReason         string
	// Synthetic harness seam; nil keeps physical native GPU resolution.
	nativeGPUDeviceProbe func(Worker, map[string]string) ([]string, error)

	DailyUse                bool
	Laptop                  bool
	Desktop                 bool
	OwnerSignals            OwnerSignalSource
	availability            *availabilityMonitor
	FileCacheRoot           string
	FileCacheLimit          int64
	fileCache               *FileCache
	sharedInputs            *SharedInputCache
	modelSnapshots          *SharedInputCache // Budgeted by CacheLimit, not FileCacheLimit.
	fileCheckpoints         bool
	ExtensionRoot           string
	JobFileRoot             string
	extensionFingerprint    string
	RequireMemoryCeilings   bool
	memoryCeilings          *memoryCeilings
	sharedExecutionCeilings *memoryCeilings
	sharedCPUDev            bool
	SharedPressureSource    func(context.Context) (SharedHostPressure, error)
	processMemory           *processMemoryProbe // Without cgroups only.
	MemorySwapMaxBytes      int64
	GPUTelemetryHelper      string // Operator-owned optional read-only telemetry executable.
	DockerEndpoint          string
	dockerEnvelope          bool
	nativeListenerProbe     func(port string, group int) error

	ContainerRetention *time.Duration
	LogRetention       time.Duration
	// Zero uses DefaultExtensionRetention; negative keeps every registration.
	ExtensionRetention time.Duration
	dockerCallTimeout  time.Duration
	dockerCommand      func(context.Context, ...string) ([]byte, error)
	dockerDaemon       func(context.Context, ...string) ([]byte, error) // In-process daemon; no CLI transport timing.
	Unit               string

	SuperviseProcess bool

	checkpointPrepare     func(Job) (string, error) // Filesystem fault seam.
	control               *processControl
	cancelStart           context.CancelFunc
	controlLatency        *workerControlLatency
	launchCtx             context.Context // Exact attempt/preparation owner; cancels handshake waits.
	serviceStop           context.Context // Installed TERM cancels observation/I/O, not durable cleanup.
	batchAdmission        func(context.Context, HostLaunch) error
	stopAdmission         func()
	DeviceBinding         string
	EnvironmentSourceRoot string
	EnvironmentRoot       string
	EnvironmentLimit      int64
	environmentCache      *EnvironmentCache
	environmentSizeLimit  func(string) (int64, error) // Test seam for cache filesystem headroom.
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
	nativeSharedInputs          []string // Verified executable shared inputs, never authored paths.
	nativeModelInputs           []string // Verified model data: reads only, including custom-code sources.
	nativeCI                    bool     // Current attempt declared isolation: ci (#1298).
	modelMounts                 []dockerFileMount
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
	cancelStart      context.CancelFunc // Immutable; cancel before taking a launch-held mutex.
	stopAdmission    func()
	resources        *resourceObserver
	saveStopIntent   func(*time.Time, bool) error
	savePriorityStop func(*PriorityPreemption) error
	priorityStop     *PriorityPreemption
	outcome          memberOutcome
	exitObservation  nativeExitObservation
	saveOutcome      func(memberOutcome) error
	saveCustody      func() error
	refreshCustody   func() error
	custodyAt        time.Time

	machine    string
	docker     *dockerAttempt
	native     *nativeGroup
	memory     *memoryAttempt // Plain command cgroup; carries live device samples.
	rssMemory  atomic.Pointer[processResidentSample]
	mu         sync.Mutex
	job        Job
	cmd        *exec.Cmd
	birth      string // Recorded at launch; never adopt a subsequently reused PID.
	done       chan workerFinish
	stopAt     *time.Time
	stopped    bool
	polite     bool
	stopGroup  bool
	userCancel bool
	shutdown   bool // Tags delivered service stops, including checkpoint handlers exiting zero.
	stopGrace  time.Duration
	killed     bool
	// Injected-clock time of the first stop request: teardown evidence (#595).
	stopRequested time.Time
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

func (p *process) cancelStarting() {
	if p.cancelStart != nil {
		p.cancelStart()
	}
}

func (p *process) stop(now time.Time, grace time.Duration, kill bool) {
	p.stopDirective(now, grace, kill, false)
}
func (p *process) stopService(now time.Time, grace time.Duration) {
	p.cancelStarting()
	if p.stopAdmission != nil {
		p.stopAdmission()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shutdown = true
	if p.docker != nil {
		p.docker.mu.Lock()
		if !p.docker.state.Shutdown {
			p.docker.state.Shutdown = true
			p.docker.dirty = true
		}
		p.docker.mu.Unlock()
	}
	p.stopGroup = !p.job.Spec.Needs.cpuOnly() && !p.userCancel
	p.stopLocked(now, grace, false)
}
func (p *process) stopDirective(now time.Time, grace time.Duration, kill, userCancel bool) {
	p.cancelStarting()
	if p.stopAdmission != nil {
		p.stopAdmission()
	}
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

// Refresh escaper custody while the attempt runs, so a setsid child whose
// parent exits before the stop is still known by birth (#1322). The cadence
// matches the Darwin CPU tree sample; each refresh is one ps inventory.
func (p *process) refreshEscapers(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refreshCustody == nil || p.killed || now.Sub(p.custodyAt) < attemptStorageSamplePeriod {
		return
	}
	p.custodyAt = now
	_ = p.refreshCustody() // A failed refresh keeps the prior custody; stop rechecks.
}

func (p *process) stopLocked(now time.Time, grace time.Duration, kill bool) {
	if p.stopAdmission != nil {
		p.stopAdmission()
	}
	if p.stopRequested.IsZero() {
		p.stopRequested = now
	}
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
			p.killed = p.signalLocked(syscall.SIGKILL)
		}
		return
	}
	deadline := now.Add(grace)
	if p.stopAt != nil && deadline.Before(*p.stopAt) {
		p.stopAt = &deadline
	}
	if !p.stopped {
		if !p.signalLocked(syscall.SIGTERM) {
			return
		}
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

func (p *process) signalLocked(signal syscall.Signal) bool {
	return p.signalLockedUsing(signal, func(delivered func()) error {
		return signalWorkerSession(p.cmd.Process.Pid, signal, delivered)
	})
}

func (p *process) signalLockedUsing(signal syscall.Signal, deliver func(func()) error) bool {
	p.exitObservation.mu.Lock()
	defer p.exitObservation.mu.Unlock()
	if p.exitObservation.exit != nil {
		p.outcome.Exit = p.exitObservation.exit
	}
	exited := p.outcome.Exit != nil
	if exited && !p.stopGroup {
		return true
	}
	if p.saveCustody != nil {
		if err := p.saveCustody(); err != nil {
			return false
		}
	}
	err := deliver(func() {
		if !exited {
			p.recordSignalLocked(signal)
		}
	})
	return err == nil
}

func (p *process) recordSignalLocked(signal syscall.Signal) {
	at := time.Now().UTC()
	name := "TERM"
	if signal == syscall.SIGKILL {
		name = "KILL"
	}
	next := p.outcome
	next.Signals = append(append([]deliveredSignal(nil), next.Signals...), deliveredSignal{Signal: name, At: at, Shutdown: p.shutdown, BeforeExit: true})
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

func (w Worker) startContext(ctx context.Context, s *workerStore, j Job) (startedProcess *process, startErr error) {
	launchCtx, cancel := context.WithCancel(ctx)
	w.launchCtx, w.cancelStart = launchCtx, cancel
	// Input/checkpoint copies outlive a pending start observation. Their ctx
	// remains the preparation owner's; only this call's handshakes are retired
	// when it returns without a process.
	defer func() {
		if startedProcess == nil {
			cancel()
		}
	}()
	// Start preparation can pause and retry without advancing the attempt.
	j.ExecutionEvidence = s.startExecutionEvidence(j)
	defer func() {
		if startErr != nil {
			startErr = &executionEvidenceError{startErr, safeExecutionEvidence(j.ExecutionEvidence, j.Attempt)}
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if j.Spec.Priority == "production" && j.Spec.StartBy == nil {
		return nil, errors.New("queue start-gate protocol mismatch: production assignment has no start_by; upgrade queue and workers together")
	}
	if parkedStorage(&j) {
		return nil, s.startFailure(j, launchRefused, errors.New(parkedStorageReason))
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
	w.Machine.UnavailableRoots = w.sampleStorageRoots(ctx, s.state.CheckpointRoot)
	if reason := w.storageRootReason(&j); reason != "" {
		return nil, s.startFailure(j, launchRefused, errors.New(reason))
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
	sharedEnv, sharedRelease, sharedErr := w.acquireSharedInputs(context.WithValue(context.WithValue(ctx, sharedInputOwnerKey{}, s), inputTransferRecoveryKey{}, &s.inputTransferRecovering), j)
	if sharedErr != nil {
		if errors.Is(sharedErr, errCheckpointIOPending) {
			return nil, fmt.Errorf("%w: shared input copy pending", errWorkerCheckpoint)
		}
		return nil, w.stagingFailure(s, j, sharedErr)
	}
	compilerPath, compilerRelease, compilerErr := w.compilerCache.acquire(ctx, s, j)
	if compilerErr != nil {
		sharedRelease()
		return nil, compilerErr
	}
	inputRelease := sharedRelease
	sharedRelease = func() { compilerRelease(); inputRelease() }
	if !j.Spec.Run.Docker {
		s.sharedRelease = sharedRelease // Docker releases when its observer exits.
	}
	w.modelMounts = dockerModelMounts(j, sharedEnv)
	w.nativeInputPaths(j, sharedEnv)
	sharedStarted := false
	defer func() {
		if !sharedStarted && !errors.Is(startErr, errWorkerReleasePending) {
			sharedRelease()
		}
	}()
	executionWorkdir := j.Spec.Run.Workdir
	if !j.Spec.Run.Docker || dockerFileJob(j.Spec) || j.Spec.extensionJob() || len(jobInputFiles(&j))+len(jobInputPaths(&j)) > 0 || sharedReaders(j.Spec) {
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
				transferCtx := context.WithValue(ctx, inputTransferRecoveryKey{}, &s.inputTransferRecovering)
				err := w.prepareExtensionFilesContext(transferCtx, &value, s.dir)
				return value.Spec.Run.Workdir, err
			})
		}, false)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			if errors.Is(err, errCheckpointIOPending) || errors.Is(err, errCheckpointIOTimeout) {
				return nil, fmt.Errorf("%w: input copy pending", errWorkerCheckpoint)
			}
			return nil, w.stagingFailure(s, j, err)
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
	if !j.Spec.Run.Docker {
		if err := checkPayloadMachOLinkage(ctx, machineTarget(&w.Machine), executionWork); err != nil {
			return nil, err
		}
	}
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
	ceilingBytes, ceilingErr := w.jobCeilingBytes(j)
	if ceilingErr != nil {
		return nil, ceilingErr
	}
	if w.MemorySource != nil {
		free, e := w.MemorySource(context.Background())
		if e != nil || !validMemoryValue(free) || free < j.Spec.Needs.MemoryGB {
			return nil, refuseAdmission("live_gpu_memory", "GiB", j.Spec.Needs.MemoryGB, free, e != nil)
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
		checkpoint, err = s.nasPrepare.runBounded(ctx, attemptCheckpoint(j), func() (string, error) {
			path, err := prepare(j)
			w.observeStorageError(err)
			return path, err
		}, false)
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
	if r, ok := commandEnvironmentRecipe(&j, j.EnvironmentRecipes, &w.Machine); ok {
		if r.BinaryRelative != "" && w.EnvironmentSourceRoot != "" {
			r.Binary = filepath.Join(w.EnvironmentSourceRoot, r.BinaryRelative)
		}
		if w.environmentCache == nil {
			return nil, s.startFailure(j, environmentFailed, errors.New("environment builder unavailable"))
		}
		r.readyOnly = j.CommandDiscovery != nil || j.Spec.extensionJob() || w.Machine.SharedHost != nil
		buildCtx := context.WithValue(ctx, preparationCustodyKey{}, s.retainPreparation)
		j.ExecutionEvidence.EnvironmentPreparation = executionMayHaveRun
		environmentPath, release, err = w.environmentCache.Acquire(withLaunchGate(buildCtx, func(ctx context.Context) error {
			return w.authorizeLaunchContext(ctx, HostLaunch{Machine: w.Machine.ID, Session: s.state.Session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)})
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
		environmentRelease := release
		// Docker recovery/daemon boundaries are not observed by this native proof.
		j.ExecutionEvidence.Main, j.ExecutionEvidence.Sidecars = executionUnknown, executionUnknown
		p, e := w.startDocker(s, j, checkpoint, func() {
			if environmentRelease != nil {
				environmentRelease()
			}
			sharedRelease()
		})
		environmentStarted, sharedStarted = e == nil, e == nil
		return p, e
	}
	env := jobBaseEnvironment()
	if compilerPath != "" {
		env["GOCACHE"] = compilerPath
	}
	for k, v := range j.Spec.Run.Env {
		env[k] = v
	}
	for k, v := range sharedEnv {
		env[k] = v
	}
	for k, v := range extensionEnv {
		env[k] = v
	}
	if compilerPath != "" && env["GOCACHE"] != compilerPath {
		w.compilerCache.external.Store(true)
	}
	if environmentPath != "" {
		bindEnvironment(env, environmentPath)
		if j.Spec.Run.Workdir == executionWork {
			if err := linkNodeModules(executionWork, environmentPath); err != nil {
				return nil, s.startFailure(j, environmentFailed, err)
			}
		}
	}
	if missing := missingExecutableTools(j.Spec.Needs.Tools, env["PATH"]); len(missing) > 0 {
		return nil, toolMissingError(w.Machine.ID, missing)
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
	memoryAttempt, err := w.executionCeilings().create("attempt-"+identity, ceilingBytes, w.MemorySwapMaxBytes)
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
	args := []string{"-c", `read gate || exit 125; trap 'wait "$child"; exit $?' TERM; "$@" < /dev/null 5>&- & child=$!; printf '%s\n' "$child" >&5; exec 5>&-; exec 3>&-; wait "$child"; exit $?`, "myce-supervisor-" + identity, command}
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
	jobCPUEnvironment(env, w.Machine.SharedHost, j.Spec.Needs.cores())
	// TMPDIR is worker-owned, even when run.env or activation supplied one.
	// Keep it with the managed attempt files so existing custody/retention owns it.
	tmp, err := attemptTempDir(s.dir, executionWork)
	if err != nil {
		return nil, err
	}
	env["TMPDIR"], env["TMP"], env["TEMP"] = tmp, tmp, tmp
	scratch := filepath.Join(executionWork, ".scratch")
	if err := os.MkdirAll(scratch, 0700); err != nil {
		return nil, err
	}
	env["MYCE_SCRATCH_DIR"] = scratch
	if nativeCIRequired(&j) {
		// The worker user's home is hidden from ci jobs; tools that write
		// config and caches under HOME (git, Go, npm, pip) use attempt scratch.
		home := filepath.Join(executionWork, ".home")
		if err := os.MkdirAll(home, 0700); err != nil {
			return nil, err
		}
		env["HOME"] = home
		// Git discovery must not climb into a hidden parent repository (a
		// dotfiles home, say), where a denied read is fatal rather than skipped.
		if env["GIT_CEILING_DIRECTORIES"] == "" {
			env["GIT_CEILING_DIRECTORIES"] = filepath.Dir(executionWork)
		}
	}
	w.storageObserver = w.observeAttemptStorage(j, s.dir, checkpoint, identity, workerStorageMetadata(s))
	s.storageObserver = w.storageObserver
	defer func() {
		if !started {
			w.storageObserver.finish()
		}
	}()
	if os.Getenv(processSessionVariable) != "" {
		env[processSessionVariable] = "1"
	}
	if len(j.Spec.Run.Sidecars) > 0 || j.Spec.Run.MemoryGB > 0 {
		p, e := w.startNativeGroup(s, j, identity, memoryAttempt, env, environmentPath, extensionRelease, release)
		if e == nil {
			environmentStarted, extensionStarted, sharedStarted, started = true, true, true, true
		}
		return p, e
	}
	env[processSessionVariable] = "1"
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		cmd.Env = append(cmd.Env, k+"="+env[k])
	}
	cmd.Args = append(slices.Clone(cmd.Args[:4]), sharedHostCommand(environmentCommand(cmd.Args[4:], cmd.Env), w.Machine.SharedHost)...)
	if err := w.isolateNativeAttempt(&j, nil, cmd, s.dir, executionWork, env, nil, true); err != nil {
		return nil, s.startFailure(j, launchRefused, reject("native_isolation", err))
	}
	cmd.SysProcAttr = commandProcessAttributes()
	logName := identity + ".log"
	output, err := openCommandOutput(s.dir, logName, j.Spec.Logs, os.O_EXCL)
	if err != nil {
		return nil, err
	}
	if j.Spec.Logs == "none" {
		logName = ""
	}
	childReader, childWriter, err := os.Pipe()
	if err != nil {
		output.Close()
		return nil, err
	}
	defer childReader.Close()
	defer childWriter.Close()
	cmd.ExtraFiles = []*os.File{readyWriter, approvalReader, childWriter}
	childPID := 0
	signalScope := SignalCapture{StartedAt: w.Clock.Now(), Directory: cmd.Dir}
	if memoryAttempt != nil {
		signalScope.Cgroup = signalCgroup(memoryAttempt.path)
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
	assigned := j
	assigned.ExecutionEvidence = nil
	s.state.Job = &assigned
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
	childWriter.Close()
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
		_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
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
	{
		// Birth custody also covers session members after their leader exits (#579).
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
	p := &process{cancelStart: w.cancelStart, job: j, stopAdmission: w.stopAdmission, stopGrace: w.AcceleratorStopGrace, cmd: cmd, birth: custody.Groups[cmd.Process.Pid][cmd.Process.Pid], done: make(chan workerFinish, 1), savePriorityStop: s.priorityStopRecorder(), memory: memoryAttempt}
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
	p.saveCustody = func() error {
		if err := recordWorkerGroup(&custody, cmd.Process.Pid); err != nil {
			return err
		}
		return saveWorkerJSON(s.dir, "attempt-"+identity+".json", custody)
	}
	if memoryAttempt == nil {
		// Without a cgroup, an escaper is reachable only while it is still a
		// descendant (#1322): record the live tree on the attempt's own cadence.
		p.refreshCustody = func() error {
			group := cmd.Process.Pid
			saved := maps.Clone(custody.Groups[group])
			current := workerState{}
			if err := recordWorkerGroup(&current, group); err != nil {
				return err
			}
			next := current.Groups[group]
			next[group] = saved[group]
			if maps.Equal(saved, next) {
				return nil
			}
			custody.Groups[group] = next
			return saveWorkerJSON(s.dir, "attempt-"+identity+".json", custody)
		}
	}
	p.saveOutcome = func(facts memberOutcome) error { return saveWorkerJSON(s.dir, identity+".outcome.json", facts) }
	if w.control != nil {
		w.control.set(p)
	}
	if j.Spec.StartBy == nil {
		if err = w.authorizeStart(j, s.state.Session, s.dir); err != nil {
			return nil, abort(s.startFailure(j, launchRefused, err))
		}
	}
	payloadPossible = true
	if readyReader == nil {
		j.ExecutionEvidence.Main = executionMayHaveRun
	}
	if _, err = gateWriter.Write([]byte("start\n")); err != nil {
		return nil, abort(err)
	}
	// The shell's wait status cannot distinguish a signal from exit(139).
	// Retain the actual child PID, then require a matching kernel/core record
	// before calling a shell-reported status a confirmed signal.
	_ = childReader.SetReadDeadline(time.Now().Add(2 * time.Second))
	if raw, e := io.ReadAll(io.LimitReader(childReader, 32)); e == nil {
		childPID, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		signalScope.PID = childPID
		signalScope.Birth, _ = processBirth(childPID)
		if commandNamespaceLauncher(cmd) {
			signalScope.PID = 0
			signalScope.Birth = ""
		}
	}
	if readyReader != nil {
		// Bound receipt I/O; queue startup decisions still use its injected Clock.
		_ = readyReader.SetReadDeadline(time.Now().Add(2 * time.Second))
		var ready [1]byte
		if _, e := io.ReadFull(readyReader, ready[:]); e != nil || ready[0] != 'R' {
			return nil, abort(fmt.Errorf("command start gate unavailable: %v", e))
		}
		err = w.authorizeStart(j, s.state.Session, s.dir)
		if err == nil {
			// Queue waiting must not consume the local exec-receipt I/O budget.
			_ = readyReader.SetReadDeadline(time.Now().Add(2 * time.Second))
			j.ExecutionEvidence.Main = executionMayHaveRun
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
			p.mu.Lock()
			p.outcome.Infrastructure = recorded.facts.Infrastructure
			p.mu.Unlock()
		}
	}
	result := Result{ExecutionEvidence: safeExecutionEvidence(j.ExecutionEvidence, j.Attempt), InputsStagedAt: j.InputsStagedAt, Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	environmentStarted = true
	extensionStarted = true
	sharedStarted = true
	started = true
	p.resources = w.observeResources(p, memoryAttempt)
	finishMemory := w.observeAttemptMemory(memoryAttempt, j, func() []int { return []int{cmd.Process.Pid} }, p)
	go func() {
		if extensionRelease != nil {
			defer extensionRelease()
		}
		if release != nil {
			defer release()
		}
		waitErr := cmd.Wait()
		observed := p.exitObservation.observe(cmd)
		observationErr := collectPublishedExit(observed, func(exit *memberExit) error {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.outcome.Exit = exit
			return p.saveOutcome(p.outcome)
		}, func(exit *memberExit) { w.captureSignalEvidencePID(childPID, cmd.Dir, exit, true, signalScope) })
		if !j.Spec.Needs.cpuOnly() {
			p.stop(w.Clock.Now(), acceleratorStopGrace(w.AcceleratorStopGrace), false)
		}
		p.waitStopGrace(w.Clock, cmd.Process.Pid, 0)
		p.mu.Lock()
		result.Forced = p.killed
		p.mu.Unlock()
		// Descendants must not outlive the command or append to delivered output.
		p.mu.Lock()
		custodyErr := p.saveCustody()
		p.mu.Unlock()
		groupErr := errors.Join(custodyErr, signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL), waitWorkerGroup(cmd.Process.Pid, w.Clock))
		peak := withExitMaxRSS(finishMemory(), cmd.ProcessState)
		result.Resources = p.resources.finish()
		result.Resources = withExitCPU(result.Resources, cmd.ProcessState)
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
			if errors.Is(launchErr, errWorkerMemory) {
				// Started authorizes the exec gate; a later local comparison can
				// still refuse without main ever running. Match early/group starts.
				result.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit"}
			}
		} else if waitErr != nil {
			result.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("command", waitErr))
		}
		if groupErr != nil {
			result.CleanupCause = runtimeDiagnostic(runtimeErrorFor("process_group", groupErr))
		} else if syncErr != nil || closeErr != nil {
			result.CleanupCause = runtimeDiagnostic(runtimeErrorFor("persist", errors.Join(syncErr, closeErr)))
		}
		if observed.Status == 0 && result.ExecutionCause == nil && (syncErr != nil || closeErr != nil) {
			result = finalizationResult(result, "log_persistence", "", errors.Join(syncErr, closeErr), true)
			zero := 0
			result.ExecutionCause.Finalization.CommandExitCode = &zero
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
				result = finalizationResult(result, "output_publication", "", e, true)
			}
		}
		result.StoragePeak = w.storageObserver.finish()
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
	w.Client.faultWire = &atomic.Bool{}
	w.launchCtx = stop
	if w.ServiceConfig != nil {
		w.serviceStop = stop
	}
	w.controlLatency = &workerControlLatency{}
	w.toolReport = &workerToolReport{}
	w.diskBudgetWire = &workerDiskBudgetWire{}
	w.checkpointExpiry = newWorkerCheckpointExpiry()
	if w.rootMonitor == nil {
		w.rootMonitor = &workerRootMonitor{expected: map[string]string{}}
	}
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
	w.diskBudget = newWorkerDiskBudget(w.Clock, w.DiskProbe)
	w.diskBudget.inventoryDir = s.dir
	w.diskBudget.add(diskBudgetRoot{name: "attempts", path: s.dir})
	w.compilerCache = &workerCompilerCache{cache: EnvironmentCache{Root: filepath.Join(s.dir, "compiler-cache"), Clock: w.Clock}}
	if err := w.compilerCache.cache.initStorage(); err != nil {
		return err
	}
	w.diskBudget.add(diskBudgetRoot{name: "compiler", path: w.compilerCache.cache.Root, candidates: w.compilerCache.budgetCandidates})
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
	w.environmentCache = &EnvironmentCache{Root: w.EnvironmentRoot, Limit: w.EnvironmentLimit, Clock: w.Clock, Run: w.environmentRun, sizeLimit: w.environmentSizeLimit}
	w.environmentCache.budget = w.diskBudget
	w.environmentCache.references = w.environmentReferenced
	if err = w.environmentCache.Cleanup(context.Background()); err != nil {
		return err
	}
	w.diskBudget.add(diskBudgetRoot{name: "environments", path: w.environmentCache.Root, candidates: w.environmentCache.budgetCandidates})
	environments := &environmentRuntime{authorize: w.authorizeLaunchContext, dir: s.dir, cache: w.environmentCache, reports: map[string]EnvironmentReport{}}
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
	if w.ExtensionRetention == 0 {
		w.ExtensionRetention = DefaultExtensionRetention
	}
	if w.LogRetention == 0 {
		w.LogRetention = DefaultLogRetention
	}
	if w.LogRetention < 0 {
		return errors.New("log-retention must be positive")
	}
	retentionCtx, retentionCancel := context.WithCancel(stop)
	type retentionRequest struct {
		active string
		now    time.Time
	}
	retentionRequests := make(chan retentionRequest, 1)
	retentionDone := make(chan struct{})
	w.hygieneCollector = &workerHygieneCollector{w: w}
	go func() {
		defer close(retentionDone)
		var lastRetentionErr string
		for {
			select {
			case <-retentionCtx.Done():
				return
			case request := <-retentionRequests:
				w.hygieneCollector.collect(retentionCtx)
				// Repeat a persistent error only when it changes, not every minute.
				if e := w.retainWorkerFiles(retentionCtx, s.dir, request.now); e == nil {
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
	if w.serviceStop != nil {
		stopAvailability := context.AfterFunc(w.serviceStop, availabilityCancel)
		defer stopAvailability()
	}
	availabilityDone := make(chan struct{})
	availabilityWake := make(chan struct{}, 1)
	w.availability = newAvailabilityMonitor(w.Clock, w.DailyUse, w.Laptop, w.Desktop)
	if w.OwnerSignals == nil {
		w.OwnerSignals = workerOwnerSignals
	}
	w.availability.lane.timing = probeTiming{base: hostProbeControlPeriod(w.Interval), cores: w.Machine.EffectiveCPUCores}
	go func() {
		defer close(availabilityDone)
		w.availability.run(availabilityCtx, w.OwnerSignals, availabilityWake)
	}()
	defer func() { availabilityCancel(); <-availabilityDone }()
	// All worker copies, including the model runtime and launch closures, must
	// capture the same live health owner before any model plan can be accepted.
	health, err := startWorkerHealth(availabilityCtx, w, s)
	if err != nil {
		return err
	}
	w.storageHealth = health
	defer func() { availabilityCancel(); health.wait() }()
	rt, err := startModels(w, s.dir)
	if err != nil {
		return err
	}
	// Payload policy protects the actual listener port, including a configured
	// non-loopback alias and an ephemeral ModelListen port.
	w.ModelListen = strings.TrimPrefix(rt.url, "http://")
	rt.authorize = w.authorizeLaunchContext
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
	probeCtx, cancel := context.WithTimeout(stop, 15*time.Second)
	w.extensionFingerprint, _ = ExtensionFingerprint(probeCtx, w.Machine.GPU, w.Machine.Device)
	cancel()
	w.Machine.ExtensionFingerprint = w.extensionFingerprint
	probeCtx, cancel = context.WithTimeout(stop, 15*time.Second)
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
	if host == 0 && (!unifiedCUDA(&w.Machine) || gpu == 0) {
		return nil
	}
	if w.HostMemorySource == nil {
		return refuseAdmission("live_host_ram", "GiB", hostNeed(&w.Machine, gpu, host), 0, true)
	}
	free, err := w.sharedHostLiveMemory(context.Background())
	need := hostNeed(&w.Machine, gpu, host)
	if err != nil || !hostMemoryAvailableFits(&w.Machine, gpu, host, free) {
		return refuseAdmission("live_host_ram", "GiB", need, free, err != nil)
	}
	return nil
}
