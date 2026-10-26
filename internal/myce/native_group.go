package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var errNativePortCollision = errors.New("native sidecar address already in use")

const nativeBindAttempts = 3 // Whole-group preparation attempts, including the first.

// The anchor preserves the existing attempt PGID/recovery contract. All members
// inherit its cgroup and join its process group before their exec gate opens.
// Members attach to child cgroups before their exec gates open.
type nativeMember struct {
	memberOutcome
	Name     string `json:"name"`
	PID      int    `json:"pid,omitempty"`
	Birth    string `json:"birth,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Phase    string `json:"phase"`
	Cgroup   string `json:"cgroup,omitempty"`
	memory   *memoryAttempt
	MemoryGB float64 `json:"memory_gb,omitempty"`
	cmd      *exec.Cmd
	done     chan struct{}
	err      error
}
type nativeGroup struct {
	stageFailure           bool // guarded by p.mu
	p                      *process
	worker                 Worker
	dir, identity, session string
	members                []*nativeMember
	ctx                    context.Context
	cancel                 context.CancelFunc
	main                   *nativeMember // guarded by p.mu
	releases               []func()
	memory                 *memoryAttempt
	control                *memoryAttempt
	memberMemory           map[string]*memoryAttempt
	custody                workerState
	supervisor             *exec.Cmd
}

func (g *nativeGroup) started() bool {
	g.p.mu.Lock()
	defer g.p.mu.Unlock()
	return g.main != nil
}

func nativeSidecarCount(j *Job, m *Machine) int {
	if j.Spec.Execution != nil {
		return len(j.Spec.Execution.Sidecars)
	}
	if len(j.Spec.Run.Sidecars) > 0 {
		return len(j.Spec.Run.Sidecars)
	}
	if m != nil && j.Extension != nil {
		for _, e := range j.Extensions {
			if e.Digest == j.Extension.Digest {
				if v := e.variant(machineTarget(m)); v != nil {
					return len(v.Execution.Sidecars)
				}
			}
		}
	}
	return 0
}

func (g *nativeGroup) save() error {
	g.p.mu.Lock()
	defer g.p.mu.Unlock()
	return g.saveLocked()
}

func (g *nativeGroup) saveLocked() error {
	if err := saveWorkerJSON(g.dir, "attempt-"+g.identity+".json", g.custody); err != nil {
		return err
	}
	return saveWorkerJSON(g.dir, "native-"+g.identity+".json", struct {
		Identity string          `json:"identity"`
		Members  []*nativeMember `json:"members"`
		Outcome  memberOutcome   `json:"outcome"`
	}{g.identity, g.members, g.p.outcome})
}

// Called under p.mu, also held across fork/persistence/exec approval. A stop
// cannot race another member into execution. CPU main keeps its checkpoint
// helpers; non-user accelerator stops terminate all members together.
func (g *nativeGroup) stopLocked(now time.Time, grace time.Duration, kill bool) {
	p := g.p
	firstDelivery := !p.stopped || p.stopAt == nil
	if !p.stopped && !p.killed && g.memberFailure() != nil {
		g.stageFailure = true
	}
	g.cancel()
	cancelChanged := g.custody.UserCancel != p.userCancel
	g.custody.UserCancel = p.userCancel
	deadline := now.Add(grace)
	if p.stopAt == nil || deadline.Before(*p.stopAt) || cancelChanged {
		p.stopAt = &deadline
		g.custody.StopAt = &deadline
		if err := saveWorkerJSON(g.dir, "attempt-"+g.identity+".json", g.custody); err != nil {
			// Failed stop persistence cannot authorize additional execution.
			if p.stopGroup {
				g.signalLocked(nil, syscall.SIGTERM)
				p.stopped = true
			} else {
				g.signalLocked(nil, syscall.SIGKILL)
				p.killed = true
			}
		}
	}
	if kill {
		if !p.killed {
			g.signalLocked(nil, syscall.SIGKILL)
			p.killed = true
		}
		return
	}
	if firstDelivery {
		p.stopped = true
		if g.main != nil && !p.stopGroup {
			g.signalLocked(g.main, syscall.SIGTERM)
		} else {
			g.signalLocked(nil, syscall.SIGTERM)
		}
		if p.stopGroup {
			_ = g.memory.termOutsideGroups(map[int]bool{g.supervisor.Process.Pid: true})
		}
	}
}

func (g *nativeGroup) signalLocked(member *nativeMember, signal syscall.Signal) {
	if member != nil && member.Exit != nil {
		return
	}
	pid := -g.supervisor.Process.Pid
	if member != nil {
		pid = member.PID
	}
	if syscall.Kill(pid, signal) != nil {
		return
	}
	at := time.Now().UTC()
	name := "TERM"
	if signal == syscall.SIGKILL {
		name = "KILL"
	}
	record := deliveredSignal{Signal: name, At: at}
	prior := g.p.outcome.Signals
	g.p.outcome.Signals = append(append([]deliveredSignal(nil), prior...), record)
	changed := map[*nativeMember][]deliveredSignal{}
	for _, m := range g.members {
		if (member == nil || member == m) && m.PID != 0 && m.Exit == nil {
			changed[m] = m.Signals
			m.Signals = append(append([]deliveredSignal(nil), m.Signals...), record)
		}
	}
	if g.saveLocked() != nil {
		g.p.outcome.Signals = prior
		for m, signals := range changed {
			m.Signals = signals
		}
	}
}

// Snapshot the member that ended execution before cleanup signals any sibling.
func (g *nativeGroup) outcomeLocked(runErr error, stage bool) (facts memberOutcome) {
	defer func() {
		facts.Infrastructure = append(append([]infrastructureFact(nil), facts.Infrastructure...), g.p.outcome.Infrastructure...)
	}()
	for _, m := range g.members {
		if m.Name != "main" && m.Exit != nil && (g.main == nil || g.main.Exit == nil || m.Exit.ObservedAt.Before(g.main.Exit.ObservedAt)) {
			r := classifyAttemptOutcome(Result{}, attemptCause{memberOutcome: m.memberOutcome})
			if !r.Interrupted {
				return m.memberOutcome
			}
		}
	}
	if g.main != nil && g.main.Exit != nil {
		return g.main.memberOutcome
	}
	facts = g.p.outcome
	facts.Infrastructure = nil
	if runErr != nil {
		facts.Exit = &memberExit{Status: 1, ObservedAt: time.Now().UTC()}
		if stage {
			facts.Signals = nil
		}
	}
	return facts
}

func (g *nativeGroup) recordInfrastructure(kind, member, port string, cause error) error {
	g.p.mu.Lock()
	defer g.p.mu.Unlock()
	return g.recordInfrastructureLocked(kind, member, port, cause)
}
func (g *nativeGroup) recordInfrastructureLocked(kind, member, port string, cause error) error {
	group := 0
	if g.supervisor != nil {
		group = g.supervisor.Process.Pid
	}
	prior := g.p.outcome.Infrastructure
	g.p.outcome.Infrastructure = append(append([]infrastructureFact(nil), prior...), infrastructureFact{Kind: kind, At: time.Now().UTC(), Member: member, Port: port, Group: group})
	if err := g.saveLocked(); err != nil {
		g.p.outcome.Infrastructure = prior
		return errors.Join(cause, err)
	}
	return cause
}

func (w Worker) startNativeGroup(s *workerStore, j Job, identity string, memory *memoryAttempt, env map[string]string, mainEnvironment string, releases ...func()) (*process, error) {
	logName := identity + ".log"
	output, err := openCommandOutput(s.dir, logName, j.Spec.Logs, os.O_EXCL)
	if err != nil {
		return nil, err
	}
	if j.Spec.Logs == "none" {
		logName = ""
	}
	s.state.Job = &j
	s.state.Identity = identity
	s.state.Log = logName
	s.state.Offset = 0
	s.state.Pending = 0
	s.state.Result = nil
	s.state.OperatorStopped = false
	p := &process{stopGrace: w.AcceleratorStopGrace, job: j, done: make(chan workerFinish, 1), savePriorityStop: s.priorityStopRecorder()}
	ctx, cancel := context.WithCancel(context.Background())
	g := &nativeGroup{p: p, worker: w, dir: s.dir, identity: identity, session: s.state.Session, ctx: ctx, cancel: cancel, memory: memory, custody: workerState{Identity: identity, StopGrace: acceleratorStopGrace(w.AcceleratorStopGrace), Groups: map[int]map[int]string{}}}
	p.native = g
	if memory != nil {
		g.custody.Cgroup = memory.path
	}
	if err := g.setupMemory(); err != nil {
		cancel()
		output.Close()
		return nil, err
	}
	writer, err := g.startSupervisor(s, env)
	if err != nil {
		cancel()
		output.Close()
		return nil, err
	}
	if w.control != nil {
		w.control.set(p)
	}
	// A port-collision retry replaces the supervisor and its process group.
	finishMemory := w.observeAttemptMemory(memory, j, func() []int {
		p.mu.Lock()
		defer p.mu.Unlock()
		return []int{g.supervisor.Process.Pid}
	})
	go func() {
		defer cancel()
		for _, release := range releases {
			if release != nil {
				defer release()
			}
		}
		var runErr error
		var stage bool
		for groupAttempt := 0; groupAttempt < nativeBindAttempts; groupAttempt++ {
			runErr, stage = g.run(j, env, mainEnvironment, output)
			if !errors.Is(runErr, errNativePortCollision) || groupAttempt+1 == nativeBindAttempts {
				break
			}
			// No advertised endpoint may change while an old consumer survives.
			// Keep custody until the entire old group is verified empty.
			if err := g.cleanupProcesses(writer, true); err != nil {
				runErr = errors.Join(runErr, err)
				break
			}
			if err := g.memoryFailure(); err != nil {
				runErr = err
				break
			}
			writer, runErr = g.startSupervisor(nil, env)
			if runErr != nil {
				break
			}
		}
		defer func() {
			for _, release := range g.releases {
				release()
			}
		}()
		p.waitStopGrace(w.Clock, g.supervisor.Process.Pid, g.supervisor.Process.Pid)
		p.mu.Lock()
		forced, polite := p.killed, p.polite
		stage = stage || g.stageFailure
		facts := g.outcomeLocked(runErr, stage)
		priorityStop := p.priorityStop
		p.mu.Unlock()
		groupErr := g.cleanupProcesses(writer, false)
		peak := finishMemory()
		oom := g.memoryFailure()
		cleanup := errors.Join(groupErr, memory.cleanup(w.Clock), g.save())
		if cleanup == nil {
			s.releaseSharedInputs()
		}
		persist := errors.Join(syncCommandOutput(output), output.Close())
		r := Result{MemoryPeak: peak, Machine: w.Machine.ID, Session: g.session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Success: runErr == nil && oom == nil && cleanup == nil && persist == nil, Forced: forced, PoliteStop: polite, ReleasePending: cleanup != nil}
		if runErr != nil {
			r.Error = runErr.Error()
			r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("command", runErr))
			if stage && r.ExecutionCause.Class == "io_error" {
				r.ExecutionCause.Class = "stage_failed"
			}
		}
		if oom != nil {
			r.Error = oom.Error()
			r.ExecutionCause = &RuntimeCause{Operation: "command", Class: "resource_limit"}
		}
		if cleanup != nil {
			r.Error = errors.Join(runErr, cleanup).Error()
			r.CleanupCause = runtimeDiagnostic(runtimeErrorFor("process_group", cleanup))
		}
		if persist != nil {
			r.Success = false
			r.Error = errors.Join(runErr, persist).Error()
			r.CleanupCause = runtimeDiagnostic(runtimeErrorFor("persist", persist))
		}
		if j.Spec.Program != nil {
			main := g.members[len(g.members)-1]
			if main.cmd != nil {
				collectProgramUsage(&r, main.cmd.ProcessState, programUsageFile(g.dir, identity))
			}
		}
		r = classifyAttemptOutcome(r, attemptCause{PriorityStop: priorityStop, memberOutcome: facts, OutOfMemory: r.OutOfMemory || oom != nil && strings.Contains(oom.Error(), "exceeded its ")})
		if r.Success {
			if e := w.publishExtensionFiles(j); e != nil {
				r.Success = false
				r.Error = e.Error()
				r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("outputs", e))
			}
		}
		saveErr := saveWorkerJSON(g.dir, identity+".result.json", r)
		p.done <- workerFinish{result: r, err: saveErr}
	}()
	return p, nil
}

// Start a new anchor only after the previous group has been verified empty.
// Persist intent and kernel birth custody before permitting any member exec.
// Only the initial launch updates worker state; later groups remain in the same
// durable attempt custody so the worker loop retains sole ownership of state.
func (g *nativeGroup) startSupervisor(s *workerStore, env map[string]string) (*os.File, error) {
	g.p.mu.Lock()
	defer g.p.mu.Unlock()
	if err := g.ctx.Err(); err != nil {
		return nil, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	cmd := exec.Command("sh", "-c", `trap '' TERM; while read line; do :; done`, "myce-supervisor-"+g.identity)
	cmd.Stdin = reader
	cmd.Env = environmentList(env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if s != nil {
		s.state.PID = 0
		s.state.LaunchPending = true
		err = s.save()
	}
	if err == nil {
		err = saveWorkerJSON(g.dir, "attempt-"+g.identity+".json", g.custody)
	}
	if err == nil {
		err = g.worker.authorizeLaunch(HostLaunch{Machine: g.worker.Machine.ID, Session: g.session, Kind: "batch", Key: g.p.job.ID, Attempt: strconv.Itoa(g.p.job.Attempt)})
		if err != nil {
			if s != nil {
				err = s.startFailure(g.p.job, launchRefused, err)
			} else {
				err = g.recordInfrastructureLocked(launchRefused, "control", "", err)
			}
		}
	}
	if err == nil {
		err = cmd.Start()
		if err != nil {
			if s != nil {
				err = s.startFailure(g.p.job, launchFailed, err)
			} else {
				err = g.recordInfrastructureLocked(launchFailed, "control", "", err)
			}
		}
	}
	if err != nil {
		writer.Close()
		return nil, err
	}
	abort := func(e error) (*os.File, error) {
		_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
		writer.Close()
		_ = cmd.Wait()
		if groupErr := waitWorkerGroup(cmd.Process.Pid, g.worker.Clock); groupErr != nil {
			return nil, errors.Join(e, fmt.Errorf("%w: %v", errWorkerReleasePending, groupErr))
		}
		return nil, e
	}
	birth, err := processBirth(cmd.Process.Pid)
	if err != nil {
		return abort(err)
	}
	if s != nil {
		g.custody.PID = cmd.Process.Pid
		g.custody.Birth = birth
	}
	g.custody.Groups[cmd.Process.Pid] = map[int]string{cmd.Process.Pid: birth}
	if err = saveWorkerJSON(g.dir, "attempt-"+g.identity+".json", g.custody); err != nil {
		return abort(err)
	}
	if err = g.control.attach(cmd.Process.Pid); err != nil {
		return abort(err)
	}
	if s != nil {
		s.state.PID = cmd.Process.Pid
		s.state.LaunchPending = false
		if err = s.save(); err != nil {
			return abort(err)
		}
		g.p.cmd = cmd
	}
	g.supervisor = cmd
	g.p.outcome = memberOutcome{} // New members cannot inherit a previous group's probe facts.
	g.members = nil
	g.custody.StopAt = nil // The previous group is verified empty before retry.
	for _, side := range g.p.job.Spec.Run.Sidecars {
		g.members = append(g.members, &nativeMember{Name: side.Name, Phase: "intent", MemoryGB: side.MemoryGB})
	}
	g.members = append(g.members, &nativeMember{Name: "main", Phase: "intent", MemoryGB: g.p.job.Spec.Run.MemoryGB})
	for _, m := range g.members {
		m.memory = g.memberMemory[m.Name]
		if m.memory != nil {
			m.Cgroup = m.memory.path
		}
	}
	if err = g.saveLocked(); err != nil {
		return abort(err)
	}
	return writer, nil
}

func (g *nativeGroup) cleanupProcesses(writer *os.File, retry bool) error {
	g.p.mu.Lock()
	graceful := !g.p.job.Spec.Needs.cpuOnly() && !g.p.userCancel
	g.p.mu.Unlock()
	if graceful && retry {
		// A bind retry drains this group without cancelling the attempt's next
		// preparation. External stops still cancel g.ctx and forbid relaunch.
		g.p.mu.Lock()
		deadline := g.worker.Clock.Now().Add(acceleratorStopGrace(g.worker.AcceleratorStopGrace))
		g.custody.StopAt = &deadline
		persistErr := g.saveLocked()
		g.signalLocked(nil, syscall.SIGTERM)
		termErr := g.memory.termOutsideGroups(map[int]bool{g.supervisor.Process.Pid: true})
		g.p.mu.Unlock()
		if termErr != nil {
			return termErr
		}
		if persistErr != nil {
			return persistErr
		}
		tick := time.NewTicker(10 * time.Millisecond)
		for g.worker.Clock.Now().Before(deadline) {
			members, err := workerGroupMembers(g.supervisor.Process.Pid)
			populated, populationErr := g.memory.outsideGroups(map[int]bool{g.supervisor.Process.Pid: true}, false)
			if err == nil && populationErr == nil && !populated && (len(members) == 0 || len(members) == 1 && members[0] == g.supervisor.Process.Pid) {
				break
			}
			<-tick.C
		}
		tick.Stop()
	} else if graceful {
		g.p.stop(g.worker.Clock.Now(), acceleratorStopGrace(g.worker.AcceleratorStopGrace), false)
		g.p.waitStopGrace(g.worker.Clock, g.supervisor.Process.Pid, g.supervisor.Process.Pid)
	}

	// If only the anchor remains, let EOF end it without signaling an exited payload.
	g.p.mu.Lock()
	groupStop := g.p.stopGroup
	g.p.mu.Unlock()
	members, err := workerGroupMembers(g.supervisor.Process.Pid)
	if groupStop && err == nil && len(members) == 1 && members[0] == g.supervisor.Process.Pid && writer != nil {
		writer.Close()
		_ = g.supervisor.Wait()
	}

	// Retain the cgroup reservation, but kill and verify its entire subtree,
	// including descendants that left the process group, before relaunching.
	cleanup := errors.Join(g.memory.stop(g.worker.Clock), cleanupAttemptGroups(workerState{Groups: g.custody.Groups}, g.worker.Clock))
	if writer != nil {
		writer.Close()
	}
	if cleanup != nil {
		return cleanup
	}
	for _, m := range g.members {
		if m.done != nil {
			<-m.done
			g.p.mu.Lock()
			m.Phase = "exited"
			code := m.cmd.ProcessState.ExitCode()
			m.ExitCode = &code
			g.p.mu.Unlock()
		}
	}
	if g.supervisor != nil {
		_ = g.supervisor.Wait()
	}
	return g.save()
}

func environmentList(env map[string]string) []string {
	keys := slices.Sorted(maps.Keys(env))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != "MYCE_TOKEN" {
			out = append(out, k+"="+env[k])
		}
	}
	return out
}

func (g *nativeGroup) launch(m *nativeMember, argv []string, env map[string]string, dir string, output *os.File) error {
	if err := g.worker.verifyDevice(); err != nil {
		return g.recordInfrastructure(launchRefused, m.Name, "", err)
	}
	l := HostLaunch{Machine: g.worker.Machine.ID, Session: g.session, Kind: "batch", Key: g.p.job.ID, Attempt: strconv.Itoa(g.p.job.Attempt)}
	if err := g.worker.authorizeLaunch(l); err != nil {
		return g.recordInfrastructure(launchRefused, m.Name, "", err)
	}
	if m.Name == "main" {
		if err := g.worker.authorizeStart(g.p.job, g.session); err != nil {
			return g.recordInfrastructure(launchRefused, m.Name, "", err)
		}
	}
	g.p.mu.Lock()
	defer g.p.mu.Unlock()
	if err := g.ctx.Err(); err != nil {
		return err
	}
	command := argv[0]
	if !strings.Contains(command, "/") {
		for _, bin := range filepath.SplitList(env["PATH"]) {
			candidate := filepath.Join(bin, command)
			if st, e := os.Stat(candidate); e == nil && !st.IsDir() && st.Mode()&0111 != 0 {
				command = candidate
				break
			}
		}
	} else if !filepath.IsAbs(command) {
		command = filepath.Join(dir, command)
	}
	args := append([]string{"-c", `read gate || exit 125; exec "$@" < /dev/null`, "myce-member-" + m.Name, command}, argv[1:]...)
	cmd := exec.Command("sh", args...)
	cmd.Dir = dir
	env = maps.Clone(env)
	bound, e := g.worker.deviceEnvironment()
	if e != nil {
		return e
	}
	for k, v := range bound {
		env[k] = v
	}
	for k, v := range placementEnvironment(g.worker.Machine) {
		env[k] = v
	}
	delete(env, "MYCE_TOKEN")
	if m.memory != nil {
		env["MYCE_MEMORY_LIMIT_BYTES"] = strconv.FormatInt(m.memory.bytes, 10)
	}
	cmd.Env = environmentList(env)
	cmd.Stdout = output
	cmd.Stderr = output
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: g.supervisor.Process.Pid}
	reader, writer, err := os.Pipe()
	if err != nil {
		return err
	}
	defer reader.Close()
	defer writer.Close()
	cmd.Stdin = reader
	if err = g.saveLocked(); err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return g.recordInfrastructureLocked(launchFailed, m.Name, "", err)
	}
	m.cmd = cmd
	m.PID = cmd.Process.Pid
	m.Phase = "gated"
	m.done = make(chan struct{})
	go func() {
		waitErr := cmd.Wait()
		observed := observedNativeExit(cmd, time.Now().UTC())
		g.p.mu.Lock()
		m.err = waitErr
		m.Exit = observed
		saveErr := g.saveLocked()
		if saveErr != nil {
			m.err = errors.Join(waitErr, saveErr)
		}
		g.p.mu.Unlock()
		close(m.done)
	}()
	m.Birth, err = processBirth(m.PID)
	if err != nil {
		return err
	}
	g.custody.Groups[g.supervisor.Process.Pid][m.PID] = m.Birth
	if err = g.saveLocked(); err != nil {
		return err
	}
	if err = m.memory.attach(m.PID); err != nil {
		return err
	}
	if err = g.saveLocked(); err != nil {
		return err
	}
	if err = g.worker.authorizeLaunch(l); err != nil {
		return g.recordInfrastructureLocked(launchRefused, m.Name, "", err)
	}
	if m.Name == "main" {
		g.main = m
	}
	if _, err = writer.WriteString("start\n"); err != nil {
		return err
	}
	m.Phase = "running"
	return g.saveLocked()
}

func (g *nativeGroup) memberFailure(except ...*nativeMember) error {
	for _, m := range g.members {
		if m.Name == "main" || m.done == nil || len(except) > 0 && m == except[0] {
			continue
		}
		select {
		case <-m.done:
			return fmt.Errorf("native sidecar %s exited before main: %v", m.Name, m.err)
		default:
		}
	}
	return nil
}

func (g *nativeGroup) independentMemberFailure() bool {
	for _, m := range g.members {
		if m.Name == "main" || m.done == nil {
			continue
		}
		select {
		case <-m.done:
			g.p.mu.Lock()
			r := classifyAttemptOutcome(Result{}, attemptCause{memberOutcome: m.memberOutcome})
			g.p.mu.Unlock()
			if r.Interrupted {
				continue
			}
			return true
		default:
		}
	}
	return false
}

func (g *nativeGroup) run(j Job, base map[string]string, mainEnvironment string, output *os.File) (error, bool) {
	// Acquire all helper environments before starting any payload; hold cache use
	// protections until every member has exited (the outer goroutine defers release).
	roots := map[string]string{}
	for _, side := range j.Spec.Run.Sidecars {
		root, release, err := g.sidecarEnvironment(j, side, mainEnvironment)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				err = g.recordInfrastructure(environmentFailed, side.Name, "", err)
			}
			return err, false
		}
		roots[side.Name] = root
		if release != nil {
			g.releases = append(g.releases, release)
		}
	}
	reservations := map[string]net.Listener{}
	defer func() {
		for _, l := range reservations {
			l.Close()
		}
	}()
	env := maps.Clone(base)
	setPort := func(side NativeSidecar, l net.Listener) {
		port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
		key := strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(side.Name))
		env["MYCE_SIDECAR_"+key+"_PORT"] = port
		env["MYCE_SIDECAR_"+key+"_URL"] = "http://127.0.0.1:" + port
	}
	replacer := func() *strings.Replacer {
		pairs := []string{}
		for _, side := range j.Spec.Run.Sidecars {
			if side.Port != nil {
				key := strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(side.Name))
				pairs = append(pairs, "{sidecars."+side.Name+".url}", env["MYCE_SIDECAR_"+key+"_URL"], "{sidecars."+side.Name+".port}", env["MYCE_SIDECAR_"+key+"_PORT"])
			}
		}
		return strings.NewReplacer(pairs...)
	}
	for _, side := range j.Spec.Run.Sidecars {
		if side.Port != nil {
			l, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(*side.Port)))
			if err != nil {
				if errors.Is(err, syscall.EADDRINUSE) {
					return g.recordInfrastructure(launchFailed, side.Name, strconv.Itoa(*side.Port), fmt.Errorf("sidecar %s port: %w", side.Name, errNativePortCollision)), false
				}
				return fmt.Errorf("sidecar %s port: %w", side.Name, err), true
			}
			reservations[side.Name] = l
			setPort(side, l)
		}
	}
	for i, side := range j.Spec.Run.Sidecars {
		if err := g.memberFailure(); err != nil {
			g.p.mu.Lock()
			stopped := g.p.stopped || g.p.killed
			g.p.mu.Unlock()
			return err, !stopped || g.independentMemberFailure()
		}
		replace := replacer()
		memberEnv := maps.Clone(env)
		// Helpers inherit main's environment, but not main's authored env variables.
		base := jobBaseEnvironment()
		for k := range j.Spec.Run.Env {
			delete(memberEnv, k)
			if value, ok := base[k]; ok {
				memberEnv[k] = value
			}
		}
		port := ""
		if l := reservations[side.Name]; l != nil {
			port = strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
			l.Close()
			delete(reservations, side.Name)
			memberEnv["MYCE_SIDECAR_PORT"] = port
			memberEnv["MYCE_SIDECAR_URL"] = "http://127.0.0.1:" + port
		}
		r := strings.NewReplacer("{port}", port)
		for k, v := range side.Env {
			memberEnv[k] = r.Replace(replace.Replace(v))
		}
		if root := roots[side.Name]; root != "" && side.Environment != "" {
			if mainEnvironment != "" {
				prefix := filepath.Join(mainEnvironment, "venv", "bin") + string(os.PathListSeparator) + filepath.Join(mainEnvironment, "bin") + string(os.PathListSeparator)
				memberEnv["PATH"] = strings.TrimPrefix(memberEnv["PATH"], prefix)
			}
			bindEnvironment(memberEnv, root)
		}
		// Reserved env was validated; defensive stripping also covers direct starts.
		delete(memberEnv, "MYCE_TOKEN")
		reference := side.Environment
		if reference == "" {
			reference = j.Spec.Environment.Name
		}
		er := executionReplacer(j.Spec.Run.Workdir, roots[side.Name], reference, machineTarget(&g.worker.Machine))
		argv := expandArgs(expandArgs(expandArgs(side.Command, replace), r), er)
		for k, v := range memberEnv {
			memberEnv[k] = er.Replace(v)
		}
		if err := g.launch(g.members[i], argv, memberEnv, j.Spec.Run.Workdir, output); err != nil {
			return err, !errors.Is(err, context.Canceled) && !errors.Is(err, errWorkerStartAuthorization)
		}
		if side.Readiness != nil {
			g.p.mu.Lock()
			g.members[i].Phase = "waiting_ready"
			err := g.saveLocked()
			g.p.mu.Unlock()
			if err != nil {
				return err, true
			}
			if err := g.waitReady(g.members[i], port, *side.Readiness); err != nil {
				if errors.Is(err, errNativePortCollision) {
					return err, false
				}
				g.p.mu.Lock()
				stopped := g.p.stopped || g.p.killed
				g.p.mu.Unlock()
				return err, !errors.Is(err, context.Canceled) && !stopped
			}
		}
	}
	if err := g.memberFailure(); err != nil {
		g.p.mu.Lock()
		stopped := g.p.stopped || g.p.killed
		g.p.mu.Unlock()
		return err, !stopped || g.independentMemberFailure()
	}
	replace := replacer()
	for k, v := range j.Spec.Run.Env {
		env[k] = replace.Replace(v)
	}
	main := g.members[len(g.members)-1]
	if err := g.launch(main, expandArgs(j.Spec.Run.Command, replace), env, j.Spec.Run.Workdir, output); err != nil {
		return err, !errors.Is(err, context.Canceled) && !errors.Is(err, errWorkerStartAuthorization)
	}
	wake := time.NewTicker(10 * time.Millisecond)
	defer wake.Stop()
	for {
		if err := g.memberFailure(); err != nil {
			g.p.mu.Lock()
			stopped := g.p.stopped || g.p.killed
			g.p.mu.Unlock()
			return err, !stopped || g.independentMemberFailure()
		}
		select {
		case <-main.done:
			g.p.mu.Lock()
			stopped := g.p.stopped || g.p.killed
			g.p.mu.Unlock()
			return main.err, main.err != nil && !stopped
		case <-wake.C:
		}
	}
}

func (w Worker) verifyNativeListener(ctx context.Context, port string, group int) error {
	if w.nativeListenerProbe != nil {
		return w.nativeListenerProbe(port, group)
	}
	return verifyNativeListenerContext(ctx, port, group)
}

func (g *nativeGroup) waitReady(member *nativeMember, port string, readiness NativeReadiness) error {
	timeout, _ := time.ParseDuration(readiness.Timeout)
	deadline := g.worker.Clock.Now().Add(timeout)
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 200 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	wake := time.NewTicker(20 * time.Millisecond)
	defer wake.Stop()
	for {
		if err := g.memberFailure(member); err != nil {
			return err
		}
		if err := g.memberFailure(); err != nil {
			// A failed helper plus a foreign listener on its reserved port is
			// positive handoff-collision evidence, independent of exit codes/logs.
			if port != "" && errors.Is(g.worker.verifyNativeListener(g.ctx, port, g.supervisor.Process.Pid), errNativeForeignListener) {
				return g.recordInfrastructure(foreignPort, member.Name, port, errNativePortCollision)
			}
			return err
		}
		if err := g.ctx.Err(); err != nil {
			return err
		}
		if !g.worker.Clock.Now().Before(deadline) {
			return &runtimeError{diagnostic: RuntimeCause{Operation: "readiness", Class: "timeout"}, cause: errors.New("native sidecar HTTP readiness timeout")}
		}
		req, _ := http.NewRequestWithContext(g.ctx, "GET", "http://127.0.0.1:"+port+readiness.Path, nil)
		response, err := client.Do(req)
		if err == nil {
			response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				if err = g.worker.verifyNativeListener(g.ctx, port, g.supervisor.Process.Pid); err != nil {
					if errors.Is(err, errNativeForeignListener) {
						return g.recordInfrastructure(foreignPort, member.Name, port, errNativePortCollision)
					}
					return err
				}
				return g.memberFailure()
			}
		}
		select {
		case <-g.ctx.Done():
			return g.ctx.Err()
		case <-wake.C:
		}
	}
}

func (g *nativeGroup) setupMemory() error {
	c, err := g.memory.children()
	if err != nil || c == nil {
		return err
	}
	whole := g.p.job.Spec.Needs.Memory == "auto" || g.p.job.Spec.Run.MemoryGB == 0
	for _, side := range g.p.job.Spec.Run.Sidecars {
		whole = whole || side.MemoryGB == 0
	}
	if err := g.memory.setOOMGroup(whole); err != nil {
		return err
	}
	g.control, err = c.create("control", g.memory.bytes, 0)
	if err != nil {
		return err
	}
	g.memberMemory = map[string]*memoryAttempt{}
	members := map[string]float64{"main": g.p.job.Spec.Run.MemoryGB}
	for _, side := range g.p.job.Spec.Run.Sidecars {
		members[side.Name] = side.MemoryGB
	}
	for name, gb := range members {
		bytes := g.memory.bytes
		if gb > 0 {
			bytes, err = memoryBytes(gb)
			if err != nil {
				return err
			}
		}
		leaf := name
		if name != "main" {
			leaf = "sidecar-" + name
		}
		g.memberMemory[name], err = c.create(leaf, bytes, g.worker.MemorySwapMaxBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *nativeGroup) memoryFailure() error {
	for _, m := range g.members {
		if err := m.memory.localLimitFailure(); err != nil {
			return fmt.Errorf("native member %s: %w", m.Name, err)
		}
	}
	return g.memory.localLimitFailure()
}

// Inspect durable member evidence before orphan cleanup removes kernel counters.
// Persist the classified result with a release hold so a second crash cannot
// erase the observed OOM. Startup clears the hold only after whole-group proof.
func (s *workerStore) recoverNativeCause(machine string, fs memoryFS) error {
	if s.state.Result != nil {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(s.dir, "native-"+s.state.Identity+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var record struct {
		Identity string
		Members  []nativeMember
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return err
	}
	if record.Identity != s.state.Identity {
		return errors.New("mismatched native outcome custody")
	}
	data, err = os.ReadFile(filepath.Join(s.dir, "attempt-"+s.state.Identity+".json"))
	if err != nil {
		return err
	}
	var custody workerState
	if err := json.Unmarshal(data, &custody); err != nil {
		return err
	}
	if custody.Identity != s.state.Identity {
		return errors.New("mismatched native attempt custody")
	}
	// Recover member own-cap failures before the attempt parent's local OOM.
	scopes := append(slices.Clone(record.Members), nativeMember{Cgroup: custody.Cgroup})
	for i, m := range scopes {
		if m.Cgroup == "" {
			continue
		}
		leaf := "main"
		if m.Name != "main" {
			leaf = "sidecar-" + m.Name
		}
		if filepath.Base(custody.Cgroup) != "attempt-"+s.state.Identity || i < len(record.Members) && m.Cgroup != filepath.Join(custody.Cgroup, leaf) {
			return errors.New("mismatched member cgroup custody")
		}
		limit, err := fs.ReadFile(filepath.Join(m.Cgroup, "memory.max"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		bytes, err := strconv.ParseInt(strings.TrimSpace(string(limit)), 10, 64)
		if err != nil || bytes <= 0 {
			return errors.New("invalid saved member memory cap")
		}
		err = (&memoryAttempt{fs: fs, path: m.Cgroup, bytes: bytes}).localLimitFailure()
		if err == nil {
			continue
		}
		if ceilingDiagnostic(err.Error(), "") == "" {
			return err
		}
		cause := err.Error()
		if i < len(record.Members) {
			cause = "native member " + m.Name + ": " + cause
		}
		r := classifyAttemptOutcome(Result{Machine: machine, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, ReleasePending: true, Error: cause, ExecutionCause: &RuntimeCause{Operation: "command", Class: "resource_limit"}}, attemptCause{OutOfMemory: true})
		if err := saveWorkerJSON(s.dir, s.state.Identity+".result.json", r); err != nil {
			return err
		}
		s.state.Result = &r
		return nil
	}
	return nil
}
