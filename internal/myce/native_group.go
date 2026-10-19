package myce

import (
	"context"
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

const nativeBindAttempts = 3

// The anchor preserves the existing attempt PGID/recovery contract. All members
// inherit its cgroup and join its process group before their exec gate opens.
// MemoryGB stays on the member record for D5's child-controller adapter.
type nativeMember struct {
	Name     string  `json:"name"`
	PID      int     `json:"pid,omitempty"`
	Birth    string  `json:"birth,omitempty"`
	ExitCode *int    `json:"exit_code,omitempty"`
	Phase    string  `json:"phase"`
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
	custody                workerState
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
	if err := saveWorkerJSON(g.dir, "attempt-"+g.identity+".json", g.custody); err != nil {
		return err
	}
	return saveWorkerJSON(g.dir, "native-"+g.identity+".json", struct {
		Identity string          `json:"identity"`
		Members  []*nativeMember `json:"members"`
	}{g.identity, g.members})
}

// Called under p.mu, also held across fork/persistence/exec approval. A stop
// cannot race another member into execution. Main gets checkpoint grace while
// helpers remain available; prep has no main and terminates the whole group.
func (g *nativeGroup) stopLocked(now time.Time, grace time.Duration, kill bool) {
	p := g.p
	if !p.stopped && !p.killed && g.memberFailure() != nil {
		g.stageFailure = true
	}
	g.cancel()
	if kill {
		if !p.killed {
			_ = signalWorkerGroup(p.cmd.Process.Pid, syscall.SIGKILL)
			p.killed = true
		}
		return
	}
	deadline := now.Add(grace)
	if p.stopAt == nil || deadline.Before(*p.stopAt) {
		p.stopAt = &deadline
	}
	if !p.stopped {
		p.stopped = true
		if g.main != nil {
			_ = syscall.Kill(g.main.PID, syscall.SIGTERM)
		} else {
			_ = signalWorkerGroup(p.cmd.Process.Pid, syscall.SIGTERM)
		}
	}
}

func (w Worker) startNativeGroup(s *workerStore, j Job, identity string, memory *memoryAttempt, env map[string]string, mainEnvironment string, releases ...func()) (*process, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	cmd := exec.Command("sh", "-c", `trap '' TERM; while read line; do :; done`, "myce-supervisor-"+identity)
	cmd.Stdin = reader
	cmd.Env = environmentList(env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	logName := identity + ".log"
	output, err := openCommandOutput(s.dir, logName, j.Spec.Logs, os.O_EXCL)
	if err != nil {
		writer.Close()
		return nil, err
	}
	if j.Spec.Logs == "none" {
		logName = ""
	}
	s.state.Job = &j
	s.state.Identity = identity
	s.state.PID = 0
	s.state.LaunchPending = true
	s.state.Log = logName
	s.state.Offset = 0
	s.state.Pending = 0
	s.state.Result = nil
	s.state.OperatorStopped = false
	custody := workerState{Identity: identity, Groups: map[int]map[int]string{}}
	if memory != nil {
		custody.Cgroup = memory.path
	}
	if err = s.save(); err == nil {
		err = saveWorkerJSON(s.dir, "attempt-"+identity+".json", custody)
	}
	if err == nil {
		err = w.authorizeLaunch(HostLaunch{Machine: w.Machine.ID, Session: s.state.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt)})
	}
	if err == nil {
		err = cmd.Start()
	}
	if err != nil {
		writer.Close()
		output.Close()
		return nil, err
	}
	abort := func(e error) (*process, error) {
		_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
		writer.Close()
		_ = cmd.Wait()
		output.Close()
		groupErr := waitWorkerGroup(cmd.Process.Pid, w.Clock)
		if groupErr != nil {
			return nil, errors.Join(e, fmt.Errorf("%w: %v", errWorkerReleasePending, groupErr))
		}
		return nil, e
	}
	custody.PID = cmd.Process.Pid
	custody.Birth, err = processBirth(custody.PID)
	if err != nil {
		return abort(err)
	}
	custody.Groups[custody.PID] = map[int]string{custody.PID: custody.Birth}
	if err = saveWorkerJSON(s.dir, "attempt-"+identity+".json", custody); err != nil {
		return abort(err)
	}
	if err = memory.attach(cmd.Process.Pid); err != nil {
		return abort(err)
	}
	s.state.PID = cmd.Process.Pid
	s.state.LaunchPending = false
	if err = s.save(); err != nil {
		return abort(err)
	}
	p := &process{job: j, cmd: cmd, done: make(chan workerFinish, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	g := &nativeGroup{p: p, worker: w, dir: s.dir, identity: identity, session: s.state.Session, ctx: ctx, cancel: cancel, memory: memory, custody: custody}
	p.native = g
	for _, side := range j.Spec.Run.Sidecars {
		g.members = append(g.members, &nativeMember{Name: side.Name, Phase: "intent", MemoryGB: side.MemoryGB})
	}
	g.members = append(g.members, &nativeMember{Name: "main", Phase: "intent"})
	if err = g.save(); err != nil {
		cancel()
		return abort(err)
	}
	if w.control != nil {
		w.control.set(p)
	}
	go func() {
		defer cancel()
		for _, release := range releases {
			if release != nil {
				defer release()
			}
		}
		runErr, stage := g.run(j, env, mainEnvironment, output)
		defer func() {
			for _, release := range g.releases {
				release()
			}
		}()
		p.mu.Lock()
		infrastructure, forced, polite := p.stopped || p.killed, p.killed, p.polite
		stage = stage || g.stageFailure
		p.mu.Unlock()
		_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
		writer.Close()
		for _, m := range g.members {
			if m.done != nil {
				<-m.done
				m.Phase = "exited"
				code := m.cmd.ProcessState.ExitCode()
				m.ExitCode = &code
			}
		}
		_ = cmd.Wait()
		oom := memory.failure()
		cleanup := errors.Join(waitWorkerGroup(cmd.Process.Pid, w.Clock), memory.cleanup(w.Clock), g.save())
		persist := errors.Join(syncCommandOutput(output), output.Close())
		r := Result{Machine: w.Machine.ID, Session: g.session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Success: runErr == nil && oom == nil && cleanup == nil && persist == nil, Forced: forced, PoliteStop: polite, ReleasePending: cleanup != nil}
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
		r = classifyAttemptOutcome(r, attemptCause{Infrastructure: infrastructure || errors.Is(runErr, errWorkerStartAuthorization) || errors.Is(runErr, errNativePortCollision), StageFailure: stage || oom != nil, OutOfMemory: r.OutOfMemory || oom != nil && strings.HasPrefix(ceilingDiagnostic(oom.Error(), ""), "exceeded its ")})
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
	l := HostLaunch{Machine: g.worker.Machine.ID, Session: g.session, Kind: "batch", Key: g.p.job.ID, Attempt: strconv.Itoa(g.p.job.Attempt)}
	if err := g.worker.authorizeLaunch(l); err != nil {
		return err
	}
	if m.Name == "main" {
		if err := g.worker.authorizeStart(g.p.job, g.session); err != nil {
			return err
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
	cmd.Env = environmentList(env)
	cmd.Stdout = output
	cmd.Stderr = output
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: g.p.cmd.Process.Pid}
	reader, writer, err := os.Pipe()
	if err != nil {
		return err
	}
	defer reader.Close()
	defer writer.Close()
	cmd.Stdin = reader
	if err = g.save(); err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	m.cmd = cmd
	m.PID = cmd.Process.Pid
	m.Phase = "gated"
	m.done = make(chan struct{})
	go func() { m.err = cmd.Wait(); close(m.done) }()
	m.Birth, err = processBirth(m.PID)
	if err != nil {
		return err
	}
	g.custody.Groups[g.p.cmd.Process.Pid][m.PID] = m.Birth
	if err = g.save(); err != nil {
		return err
	}
	if err = g.memory.attach(m.PID); err != nil {
		return err
	}
	if err = g.save(); err != nil {
		return err
	}
	if err = g.worker.authorizeLaunch(l); err != nil {
		return err
	}
	if m.Name == "main" {
		g.main = m
	}
	if _, err = writer.WriteString("start\n"); err != nil {
		return err
	}
	m.Phase = "running"
	return g.save()
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

func (g *nativeGroup) run(j Job, base map[string]string, mainEnvironment string, output *os.File) (error, bool) {
	// Acquire all helper environments before starting any payload; hold cache use
	// protections until every member has exited (the outer goroutine defers release).
	roots := map[string]string{}
	for _, side := range j.Spec.Run.Sidecars {
		root, release, err := g.sidecarEnvironment(j, side, mainEnvironment)
		if err != nil {
			return err, !errors.Is(err, context.Canceled) && !errors.Is(err, errWorkerStartAuthorization)
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
					return fmt.Errorf("sidecar %s port: %w", side.Name, errNativePortCollision), false
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
			return err, !stopped
		}
		for bindAttempt := 0; bindAttempt < nativeBindAttempts; bindAttempt++ {
			if bindAttempt > 0 {
				l, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(*side.Port)))
				if err != nil {
					if errors.Is(err, syscall.EADDRINUSE) {
						if bindAttempt+1 == nativeBindAttempts {
							return errNativePortCollision, false
						}
						continue
					}
					return err, true
				}
				reservations[side.Name] = l
				setPort(side, l)
			}
			replace := replacer()
			memberEnv := maps.Clone(env)
			// Helpers inherit main's environment, but not main's authored env variables.
			for k := range j.Spec.Run.Env {
				delete(memberEnv, k)
				if value, ok := os.LookupEnv(k); ok {
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
				g.members[i].Phase = "waiting_ready"
				if err := g.save(); err != nil {
					return err, true
				}
				if err := g.waitReady(g.members[i], port, *side.Readiness); err != nil {
					if errors.Is(err, errNativePortCollision) {
						// Only this member is retried. Previous forked descendants stay
						// in durable attempt custody until whole-group teardown.
						m := g.members[i]
						select {
						case <-m.done: // Already reaped; its PID may have been reused.
						default:
							_ = syscall.Kill(m.PID, syscall.SIGKILL)
						}
						<-m.done
						if bindAttempt+1 == nativeBindAttempts {
							return errNativePortCollision, false
						}
						g.p.mu.Lock()
						m.done = nil
						m.err = nil
						g.p.mu.Unlock()
						continue
					}
					g.p.mu.Lock()
					stopped := g.p.stopped || g.p.killed
					g.p.mu.Unlock()
					return err, !errors.Is(err, context.Canceled) && !stopped
				}
			}
			break
		}
	}
	if err := g.memberFailure(); err != nil {
		g.p.mu.Lock()
		stopped := g.p.stopped || g.p.killed
		g.p.mu.Unlock()
		return err, !stopped
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
			return err, !stopped
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
			if port != "" && errors.Is(verifyNativeListener(port, g.p.cmd.Process.Pid), errNativeForeignListener) {
				return errNativePortCollision
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
				if err = verifyNativeListener(port, g.p.cmd.Process.Pid); err != nil {
					if errors.Is(err, errNativeForeignListener) {
						return errNativePortCollision
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
