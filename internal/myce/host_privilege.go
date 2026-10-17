package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// HostInvocationError is stable across the child protocol. It deliberately has
// no raw command/account/config cause, which could contain private credentials.
type HostInvocationError struct{ Code, Message string }

func (e *HostInvocationError) Is(target error) bool {
	if target == ErrHostTarget && e.Code == "root_binary_untrusted" {
		return true
	}
	return false
}
func (e *HostInvocationError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

var (
	ErrHostIdentity             = &HostInvocationError{"host_identity_mismatch", "select the installed worker account and state directory"}
	ErrHostTarget               = &HostInvocationError{"host_target_unsafe", "target is symlinked, unowned, or inaccessible"}
	ErrHostUserManager          = &HostInvocationError{"host_user_manager_unavailable", ""}
	ErrHostChild                = &HostInvocationError{"host_child_failed", "mutation not confirmed"}
	ErrHostPrivilegeUnsupported = &HostInvocationError{"host_privilege_unsupported", "root host control requires Linux"}
	ErrHostDeadline             = &HostInvocationError{"host_deadline", "intent may be durable; release is unconfirmed"}
	ErrHostReconcileUnavailable = &HostInvocationError{"host_reconcile_unavailable", "mandatory reconciliation is required"}
)

// WorkerAccount comes from the OS account database, never HOME or the caller's bus.
type WorkerAccount struct {
	Name   string   `json:"name"`
	UID    uint32   `json:"uid"`
	GID    uint32   `json:"gid"`
	Groups []uint32 `json:"groups"`
	Home   string   `json:"home"`
}

func LookupWorkerAccount(name string) (WorkerAccount, error) {
	var a WorkerAccount
	var u *user.User
	var err error
	if name == "" {
		u, err = user.LookupId(strconv.Itoa(os.Geteuid()))
	} else {
		u, err = user.Lookup(name)
	}
	if err != nil {
		return a, ErrHostIdentity
	}
	uid, e1 := strconv.ParseUint(u.Uid, 10, 32)
	gid, e2 := strconv.ParseUint(u.Gid, 10, 32)
	groups, e3 := u.GroupIds()
	if e1 != nil || e2 != nil || e3 != nil {
		return a, ErrHostIdentity
	}
	a = WorkerAccount{Name: u.Username, UID: uint32(uid), GID: uint32(gid), Home: u.HomeDir}
	for _, g := range groups {
		n, e := strconv.ParseUint(g, 10, 32)
		if e != nil {
			return WorkerAccount{}, ErrHostIdentity
		}
		a.Groups = append(a.Groups, uint32(n))
	}
	return a, nil
}

// HostIdentityResolver has injectable OS boundaries for laptop/dev tests.
type HostIdentityResolver struct {
	Lookup       func(string) (WorkerAccount, error)
	ReadConfig   func(string) (ServiceConfig, error)
	ValidatePath func(string, uint32, bool) error
	EUID         int
	OS           string
}

func LocalHostIdentityResolver() HostIdentityResolver {
	return HostIdentityResolver{Lookup: LookupWorkerAccount, ReadConfig: readHostServiceConfig, ValidatePath: validateHostTarget, EUID: os.Geteuid(), OS: runtime.GOOS}
}

type HostInvocation struct {
	Account    WorkerAccount
	Config     ServiceConfig
	ConfigPath string
	StateDir   string
	Privileged bool
	// ForceChild is test-only; no CLI or environment switch enables it.
	ForceChild bool
}

// Select performs only account-database lookup and lexical path validation.
// In particular EUID 0 must never stat, open, glob or walk worker paths.
func (r HostIdentityResolver) Select(workerUser, configPath, stateDir string) (HostInvocation, error) {
	var v HostInvocation
	if r.EUID == 0 && r.OS != "linux" {
		return v, ErrHostPrivilegeUnsupported
	}
	if r.Lookup == nil || (r.EUID == 0 && workerUser == "") {
		return v, ErrHostIdentity
	}
	a, e := r.Lookup(workerUser)
	if e != nil || a.UID == 0 || !validWorkerAccount(a) || (r.EUID != 0 && uint32(r.EUID) != a.UID) {
		return v, ErrHostIdentity
	}
	if configPath == "" {
		configPath = filepath.Join(a.Home, ".local", "share", "mycelium", "worker", "config.json")
	}
	for _, p := range []string{configPath, stateDir} {
		if p != "" && (!filepath.IsAbs(p) || filepath.Clean(p) != p) {
			return v, ErrHostTarget
		}
	}
	return HostInvocation{Account: a, ConfigPath: configPath, StateDir: stateDir, Privileged: r.EUID == 0}, nil
}

// Resolve reads installed identity and state only in the worker process. Root
// receives a selection and must use a read-only child resolve/status operation.
func (r HostIdentityResolver) Resolve(workerUser, configPath, stateDir string) (HostInvocation, error) {
	v, e := r.Select(workerUser, configPath, stateDir)
	if e != nil || v.Privileged {
		return v, e
	}
	if r.ReadConfig == nil || r.ValidatePath == nil {
		return HostInvocation{}, ErrHostIdentity
	}
	a := v.Account
	configPath = v.ConfigPath
	if e = r.ValidatePath(configPath, a.UID, false); e != nil {
		return HostInvocation{}, ErrHostTarget
	}
	c, e := r.ReadConfig(configPath)
	if e != nil {
		return HostInvocation{}, hostProtocolError(hostProtocolCode(e))
	}
	if c.Role != "worker" || c.HostID == "" || (c.Config != "" && c.Config != configPath) || (c.WorkerUser != "" && c.WorkerUser != a.Name) || (c.WorkerHome != "" && c.WorkerHome != a.Home) {
		return HostInvocation{}, ErrHostIdentity
	}
	if stateDir == "" {
		stateDir = c.HostStateDir
	}
	if stateDir == "" {
		stateDir = filepath.Join(a.Home, ".local", "share", "myce")
	}
	for _, p := range []string{stateDir, c.State} {
		if p == "" || r.ValidatePath(p, a.UID, true) != nil {
			return HostInvocation{}, ErrHostTarget
		}
	}
	if e = ValidateHostControlTree(stateDir, a.UID); e != nil {
		return HostInvocation{}, ErrHostTarget
	}
	s, e := ReadHostIntent(stateDir)
	if e != nil {
		return HostInvocation{}, hostProtocolError(hostProtocolCode(e))
	}
	if s.Host != c.HostID {
		return HostInvocation{}, ErrHostIdentity
	}
	v.Config = c
	v.StateDir = stateDir
	return v, nil
}
func validWorkerAccount(a WorkerAccount) bool {
	if a.Name == "" || !filepath.IsAbs(a.Home) {
		return false
	}
	for _, c := range a.Name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

// Only root-owned symlinked ancestors are permitted (for example /home ->
// /var/home). Targets and non-root-owned ancestors must not be writable by
// group/other. Writable root-owned ancestors require ModeSticky (for example /tmp).
func validateHostTarget(path string, uid uint32, directory bool) error {
	return validateHostTargetLinks(path, uid, directory, 0, hostAccessACL)
}
func validateHostTargetLinks(path string, uid uint32, directory bool, links int, checkACL func(string) error) error {
	if links > 40 || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrHostTarget
	}
	parts := append([]string{"/"}, strings.Split(strings.TrimPrefix(path, "/"), "/")...)
	cur := ""
	for i, part := range parts {
		cur = filepath.Join(cur, part)
		st, e := os.Lstat(cur)
		if e != nil {
			return ErrHostTarget
		}
		owner, ok := st.Sys().(*syscall.Stat_t)
		if !ok {
			return ErrHostTarget
		}
		if checkACL(cur) != nil {
			return ErrHostTarget
		}
		last := i == len(parts)-1
		if st.Mode()&os.ModeSymlink != 0 {
			if last || owner.Uid != 0 {
				return ErrHostTarget
			}
			target, e := os.Readlink(cur)
			if e != nil {
				return ErrHostTarget
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(cur), target)
			}
			return validateHostTargetLinks(filepath.Join(append([]string{target}, parts[i+1:]...)...), uid, directory, links+1, checkACL)
		}
		if last {
			if owner.Uid != uid || st.Mode().Perm()&0022 != 0 || st.IsDir() != directory || (!directory && !st.Mode().IsRegular()) {
				return ErrHostTarget
			}
		} else if !safeHostAncestor(st, uid) {
			return ErrHostTarget
		}
	}
	return nil
}
func safeHostAncestor(st os.FileInfo, uid uint32) bool {
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.IsDir() {
		return false
	}
	if owner.Uid == 0 {
		return st.Mode().Perm()&0022 == 0 || st.Mode()&os.ModeSticky != 0
	}
	return owner.Uid == uid && st.Mode().Perm()&0022 == 0
}

func ValidateHostControlTree(dir string, uid uint32) error {
	if os.Geteuid() == 0 {
		return ErrHostIdentity
	}
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return ErrHostTarget
		}
		return validateHostTarget(path, uid, d.IsDir())
	})
}

// HostObservationContext is the root-only read boundary used by S07. Observe
// receives no mutation runner; non-root callers cannot supply privileged proof.
type HostObservationContext struct {
	Account    WorkerAccount
	StateDir   string
	Privileged bool
}

func (v HostInvocation) ObservationContext() HostObservationContext {
	return HostObservationContext{v.Account, v.StateDir, v.Privileged}
}
func (o HostObservationContext) SystemdArgs(args ...string) []string {
	prefix := []string{"--user"}
	if o.Privileged {
		prefix = append(prefix, "--machine="+o.Account.Name+"@.host")
	}
	return append(prefix, args...)
}
func (o HostObservationContext) CheckUserManager(ctx context.Context, command func(context.Context, string, ...string) ([]byte, error)) error {
	if command == nil {
		return ErrHostUserManager
	}
	if _, e := command(ctx, "systemctl", o.SystemdArgs("show", "--property=Version", "--value")...); e != nil {
		return ErrHostUserManager
	}
	return nil
}

func (o HostObservationContext) Observe(ctx context.Context, hook func(context.Context, HostObservationContext) error) error {
	if hook == nil {
		return errors.New("host_observer_unavailable")
	}
	return hook(ctx, o)
}

// HostMutation has a closed operation vocabulary, no arbitrary command/argv/env.
// Inputs and results travel over pipes, never command-line arguments or a shell.
type HostMutation struct {
	Unit          string        `json:"unit,omitempty"`
	Operation     string        `json:"operation"`
	WorkerUser    string        `json:"worker_user"`
	ConfigPath    string        `json:"config"`
	StateDir      string        `json:"state_dir"`
	Reason        string        `json:"reason,omitempty"`
	Grace         time.Duration `json:"grace,omitempty"`
	Timeout       time.Duration `json:"timeout,omitempty"`
	Expected      HostRevision  `json:"expected"`
	CloseExpected *HostRevision `json:"close_expected,omitempty"`
}
type HostMutationResult struct {
	StateDir string `json:"state_dir,omitempty"`
	Desired  string `json:"desired,omitempty"`
	Partial  bool   `json:"partial"`
	HostRevision
	Stopped int `json:"stopped"`
	// CleanupConfirmed is not the full local-exclusion PASS (S07).
	CleanupConfirmed bool `json:"cleanup_confirmed"`
}
type hostChildResponse struct {
	Version   int                `json:"version"`
	Operation string             `json:"operation"`
	Result    HostMutationResult `json:"result"`
	Code      string             `json:"code,omitempty"`
}
type HostMutationHandler func(context.Context, HostInvocation, HostMutation) (HostMutationResult, error)
type HostChildLaunch struct {
	ForceChild bool
	Account    WorkerAccount
	Env        []string
	Request    HostMutation
}
type HostChildLauncher func(context.Context, HostChildLaunch) ([]byte, error)

func WorkerChildEnvironment(a WorkerAccount) []string {
	return []string{"HOME=" + a.Home, "USER=" + a.Name, "LOGNAME=" + a.Name, "XDG_RUNTIME_DIR=/run/user/" + strconv.FormatUint(uint64(a.UID), 10), "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C.UTF-8"}
}
func (v HostInvocation) Mutate(ctx context.Context, m HostMutation, launch HostChildLauncher, handle HostMutationHandler) (HostMutationResult, error) {
	if !validHostMutation(m) {
		return HostMutationResult{}, ErrHostChild
	}
	m.WorkerUser, m.ConfigPath, m.StateDir = v.Account.Name, v.ConfigPath, v.StateDir
	if !v.Privileged && !v.ForceChild {
		return executeResolvedHost(ctx, v, m, handle)
	}
	if launch == nil {
		launch = LaunchHostWorkerChild
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		timeout := m.Timeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
		deadline, _ = ctx.Deadline()
	}
	m.Timeout = time.Until(deadline) - 2*time.Second
	if m.Timeout <= 0 {
		return HostMutationResult{}, ErrHostDeadline
	}
	b, e := launch(ctx, HostChildLaunch{Account: v.Account, Env: WorkerChildEnvironment(v.Account), Request: m, ForceChild: v.ForceChild})
	if e != nil {
		return HostMutationResult{}, ErrHostChild
	}
	var r hostChildResponse
	if decodeHostProtocol(bytes.NewReader(b), &r) != nil || r.Version != 1 || r.Operation != m.Operation {
		return HostMutationResult{}, ErrHostChild
	}
	if ctx.Err() != nil && r.Code == "" {
		r.Code = "deadline"
		r.Result.Partial = true
		r.Result.CleanupConfirmed = false
	}
	if r.Code != "" {
		if r.Code == "deadline" && r.Result.Partial && !r.Result.CleanupConfirmed {
			return r.Result, ErrHostDeadline
		}
		return HostMutationResult{}, hostProtocolError(r.Code)
	}
	if (m.Operation == "close" || m.Operation == "open" || m.Operation == "reconcile") && (r.Result.Revision == 0 || r.Result.Operation == "") {
		return HostMutationResult{}, ErrHostChild
	}
	return r.Result, nil
}
func validHostMutation(m HostMutation) bool {
	return (m.Operation == "resolve" || m.Operation == "status" || m.Operation == "close" || m.Operation == "open" || m.Operation == "reconcile" || (ValidWorkerUnit(m.Unit) && (m.Operation == "unit-stop" || m.Operation == "unit-kill" || m.Operation == "unit-start"))) && m.Grace >= 0 && m.Grace <= time.Minute && m.Timeout >= 0 && len(m.Reason) <= 4096
}
func hostProtocolError(code string) error {
	switch code {
	case "deadline":
		return ErrHostDeadline
	case "revision":
		return ErrHostRevision
	case "state":
		return ErrHostState
	case "identity":
		return ErrHostIdentity
	case "target":
		return ErrHostTarget
	case "user_manager":
		return ErrHostUserManager
	case "reconcile":
		return ErrHostReconcileUnavailable
	}
	return ErrHostChild
}
func hostProtocolCode(e error) string {
	switch {
	case errors.Is(e, ErrHostRevision):
		return "revision"
	case errors.Is(e, ErrHostTarget):
		return "target"
	case errors.Is(e, ErrHostState):
		return "state"
	case errors.Is(e, ErrHostDeadline) || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled):
		return "deadline"
	case e == nil:
		return ""
	case errors.Is(e, ErrHostIdentity):
		return "identity"
	case errors.Is(e, ErrHostUserManager):
		return "user_manager"
	case errors.Is(e, ErrHostReconcileUnavailable):
		return "reconcile"
	}
	return "child"
}
func decodeHostProtocol(in io.Reader, v any) error {
	d := json.NewDecoder(io.LimitReader(in, 64<<10))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return ErrHostChild
	}
	return nil
}

// ServeHostWorkerChild is also used by dev with a fake identity/handler. Production
// passes LocalHostIdentityResolver, so the dropped child's actual UID is checked.
func ServeHostWorkerChild(ctx context.Context, in io.Reader, out io.Writer, r HostIdentityResolver, handle HostMutationHandler) error {
	var m HostMutation
	var result HostMutationResult
	e := decodeHostProtocol(in, &m)
	if e == nil && !validHostMutation(m) {
		e = ErrHostChild
	}
	timeout := m.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var v HostInvocation
	if e == nil {
		if r.EUID == 0 {
			e = ErrHostIdentity
		} else {
			v, e = r.Resolve(m.WorkerUser, m.ConfigPath, m.StateDir)
		}
	}
	if e == nil {
		e = ctx.Err()
	}
	if e == nil {
		result, e = executeResolvedHost(ctx, v, m, handle)
	}
	// A handler finishing at the deadline cannot turn cancellation into success.
	if e == nil {
		e = ctx.Err()
	}
	code := hostProtocolCode(e)
	if code == "deadline" {
		result.CleanupConfirmed = false
		result.Partial = true
		if result.Revision == 0 && v.StateDir != "" && m.Operation == "close" {
			if s, err := ReadHostIntent(v.StateDir); err == nil && s.Desired == "closed" {
				result.HostRevision = s.HostRevision
				result.Desired = s.Desired
			}
		}
	} else if e != nil {
		result = HostMutationResult{}
	}
	return json.NewEncoder(out).Encode(hostChildResponse{Version: 1, Operation: m.Operation, Result: result, Code: code})
}

func executeResolvedHost(ctx context.Context, v HostInvocation, m HostMutation, handle HostMutationHandler) (HostMutationResult, error) {
	var zero HostMutationResult
	if e := ctx.Err(); e != nil {
		return zero, e
	}
	s, e := ReadHostIntent(v.StateDir)
	if e != nil {
		return zero, hostProtocolError(hostProtocolCode(e))
	}
	if m.Expected.Revision != 0 && m.Expected.Operation == "" {
		if s.Revision != m.Expected.Revision {
			return zero, ErrHostRevision
		}
		m.Expected = s.HostRevision
	}
	if m.CloseExpected != nil && m.CloseExpected.Operation == "" {
		if s.Revision != m.CloseExpected.Revision {
			return zero, ErrHostRevision
		}
		expected := s.HostRevision
		m.CloseExpected = &expected
	}
	if m.Operation == "resolve" || m.Operation == "status" {
		return HostMutationResult{HostRevision: s.HostRevision, StateDir: v.StateDir, Desired: s.Desired}, nil
	}
	if handle == nil {
		return zero, ErrHostChild
	}
	return handle(ctx, v, m)
}

// HostWorkerChildEntry is shared by the production executable and test binary.
func HostWorkerChildEntry(ctx context.Context, in io.Reader, out io.Writer, r HostIdentityResolver, handle HostMutationHandler) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer stop()
	return ServeHostWorkerChild(ctx, in, out, r, handle)
}

const hostChildWaitDelay = time.Second

func hostChildLaunchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		// Reserve WaitDelay inside the caller's deadline, including forced exit.
		return context.WithDeadline(ctx, deadline.Add(-hostChildWaitDelay))
	}
	return context.WithCancel(ctx)
}

func LaunchHostWorkerChild(ctx context.Context, s HostChildLaunch) ([]byte, error) {
	executable, e := hostChildExecutable(s.ForceChild)
	if e != nil {
		return nil, e
	}
	b, e := json.Marshal(s.Request)
	if e != nil {
		return nil, ErrHostChild
	}
	ctx, cancel := hostChildLaunchContext(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "_host-worker-mutation")
	cmd.Env = WorkerChildEnvironment(s.Account)
	cmd.Dir = "/"
	cmd.Stdin = bytes.NewReader(b)
	cmd.Stderr = io.Discard
	if s.ForceChild {
		if os.Geteuid() == 0 || uint32(os.Geteuid()) != s.Account.UID {
			return nil, ErrHostIdentity
		}
	} else if e = setHostWorkerCredential(cmd, s.Account); e != nil {
		return nil, e
	}
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = hostChildWaitDelay
	var output bytes.Buffer
	cmd.Stdout = &limitedHostOutput{buffer: &output, remaining: 64 << 10}
	e = cmd.Run()
	if e != nil {
		var response hostChildResponse
		if decodeHostProtocol(bytes.NewReader(output.Bytes()), &response) != nil {
			return nil, ErrHostChild
		}
		if ctx.Err() != nil && response.Code == "" {
			response.Code = "deadline"
			response.Result.Partial = true
			response.Result.CleanupConfirmed = false
			return json.Marshal(response)
		}
		if response.Code != "deadline" {
			return nil, ErrHostChild
		}
	}
	return output.Bytes(), nil
}

type limitedHostOutput struct {
	buffer    *bytes.Buffer
	remaining int
}

func (w *limitedHostOutput) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, ErrHostChild
	}
	w.remaining -= len(p)
	return w.buffer.Write(p)
}

// CheckHostUserManager is discovery: callers must persist close before calling.
func CheckHostUserManager(ctx context.Context, command func(context.Context, string, ...string) ([]byte, error)) error {
	return (HostObservationContext{}).CheckUserManager(ctx, command)
}
