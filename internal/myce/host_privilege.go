package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

// HostInvocationError is stable across the child protocol. It deliberately has
// no raw command/account/config cause, which could contain private credentials.
type HostInvocationError struct{ Code, Message string }

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
	return HostIdentityResolver{Lookup: LookupWorkerAccount, ReadConfig: ReadServiceConfig, ValidatePath: validateHostTarget, EUID: os.Geteuid(), OS: runtime.GOOS}
}

type HostInvocation struct {
	Account    WorkerAccount
	Config     ServiceConfig
	ConfigPath string
	StateDir   string
	Privileged bool
}

// stateDir overrides only the shared host-control directory, never the worker
// spool, account, config, service, or queue. Existing state must name the same host.
func (r HostIdentityResolver) Resolve(workerUser, configPath, stateDir string) (HostInvocation, error) {
	var v HostInvocation
	if r.EUID == 0 && r.OS != "linux" {
		return v, ErrHostPrivilegeUnsupported
	}
	if r.EUID == 0 && workerUser == "" {
		return v, ErrHostIdentity
	}
	if r.Lookup == nil || r.ReadConfig == nil || r.ValidatePath == nil {
		return v, ErrHostIdentity
	}
	a, e := r.Lookup(workerUser)
	if e != nil || a.UID == 0 || !validWorkerAccount(a) || (r.EUID != 0 && uint32(r.EUID) != a.UID) {
		return v, ErrHostIdentity
	}
	if configPath == "" {
		configPath = filepath.Join(a.Home, ".local", "share", "mycelium", "worker", "config.json")
	}
	if e = r.ValidatePath(configPath, a.UID, false); e != nil {
		return v, ErrHostTarget
	}
	c, e := r.ReadConfig(configPath)
	if e != nil || c.Role != "worker" || c.HostID == "" || (c.Config != "" && c.Config != configPath) || (c.WorkerUser != "" && c.WorkerUser != a.Name) || (c.WorkerHome != "" && c.WorkerHome != a.Home) {
		return v, ErrHostIdentity
	}
	if stateDir == "" {
		stateDir = c.HostStateDir
	}
	if stateDir == "" {
		stateDir = filepath.Join(a.Home, ".local", "share", "myce")
	}
	for _, p := range []string{stateDir, c.State} {
		if p == "" || r.ValidatePath(p, a.UID, true) != nil {
			return v, ErrHostTarget
		}
	}
	// This is read-only identity validation; it creates neither a directory nor a lock.
	s, e := ReadHostIntent(stateDir)
	if e != nil {
		return v, e
	}
	if s.Host != c.HostID {
		return v, ErrHostIdentity
	}
	v = HostInvocation{Account: a, Config: c, ConfigPath: configPath, StateDir: stateDir, Privileged: r.EUID == 0}
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

// Ancestors may be root-owned, but no component may be a symlink. The selected
// file/directory and every active host-control entry must belong to the worker.
func validateHostTarget(path string, uid uint32, directory bool) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrHostTarget
	}
	for p := path; ; p = filepath.Dir(p) {
		st, e := os.Lstat(p)
		if e != nil || st.Mode()&os.ModeSymlink != 0 {
			return ErrHostTarget
		}
		if p == path {
			owner, ok := st.Sys().(*syscall.Stat_t)
			if !ok || owner.Uid != uid || st.IsDir() != directory || (!directory && !st.Mode().IsRegular()) {
				return ErrHostTarget
			}
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	return nil
}

// ValidateHostControlTree rejects pre-existing foreign locks and symlinks before
// root launches a child. It never repairs ownership or follows links.
func ValidateHostControlTree(dir string, uid uint32) error {
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
	Account WorkerAccount
	Env     []string
	Request HostMutation
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
	if !v.Privileged {
		if handle == nil {
			return HostMutationResult{}, ErrHostChild
		}
		return handle(ctx, v, m)
	}
	if launch == nil {
		launch = LaunchHostWorkerChild
	}
	b, e := launch(ctx, HostChildLaunch{v.Account, WorkerChildEnvironment(v.Account), m})
	if e != nil {
		return HostMutationResult{}, ErrHostChild
	}
	var r hostChildResponse
	if decodeHostProtocol(bytes.NewReader(b), &r) != nil || r.Version != 1 || r.Operation != m.Operation {
		return HostMutationResult{}, ErrHostChild
	}
	if r.Code != "" {
		return HostMutationResult{}, hostProtocolError(r.Code)
	}
	if (m.Operation == "close" || m.Operation == "open" || m.Operation == "reconcile") && (r.Result.Revision == 0 || r.Result.Operation == "") {
		return HostMutationResult{}, ErrHostChild
	}
	return r.Result, nil
}
func validHostMutation(m HostMutation) bool {
	return (m.Operation == "close" || m.Operation == "open" || m.Operation == "reconcile" || (ValidWorkerUnit(m.Unit) && (m.Operation == "unit-stop" || m.Operation == "unit-kill" || m.Operation == "unit-start"))) && m.Grace >= 0 && m.Grace <= time.Minute && m.Timeout >= 0 && len(m.Reason) <= 4096
}
func hostProtocolError(code string) error {
	switch code {
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
	case e == nil:
		return ""
	case errors.Is(e, ErrHostIdentity):
		return "identity"
	case errors.Is(e, ErrHostTarget):
		return "target"
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
	var v HostInvocation
	if e == nil {
		if r.EUID == 0 {
			e = ErrHostIdentity
		} else {
			v, e = r.Resolve(m.WorkerUser, m.ConfigPath, m.StateDir)
		}
	}
	if e == nil {
		e = ValidateHostControlTree(v.StateDir, v.Account.UID)
	}
	if e == nil {
		timeout := m.Timeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ctx = childCtx
		if handle == nil {
			e = ErrHostChild
		} else {
			result, e = handle(ctx, v, m)
		}
	}
	if e != nil {
		result = HostMutationResult{}
	}
	// Deliberately never forward raw handler errors, command output, or stderr:
	// those can contain credentials read by the worker after durable close.
	return json.NewEncoder(out).Encode(hostChildResponse{Version: 1, Operation: m.Operation, Result: result, Code: hostProtocolCode(e)})
}

func LaunchHostWorkerChild(ctx context.Context, s HostChildLaunch) ([]byte, error) {
	executable, e := os.Executable()
	if e != nil {
		return nil, ErrHostChild
	}
	b, e := json.Marshal(s.Request)
	if e != nil {
		return nil, ErrHostChild
	}
	cmd := exec.CommandContext(ctx, executable, "_host-worker-mutation")
	cmd.Env = WorkerChildEnvironment(s.Account)
	cmd.Dir = "/"
	cmd.Stdin = bytes.NewReader(b)
	cmd.Stderr = io.Discard
	if e = setHostWorkerCredential(cmd, s.Account); e != nil {
		return nil, e
	}
	var output bytes.Buffer
	cmd.Stdout = &limitedHostOutput{buffer: &output, remaining: 64 << 10}
	if e = cmd.Run(); e != nil {
		return nil, ErrHostChild
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
