package myce

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Landlock can only allow, so withholding a few paths from a plain job means
// granting file reads everywhere else (#1309). REFER (ABI 2) must be handled
// too: an unhandled REFER would forbid every cross-directory rename and link.
const nativeCredentialAccess = unix.LANDLOCK_ACCESS_FS_READ_FILE | unix.LANDLOCK_ACCESS_FS_REFER

var nativeCredentialSupport = sync.OnceValue(func() bool {
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi < 2 {
		log.Printf("WARNING plain native jobs can read worker credentials: Landlock ABI 2 unavailable")
		return false
	}
	return true
})

// The payload, not the worker-owned supervisor, runs behind the helper. Its
// authored environment travels in the policy so loader variables cannot run
// code in the helper before the restriction applies.
func (w Worker) withholdNativeCredentials(cmd *exec.Cmd, spool, work string, env map[string]string) error {
	if err := nativeWrapPayloadCheck(cmd); err != nil {
		return err
	}
	if !nativeCredentialSupport() {
		return nil
	}
	deny, allow, err := w.nativeCredentialPaths(spool, []string{work, env["MYCE_CHECKPOINT_DIR"], env["MYCE_USAGE_FILE"]})
	if err != nil || len(deny) == 0 {
		return err
	}
	payloadEnv := cmd.Env
	if payloadEnv == nil {
		payloadEnv = os.Environ()
	}
	args, err := nativePolicyArgs(nativeLinuxPolicy{Plain: true, Deny: deny, Read: allow, Env: payloadEnv}, cmd.Args[4:])
	if err != nil {
		return err
	}
	if err := nativeWrapPayload(cmd, args); err != nil {
		return err
	}
	cmd.Env = environmentList(jobBaseEnvironment())
	return nil
}

// Grant file reads on every entry beside the path from / to each denied path,
// plus the explicit allows inside denied trees. Entries created later directly
// in one of those ancestor directories (/, home, ~/.config...) are not
// readable by this job. Symlinks need no rule: reads resolve to their target.
// Directory entry types avoid a stat per grant, which could block on a hung
// network mount beside home.
func nativeCredentialGrants(deny, allow []string) (dirs, files []string, err error) {
	ancestors := map[string]bool{}
	for _, path := range deny {
		for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
			ancestors[dir] = true
			if dir == filepath.Dir(dir) {
				break
			}
		}
	}
	add := func(path string, mode os.FileMode) {
		switch {
		case mode&os.ModeSymlink != 0:
		case mode.IsDir():
			dirs = append(dirs, path)
		default:
			files = append(files, path)
		}
	}
	for dir := range ancestors {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, nil, err
		}
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			if !ancestors[path] && !slices.Contains(deny, path) {
				add(path, entry.Type())
			}
		}
	}
	for _, path := range allow {
		if slices.ContainsFunc(deny, func(d string) bool { return nativePathContains(path, d) }) {
			continue
		}
		if info, err := os.Lstat(path); err == nil {
			add(path, info.Mode())
		}
	}
	slices.Sort(dirs)
	slices.Sort(files)
	return dirs, files, nil
}

func nativeApplyCredentialDeny(deny, allow []string) error {
	dirs, files, err := nativeCredentialGrants(deny, allow)
	if err != nil {
		return err
	}
	attr := unix.LandlockRulesetAttr{Access_fs: nativeCredentialAccess}
	fd, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&attr)), unsafe.Sizeof(attr), 0)
	if errno != 0 {
		return errno
	}
	defer unix.Close(int(fd))
	grant := func(path string, access uint64) error {
		pathFD, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(err, unix.ENOENT) {
			return nil // Removed since the directory was listed.
		}
		if err != nil {
			return err
		}
		defer unix.Close(pathFD)
		rule := unix.LandlockPathBeneathAttr{Allowed_access: access, Parent_fd: int32(pathFD)}
		_, _, errno := unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, fd, unix.LANDLOCK_RULE_PATH_BENEATH, uintptr(unsafe.Pointer(&rule)), 0, 0, 0)
		if errno == unix.EINVAL && access != unix.LANDLOCK_ACCESS_FS_READ_FILE {
			// Replaced by a non-directory since it was listed.
			rule.Allowed_access = unix.LANDLOCK_ACCESS_FS_READ_FILE
			_, _, errno = unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, fd, unix.LANDLOCK_RULE_PATH_BENEATH, uintptr(unsafe.Pointer(&rule)), 0, 0, 0)
		}
		if errno != 0 {
			return errno
		}
		return nil
	}
	for _, path := range dirs {
		if err := grant(path, nativeCredentialAccess); err != nil {
			return err
		}
	}
	for _, path := range files {
		if err := grant(path, unix.LANDLOCK_ACCESS_FS_READ_FILE); err != nil {
			return err
		}
	}
	if _, _, errno = unix.Syscall(unix.SYS_LANDLOCK_RESTRICT_SELF, fd, 0, 0); errno != 0 {
		return errno
	}
	return nil
}

// A service started with an EnvironmentFile carries the fleet token in its
// initial environment; same-user jobs could read it from /proc/PID/environ or
// attach to the process. Non-dumpable services keep both from them (#1309).
// Payloads become dumpable again at exec.
func protectServiceCredentials() error {
	return unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0)
}
