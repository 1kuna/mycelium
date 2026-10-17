package myce

import (
	"os"
	"path/filepath"
	"syscall"
)

const RootHostExecutable = "/usr/local/libexec/myce/myce"

var ErrRootBinaryUntrusted = &HostInvocationError{"root_binary_untrusted", "use the separately installed root-owned host-control binary"}

// RootBinaryProbe inspects the executing inode, not just its pathname.
type RootBinaryProbe struct {
	Executing func() (os.FileInfo, error)
	Readlink  func(string) (string, error)
	Lstat     func(string) (os.FileInfo, error)
	AccessACL func(string) error
}

func CheckRootBinary(p RootBinaryProbe) error {
	if p.Executing == nil || p.Readlink == nil || p.Lstat == nil || p.AccessACL == nil {
		return ErrRootBinaryUntrusted
	}
	running, e := p.Executing()
	if e != nil || !trustedRootMode(running, false) {
		return ErrRootBinaryUntrusted
	}
	path, e := p.Readlink("/proc/self/exe")
	if e != nil || path != RootHostExecutable {
		return ErrRootBinaryUntrusted
	}
	st, e := p.Lstat(path)
	if e != nil || !trustedRootMode(st, false) || !sameRootInode(running, st) || p.AccessACL(path) != nil {
		return ErrRootBinaryUntrusted
	}
	for path = filepath.Dir(path); ; path = filepath.Dir(path) {
		st, e = p.Lstat(path)
		if e != nil || !trustedRootMode(st, true) || p.AccessACL(path) != nil {
			return ErrRootBinaryUntrusted
		}
		if filepath.Dir(path) == path {
			break
		}
	}
	return nil
}
func sameRootInode(a, b os.FileInfo) bool {
	x, xok := a.Sys().(*syscall.Stat_t)
	y, yok := b.Sys().(*syscall.Stat_t)
	return xok && yok && x.Dev == y.Dev && x.Ino == y.Ino
}
func trustedRootMode(st os.FileInfo, directory bool) bool {
	if st == nil || st.Mode().Perm()&0022 != 0 || st.IsDir() != directory || (!directory && st.Mode() != 0755) {
		return false
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	return ok && owner.Uid == 0
}

// CheckRootCommand is the sole command allowlist for an EUID-0 invocation.
func CheckRootCommand(args []string, euid int) error {
	if euid != 0 {
		return nil
	}
	if len(args) == 1 && args[0] == "_host-worker-mutation" {
		return nil
	}
	if len(args) >= 3 && args[0] == "host" {
		switch args[1] {
		case "close", "open", "status", "reconcile":
			for _, arg := range args[2:] {
				if arg == "--local" || arg == "--local=true" {
					return nil
				}
			}
		}
	}
	return ErrHostIdentity
}
