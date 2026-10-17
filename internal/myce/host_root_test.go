package myce

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type rootStatFixture struct {
	uid   uint32
	mode  os.FileMode
	inode uint64
}

func (s rootStatFixture) Name() string       { return "fixture" }
func (s rootStatFixture) Size() int64        { return 1 }
func (s rootStatFixture) Mode() os.FileMode  { return s.mode }
func (s rootStatFixture) ModTime() time.Time { return time.Time{} }
func (s rootStatFixture) IsDir() bool        { return s.mode.IsDir() }
func (s rootStatFixture) Sys() any           { return &syscall.Stat_t{Uid: s.uid, Ino: s.inode, Dev: 1} }

func TestRootBinaryGuard(t *testing.T) {
	for _, bad := range []string{"", "inode", "file-owner", "file-write", "file-setuid", "file-mode", "ancestor-owner", "ancestor-write", "symlink", "worker-copy", "deleted", "fstat", "file-acl", "ancestor-acl"} {
		t.Run(bad, func(t *testing.T) {
			p := RootBinaryProbe{Executing: func() (os.FileInfo, error) {
				if bad == "fstat" {
					return nil, os.ErrPermission
				}
				return rootStatFixture{0, 0755, 1}, nil
			}, Readlink: func(string) (string, error) {
				if bad == "worker-copy" {
					return "/home/worker/myce", nil
				}
				if bad == "deleted" {
					return RootHostExecutable + " (deleted)", nil
				}
				return RootHostExecutable, nil
			}, Lstat: func(path string) (os.FileInfo, error) {
				if path == RootHostExecutable {
					s := rootStatFixture{0, 0755, 1}
					switch bad {
					case "inode":
						s.inode = 2
					case "file-owner":
						s.uid = 1001
					case "file-write":
						s.mode = 0775
					case "file-setuid":
						s.mode = os.ModeSetuid | 0755
					case "file-mode":
						s.mode = 0700
					}
					return s, nil
				}
				s := rootStatFixture{0, os.ModeDir | 0755, 2}
				if path == filepath.Dir(RootHostExecutable) {
					switch bad {
					case "ancestor-owner":
						s.uid = 1001
					case "ancestor-write":
						s.mode = os.ModeDir | 0777
					case "symlink":
						s.mode = os.ModeSymlink | 0777
					}
				}
				return s, nil
			}, AccessACL: func(path string) error {
				if (bad == "file-acl" && path == RootHostExecutable) || (bad == "ancestor-acl" && path == "/usr") {
					return ErrHostTarget
				}
				return nil
			}}
			err := CheckRootBinary(p)
			if bad == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var typed *HostInvocationError
			if !errors.Is(err, ErrHostTarget) || !errors.As(err, &typed) || typed.Code != "root_binary_untrusted" {
				t.Fatal(err)
			}
		})
	}
}
func TestRootCommandAllowlist(t *testing.T) {
	for _, args := range [][]string{{"host", "convert", "--local"}, {"resolve-docker"}, {"install"}, {"service"}, {"dev"}, {"_command-exec"}, {"_host-start-check"}, {"host", "status"}, {"host", "status", "--local=false"}} {
		if err := CheckRootCommand(args, 0); err == nil {
			t.Fatal(args)
		}
		if err := CheckRootCommand(args, 1001); err != nil {
			t.Fatal(args, err)
		}
	}
	for _, op := range []string{"close", "open", "status", "reconcile"} {
		if err := CheckRootCommand([]string{"host", op, "--local"}, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := CheckRootCommand([]string{"_host-worker-mutation"}, 0); err != nil {
		t.Fatal(err)
	}
}
