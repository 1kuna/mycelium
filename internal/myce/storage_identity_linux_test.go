package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestStorageIdentityLinuxFields(t *testing.T) {
	root := t.TempDir()
	var fs syscall.Statfs_t
	var st syscall.Stat_t
	if err := syscall.Statfs(root, &fs); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Stat(root, &st); err != nil {
		t.Fatal(err)
	}
	before := linuxStorageFilesystemIdentity(fs, st)
	if got, err := ObserveStorageBackingIdentity(root); err != nil || got != identityHash(before) || before.Device != uint64(st.Dev) || before.FSID != fs.Fsid.X__val {
		t.Fatal("native Linux observation did not use f_fsid and st_dev", got, err)
	}
	// Real syscall structures, with only mutable observations changed.
	fs.Bfree, fs.Bavail, fs.Ffree = 0, 0, 0
	st.Ino, st.Size, st.Blocks = 999, 1234, 8
	if linuxStorageFilesystemIdentity(fs, st) != before {
		t.Fatal("Linux usage or root inode participated in volume identity")
	}
	fs.Fsid.X__val[0] ^= 1
	if linuxStorageFilesystemIdentity(fs, st) == before {
		t.Fatal("Linux different filesystem ID not detected")
	}
	fs.Fsid.X__val = before.FSID
	st.Dev++
	if linuxStorageFilesystemIdentity(fs, st) == before {
		t.Fatal("Linux different device not detected")
	}
}

func TestStorageIdentityNativeLinuxLoop(t *testing.T) {
	if os.Getenv("MYCE_TEST_STORAGE_IDENTITY_NATIVE") != "1" {
		t.Skip("opt-in: disposable ext4 loop filesystems; no sudo or root setup")
	}
	if _, err := os.Stat("/dev/loop-control"); err != nil {
		t.Skip("native loop qualification unavailable: no /dev/loop-control")
	}
	for _, command := range []string{"losetup", "mkfs.ext4", "mount", "umount"} {
		if _, err := exec.LookPath(command); err != nil {
			t.Skipf("native loop qualification unavailable: %s missing", command)
		}
	}
	scratch := os.Getenv("MYCE_TEST_STORAGE_IDENTITY_SCRATCH")
	if scratch == "" {
		t.Fatal("set MYCE_TEST_STORAGE_IDENTITY_SCRATCH outside isolated HOME/TMPDIR")
	}
	dir, err := os.MkdirTemp(scratch, "linux-identity-")
	if err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(dir, "mount")
	if err := os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	var loops []string
	mounted := false
	run := func(command string, args ...string) (string, error) {
		out, err := exec.Command(command, args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	t.Cleanup(func() {
		if mounted {
			if out, err := run("umount", mount); err != nil {
				t.Errorf("unmount failed; retain %s: %v %s", dir, err, out)
				return
			}
		}
		for _, loop := range loops {
			if out, err := run("losetup", "--detach", loop); err != nil {
				t.Errorf("loop detach failed; retain %s: %v %s", dir, err, out)
				return
			}
		}
		parentID, parentErr := ObserveStorageBackingIdentity(dir)
		mountID, mountErr := ObserveStorageBackingIdentity(mount)
		if parentErr != nil || mountErr != nil || parentID != mountID {
			t.Errorf("mount remains active or unverifiable; retain %s", dir)
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	for _, name := range []string{"a.img", "b.img"} {
		path := filepath.Join(dir, name)
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		err = f.Truncate(64 << 20) // Identity proof only, not availability backing.
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		if out, err := run("mkfs.ext4", "-q", "-F", path); err != nil {
			t.Fatal("format disposable image", err, out)
		}
		loop, err := run("losetup", "--find", "--show", path)
		if err != nil {
			t.Skipf("native loop qualification unavailable: %v %s", err, loop)
		}
		loops = append(loops, loop)
	}
	if out, err := run("mount", loops[0], mount); err != nil {
		t.Skipf("native loop mount unavailable without additional privilege: %v %s", err, out)
	}
	mounted = true
	switchVolume := func(which int) {
		t.Helper()
		if out, err := run("umount", mount); err != nil {
			t.Fatal("unmount disposable loop", err, out)
		}
		mounted = false
		if out, err := run("mount", loops[which], mount); err != nil {
			t.Fatal("mount disposable loop", err, out)
		}
		mounted = true
	}
	proveStorageIdentityVolumes(t, mount, switchVolume)
}
