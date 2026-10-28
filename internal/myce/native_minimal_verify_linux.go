package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sys/unix"
)

// A trusted launcher constructs the root; the final helper verifies it before
// adding the namespace-only socket exception. No filesystem/userns change is
// possible after seccomp. All members join the same anchor mount namespace.
func nativeVerifyMinimalRoot(p nativeLinuxPolicy) ([]string, error) {
	if !p.Namespace {
		return nil, errors.New("minimal-root requires namespaces")
	}
	if err := nativeMinimalInheritedFiles(); err != nil {
		return nil, err
	}
	dir := p.Workdir
	if dir == "" && len(p.Write) > 0 {
		dir = p.Write[0]
	}
	if err := nativeMinimalDestination(dir); err != nil {
		return nil, err
	}
	var currentWD, mountedWD unix.Statx_t
	if unix.Statx(unix.AT_FDCWD, ".", 0, unix.STATX_MNT_ID|unix.STATX_INO, &currentWD) != nil || unix.Statx(unix.AT_FDCWD, dir, 0, unix.STATX_MNT_ID|unix.STATX_INO, &mountedWD) != nil || currentWD.Mnt_id == 0 || currentWD.Mnt_id != mountedWD.Mnt_id || currentWD.Ino != mountedWD.Ino {
		return nil, errors.New("minimal-root cwd is outside mounted workdir")
	}
	var fs unix.Statfs_t
	if err := unix.Statfs("/", &fs); err != nil || fs.Type != unix.TMPFS_MAGIC {
		return nil, errors.New("minimal-root empty tmpfs root missing")
	}
	fd, err := unix.Open(nativeMinimalReceiptPath, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("minimal-root trusted mounted receipt missing")
	}
	receiptFile := os.NewFile(uintptr(fd), "minimal-root receipt")
	defer receiptFile.Close()
	var receiptStat unix.Stat_t
	if err := unix.Fstat(fd, &receiptStat); err != nil || receiptStat.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, errors.New("minimal-root receipt is not regular")
	}
	data, err := io.ReadAll(receiptFile)
	if err != nil {
		return nil, err
	}
	var receipt nativeMinimalReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return nil, err
	}
	for _, name := range []string{"user", "mnt", "pid", "ipc", "net"} {
		var st unix.Stat_t
		if err := unix.Stat("/proc/self/ns/"+name, &st); err != nil || receipt.HostNamespaces[name] == 0 || st.Ino == receipt.HostNamespaces[name] {
			return nil, fmt.Errorf("minimal-root private namespace missing: %s", name)
		}
	}
	var initNS, ownNS unix.Stat_t
	if unix.Stat("/proc/1/ns/pid", &initNS) != nil || unix.Stat("/proc/self/ns/pid", &ownNS) != nil || initNS.Ino != ownNS.Ino {
		return nil, errors.New("minimal-root proc does not describe current PID namespace")
	}
	// Writable grants cannot be enlarged by a member policy. The trusted
	// anchor has already installed exactly these inodes, shared by its members.
	for _, path := range p.Write {
		if !slices.ContainsFunc(receipt.Writable, func(o nativeMinimalObject) bool { return o.Path == path }) {
			return nil, errors.New("minimal-root member writable path absent from anchor")
		}
	}
	expected := map[string]bool{nativeMinimalReceiptPath: true}
	for _, path := range receipt.ReadOnlyDirectories {
		if err := nativeMinimalDestination(path); err != nil {
			return nil, err
		}
		var st unix.Stat_t
		if path == "/tmp" || !nativePathContains("/tmp", path) || unix.Lstat(path, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || unix.Statfs(path, &fs) != nil || fs.Type != unix.TMPFS_MAGIC {
			return nil, errors.New("minimal-root invalid imported directory skeleton")
		}
		expected[path] = true
	}
	for _, objects := range [][]nativeMinimalObject{receipt.Files, receipt.Writable, receipt.Devices, receipt.KernelDirectories} {
		for _, o := range objects {
			if err := nativeMinimalDestination(o.Path); err != nil {
				return nil, err
			}
			var st unix.Stat_t
			if err := unix.Lstat(o.Path, &st); err != nil || uint64(st.Dev) != o.Dev || st.Ino != o.Ino || st.Mode&unix.S_IFMT != o.Mode&unix.S_IFMT {
				return nil, fmt.Errorf("minimal-root mounted inode mismatch: %s", o.Path)
			}
			expected[o.Path] = true
		}
	}
	for _, o := range receipt.KernelDirectories {
		if !nativeMinimalKernelRoot(o.Path) || unix.Statfs(o.Path, &fs) != nil || fs.Type != unix.SYSFS_MAGIC {
			return nil, errors.New("minimal-root untrusted discovery directory")
		}
	}
	for _, path := range []string{"/run", "/tmp", "/dev/shm"} {
		if err := unix.Statfs(path, &fs); err != nil || fs.Type != unix.TMPFS_MAGIC {
			return nil, fmt.Errorf("minimal-root private tmpfs missing: %s", path)
		}
	}
	mountinfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	found := map[string]bool{}
	for _, line := range strings.Split(string(mountinfo), "\n") {
		if line == "" {
			continue
		}
		before, after, ok := strings.Cut(line, " - ")
		fields := strings.Fields(before)
		if !ok || len(fields) < 6 {
			return nil, errors.New("minimal-root invalid mountinfo")
		}
		path := unescape.Replace(fields[4])
		found[path] = true
		switch path {
		case "/", "/run", "/tmp", "/dev", "/dev/shm":
			if path == "/" && !slices.Contains(strings.Split(fields[5], ","), "ro") {
				return nil, errors.New("minimal-root synthetic root is writable")
			}
			if !strings.HasPrefix(after, "tmpfs ") {
				return nil, errors.New("minimal-root private filesystem missing")
			}
		case "/proc":
			if !strings.HasPrefix(after, "proc ") {
				return nil, errors.New("minimal-root private proc missing")
			}
		case "/dev/pts":
			if !strings.HasPrefix(after, "devpts ") {
				return nil, errors.New("minimal-root invalid devpts")
			}
		default:
			// Standard bwrap --dev can install exact safe character devices.
			if slices.Contains([]string{"/dev/null", "/dev/zero", "/dev/full", "/dev/random", "/dev/urandom", "/dev/tty", "/dev/console"}, path) {
				continue
			}
			if !expected[path] {
				return nil, fmt.Errorf("minimal-root unexpected mount: %s", path)
			}
			if path == nativeMinimalReceiptPath || slices.Contains(receipt.ReadOnlyDirectories, path) || slices.ContainsFunc(receipt.Files, func(o nativeMinimalObject) bool { return o.Path == path }) || slices.ContainsFunc(receipt.KernelDirectories, func(o nativeMinimalObject) bool { return o.Path == path }) {
				if !slices.Contains(strings.Split(fields[5], ","), "ro") {
					return nil, errors.New("minimal-root imported mount is not read-only")
				}
			}
		}
	}
	for path := range expected {
		if !found[path] {
			return nil, fmt.Errorf("minimal-root exact mount missing: %s", path)
		}
	}
	for _, path := range []string{"/", "/proc", "/run", "/tmp", "/dev", "/dev/shm"} {
		if !found[path] {
			return nil, errors.New("minimal-root private mount missing")
		}
	}
	// No cgroup2 mount is needed (or allowed). Resolve this helper's kernel
	// membership using the trusted host mapping captured after attachment.
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil, err
	}
	own, err := nativeCurrentCgroup(membership, receipt.CgroupMountinfo)
	if err != nil {
		return nil, err
	}
	var metrics []string
	for _, path := range receipt.MetricFiles {
		if !slices.ContainsFunc(receipt.Files, func(o nativeMinimalObject) bool { return o.Path == path }) || !slices.Contains(nativeMemoryMetricNames, filepath.Base(path)) {
			return nil, errors.New("minimal-root invalid metric receipt")
		}
		metrics = append(metrics, path)
	}
	if !slices.ContainsFunc(metrics, func(path string) bool { return filepath.Dir(path) == own }) {
		return nil, errors.New("minimal-root current cgroup is not a mounted payload leaf")
	}
	for _, path := range p.Devices {
		if !slices.ContainsFunc(receipt.Devices, func(o nativeMinimalObject) bool { return o.Path == path }) {
			return nil, errors.New("minimal-root assigned device absent from anchor")
		}
	}
	return metrics, nil
}
