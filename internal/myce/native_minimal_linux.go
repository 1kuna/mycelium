package myce

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const nativeMinimalReceiptPath = "/.myce-minimal-root"

var nativeMinimalDetection sync.Map // Per operator configuration, in this service context.

func (w Worker) nativeLinuxSupport() nativeLinuxSupport {
	if !w.NativeMinimalRoot {
		return nativeLinuxDetection()
	}
	probe, _ := nativeMinimalDetection.LoadOrStore(nativeMinimalSupportKey(w), sync.OnceValue(func() nativeLinuxSupport { return detectNativeMinimalSupport(w) }))
	return probe.(func() nativeLinuxSupport)()
}

func nativeMinimalSupportKey(w Worker) string {
	key, _ := json.Marshal([]any{w.NativeExecuteRoots, w.NativeReadRoots, w.EnvironmentRoot, w.ExtensionRoot, w.CacheRoot, w.ModelRoot, w.StateDir})
	return string(key)
}

func detectNativeMinimalSupport(w Worker) nativeLinuxSupport {
	s := nativeLinuxSupport{mode: "unavailable", reason: "minimal_root_probe_failed"}
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi < 6 {
		s.reason = "landlock_abi6_required"
		return s
	}
	s.bwrap, _ = exec.LookPath("bwrap")
	s.nsenter, _ = exec.LookPath("nsenter")
	if s.bwrap == "" || s.nsenter == "" {
		s.reason = "namespace_tools_missing"
		return s
	}
	helpCtx, helpCancel := context.WithTimeout(context.Background(), 2*time.Second)
	help, helpErr := exec.CommandContext(helpCtx, s.bwrap, "--help").CombinedOutput()
	helpCancel()
	if helpErr != nil || !strings.Contains(string(help), "--ro-bind-fd") || !strings.Contains(string(help), "--bind-fd") {
		s.reason = "minimal_root_fd_bind_required"
		return s
	}
	if len(w.NativeExecuteRoots) == 0 {
		s.reason = "minimal_root_runtime_required"
		return s
	}
	work, err := os.MkdirTemp("", "myce-minimal-probe-")
	if err != nil {
		return s
	}
	defer os.RemoveAll(work)
	cmd := exec.Command("/bin/true")
	cmd.Dir = work
	probeWorker := w
	probeWorker.Machine.GPU = "cpu" // Probe confinement, never accelerator initialization.
	p, err := probeWorker.nativeLinuxPolicy(cmd, w.StateDir, work, jobBaseEnvironment(), nil)
	if err != nil {
		s.reason = "minimal_root_layout_refused"
		return s
	}
	p.Namespace = true
	// Bound a tiny capability helper, not workload computation. No package,
	// AppArmor, sysctl or service mutation is attempted on failure.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := nativeMinimalServiceProbe(ctx, p, s, work, []string{"/bin/true"})
	if err != nil {
		message := strings.ToLower(string(out))
		if strings.Contains(message, "bwrap:") && (strings.Contains(message, "operation not permitted") || strings.Contains(message, "permission denied")) {
			s.reason = "minimal_root_policy_denied"
		}
		return s // Explicit opt-in never selects the old root or fallback.
	}
	s.mode, s.reason = "minimal_root", ""
	return s
}

// Exercise the actual shared-anchor join as well as root construction,
// Landlock/seccomp, metrics and exec in the service's security context.
func nativeMinimalServiceProbe(ctx context.Context, p nativeLinuxPolicy, support nativeLinuxSupport, work string, payload []string) ([]byte, error) {
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil, err
	}
	mountinfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	own, err := nativeCurrentCgroup(membership, mountinfo)
	if err != nil {
		return nil, err
	}
	p.MetricCgroups = append(slices.Clone(p.MetricCgroups), own) // Probe current leaf; never a submitted job.
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	args, err := nativeMinimalLaunchArgs(support.bwrap, p, []string{executable, "_native-exec", "minimal-probe-anchor"}, true)
	if err != nil {
		return nil, err
	}
	n := &nativeNamespace{support: support}
	n.infoReader, n.infoWriter, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	defer n.close()
	n.readyReader, n.readyWriter, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	defer writer.Close()
	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Dir, cmd.Env, cmd.Stdin = work, environmentList(jobBaseEnvironment()), reader
	cmd.Stdout, cmd.Stderr = &output, &output
	cmd.ExtraFiles = []*os.File{n.infoWriter, n.readyWriter}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	finish := func() { writer.Close(); _ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }
	// activate normally waits only in namespace modes; the intended probe is
	// still unavailable until this entire round succeeds.
	n.support.mode = "minimal_root"
	if err := n.activate(ctx, writer); err != nil {
		finish()
		return output.Bytes(), err
	}
	smoke, err := nativePolicyArgs(p, payload)
	if err != nil {
		finish()
		return output.Bytes(), err
	}
	enter := n.enterArgs(work)
	joined := exec.CommandContext(ctx, enter[0], append(enter[1:], smoke...)...)
	joined.Env = environmentList(jobBaseEnvironment())
	out, err := joined.CombinedOutput()
	finish()
	return append(output.Bytes(), out...), err
}

type nativeMinimalObject struct {
	Path     string
	Dev, Ino uint64
	Mode     uint32
}

// A sealed metadata FD is copied by bwrap into an exact read-only receipt. It records
// actual mounted inodes and the trusted host cgroup mapping, not authored env.
type nativeMinimalReceipt struct {
	Files, Writable, Devices []nativeMinimalObject
	KernelDirectories        []nativeMinimalObject
	MetricFiles              []string
	CgroupMountinfo          []byte
	HostNamespaces           map[string]uint64
}

type nativeMinimalBuilder struct {
	args        []string
	fds         []*os.File
	seen        map[string]bool
	read, write []string
	receipt     nativeMinimalReceipt
	pinErr      error
}

func (b *nativeMinimalBuilder) close() {
	for _, f := range b.fds {
		f.Close()
	}
	b.fds = nil
}

func (b *nativeMinimalBuilder) keep(fd int, name string) string {
	if fd < 5 {
		duplicate, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 10)
		if err != nil {
			b.pinErr = err
			unix.Close(fd)
			return ""
		}
		unix.Close(fd)
		fd = duplicate
	}
	b.fds = append(b.fds, os.NewFile(uintptr(fd), name))
	return strconv.Itoa(fd)
}

func nativeMinimalStat(path string, st *unix.Stat_t) nativeMinimalObject {
	return nativeMinimalObject{path, uint64(st.Dev), st.Ino, st.Mode}
}

// No directory from a read tree is bound. Children are opened relative to a
// pinned directory with O_NOFOLLOW; each regular file is then individually
// bound using bwrap's bind-fd inode comparison before executable startup.
// Renames can retain the pinned inode or refuse; a socket replacement never
// passes that comparison. No source directory's children remain exposed.
func (b *nativeMinimalBuilder) tree(parent int, name, dest string) error {
	if b.seen[dest] {
		return nil
	}
	if err := nativeMinimalDestination(dest); err != nil {
		return err
	}
	fd, err := unix.Openat(parent, name, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return err
	}
	b.seen[dest] = true
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		defer unix.Close(fd)
		buf := make([]byte, 4096)
		n, err := unix.Readlinkat(fd, "", buf)
		if err != nil || n == len(buf) {
			return errors.New("minimal-root symlink unreadable or too long")
		}
		target := string(buf[:n])
		if err := nativeMinimalLink(dest, target, b.read, b.write); err != nil {
			return fmt.Errorf("%s: %w", dest, err)
		}
		b.args = append(b.args, "--symlink", target, dest)
	case unix.S_IFREG:
		source := b.keep(fd, dest)
		b.args = append(b.args, "--ro-bind-fd", source, dest)
		b.receipt.Files = append(b.receipt.Files, nativeMinimalStat(dest, &st))
	case unix.S_IFDIR:
		defer unix.Close(fd)
		// Open the directory inode itself, never its replaceable host pathname.
		dirFD, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		dir := os.NewFile(uintptr(dirFD), dest)
		defer dir.Close()
		names, err := dir.Readdirnames(-1)
		if err != nil {
			return err
		}
		slices.Sort(names)
		b.args = append(b.args, "--dir", dest)
		for _, child := range names {
			if err := b.tree(fd, child, filepath.Join(dest, child)); err != nil {
				return err
			}
		}
	default:
		unix.Close(fd)
		return fmt.Errorf("minimal-root socket/special file refused: %s", dest)
	}
	return nil
}

func nativeMinimalMemfd(name string, data []byte) (int, error) {
	fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return -1, err
	}
	for len(data) > 0 {
		n, err := unix.Write(fd, data)
		if err != nil {
			unix.Close(fd)
			return -1, err
		}
		data = data[n:]
	}
	if _, err = unix.Seek(fd, 0, 0); err == nil {
		_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_SEAL|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_WRITE)
	}
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func nativeMinimalLaunchArgs(bwrap string, p nativeLinuxPolicy, payload []string, anchor bool) ([]string, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return append([]string{executable, "_native-exec", "minimal-launch", bwrap, strconv.FormatBool(anchor), base64.RawStdEncoding.EncodeToString(data)}, payload...), nil
}

func nativeMinimalLaunch(args []string) error {
	if len(args) < 4 {
		return errors.New("minimal-root launcher arguments missing")
	}
	var p nativeLinuxPolicy
	data, err := base64.RawStdEncoding.DecodeString(args[2])
	if err != nil || json.Unmarshal(data, &p) != nil || !p.MinimalRoot {
		return errors.New("minimal-root policy missing")
	}
	b, err := nativeBuildMinimalRoot(p, args[1] == "true")
	if err != nil {
		return fmt.Errorf("minimal-root construction: %w", err)
	}
	defer b.close()
	argv := append(b.args, "--")
	argv = append(argv, args[3:]...)
	// --args consumes NUL-separated arguments, avoiding ARG_MAX for real
	// runtime trees. No model contents are copied. Descriptor/mount exhaustion
	// is a visible refusal, never a directory-bind or host-root fallback.
	fd, err := nativeMinimalMemfd("myce-bwrap-args", []byte(strings.Join(argv, "\x00")+"\x00"))
	if err != nil {
		return err
	}
	if b.keep(fd, "bwrap arguments") == "" {
		return errors.New("minimal-root descriptor pinning failed")
	}
	fd = int(b.fds[len(b.fds)-1].Fd())
	for _, f := range b.fds {
		if _, err := unix.FcntlInt(f.Fd(), unix.F_SETFD, 0); err != nil {
			return err
		}
	}
	// Preserve the existing monitor's TERM/checkpoint behavior and PID.
	return nativeNamespaceMonitor([]string{args[0], "--args", strconv.Itoa(fd)})
}

func nativeBuildMinimalRoot(p nativeLinuxPolicy, anchor bool) (*nativeMinimalBuilder, error) {
	b := &nativeMinimalBuilder{seen: map[string]bool{}, read: append(slices.Clone(p.Read), p.ReadOnly...), write: p.Write}
	ok := false
	defer func() {
		if !ok {
			b.close()
		}
	}()
	b.receipt.HostNamespaces = map[string]uint64{}
	for _, name := range []string{"user", "mnt", "pid", "ipc", "net"} {
		var st unix.Stat_t
		if err := unix.Stat("/proc/self/ns/"+name, &st); err != nil {
			return nil, err
		}
		b.receipt.HostNamespaces[name] = st.Ino
	}
	b.args = []string{"--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-net", "--die-with-parent", "--cap-drop", "ALL", "--tmpfs", "/", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/run", "--tmpfs", "/tmp", "--tmpfs", "/dev/shm"}
	if anchor {
		b.args = append(b.args, "--info-fd", "3")
	}
	// Resolve metrics here, after attachment, while trusted host mountinfo is
	// still visible. The shared anchor exposes payload leaves only, never its
	// own control leaf or the aggregate attempt group.
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil, err
	}
	b.receipt.CgroupMountinfo, err = os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	if _, err := nativeCgroupMetricFiles(membership, b.receipt.CgroupMountinfo, p); err != nil {
		return nil, err
	}
	if anchor {
		if len(p.MetricCgroups) == 0 {
			return nil, errors.New("minimal-root anchor requires trusted payload metric leaves")
		}
	} else {
		own, err := nativeCurrentCgroup(membership, b.receipt.CgroupMountinfo)
		if err != nil {
			return nil, err
		}
		p.MetricCgroups = append(slices.Clone(p.MetricCgroups), own)
	}
	for _, group := range p.MetricCgroups {
		if err := nativeMinimalDestination(group); err != nil {
			return nil, err
		}
		for _, name := range nativeMemoryMetricNames {
			path := filepath.Join(group, name)
			st, err := os.Lstat(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			} // Same optional-controller contract.
			if err != nil || !st.Mode().IsRegular() {
				return nil, fmt.Errorf("minimal-root invalid metric: %s", path)
			}
			b.receipt.MetricFiles = append(b.receipt.MetricFiles, path)
		}
	}
	if len(b.receipt.MetricFiles) == 0 {
		return nil, errors.New("minimal-root current metrics unavailable")
	}
	// Synthetic proc is mounted separately; importing host proc self/task
	// paths or broad cgroup trees through a runtime declaration is forbidden.
	for _, path := range b.read {
		if path == "/proc" {
			return nil, errors.New("minimal-root broad proc root refused")
		}
		if strings.HasPrefix(path, "/proc/") {
			continue
		}
		if err := nativeMinimalDestination(path); err != nil {
			return nil, err
		}
		if path == "/run" || strings.HasPrefix(path, "/run/") || path == "/dev" || strings.HasPrefix(path, "/dev/") || path == "/sys/fs/cgroup" || strings.HasPrefix(path, "/sys/fs/cgroup/") {
			return nil, fmt.Errorf("minimal-root unsafe runtime root: %s", path)
		}
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		source, err := nativeCanonical(path)
		if err != nil {
			return nil, err
		}
		// sysfs discovery is a kernel-generated filesystem: users cannot add
		// socket inodes or arbitrary symlinks there. Pin only these fixed
		// discovery roots, never /sys or cgroupfs, and reject submount aliases.
		if nativeMinimalKernelRoot(path) {
			if err := b.kernelDirectory(source, path); err != nil {
				return nil, err
			}
			continue
		}
		if err := b.tree(unix.AT_FDCWD, source, path); err != nil {
			return nil, err
		}
	}
	if err := nativeMinimalReadAliases(p, b.receipt.Files); err != nil {
		return nil, err
	}
	for _, path := range b.receipt.MetricFiles {
		if err := b.tree(unix.AT_FDCWD, path, path); err != nil {
			return nil, err
		}
	}
	for _, path := range p.Write {
		if err := nativeMinimalDestination(path); err != nil {
			return nil, err
		}
		for _, root := range b.read {
			if nativePathContains(path, root) || nativePathContains(root, path) {
				return nil, errors.New("minimal-root writable/runtime overlap")
			}
		}
		if path == "/tmp" || path == "/run" || strings.HasPrefix(path, "/run/") || path == "/dev" || strings.HasPrefix(path, "/dev/") || path == "/proc" || strings.HasPrefix(path, "/proc/") || path == "/sys" || strings.HasPrefix(path, "/sys/") {
			return nil, errors.New("minimal-root writable overlay replaces private/control filesystem")
		}
		fd, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			unix.Close(fd)
			return nil, err
		}
		if err := nativeMinimalWritableFD(fd, b.receipt.Files); err != nil {
			unix.Close(fd)
			return nil, err
		}
		b.args = append(b.args, "--bind-fd", b.keep(fd, path), path)
		b.receipt.Writable = append(b.receipt.Writable, nativeMinimalStat(path, &st))
	}
	for _, path := range p.Devices {
		if err := nativeMinimalDestination(path); err != nil {
			return nil, err
		}
		fd, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("assigned device missing: %w", err)
		}
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFCHR {
			unix.Close(fd)
			return nil, errors.New("assigned device is not a character node")
		}
		b.keep(fd, path) // Pin identity against inode reuse; /dev is operator-owned.
		b.args = append(b.args, "--dev-bind", path, path)
		b.receipt.Devices = append(b.receipt.Devices, nativeMinimalStat(path, &st))
	}
	data, err := json.Marshal(b.receipt)
	if err != nil {
		return nil, err
	}
	fd, err := nativeMinimalMemfd("myce-minimal-receipt", data)
	if err != nil {
		return nil, err
	}
	dir := p.Workdir
	if dir == "" && len(p.Write) > 0 {
		dir = p.Write[0]
	}
	if err := nativeMinimalDestination(dir); err != nil {
		unix.Close(fd)
		return nil, err
	}
	// Explicitly discard the old-root cwd after mount construction.
	b.args = append(b.args, "--ro-bind-data", b.keep(fd, "minimal-root receipt"), nativeMinimalReceiptPath, "--remount-ro", "/", "--chdir", dir)
	if b.pinErr != nil {
		return nil, fmt.Errorf("minimal-root descriptor limit: %w", b.pinErr)
	}
	ok = true
	return b, nil
}

func nativeMinimalKernelRoot(path string) bool {
	return slices.Contains([]string{"/sys/bus/pci", "/sys/class/drm", "/sys/devices", "/sys/module/nvidia", "/sys/module/nvidia_uvm", "/sys/module/xe", "/sys/module/i915"}, path)
}

func (b *nativeMinimalBuilder) kernelDirectory(source, dest string) error {
	if b.seen[dest] {
		return nil
	}
	fd, err := unix.Open(source, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	var fs unix.Statfs_t
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.SYSFS_MAGIC {
		unix.Close(fd)
		return errors.New("minimal-root discovery source is not kernel sysfs")
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, line := range strings.Split(string(b.receipt.CgroupMountinfo), "\n") {
		fields := strings.Fields(strings.SplitN(line, " - ", 2)[0])
		if len(fields) >= 5 && nativePathContains(source, unescape.Replace(fields[4])) {
			unix.Close(fd)
			return errors.New("minimal-root nested discovery mount refused")
		}
	}
	b.seen[dest] = true
	b.args = append(b.args, "--ro-bind-fd", b.keep(fd, dest), dest)
	b.receipt.KernelDirectories = append(b.receipt.KernelDirectories, nativeMinimalStat(dest, &st))
	return nil
}

// The worker's verified custody and exclusive preparation gate own writable
// trees until this scan/bind completes. Other attempts have no MAKE/REFER rights
// here. Pin first so replacing the top-level pathname cannot swap the tree.
func nativeMinimalWritableFD(fd int, read []nativeMinimalObject) error {
	ids := map[[2]uint64]bool{}
	for _, o := range read {
		ids[[2]uint64{o.Dev, o.Ino}] = true
	}
	return nativeMinimalWalkWritable(fd, ids)
}

func nativeMinimalWalkWritable(fd int, read map[[2]uint64]bool) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG && st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("minimal-root unsafe existing writable entry")
	}
	if read[[2]uint64{uint64(st.Dev), st.Ino}] {
		return errors.New("minimal-root writable inode aliases read content")
	}
	if st.Mode&unix.S_IFMT == unix.S_IFREG {
		return nil
	}
	dirFD, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	dir := os.NewFile(uintptr(dirFD), "writable attempt")
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		child, err := unix.Openat(fd, name, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		err = nativeMinimalWalkWritable(child, read)
		unix.Close(child)
		if err != nil {
			return err
		}
	}
	return nil
}

// Kept for existing default-path callers/tests; opt-in always uses the worker.
func nativeIsolationSupport(network bool) (nativeLinuxSupport, error) {
	return (Worker{}).nativeIsolationSupport(network)
}

// Runtime descriptors are needed through bubblewrap setup only. The final
// helper keeps receipt pipes 3/4, and marks every other inherited FD CLOEXEC.
func nativeMinimalInheritedFiles() error {
	for fd := 0; fd <= 4; fd++ {
		var st unix.Stat_t
		err := unix.Fstat(fd, &st)
		if fd >= 3 && errors.Is(err, unix.EBADF) {
			continue
		}
		if err != nil {
			return err
		}
		kind := st.Mode & unix.S_IFMT
		if fd >= 3 && kind != unix.S_IFIFO {
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
				return err
			}
		}
		if kind == unix.S_IFSOCK || kind == unix.S_IFDIR {
			return errors.New("minimal-root inherited host socket/directory")
		}
	}
	return nil
}

func nativeMinimalCloseInfoFD() {
	// bwrap may already have closed its info pipe; Go can then reuse FD 3
	// for runtime bookkeeping. Close only an actual surviving pipe.
	var st unix.Stat_t
	if unix.Fstat(3, &st) == nil && st.Mode&unix.S_IFMT == unix.S_IFIFO {
		_ = unix.Close(3)
	}
}

func nativeMinimalReadAliases(p nativeLinuxPolicy, files []nativeMinimalObject) error {
	executable := map[[2]uint64]bool{}
	for _, file := range files {
		for _, root := range p.Read {
			if nativePathContains(root, file.Path) {
				executable[[2]uint64{file.Dev, file.Ino}] = true
			}
		}
	}
	for _, file := range files {
		for _, root := range p.ReadOnly {
			if nativePathContains(root, file.Path) && executable[[2]uint64{file.Dev, file.Ino}] {
				return errors.New("minimal-root read-only inode aliases executable content")
			}
		}
	}
	return nil
}
