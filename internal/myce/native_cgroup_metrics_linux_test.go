package myce

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func init() {
	if len(os.Args) == 3 && os.Args[1] == "_native-cgroup-metrics-test" {
		if err := nativeCgroupMetricAttack(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func TestNativeCgroupMetricFiles(t *testing.T) {
	// The cgroup2 mount exposes only a delegated subtree. Include an unrelated
	// mount first so the resolver cannot simply append membership to /sys.
	mounts := []byte("31 20 0:27 /elsewhere /unrelated rw - cgroup2 cgroup rw\n" +
		"32 20 0:28 /service /sys/fs/cgroup rw - cgroup2 cgroup rw\n")
	main := "/sys/fs/cgroup/attempt/main"
	helper := "/sys/fs/cgroup/attempt/sidecar-helper"
	files, err := nativeCgroupMetricFiles([]byte("0::/service/attempt/main\n"), mounts, nativeLinuxPolicy{MetricCgroups: []string{helper, main}})
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, leaf := range []string{main, helper} {
		for _, name := range []string{"memory.peak", "memory.max", "memory.events", "memory.current", "memory.stat"} {
			want = append(want, filepath.Join(leaf, name))
		}
	}
	if !slices.Equal(files, want) {
		t.Fatalf("wrong exact metric grants: got %v, want %v", files, want)
	}
	// An inherited outer attempt has no trusted member list. It must expose
	// that exact leaf, never its aggregate parent or another attempt.
	files, err = nativeCgroupMetricFiles([]byte("0::/service/attempt/main\n"), mounts, nativeLinuxPolicy{})
	if err != nil || !slices.Equal(files, want[:5]) {
		t.Fatalf("inherited current-leaf metrics: %v, %v", files, err)
	}
}

func TestNativeCurrentCgroupMountRoots(t *testing.T) {
	for _, tc := range []struct{ name, membership, mounts, want string }{
		{"host", "0::/service/attempt/main\n", "32 20 0:28 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n", "/sys/fs/cgroup/service/attempt/main"},
		{"namespace root", "0::/\n", "32 20 0:28 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n", "/sys/fs/cgroup"},
		{"escaped mount", "0::/service name/attempt/main\n", `32 20 0:28 /service\040name /cgroup\040mount rw - cgroup2 cgroup rw` + "\n", "/cgroup mount/attempt/main"},
		{"outside mount", "0::/service-other/main\n", "32 20 0:28 /service /sys/fs/cgroup rw - cgroup2 cgroup rw\n", ""},
		{"no unified mount", "0::/service/main\n", "32 20 0:28 / /sys/fs/cgroup rw - cgroup cgroup rw\n", ""},
		{"traversal", "0::/../service/main\n", "32 20 0:28 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nativeCurrentCgroup([]byte(tc.membership), []byte(tc.mounts))
			if tc.want == "" {
				if err == nil {
					t.Fatalf("unresolvable membership accepted: %s", got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("current leaf: %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestNativeCgroupMetricsRejectRuntimeAliases(t *testing.T) {
	root := t.TempDir()
	currentMount := filepath.Join(root, "current")
	customMount := filepath.Join(root, "runtime", "cgroup")
	for _, path := range []string{currentMount, customMount} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(customMount, alias); err != nil {
		t.Fatal(err)
	}
	// The second cgroup2 mount does not contain current membership. Runtime
	// grants must still be checked against it, including through symlinks.
	mounts := []byte(fmt.Sprintf("32 20 0:28 / %s rw - cgroup2 cgroup rw\n33 20 0:28 /other %s rw - cgroup2 cgroup rw\n", currentMount, customMount))
	membership := []byte("0::/attempt/main\n")
	for _, p := range []nativeLinuxPolicy{
		{Read: []string{filepath.Dir(customMount)}},
		{ReadOnly: []string{filepath.Join(alias, "memory.max")}},
		{Write: []string{filepath.Join(customMount, "attempt", "main")}},
	} {
		if _, err := nativeCgroupMetricFiles(membership, mounts, p); err == nil || !strings.Contains(err.Error(), "overlaps cgroup mount") {
			t.Fatalf("runtime authority exposed cgroup mount: %+v, %v", p, err)
		}
	}
	// Ordinary model content is not a cgroup grant, and exact current metrics
	// are added only after all existing runtime grants pass validation.
	p := nativeLinuxPolicy{ReadOnly: []string{filepath.Join(root, "models")}}
	if _, err := nativeCgroupMetricFiles(membership, mounts, p); err != nil {
		t.Fatal("unrelated read-only model content was refused", err)
	}
}

func TestNativeMetricsPolicyExcludesCgroupTree(t *testing.T) {
	state, work := t.TempDir(), t.TempDir()
	cmd := exec.Command("/bin/true")
	cmd.Dir = work
	trusted := "/sys/fs/cgroup/attempt/main"
	w := Worker{nativeMetricCgroups: []string{trusted}}
	p, err := w.nativeLinuxPolicy(cmd, state, work, map[string]string{
		"MYCE_CGROUP_DIR": "/sys/fs/cgroup/other-attempt",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(p.MetricCgroups, []string{trusted}) {
		t.Fatalf("metrics did not come from trusted worker custody: %v", p.MetricCgroups)
	}
	for _, path := range append(slices.Clone(p.Read), p.ReadOnly...) {
		rel, err := filepath.Rel(path, "/sys/fs/cgroup")
		if err == nil && (rel == "." || filepath.IsLocal(rel)) || strings.HasPrefix(path, "/sys/fs/cgroup/") {
			t.Fatalf("base runtime grant exposes cgroups: %s", path)
		}
	}
	for _, path := range []string{"/sys/bus/pci", "/sys/class/drm", "/sys/devices", "/proc/meminfo"} {
		if !slices.Contains(p.Read, path) {
			t.Fatalf("runtime discovery grant missing: %s", path)
		}
	}
}

// A regular-file fixture proves the read/write boundary without creating or
// changing a real cgroup. Actual current membership is resolved by NativeExec.
func TestNativeCgroupMetricsConfinement(t *testing.T) {
	if support := nativeLinuxDetection(); support.mode == "unavailable" {
		t.Skipf("host confinement unavailable: %s", support.reason)
	}
	root := t.TempDir()
	for _, leaf := range []string{"", "main", "sidecar-helper", "sibling", "worker", "control"} {
		dir := filepath.Join(root, leaf)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"memory.peak", "memory.max", "memory.events", "memory.current", "memory.stat", "memory.high", "cgroup.procs", "cgroup.subtree_control"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("synthetic"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := nativeLinuxPolicy{
		Read:          []string{"/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache", "/proc/meminfo", executable},
		MetricCgroups: []string{filepath.Join(root, "main"), filepath.Join(root, "sidecar-helper")},
		Env:           environmentList(jobBaseEnvironment()),
	}
	args, err := nativePolicyArgs(p, []string{executable, "_native-cgroup-metrics-test", root})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = environmentList(jobBaseEnvironment())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("confined metrics boundary: %v\n%s", err, out)
	}
}

func nativeCgroupMetricAttack(root string) error {
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return fmt.Errorf("own membership: %w", err)
	}
	if _, err := os.ReadFile("/proc/meminfo"); err != nil {
		return fmt.Errorf("host memory summary: %w", err)
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	current, err := nativeCurrentCgroup(membership, mounts)
	if err != nil {
		return err
	}
	for _, name := range []string{"memory.peak", "memory.max", "memory.events", "memory.current", "memory.stat"} {
		if _, err := os.ReadFile(filepath.Join(current, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("current leaf %s: %w", name, err)
		}
		for _, leaf := range []string{"main", "sidecar-helper"} {
			path := filepath.Join(root, leaf, name)
			if data, err := os.ReadFile(path); err != nil || string(data) != "synthetic" {
				return fmt.Errorf("own member metric %s: %q, %v", path, data, err)
			}
			// Open only: never issue writes to cgroup/control files, even if
			// confinement regresses. These are disposable fixture files.
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err == nil {
				f.Close()
				return fmt.Errorf("own member metric writable: %s", path)
			}
			if !errors.Is(err, os.ErrPermission) {
				return fmt.Errorf("own member metric write denial: %w", err)
			}
		}
		for _, leaf := range []string{"", "sibling", "worker", "control"} {
			if _, err := os.ReadFile(filepath.Join(root, leaf, name)); !errors.Is(err, os.ErrPermission) {
				return fmt.Errorf("non-member metric %s/%s not denied: %v", leaf, name, err)
			}
		}
	}
	for _, leaf := range []string{"main", "sidecar-helper"} {
		for _, name := range []string{"memory.high", "cgroup.procs", "cgroup.subtree_control"} {
			if _, err := os.ReadFile(filepath.Join(root, leaf, name)); !errors.Is(err, os.ErrPermission) {
				return fmt.Errorf("member control %s/%s not denied: %v", leaf, name, err)
			}
		}
	}
	return nil
}
