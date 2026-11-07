package myce

import (
	"errors"
	"os"
	"testing"
)

func TestCgroupMemoryHeadroomHierarchy(t *testing.T) {
	for _, unified := range []bool{true, false} {
		name := "v1"
		membership := "4:cpu:/other\n5:memory:/tenant/queue\n"
		mount := "1 2 0:1 /tenant /memory rw - cgroup cgroup rw,memory\n"
		maxName, usedName := "memory.limit_in_bytes", "memory.usage_in_bytes"
		if unified {
			name = "v2"
			membership = "0::/tenant/queue\n"
			mount = "1 2 0:1 /tenant /memory rw - cgroup2 cgroup rw\n"
			maxName, usedName = "memory.max", "memory.current"
		}
		t.Run(name, func(t *testing.T) {
			files := map[string]string{
				"/proc/self/cgroup": membership, "/proc/self/mountinfo": mount,
				"/memory/queue/" + maxName: "1000", "/memory/queue/" + usedName: "200",
				"/memory/" + maxName: "600", "/memory/" + usedName: "300",
			}
			read := func(path string) ([]byte, error) {
				s, ok := files[path]
				if !ok {
					return nil, errors.New("unavailable")
				}
				return []byte(s), nil
			}
			headroom, err := cgroupMemoryHeadroom(1000000, read)
			if err != nil || headroom != 300 {
				t.Fatal(headroom, err)
			}
			delete(files, "/memory/queue/"+usedName)
			if _, err = cgroupMemoryHeadroom(1000000, read); err == nil {
				t.Fatal("unreadable container treated as host RAM")
			}
			files["/proc/self/cgroup"] = "0::/\n"
			if unified {
				files["/memory/"+maxName] = "max"
				headroom, err = cgroupMemoryHeadroom(1234, read)
				if err != nil || headroom != 1234 {
					t.Fatal("namespace/unlimited root", headroom, err)
				}
			}
		})
	}
}

func TestProcessMemoryHeadroomMeasuredBoundary(t *testing.T) {
	available, err := processMemoryHeadroom(1)
	if err != nil {
		t.Fatal("cannot establish this process's container boundary", err)
	}
	if available < 0 || available > 1 {
		t.Fatal("physical headroom grew", available)
	}
}

func TestCgroupMemoryHybridAndHostRoot(t *testing.T) {
	files := map[string]string{
		"/proc/self/cgroup":                    "0::/unified\n5:memory:/legacy\n",
		"/proc/self/mountinfo":                 "1 2 0:1 / /unified rw - cgroup2 cgroup rw\n2 3 0:2 / /memory rw - cgroup cgroup rw,memory\n",
		"/memory/legacy/memory.limit_in_bytes": "1000", "/memory/legacy/memory.usage_in_bytes": "900",
		"/memory/memory.limit_in_bytes": "10000", "/memory/memory.usage_in_bytes": "1000",
	}
	read := func(path string) ([]byte, error) {
		v, ok := files[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		return []byte(v), nil
	}
	if n, err := cgroupMemoryHeadroom(1000000, read); err != nil || n != 100 {
		t.Fatal(n, err)
	}
	files["/proc/self/cgroup"] = "0::/child\n"
	files["/unified/memory.max"], files["/unified/memory.current"] = "1000", "900"
	if n, err := cgroupMemoryHeadroom(1234, read); err != nil || n != 100 {
		t.Fatal("disabled child controller lost enabled ancestor", n, err)
	}
	delete(files, "/unified/memory.max")
	delete(files, "/unified/memory.current")
	files["/proc/self/cgroup"] = "0::/\n"
	if n, err := cgroupMemoryHeadroom(1234, read); err != nil || n != 1234 {
		t.Fatal("unconfigured unified memory controller", n, err)
	}
	files["/unified/memory.current"] = "900"
	if n, err := cgroupMemoryHeadroom(1234, read); err != nil || n != 1234 {
		t.Fatal("host root without configurable memory.max", n, err)
	}
}
