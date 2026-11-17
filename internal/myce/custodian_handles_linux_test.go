package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func custodianProcFixture(t *testing.T) (string, string, custodianHandleFile) {
	t.Helper()
	root := t.TempDir()
	file := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(file, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	proc := filepath.Join(root, "42")
	if err := os.MkdirAll(filepath.Join(proc, "fd"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proc, "maps"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	return root, file, custodianHandleFile{uint64(st.Dev), st.Ino}
}

func TestCustodianKernelHandles(t *testing.T) {
	for _, kind := range []string{"clear", "open", "hardlink", "unlinked", "mapping", "missing-fd-inventory", "missing-maps", "malformed-maps", "cancelled", "empty-inventory"} {
		t.Run(kind, func(t *testing.T) {
			root, file, target := custodianProcFixture(t)
			proc := filepath.Join(root, "42")
			ctx := t.Context()
			switch kind {
			case "open", "hardlink", "unlinked":
				dest := file
				if kind == "hardlink" {
					dest = file + ".alias"
					if err := os.Link(file, dest); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "unlinked" {
					f, err := os.Open(file)
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()
					if err := os.Remove(file); err != nil {
						t.Fatal(err)
					}
					dest = "/proc/self/fd/" + strconv.Itoa(int(f.Fd()))
				}
				if err := os.Symlink(dest, filepath.Join(proc, "fd", "7")); err != nil {
					t.Fatal(err)
				}
			case "mapping":
				line := fmt.Sprintf("1000-2000 r--p 00000000 %x:%x %d /renamed-or-unlinked-file\n", unix.Major(target.dev), unix.Minor(target.dev), target.inode)
				if err := os.WriteFile(filepath.Join(proc, "maps"), []byte(line), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-fd-inventory":
				if err := os.Remove(filepath.Join(proc, "fd")); err != nil {
					t.Fatal(err)
				}
			case "missing-maps":
				if err := os.Remove(filepath.Join(proc, "maps")); err != nil {
					t.Fatal(err)
				}
			case "malformed-maps":
				if err := os.WriteFile(filepath.Join(proc, "maps"), []byte("incomplete\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "empty-inventory":
				root = t.TempDir()
			}
			err := inspectCustodianProcHandles(ctx, root, []custodianHandleFile{target})
			if kind == "clear" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("unproved handle absence accepted", kind)
			}
		})
	}
}

func TestCustodianPrivilegedInspectorProtocol(t *testing.T) {
	bin := t.TempDir()
	sudo := filepath.Join(bin, "sudo")
	t.Setenv("PATH", bin)
	t.Setenv("MYCE_TOKEN", "must-not-reach-inspector")
	script := `#!/bin/sh
[ "$1" = "-n" ] && [ "$2" = "--" ] || exit 20
[ -z "${MYCE_TOKEN+x}" ] || exit 21
shift 3
[ "$1" = "_inspect-custodian-handles" ] || exit 22
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--custodian-id" ]; then shift; owner=$1; fi
 shift
done
printf 'custodian handles clear: %s\n' "$owner"
`
	if err := os.WriteFile(sudo, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := privilegedCustodianHandlesEmpty(t.Context(), t.TempDir(), ID()); err != nil {
		t.Fatal(err)
	}
	for _, script := range []string{"#!/bin/sh\necho incomplete >&2\nexit 1\n", "#!/bin/sh\necho forged-clear\n"} {
		if err := os.WriteFile(sudo, []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		if err := privilegedCustodianHandlesEmpty(t.Context(), t.TempDir(), ID()); err == nil {
			t.Fatal("failed/invalid privilege inspection accepted")
		}
	}
}
