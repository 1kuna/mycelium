package myce

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Run inside the real confinement boundary without TestMain's host setup.
func init() {
	if len(os.Args) > 1 && os.Args[1] == "_native-namespace-listener" {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		fmt.Println(server.Listener.Addr().(*net.TCPAddr).Port)
		_, err := io.Copy(io.Discard, os.Stdin)
		server.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "_native-namespace-identity" {
		var caps [2]unix.CapUserData
		header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
		if err := unix.Capget(&header, &caps[0]); err != nil || caps != [2]unix.CapUserData{} {
			fmt.Fprintln(os.Stderr, "payload has unexpected capabilities:", caps, err)
			os.Exit(1)
		}
		if value, err := unix.PrctlRetInt(unix.PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0); err != nil || value != 1 {
			fmt.Fprintln(os.Stderr, "payload lacks no_new_privs:", value, err)
			os.Exit(1)
		}
		if _, exists := os.LookupEnv("MYCE_TOKEN"); exists {
			fmt.Fprintln(os.Stderr, "payload received a fleet token")
			os.Exit(1)
		}
		fmt.Println(os.Getuid(), os.Getgid())
		for _, name := range []string{"user", "mnt", "pid", "ipc", "net"} {
			link, err := os.Readlink("/proc/self/ns/" + name)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Println(link)
		}
		if _, err := os.Stat("cwd-marker"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "_native-namespace-lifecycle-child" {
		signal.Ignore(syscall.SIGTERM)
		if err := os.WriteFile("child-ready", nil, 0600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
}

func TestNativeNamespaceLinuxSharedGroup(t *testing.T) {
	if support := nativeLinuxDetection(); support.mode != "namespace" {
		t.Skipf("real namespaces unavailable: mode=%s reason=%s", support.mode, support.reason)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	spool, work := t.TempDir(), t.TempDir()
	dir := filepath.Join(work, "member workdir")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cwd-marker"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	reader, gate, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer gate.Close()
	anchor := exec.CommandContext(ctx, executable, "_native-namespace-listener")
	anchor.Stdin, anchor.Stderr = reader, os.Stderr
	anchor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := anchor.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	w := Worker{}
	n, err := w.prepareNativeGroupIsolation(anchor, spool, work, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer n.close()
	if err := anchor.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		gate.Close()
		if err := anchor.Wait(); err != nil {
			t.Error("namespace anchor exit:", err)
		}
	}()
	if err := n.activate(ctx, gate); err != nil {
		t.Fatal(err)
	}
	port, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	want := []string{fmt.Sprint(os.Getuid(), " ", os.Getgid())}
	for _, name := range []string{"user", "mnt", "pid", "ipc", "net"} {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/%s", n.pid, name))
		if err != nil {
			t.Fatal(err)
		}
		host, err := os.Readlink("/proc/self/ns/" + name)
		if err != nil || link == host {
			t.Fatalf("anchor lacks a private %s namespace: %v", name, err)
		}
		want = append(want, link)
	}
	for _, name := range []string{"sidecar", "main"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `read gate || exit 125; exec "$@" < /dev/null`, "myce-member-"+name, executable, "_native-namespace-identity")
			cmd.Dir = dir
			cmd.Stdin = strings.NewReader("start\nexec\n")
			if err := n.isolate(w, cmd, spool, work, map[string]string{"MYCE_TOKEN": "fixture"}, nil, false, name == "main"); err != nil {
				t.Fatal(err)
			}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("member launch: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != strings.Join(want, "\n") {
				t.Fatalf("member identity or namespaces differ from the anchor: %q", got)
			}
			t.Log("member shares the anchor's user, mount, PID, IPC and network namespaces with no payload capabilities")
		})
	}
	t.Run("readiness", func(t *testing.T) {
		handled, ready, err := n.probe(ctx, strings.TrimSpace(port), anchor.Process.Pid, "/ready", false)
		if !handled || !ready || err != nil {
			t.Fatalf("readiness probe failed in the shared network namespace: handled=%t ready=%t err=%v", handled, ready, err)
		}
	})
}

func TestNativeNamespaceLinuxCheckpointGraceAndCleanup(t *testing.T) {
	if support := nativeLinuxDetection(); support.mode != "namespace" {
		t.Skipf("real namespaces unavailable: mode=%s reason=%s", support.mode, support.reason)
	}
	for _, mode := range []string{"accelerator-direct", "accelerator-group", "cpu-group"} {
		t.Run(mode, func(t *testing.T) {
			w := recoveryWorker(t)
			w.AcceleratorStopGrace = 2 * time.Second
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			// A shell cannot install a trap if it inherited SIG_IGN. This
			// exercises default TERM restoration as well as monitor survival.
			j := recoveryJob(t, []string{"/bin/sh", "-c", `
mkfifo save-fifo || exit 1
exec 5<> save-fifo
trap 'printf term > term-started; read save <&5; if [ "$CHECKPOINT_HELPER" = 1 ]; then printf save > helper-request; read reply < helper-answer; [ "$reply" = ready ] || exit 1; fi; printf checkpoint-complete > "$MYCE_CHECKPOINT_DIR/saved"; exit 0' TERM
"$1" _native-namespace-lifecycle-child &
printf ready > main-ready
while :; do read line <&5; done
`, "namespace-checkpoint", executable})
			j.Spec.Resumable, j.Spec.Logs = false, "keep"
			j.Spec.Run.Env = map[string]string{"GORACE": "atexit_sleep_ms=0"}
			if mode != "accelerator-direct" {
				j.Spec.Run.MemoryGB = .125
			}
			if mode == "cpu-group" {
				j.Spec.Needs = ResourceNeeds{Platform: Platform{Required: []string{"cpu"}}, CPUCores: 1, HostMemoryGB: .25}
				j.Spec.Run.Env["CHECKPOINT_HELPER"] = "1"
				j.Spec.Run.Sidecars = []NativeSidecar{{Name: "checkpoint", MemoryGB: .0625, Command: []string{"/bin/sh", "-c", `
mkfifo helper-request helper-answer || exit 1
exec 5<> helper-request
exec 6<> helper-answer
trap 'exit 17' TERM
printf ready > helper-ready
while read request <&5; do printf 'ready\n' >&6; done
`}}}
			}
			p, err := startAfterFixtureInputs(t, w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			finished := false
			t.Cleanup(func() {
				if !finished {
					p.stop(w.Clock.Now(), 0, true)
					// Accelerator stops keep their grace; explicitly enforce its
					// end when an assertion aborts this fixture before the save.
					p.enforceGrace(w.Clock.Now().Add(3 * time.Second))
					select {
					case <-p.done:
					case <-time.After(testWaitTimeout):
						t.Error("namespace fixture cleanup timed out")
					}
				}
			})
			work := filepath.Join(store.dir, "job-files", j.ID, strconv.Itoa(j.Attempt))
			waitFile := func(name string) {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					if _, err := os.Stat(filepath.Join(work, name)); err == nil {
						return
					}
					select {
					case finish := <-p.done:
						finished = true
						out, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
						t.Fatalf("namespace exited before %s: %+v\n%s", name, finish, out)
					case <-time.After(5 * time.Millisecond):
					}
				}
				t.Fatalf("namespace did not produce %s", name)
			}
			waitFile("main-ready")
			waitFile("child-ready")
			if mode == "cpu-group" {
				waitFile("helper-ready")
			}
			saveGate, err := os.OpenFile(filepath.Join(work, "save-fifo"), os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer saveGate.Close()
			pgid := p.cmd.Process.Pid
			members, err := workerGroupMembers(pgid)
			if err != nil {
				t.Fatal(err)
			}
			// Capture real host identities before TERM. Namespace-local PIDs
			// cannot prove that host descendants are gone after final cleanup.
			births := map[int]string{}
			for _, pid := range members {
				birth, err := processBirth(pid)
				if err != nil {
					t.Fatal(err)
				}
				births[pid] = birth
			}
			p.stop(w.Clock.Now(), 2*time.Second, false)
			waitFile("term-started")
			select {
			case finish := <-p.done:
				finished = true
				t.Fatalf("namespace ended while checkpoint was held: %+v", finish)
			case <-time.After(150 * time.Millisecond):
			}
			saved := filepath.Join(attemptCheckpoint(j), "saved")
			if _, err := os.Stat(saved); !os.IsNotExist(err) {
				t.Fatalf("checkpoint passed its unreleased save barrier: %v", err)
			}
			if _, err := saveGate.WriteString("save\n"); err != nil {
				t.Fatal(err)
			}
			select {
			case finish := <-p.done:
				finished = true
				if finish.err != nil || finish.result.ReleasePending {
					t.Errorf("namespace cleanup unconfirmed: %+v", finish)
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("namespace did not finish cleanup")
			}
			if data, err := os.ReadFile(saved); err != nil || string(data) != "checkpoint-complete" {
				t.Errorf("delayed TERM checkpoint lost: %q %v", data, err)
			}
			if remaining, err := workerGroupMembers(pgid); err != nil || len(remaining) != 0 {
				t.Errorf("namespace process group survived: %v %v", remaining, err)
			}
			for pid, birth := range births {
				current, err := processBirth(pid)
				if err != nil && !os.IsNotExist(err) {
					t.Errorf("cannot verify descendant cleanup: host PID %d: %v", pid, err)
				} else if err == nil && current == birth {
					t.Errorf("namespace descendant survived cleanup: host PID %d", pid)
				}
			}
		})
	}
}
