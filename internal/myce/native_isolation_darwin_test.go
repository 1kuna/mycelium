package myce

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeIsolationNetworkDeclaration(t *testing.T) {
	// Seatbelt's localhost filter includes the host's LAN addresses as well as
	// loopback. A declaration must not expose a local control listener by alias.
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var address string
	for _, entry := range addresses {
		if ip, ok := entry.(*net.IPNet); ok && ip.IP.To4() != nil && !ip.IP.IsLoopback() && !ip.IP.IsLinkLocalUnicast() {
			address = ip.IP.String()
			break
		}
	}
	if address == "" {
		t.Skip("no non-loopback IPv4 address for local egress fixture")
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort(address, "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	work := t.TempDir()
	for _, tc := range []struct {
		name                      string
		network, control, sidecar bool
		want                      bool
	}{{"offline", false, false, false, false}, {"declared-local", true, false, false, false}, {"sidecar", false, false, true, true}, {"control-alias", true, true, false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			w := Worker{}
			if tc.control {
				w.ModelListen = listener.Addr().String()
			}
			cmd := exec.Command("/usr/bin/nc", "-z", "-w", "1", address, port)
			cmd.Dir = work
			var ports []string
			if tc.sidecar {
				ports = []string{port}
			}
			if err := w.isolateNativeCommand(cmd, work, work, map[string]string{}, ports, tc.network); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Run(); (err == nil) != tc.want {
				t.Fatalf("network grant=%v, want %v: %v", err == nil, tc.want, err)
			}
		})
	}
}

func TestNativeIsolationExternalEgress(t *testing.T) {
	address := os.Getenv("MYCE_TEST_EGRESS_ADDRESS")
	if address == "" {
		t.Skip("explicit external egress canary only; laptop suite needs no network")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		t.Fatal("external fixture unavailable", err)
	}
	conn.Close()
	work := t.TempDir()
	for _, network := range []bool{false, true} {
		cmd := exec.Command("/usr/bin/nc", "-z", "-w", "3", host, port)
		cmd.Dir = work
		if err := (Worker{}).isolateNativeCommand(cmd, work, work, map[string]string{}, nil, network); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Run(); (err == nil) != network {
			t.Fatalf("declared network=%v: %v", network, err)
		}
	}
}

func TestNativeIsolationProfile(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "job-files", "job", "1")
	if err := os.MkdirAll(work, 0700); err != nil {
		t.Fatal(err)
	}
	w := Worker{}
	cmd := exec.Command("/bin/sh", "-c", `read gate; env >/dev/null; echo isolated`)
	cmd.Stdin = strings.NewReader("start\n")
	cmd.Dir = work
	if err := w.isolateNativeCommand(cmd, root, work, map[string]string{}, nil, false); err != nil {
		t.Fatal(err)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("profile: %s: %v\n%s", out, err, cmd.Args[2])
	}
}

func TestNativeIsolationSiblingAndControl(t *testing.T) {
	setWorkerSession(t)
	w := recoveryWorker(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"ok":true,"start_gate":1}`)
	}))
	defer api.Close()
	// Use the existing worker fixture transport for launch authorization; the
	// endpoint below is a separate real local control listener for the payload.
	_, controlPort, _ := net.SplitHostPort(strings.TrimPrefix(api.URL, "http://"))
	short, err := os.MkdirTemp("/tmp", "myce-isolation-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	socket := filepath.Join(short, "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	install := t.TempDir()
	credential := filepath.Join(install, "joining.json")
	if err := os.WriteFile(credential, []byte("synthetic-worker-credential"), 0600); err != nil {
		t.Fatal(err)
	}
	w.ServiceConfig = &ServiceConfig{Config: filepath.Join(install, "config.json"), Credentials: filepath.Join(install, "credentials.json"), JoiningFile: credential}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	start := func(command string, env map[string]string) (*process, *workerStore) {
		t.Helper()
		job := recoveryJob(t, []string{"/bin/sh", "-c", command})
		job.Spec.Resumable = false
		job.Spec.Logs = "keep"
		job.Spec.Run.Env = env
		store, err := newAttemptStore(root, job)
		if err != nil {
			t.Fatal(err)
		}
		p, err := w.start(store, job)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.stop(w.Clock.Now(), 0, true) })
		return p, store
	}
	sibling, siblingStore := start(`printf sibling > secret; printf sibling > "$TMPDIR/secret"; touch ready; while :; do sleep 0.05; done`, nil)
	siblingWork := sibling.job.Spec.Run.Workdir
	await(t, func() bool {
		select {
		case finish := <-sibling.done:
			out, _ := os.ReadFile(filepath.Join(siblingStore.dir, siblingStore.state.Log))
			t.Fatalf("sibling exited: %+v\n%s", finish, out)
		default:
		}
		_, err := os.Stat(filepath.Join(siblingWork, "ready"))
		return err == nil
	})
	defer func() {
		sibling.stop(w.Clock.Now(), 0, true)
		select {
		case f := <-sibling.done:
			if f.err != nil || f.result.ReleasePending {
				t.Errorf("sibling cleanup: %+v", f)
			}
		case <-time.After(10 * time.Second):
			t.Error("sibling cleanup timed out")
		}
	}()
	for _, path := range []string{filepath.Join(siblingWork, "secret"), filepath.Join(siblingWork, ".tmp", "secret"), credential} {
		if _, err := os.ReadFile(path); err != nil {
			t.Fatal("probe target unavailable", err)
		}
	}
	for _, target := range []struct{ network, address string }{{"unix", socket}, {"tcp", strings.TrimPrefix(api.URL, "http://")}} {
		conn, err := net.DialTimeout(target.network, target.address, time.Second)
		if err != nil {
			t.Fatal("control fixture unavailable", err)
		}
		conn.Close()
	}
	attack, store := start(`
failures=0
for path in "$SIBLING_WORK/secret" "$SIBLING_WORK/.tmp/secret" "$WORKER_CREDENTIAL"; do
  if cat "$path" >/dev/null 2>&1; then echo "read escaped: $path"; failures=$((failures+1)); fi
done
if /usr/bin/nc -z -w 1 -U "$CONTROL_SOCKET" 2>/dev/null; then echo socket-escaped; failures=$((failures+1)); fi
if /usr/bin/nc -z -w 1 127.0.0.1 "$CONTROL_PORT" 2>/dev/null; then echo control-escaped; failures=$((failures+1)); fi
if [ -n "$MYCE_TOKEN$MYCE_QUEUE$SSH_AUTH_SOCK$DBUS_SESSION_BUS_ADDRESS" ]; then echo env-escaped; failures=$((failures+1)); fi
printf own > own
printf own > "$TMPDIR/own"
test "$(cd "$TMPDIR" && pwd -P)" = "$(pwd -P)/.tmp" || { echo temp-not-private; failures=$((failures+1)); }
test "$failures" = 0 && echo isolation-passed
`, map[string]string{"SIBLING_WORK": siblingWork, "WORKER_CREDENTIAL": credential, "CONTROL_SOCKET": socket, "CONTROL_PORT": controlPort, "TMPDIR": "/tmp/ignored-authored-temp"})
	select {
	case finish := <-attack.done:
		out, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
		if finish.err != nil || !finish.result.Success || finish.result.ReleasePending || !strings.Contains(string(out), "isolation-passed") {
			t.Fatalf("attack result: %+v\n%s", finish, out)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("attack job timed out")
	}
	if _, err := os.Stat(filepath.Join(attack.job.Spec.Run.Workdir, "own")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(attack.job.Spec.Run.Workdir, ".tmp", "own")); err != nil {
		t.Fatal(err)
	}
}
