package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// The namespace init is a descendant of the existing PGID anchor. Custody,
// cgroup attachment and recovery continue to use host PIDs and the old PGID.
type nativeNamespace struct {
	support                  nativeLinuxSupport
	pid                      int
	infoReader, infoWriter   *os.File
	readyReader, readyWriter *os.File
}

func (w Worker) prepareNativeGroupIsolation(cmd *exec.Cmd, spool, work string, env map[string]string, network bool) (*nativeNamespace, error) {
	s, err := nativeIsolationSupport(network)
	if err != nil {
		return nil, err
	}
	n := &nativeNamespace{support: s}
	if s.mode != "namespace" {
		return n, nil
	}
	cmd.Dir = work
	p, err := w.nativeLinuxPolicy(cmd, spool, work, env, nil)
	if err != nil {
		return nil, err
	}
	n.infoReader, n.infoWriter, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	n.readyReader, n.readyWriter, err = os.Pipe()
	if err != nil {
		n.close()
		return nil, err
	}
	args := append(nativeBwrapPolicyArgs(p), "--info-fd", "3", "--")
	args = append(args, "/bin/sh", "-c", `printf R >&4; exec 4>&-; exec "$@"`, "myce-namespace-ready")
	args = append(args, cmd.Args...)
	// The bwrap fork itself follows the gate: all descendants inherit the
	// anchor's control cgroup, and a failed attach never creates namespaces.
	cmd.Path = "/bin/sh"
	cmd.Args = append([]string{"/bin/sh", "-c", `read gate || exit 125; exec "$@"`, "myce-namespace-anchor", s.bwrap}, args...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, n.infoWriter, n.readyWriter)
	return n, nil
}

func (n *nativeNamespace) close() {
	if n == nil {
		return
	}
	if n.infoReader != nil {
		n.infoReader.Close()
		n.infoReader = nil
	}
	if n.infoWriter != nil {
		n.infoWriter.Close()
		n.infoWriter = nil
	}
	if n.readyReader != nil {
		n.readyReader.Close()
		n.readyReader = nil
	}
	if n.readyWriter != nil {
		n.readyWriter.Close()
		n.readyWriter = nil
	}
}

func (n *nativeNamespace) activate(ctx context.Context, gate *os.File) error {
	if n == nil || n.support.mode != "namespace" {
		return nil
	}
	defer n.close()
	n.infoWriter.Close()
	n.infoWriter = nil
	n.readyWriter.Close()
	n.readyWriter = nil
	if _, err := gate.WriteString("start\n"); err != nil {
		return err
	}
	// A namespace launcher that never reports its child is broken, not a
	// job to wait on. Bound this tiny handshake, then let the caller kill and
	// verify its existing PGID/cgroup using the ordinary abort path.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	type result struct {
		pid int
		err error
	}
	done := make(chan result, 1)
	reader := n.infoReader
	ready := n.readyReader
	go func() {
		var info struct {
			PID int `json:"child-pid"`
		}
		err := json.NewDecoder(reader).Decode(&info)
		if err == nil {
			var marker [1]byte
			_, err = io.ReadFull(ready, marker[:])
			if err == nil && marker[0] != 'R' {
				err = errors.New("invalid namespace readiness marker")
			}
		}
		done <- result{info.PID, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return fmt.Errorf("native namespace anchor handshake: %w", r.err)
		}
		if r.pid <= 0 {
			return errors.New("native namespace anchor returned no child PID")
		}
		n.pid = r.pid
		return nil
	case <-ctx.Done():
		return fmt.Errorf("native namespace anchor handshake: %w", ctx.Err())
	}
}

func (n *nativeNamespace) isolate(w Worker, cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, network bool) error {
	if n == nil || n.pid == 0 {
		return w.isolateNativeCommand(cmd, spool, work, env, ports, network)
	}
	if network {
		return errors.New("native_network_unavailable: Linux outbound transport is not qualified")
	}
	if err := nativeWrapPayloadCheck(cmd); err != nil {
		return err
	}
	p, err := w.nativeLinuxPolicy(cmd, spool, work, env, ports)
	if err != nil {
		return err
	}
	p.Namespace = true
	args, err := nativePolicyArgs(p, cmd.Args[4:])
	if err != nil {
		return err
	}
	// nsenter must fork after setns(CLONE_NEWPID), its default behavior. Do not
	// use --no-fork: that would execute the payload outside the PID namespace.
	enter := []string{n.support.nsenter, "--target", strconv.Itoa(n.pid), "--user", "--mount", "--pid", "--ipc", "--net", "--root", "--wdns", cmd.Dir, "--preserve-credentials", "--"}
	return nativeWrapPayload(cmd, append(enter, args...))
}

func (n *nativeNamespace) probe(ctx context.Context, port string, group int, path string, listener bool) (bool, bool, error) {
	if n == nil || n.pid == 0 {
		return false, false, nil
	}
	executable, err := os.Executable()
	if err != nil {
		return true, false, err
	}
	mode := "probe"
	if listener {
		mode = "listener"
	}
	args := []string{"--target", strconv.Itoa(n.pid), "--user", "--net", "--preserve-credentials", "--", executable, "_native-exec", mode, port, strconv.Itoa(group)}
	if !listener {
		args = append(args, path)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.support.nsenter, args...)
	cmd.Env = environmentList(jobBaseEnvironment())
	out, err := cmd.Output()
	if strings.TrimSpace(string(out)) == "foreign" {
		return true, false, errNativeForeignListener
	}
	if err != nil {
		return true, false, err
	}
	return true, strings.TrimSpace(string(out)) == "ready", nil
}

func (g *nativeGroup) verifyIsolationListener(ctx context.Context, port string, group int) error {
	if handled, ready, err := g.namespace.probe(ctx, port, group, "", true); handled {
		if err != nil {
			return err
		}
		if !ready {
			return errors.New("native listener ownership unconfirmed")
		}
		return nil
	}
	return g.worker.verifyNativeListener(ctx, port, group)
}
