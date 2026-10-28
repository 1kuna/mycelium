//go:build !linux

package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
)

type nativeNamespace struct{}

func NativeExec([]string) error { return errors.New("native exec helper requires Linux") }
func (w Worker) prepareNativeGroupIsolation(*exec.Cmd, string, string, map[string]string, bool) (*nativeNamespace, error) {
	return nil, nil
}
func (*nativeNamespace) close()                                   {}
func (*nativeNamespace) activate(context.Context, *os.File) error { return nil }
func (*nativeNamespace) isolate(w Worker, cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, network, main bool) error {
	return w.isolateNativeCommand(cmd, spool, work, env, ports, network)
}
func (g *nativeGroup) releaseMember(_ *nativeMember, gate *os.File) error {
	_, err := gate.WriteString("start\n")
	return err
}
func (m *nativeMember) signal(signal syscall.Signal) error {
	return syscall.Kill(m.PID, signal)
}
func (*nativeNamespace) probe(context.Context, string, int, string, bool) (bool, bool, error) {
	return false, false, nil
}
func (g *nativeGroup) verifyIsolationListener(ctx context.Context, port string, group int) error {
	return g.worker.verifyNativeListener(ctx, port, group)
}
