//go:build darwin || linux

package myce

import (
	"net"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Send the same socketpair operation as the client helper.
func warmPeerRequest(t *testing.T) *net.UnixConn {
	t.Helper()
	conn, sender, err := newWarmPieceChannel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	defer sender.Close()
	local, outbound, err := newWarmPieceChannel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { local.Close() })
	defer outbound.Close()
	if err := unix.Sendmsg(int(sender.Fd()), []byte{'N'}, unix.UnixRights(int(outbound.Fd())), nil, 0); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestWarmPieceReplyDescriptorClosesOnExec(t *testing.T) {
	type received struct {
		op    byte
		fds   []int
		valid bool
		err   error
	}
	conn := warmPeerRequest(t)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	done, started := make(chan received, 1), make(chan struct{})
	// Simulate another launcher being inside the fork critical section.
	// Darwin must wait before receiving a descriptor that lacks CLOEXEC.
	syscall.ForkLock.Lock()
	go func() {
		close(started)
		op, fds, valid, err := receiveWarmRequest(conn)
		done <- received{op, fds, valid, err}
	}()
	<-started
	var request received
	receivedDuringFork := false
	select {
	case request = <-done:
		receivedDuringFork = true
	case <-time.After(100 * time.Millisecond):
	}
	syscall.ForkLock.Unlock()
	if !receivedDuringFork {
		request = <-done
	}
	for _, fd := range request.fds {
		defer unix.Close(fd)
	}
	if receivedDuringFork {
		t.Fatal("received a reply descriptor while another launch could inherit it")
	}
	if request.err != nil || !request.valid || request.op != 'N' || len(request.fds) != 1 {
		t.Fatalf("read helper request: %+v", request)
	}
	fd := request.fds[0]
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("received descriptor lacks close-on-exec: flags=%d err=%v", flags, err)
	}
	cmd := exec.CommandContext(t.Context(), "/bin/sh", "-c",
		`if ( : >&"$1" ) 2>/dev/null; then exit 1; fi`, "warm-peer-test", strconv.Itoa(fd))
	if err := cmd.Run(); err != nil {
		t.Fatalf("another launch inherited the reply descriptor: %v", err)
	}
}

func TestWarmPieceReplyRequiresOriginalProcessBirth(t *testing.T) {
	pid := os.Getpid()
	birth, err := processBirth(pid)
	if err != nil {
		t.Fatal(err)
	}
	op, reply, err := readWarmRequest(warmPeerRequest(t), pid, birth)
	if reply != nil {
		reply.Close()
	}
	if err != nil || op != 'N' || reply == nil {
		t.Fatalf("verified launch identity refused: op=%q reply=%t err=%v", op, reply != nil, err)
	}
	// Model the launch record of a previous holder of this PID.
	op, reply, err = readWarmRequest(warmPeerRequest(t), pid, birth+"-previous")
	if reply != nil {
		reply.Close()
	}
	if err != nil || op != 0 || reply != nil {
		t.Fatalf("same PID with a different birth was accepted: op=%q reply=%t err=%v", op, reply != nil, err)
	}
}
