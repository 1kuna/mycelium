//go:build unix

package myce

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// #218: one write exceeds idle while individual socket writes keep progressing.
// Exercise both the actual runtime-poller socket and the review's net.Pipe probe.
func TestFileTransferSingleWriteProgress(t *testing.T) {
	for _, mode := range []string{"socket", "pipe"} {
		t.Run(mode, func(t *testing.T) {
			var sender, receiver net.Conn
			if mode == "pipe" {
				sender, receiver = net.Pipe()
			} else {
				fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
				if err != nil {
					t.Fatal(err)
				}
				conns := make([]net.Conn, 2)
				for i, fd := range fds {
					file := os.NewFile(uintptr(fd), "transfer-test")
					conns[i], err = net.FileConn(file)
					file.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
				sender, receiver = conns[0], conns[1]
				if err := sender.(*net.UnixConn).SetWriteBuffer(1024); err != nil {
					t.Fatal(err)
				}
			}
			defer sender.Close()
			defer receiver.Close()
			body := bytes.Repeat([]byte("progress"), 4096)
			done := make(chan []byte, 1)
			go func() {
				var got []byte
				buf := make([]byte, 256)
				for {
					n, err := receiver.Read(buf)
					got = append(got, buf[:n]...)
					if err != nil {
						done <- got
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
			}()
			idle := 800 * time.Millisecond // Four measured 0.2 s scheduling periods.
			start := time.Now()
			n, err := (fileTransferConn{sender, idle}).Write(body)
			sender.Close()
			got := <-done
			if err != nil || n != len(body) || sha256.Sum256(got) != sha256.Sum256(body) {
				t.Fatalf("progressing write: n=%d got=%d err=%v", n, len(got), err)
			}
			if time.Since(start) <= idle {
				t.Fatal("fixture did not exercise a write longer than idle")
			}
		})
	}
	// No partial progress must still fail typed rather than retry forever.
	sender, receiver := net.Pipe()
	defer sender.Close()
	defer receiver.Close()
	_, err := (fileTransferConn{sender, 100 * time.Millisecond}).Write([]byte("stalled"))
	if !errors.Is(err, errFileTransferStalled) {
		t.Fatal("true write stall did not fail typed", err)
	}
}
