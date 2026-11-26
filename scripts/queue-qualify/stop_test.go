package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Ctrl-C, or the duration limit, while a writer is stuck in a filesystem
// call: the process exits within the grace and lists what it left.
func TestStopExitsWithinTheGraceWhileAFileCallIsBlocked(t *testing.T) {
	for _, c := range []struct {
		name     string
		duration time.Duration
		cancel   bool
	}{{"interrupt", 20 * time.Second, true}, {"duration", 300 * time.Millisecond, false}} {
		t.Run(c.name, func(t *testing.T) {
			const grace = 200 * time.Millisecond
			var out strings.Builder
			var outMu sync.Mutex
			exited := make(chan int, 2)
			old := guard
			guard = newStopGuard(grace, func(code int) { exited <- code }, writerFunc(func(p []byte) (int, error) {
				outMu.Lock()
				defer outMu.Unlock()
				return out.Write(p)
			}))
			defer func() { guard = old }()
			release := make(chan struct{})
			defer close(release)
			blocked := make(chan struct{}, 8)
			opt := smallLoad(t.TempDir(), plentyFree)
			opt.duration = c.duration
			// Writer 0 has a healthy disk; writer 1's create hangs the way an
			// open on a stalled SMB mount does, until the test ends.
			opt.create = func(p string) (loadFile, error) {
				blocked <- struct{}{}
				if strings.Contains(filepath.Base(p), "w1-") {
					<-release
					return nil, errors.New("released")
				}
				return createExclusive(p)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go runLoad(ctx, opt)
			<-blocked
			<-blocked
			time.Sleep(50 * time.Millisecond)
			stopped := time.Now()
			if c.cancel {
				cancel()
			} else {
				stopped = time.Now().Add(c.duration - 50*time.Millisecond)
			}
			select {
			case code := <-exited:
				if took := time.Since(stopped); took > grace+time.Second {
					t.Fatalf("exited %s after the stop; the grace is %s", took, grace)
				}
				if code == 0 {
					t.Fatal("a forced exit reported success")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("still running five seconds after the stop with a blocked file call")
			}
			outMu.Lock()
			msg := out.String()
			outMu.Unlock()
			if !strings.Contains(msg, "stopped by force") || !strings.Contains(msg, runDirPrefix+"load-") || !strings.Contains(msg, "partial") {
				t.Fatalf("forced exit did not list the leftovers: %q", msg)
			}
		})
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
