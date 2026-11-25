package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// stopGrace bounds how long the process takes to stop after Ctrl-C, SIGTERM
// or a run's own time limit. A call into a hung SMB mount blocks in the
// kernel and cannot be cancelled, so waiting for it could take forever. After
// the grace the process exits anyway and lists what it left behind for the
// storage owner to remove NAS-local. Ten seconds lets a writer finish the
// sync of a file it is closing on a healthy share.
const stopGrace = 10 * time.Second

// stopGuard forces an exit once a stop has been asked for and the grace has
// passed. Describers report leftovers from memory only: a forced exit must
// never wait on the filesystem that is hung.
type stopGuard struct {
	grace time.Duration
	exit  func(int)
	out   io.Writer

	mu    sync.Mutex
	next  int
	notes map[int]func() string
	fired bool
}

var guard = newStopGuard(stopGrace, os.Exit, os.Stderr)

func newStopGuard(grace time.Duration, exit func(int), out io.Writer) *stopGuard {
	return &stopGuard{grace: grace, exit: exit, out: out, notes: map[int]func() string{}}
}

// note registers a description of leftovers to print on a forced exit.
func (g *stopGuard) note(describe func() string) (forget func()) {
	g.mu.Lock()
	id := g.next
	g.next++
	g.notes[id] = describe
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		delete(g.notes, id)
		g.mu.Unlock()
	}
}

// arm starts the grace once ctx is done. disarm, called when the work has
// returned, cancels it.
func (g *stopGuard) arm(ctx context.Context) (disarm func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		timer := time.NewTimer(g.grace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			g.force()
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

func (g *stopGuard) force() {
	g.mu.Lock()
	if g.fired {
		g.mu.Unlock()
		return
	}
	g.fired = true
	ids := make([]int, 0, len(g.notes))
	for id := range g.notes {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		lines = append(lines, g.notes[id]())
	}
	g.mu.Unlock()
	fmt.Fprintf(g.out, "queue-qualify: stopped by force %s after the stop: a file or network call did not return\n", g.grace)
	for _, l := range lines {
		fmt.Fprintln(g.out, l)
	}
	g.exit(1)
}
