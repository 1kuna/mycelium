package myce

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelCopiesActuallyRunTogether(t *testing.T) {
	c, model := cacheFixture(t)
	c.Limit = 16
	c.Parallel = 2
	a, b := model("a", "aaaaaaaa"), model("b", "bbbbbbbb")
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	c.beforeChunk = func(context.Context) {
		if calls.Add(1) <= 2 {
			entered <- struct{}{}
			<-release
		}
	}
	done := make(chan error, 2)
	for _, m := range []Model{a, b} {
		go func() { _, e := c.Prepare(context.Background(), m); done <- e }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(testWaitTimeout):
			close(release)
			t.Fatal("parallel limit still serialized copies")
		}
	}
	close(release)
	for range 2 {
		if e := <-done; e != nil {
			t.Fatal(e)
		}
	}
	if names, free, e := c.Report([]Model{a, b}); e != nil || len(names) != 2 || free != 0 {
		t.Fatal(names, free, e)
	}
}
