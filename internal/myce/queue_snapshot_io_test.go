package myce

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

// The retained 26 MB queue reproduced 0.97-1.18 s SQLite commits with the
// default cache, versus 0.15-0.20 s when old/new snapshot pages fit. Model that
// measured I/O budget behind three ordinary control transactions. The real
// worker's unchanged two-second launch deadline is the acceptance boundary.
func TestQueueSnapshotCacheAllowsLaunch(t *testing.T) {
	q, _ := fixture(t)
	var cacheSize, pageSize int
	if err := q.db.QueryRow("PRAGMA cache_size").Scan(&cacheSize); err != nil {
		t.Fatal(err)
	}
	if err := q.db.QueryRow("PRAGMA page_size").Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	cacheBytes := cacheSize * pageSize
	if cacheSize < 0 {
		cacheBytes = -cacheSize * 1024
	}
	const snapshotBytes = 26162944
	cost := 1100 * time.Millisecond
	if cacheBytes >= 2*snapshotBytes {
		cost = 200 * time.Millisecond
	}
	synctest.Test(t, func(t *testing.T) {
		store := &queuedControlStore{work: make(chan chan struct{}, 4), cost: cost}
		done, exited := make(chan struct{}), make(chan struct{})
		defer func() { close(done); <-exited }()
		go func() {
			defer close(exited)
			for {
				select {
				case reply := <-store.work:
					select {
					case <-time.After(store.cost):
						close(reply)
					case <-done:
						return
					}
				case <-done:
					return
				}
			}
		}()
		for i := 0; i < 3; i++ {
			store.work <- make(chan struct{})
		}
		worker := Worker{Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: store}}}
		if err := worker.authorizeLaunch(HostLaunch{}); err != nil {
			t.Fatalf("snapshot cache starves launch behind control updates: %v", err)
		}
	})
}
