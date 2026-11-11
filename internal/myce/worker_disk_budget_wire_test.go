package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestWorkerDiskBudgetWireStrictDowngrade(t *testing.T) {
	var version atomic.Int32
	reports := make(chan Checkin, 10)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		reports <- c
		if version.Load() == 0 && hasDiskBudgetWireFields(c) {
			rw.WriteHeader(400)
			_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
			return
		}
		reply := Reply{AttemptProtocol: keyedAttemptProtocol, HostHygieneProtocol: hostHygieneProtocol, HostCollectionProtocol: hostCollectionProtocol}
		if version.Load() > 0 {
			reply.WorkerDiskBudgetProtocol = workerDiskBudgetProtocol
		}
		if version.Load() > 1 {
			reply.WorkerDiskHeadroomProtocol = workerDiskHeadroomProtocol
		}
		_ = json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	m := machine("disk-wire", "fake")
	m.HygieneCollection = &WorkerHygieneCollection{FileCache: CacheCollection{Bytes: 99}, DiskBudget: []DiskBudgetVolume{{ID: identityHash("local"), ForeignHeadroomBytes: 5}}, Scratch: &AttemptFilesCollection{RetainedBytes: 10}}
	w := Worker{Client: Client{URL: server.URL}, diskBudgetWire: &workerDiskBudgetWire{}}
	send := func() Checkin {
		t.Helper()
		if _, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		return <-reports
	}
	if c := send(); c.Machine.HygieneCollection == nil || c.Machine.HygieneCollection.FileCache.Bytes != 99 || hasDiskBudgetWireFields(c) {
		t.Fatal("first check-in lost baseline collection", c)
	}
	if c := send(); c.Machine.HygieneCollection == nil || hasDiskBudgetWireFields(c) {
		t.Fatal("old queue negotiation lost baseline report or sent new fields", c)
	}
	version.Store(1)
	send()
	if c := send(); !hasDiskBudgetWireFields(c) || c.Machine.HygieneCollection.DiskBudget[0].ForeignHeadroomBytes != 0 {
		t.Fatal("budget-only queue lost disk budget or received headroom", c)
	}
	version.Store(2)
	send()
	if c := send(); c.Machine.HygieneCollection.DiskBudget[0].ForeignHeadroomBytes != 5 {
		t.Fatal("headroom queue lost headroom", c)
	}
	version.Store(0)
	send() // Strict rejection; checkin retries with just the old collection fields.
	fallback := <-reports
	if fallback.Machine.HygieneCollection == nil || fallback.Machine.HygieneCollection.FileCache.Bytes != 99 || hasDiskBudgetWireFields(fallback) {
		t.Fatal("downgrade lost compatible report", fallback)
	}
	if len(m.HygieneCollection.DiskBudget) != 1 || m.HygieneCollection.DiskBudget[0].ForeignHeadroomBytes != 5 || m.HygieneCollection.Scratch == nil {
		t.Fatal("negotiation mutated local retention evidence")
	}
}
