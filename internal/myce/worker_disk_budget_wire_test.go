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
	var supportsDiskBudget atomic.Bool
	reports := make(chan Checkin, 10)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		reports <- c
		if !supportsDiskBudget.Load() && hasDiskBudgetWireFields(c) {
			rw.WriteHeader(400)
			_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
			return
		}
		reply := Reply{AttemptProtocol: keyedAttemptProtocol, HostHygieneProtocol: hostHygieneProtocol, HostCollectionProtocol: hostCollectionProtocol}
		if supportsDiskBudget.Load() {
			reply.WorkerDiskBudgetProtocol = workerDiskBudgetProtocol
			reply.WorkerDiskHeadroomProtocol = workerDiskHeadroomProtocol
			reply.WorkerDiskTrendProtocol = workerDiskTrendProtocol
		}
		_ = json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	m := machine("disk-wire", "fake")
	m.HygieneCollection = &WorkerHygieneCollection{FileCache: CacheCollection{Bytes: 99}, DiskBudget: []DiskBudgetVolume{{ID: identityHash("local"), ForeignHeadroomBytes: 5, DecliningSeconds: 7, DeclineCause: "foreign_disk_growth"}}, Scratch: &AttemptFilesCollection{RetainedBytes: 10}}
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
	supportsDiskBudget.Store(true)
	send()
	if c := send(); !hasDiskBudgetWireFields(c) || c.Machine.HygieneCollection.DiskBudget[0].ForeignHeadroomBytes != 5 || c.Machine.HygieneCollection.DiskBudget[0].DecliningSeconds != 7 || c.Machine.HygieneCollection.DiskBudget[0].DeclineCause != "foreign_disk_growth" {
		t.Fatal("disk telemetry lost headroom or decline fields", c)
	}
	supportsDiskBudget.Store(false)
	send() // Strict rejection; checkin retries with just the old collection fields.
	fallback := <-reports
	if fallback.Machine.HygieneCollection == nil || fallback.Machine.HygieneCollection.FileCache.Bytes != 99 || hasDiskBudgetWireFields(fallback) {
		t.Fatal("downgrade lost compatible report", fallback)
	}
	if len(m.HygieneCollection.DiskBudget) != 1 || m.HygieneCollection.DiskBudget[0].ForeignHeadroomBytes != 5 || m.HygieneCollection.DiskBudget[0].DecliningSeconds != 7 || m.HygieneCollection.Scratch == nil {
		t.Fatal("negotiation mutated local retention evidence")
	}
}
