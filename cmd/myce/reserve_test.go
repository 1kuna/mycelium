package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestReserveCLI(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	m := maintenanceCLIWorker("worker")
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	bound, err := q.Submit(myce.Spec{Name: "validation", Project: "test", Run: myce.RunSpec{Command: []string{"true"}}, Needs: myce.ResourceNeeds{MemoryGB: 1, Platform: myce.Platform{Any: true}}})
	if err != nil {
		t.Fatal(err)
	}
	const token = "reserve-cli-fixture"
	server := httptest.NewServer(myce.API{Queue: q, Token: token}.Handler())
	defer server.Close()
	var out bytes.Buffer
	call := func(args ...string) error {
		out.Reset()
		return reserveCommand(context.Background(), append(args, "--queue", server.URL, "--token", token), &out)
	}
	if err := call("add", "host=host", "--id", "validation", "--job", bound.ID, "--owner", "operator", "--reason", "qualification"); err != nil {
		t.Fatal(err)
	}
	var r myce.Reservation
	if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.ID != "validation" || r.Owner != "operator" || !r.CreatedAt.Equal(clock.Now()) {
		t.Fatal(out.String(), err)
	}
	if err := call("list"); err != nil || !strings.Contains(out.String(), "reserved for operator: qualification") {
		t.Fatal(out.String(), err)
	}
	if err := call("release", r.ID, "--if-revision", "1", "--reason", "abandoned check"); err != nil || !strings.Contains(out.String(), `"state":"released"`) {
		t.Fatal(out.String(), err)
	}
	if err := call("release", r.ID, "--if-revision", "1", "--reason", "stale"); err == nil {
		t.Fatal("stale release succeeded")
	}
	for _, args := range [][]string{{"add", "host=host"}, {"release", r.ID, "--reason", "missing revision"}, {"list", "--owner", "bad"}} {
		if err := call(args...); err == nil {
			t.Fatal("invalid CLI accepted", args)
		}
	}
}
