package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	for _, lifetime := range []string{"25h", "3h"} {
		// The second request binds an unknown host; both reach the real API.
		selector := "host=host"
		if lifetime == "3h" {
			selector = "host=unknown"
		}
		err := call("add", selector, "--whole-host", "--id", "rejected", "--job", bound.ID, "--owner", "operator", "--max-lifetime", lifetime, "--reason", "host-wide validation")
		var api *myce.HTTPError
		if !errors.As(err, &api) || api.Status < 400 || api.Code == "" || out.Len() != 0 {
			t.Fatalf("non-creating add: error=%v output=%s", err, out.String())
		}
		records, err := q.Reservations()
		if err != nil || len(records) != 0 {
			t.Fatal("rejected add persisted a record", records, err)
		}
	}
	if err := call("add", "host=host", "--id", "validation", "--job", bound.ID, "--owner", "operator", "--reason", "qualification"); err != nil {
		t.Fatal(err)
	}
	var r myce.Reservation
	if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.ID != "validation" || r.Selector == "" || r.Job != bound.ID || r.Revision < 1 || r.Owner != "operator" || !r.CreatedAt.Equal(clock.Now()) {
		t.Fatal(out.String(), err)
	}
	records, err := q.Reservations()
	if err != nil || len(records) != 1 || records[0].ID != r.ID || records[0].Revision != r.Revision || records[0].Job != r.Job {
		t.Fatal("printed reservation was not persisted", records, err)
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

func TestReserveCLIGPUCheckScope(t *testing.T) {
	for _, wholeHost := range []bool{false, true} {
		t.Run(map[bool]string{false: "device", true: "whole host"}[wholeHost], func(t *testing.T) {
			var request myce.ReservationRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "GET /api/jobs/check":
					json.NewEncoder(w).Encode(myce.Job{ID: "check", State: "queued", Spec: myce.Spec{
						ExtensionCheck: &myce.ExtensionCheckRequest{},
						Needs:          myce.ResourceNeeds{GPU: &myce.GPUSelection{Devices: []string{"1"}}},
					}})
				case "POST /api/reservations":
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					json.NewEncoder(w).Encode(myce.Reservation{ReservationOptions: request.ReservationOptions, Job: request.Job, Revision: 1, State: "ready"})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			args := []string{"add", "host=host", "--job", "check", "--owner", "operator", "--reason", "qualification", "--queue", server.URL, "--token", "fixture"}
			want := "host=host;devices=1"
			if wholeHost {
				args = append(args, "--whole-host")
				want = "host=host"
			}
			if err := reserveCommand(context.Background(), args, &bytes.Buffer{}); err != nil {
				t.Fatal(err)
			}
			if request.Selector != want || request.Job != "check" {
				t.Fatal("wrong qualification reservation", request)
			}
		})
	}
}

func TestReserveCLIUnconfirmedResponse(t *testing.T) {
	for _, body := range []string{"", "null", "{}", `{"id":"validation","selector":"host=host","job":"check","revision":0,"state":"ready"}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "GET /api/jobs/check":
					json.NewEncoder(w).Encode(myce.Job{ID: "check", State: "queued"})
				case "POST /api/reservations":
					fmt.Fprint(w, body)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			var out bytes.Buffer
			err := reserveCommand(context.Background(), []string{"add", "host=host", "--whole-host", "--id", "validation", "--job", "check", "--owner", "operator", "--max-lifetime", "3h", "--reason", "host-wide validation", "--queue", server.URL, "--token", "fixture"}, &out)
			if !errors.Is(err, errReservationResponseInvalid) || out.Len() != 0 {
				t.Fatalf("unconfirmed add: error=%v output=%s", err, out.String())
			}
		})
	}
}
