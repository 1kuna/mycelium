package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Reproduce the loaded e2e's control delay with the queue's real freshness
// predicate. The helper is already launched, as shown by its bare error in the
// retained service log. No clock timing or scheduler luck is involved.
func TestFleetHelperRefetchesDelayedControl(t *testing.T) {
	for _, edge := range []string{"before-publication"} {
		t.Run(edge, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			q, clock, _ := fleetFixture(t, "queue")
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			i := fleetInstructionFor(t, q)
			baseClient := f.deps.client
			controls, waits, refetches := 0, 0, 0
			delayAt := map[string]int{"before-fetch": 1, "before-publication": 2, "before-restart": 3}[edge]
			f.deps.client = func(c ServiceConfig) (Client, error) {
				client, err := baseClient(c)
				if err != nil {
					return client, err
				}
				transport := client.HTTP.Transport
				if transport == nil {
					transport = http.DefaultTransport
				}
				client.HTTP.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/api/fleet/instruction" {
						refetches++
					}
					control := r.URL.Path == "/api/fleet/authorize"
					if control {
						controls++
						if controls == delayAt {
							clock.Advance(fleetRuntimeFreshFor + time.Second)
						}
						if controls >= delayAt {
							if refusal := q.authorizeFleetUpdate(i.Operation, i.Host, i.Machine, i.Session, i.Revision); refusal != nil {
								w := httptest.NewRecorder()
								respond(w, nil, refusal)
								return w.Result(), nil
							}
						}
					}
					return transport.RoundTrip(r)
				})
				return client, nil
			}
			originalWait := f.deps.health.Wait
			f.deps.health.Wait = func(ctx context.Context, d time.Duration) error {
				waits++
				clock.Advance(d)
				q.fleetQueue.Proof.ObservedAt = clock.Now()
				return originalWait(ctx, d)
			}
			if err := f.apply(); err != nil {
				t.Fatalf("delayed control stopped helper: %v", err)
			}
			if waits == 0 || refetches == 0 || f.launches != 1 || f.restarts != 1 || len(f.failureCodes) != 0 || q.s.FleetUpgrade.State != "running" {
				t.Fatalf("lost recovery or repeated side effect: waits=%d refetches=%d launches=%d restarts=%d failures=%v state=%s", waits, refetches, f.launches, f.restarts, f.failureCodes, q.s.FleetUpgrade.State)
			}
			f.assertBinary(f.next)
		})
	}
}

func TestFleetPreparedClaimRefetchesWithoutDuplicateHelper(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	base := f.deps.client
	refused := false
	f.deps.client = func(c ServiceConfig) (Client, error) {
		client, err := base(c)
		if err != nil {
			return client, err
		}
		transport := client.HTTP.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		client.HTTP.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/api/fleet/ack" && !refused {
				refused = true
				w := httptest.NewRecorder()
				respond(w, nil, fleetError("fleet_instruction_stale"))
				return w.Result(), nil
			}
			return transport.RoundTrip(r)
		})
		return client, nil
	}
	var refetch *fleetInstructionRefetchError
	if err := f.launch(); !errors.As(err, &refetch) {
		t.Fatal("prepared stale claim must yield to ordinary runtime poll", err)
	}
	r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	if err != nil || !r.LaunchPending || f.launches != 0 {
		t.Fatal(r, err)
	}
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	if f.launches != 1 || len(f.failureCodes) != 0 {
		t.Fatal("duplicate helper or stopped operation", f.launches, f.failureCodes)
	}
	if _, err := os.Stat(f.c.Binary); err != nil {
		t.Fatal(err)
	}
}

func TestFleetFencedRefetchWaitsForProofAndRejectsReplacement(t *testing.T) {
	q, clock, _ := fleetFixture(t, "queue")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	mux := newRequestMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	client := Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w.Result(), nil
	})}}
	clock.Advance(fleetRuntimeFreshFor + time.Second)
	waits := 0
	wait := func(_ context.Context, d time.Duration) error {
		waits++
		clock.Advance(d)
		q.fleetQueue.Proof.ObservedAt = clock.Now()
		return nil
	}
	if err := fleetRetryControl(context.Background(), client, i, wait, func() error { return fleetAuthorize(context.Background(), client, i) }); err != nil {
		t.Fatal(err)
	}
	if waits != 1 || q.s.FleetUpgrade.State != "running" {
		t.Fatal("expired proof did not recover", waits, q.s.FleetUpgrade.State)
	}
	// A newer operator revision alone does not revoke a helper, but a durable
	// replacement claim must terminate its refetch without reporting failure.
	q.s.FleetUpgrade.Revision++
	q.fleetActiveHost().HelperRevision = q.s.FleetUpgrade.Revision
	if err := fleetRetryControl(context.Background(), client, i, wait, func() error { return fleetAuthorize(context.Background(), client, i) }); err == nil || err.Error() != "fleet_instruction_stale" {
		t.Fatal("obsolete helper recovered", err)
	}
	if waits != 1 || q.s.FleetUpgrade.State != "running" {
		t.Fatal("obsolete helper waited or poisoned operation", waits, q.s.FleetUpgrade.State)
	}
}

func TestFleetActivationWaitRechecksLocalState(t *testing.T) {
	for _, edge := range []string{"authorization", "restarting-ack"} {
		for _, changed := range []string{"bytes", "config", "publisher"} {
			t.Run(edge+"/"+changed, func(t *testing.T) {
				f := newFleetInstallFixture(t, "linux")
				if err := f.launch(); err != nil {
					t.Fatal(err)
				}
				if edge == "authorization" {
					f.denyAt, f.denyCode = 4, "fleet_instruction_stale"
				}
				base := f.deps.client
				refused, empty := false, false
				f.deps.client = func(c ServiceConfig) (Client, error) {
					client, err := base(c)
					if err != nil {
						return client, err
					}
					transport := client.HTTP.Transport
					if transport == nil {
						transport = http.DefaultTransport
					}
					client.HTTP.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
						if r.URL.Path == "/api/fleet/ack" && edge == "restarting-ack" && !refused {
							data, err := io.ReadAll(r.Body)
							if err != nil {
								return nil, err
							}
							r.Body = io.NopCloser(bytes.NewReader(data))
							var ack FleetUpgradeAck
							_ = json.Unmarshal(data, &ack)
							if ack.Phase == "restarting" {
								refused = true
								w := httptest.NewRecorder()
								respond(w, nil, fleetError("fleet_ack_stale"))
								return w.Result(), nil
							}
						}
						if r.URL.Path == "/api/fleet/instruction" && !empty {
							empty = true
							w := httptest.NewRecorder()
							respond(w, FleetUpdateInstruction{}, nil)
							return w.Result(), nil
						}
						return transport.RoundTrip(r)
					})
					return client, nil
				}
				mutated := false
				originalWait := f.deps.health.Wait
				f.deps.health.Wait = func(ctx context.Context, d time.Duration) error {
					if !mutated {
						mutated = true
						path, data := f.c.Binary, []byte("unapproved changed executable")
						if changed == "config" {
							path = f.c.Config
							var err error
							data, err = os.ReadFile(path)
							if err != nil {
								return err
							}
							data = append(data, '\n')
						}
						if changed == "publisher" {
							path = filepath.Join(f.c.State, "fleet-publisher.pub")
							data = make([]byte, 32)
						}
						if err := os.WriteFile(path, data, 0600); err != nil {
							return err
						}
					}
					return originalWait(ctx, d)
				}
				err := f.apply()
				want := "fleet_install_changed"
				if changed == "publisher" {
					want = "fleet_publisher_mismatch"
				}
				if !mutated || err == nil || err.Error() != want || f.restarts != 0 {
					t.Fatalf("activation escaped changed %s: mutated=%v err=%v restarts=%d", changed, mutated, err, f.restarts)
				}
			})
		}
	}
}

func TestFleetFencedRefetchRejectsChangedLiveWorkerSession(t *testing.T) {
	q, clock, workers := fleetFixture(t, "host", "host")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	h := q.fleetActiveHost()
	// Reach the worker-update boundary with its frozen old-session receipt.
	h.QueueDone = true
	h.Phase = "worker_update"
	h.Machine = workers[0].ID
	h.Before = &workers[0]
	i := fleetInstructionFor(t, q)
	m := q.s.Machines[i.Machine]
	m.Session = "replacement-session"
	m.Fleet = fleetTestProof(clock, m.Session, 321, false)
	mux := newRequestMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	client := Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w.Result(), nil
	})}}
	waits := 0
	wait := func(context.Context, time.Duration) error {
		waits++
		return errors.New("unexpected wait for obsolete session")
	}
	err := fleetRetryControl(context.Background(), client, i, wait, func() error { return fleetAuthorize(context.Background(), client, i) })
	if err == nil || err.Error() != "fleet_instruction_stale" || waits != 0 {
		t.Fatal("live replacement session did not stop refetch", err, waits)
	}
}
