package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func issue102Child(t *testing.T, root myce.HostIdentityResolver, account myce.WorkerAccount, handler myce.HostMutationHandler) myce.HostChildLauncher {
	t.Helper()
	return func(ctx context.Context, launch myce.HostChildLaunch) ([]byte, error) {
		worker := root
		worker.EUID = int(account.UID)
		input, err := json.Marshal(launch.Request)
		if err != nil {
			return nil, err
		}
		var out bytes.Buffer
		err = myce.ServeHostWorkerChild(ctx, bytes.NewReader(input), &out, worker, handler)
		return out.Bytes(), err
	}
}

func TestIssue102RootStatusOpenHost(t *testing.T) {
	r, account, c := localIdentityFixture(t)
	s, err := myce.ReadHostIntent(c.HostStateDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.LastClose.Settlement, s.LastClose.SettledAt = "confirmed", &now
	data, _ := json.Marshal(s)
	if err = os.WriteFile(filepath.Join(c.HostStateDir, "host-state.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = (myce.HostControl{Dir: c.HostStateDir, Expected: s.HostRevision}).Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := snapshotHostFiles(t, c.HostStateDir)
	launch := issue102Child(t, r, account, nil)
	fences := 0
	child := func(ctx context.Context, l myce.HostChildLaunch) ([]byte, error) {
		if l.Request.Operation == "resolve" {
			fences++
		}
		return launch(ctx, l)
	}
	var out bytes.Buffer
	err = hostEnvelopeCommandWithObservation([]string{"status", "--local", "--worker-user", account.Name, "--config", c.Config, "--json"}, myce.MachineProbe{}, &out, r, child, nil, func(ctx context.Context, v myce.HostInvocation) (myce.HostExclusionReport, error) {
		input, err := v.Mutate(ctx, myce.HostMutation{Operation: "inspect-exclusion"}, child, nil)
		if err != nil {
			return myce.HostExclusionReport{}, err
		}
		// The real observer deliberately has no local_intent timestamp on open.
		report := myce.ObserveHostExclusion(ctx, v.ObservationContext(), *input.Input, myce.RealClock{}, myce.HostKernelReaders{}, nil)
		if report.Sources["local_intent"] != nil {
			t.Fatal("open observer unexpectedly supplied local intent")
		}
		return report, nil
	})
	e := validateEnvelopeOutput(t, out.Bytes())
	if err != nil || e.Desired.State == nil || *e.Desired.State != "open" || e.Desired.Revision == nil || *e.Desired.Revision != 2 || e.Operation == nil || e.Boot == nil || e.LastClose == nil || fences != 1 {
		t.Fatalf("root status lost open intent: err=%v envelope=%+v fences=%d", err, e, fences)
	}
	if !reflect.DeepEqual(before, snapshotHostFiles(t, c.HostStateDir)) {
		t.Fatal("status wrote host state")
	}
}

func TestIssue102PrivilegedCloseRelease(t *testing.T) {
	for _, verdict := range []string{"PASS", "UNKNOWN", "FAIL"} {
		t.Run(verdict, func(t *testing.T) {
			r, account, c := localIdentityFixture(t)
			clock := &waitClock{now: time.Now().UTC()}
			queueRoot := filepath.Join(account.Home, "queue")
			if err := myce.PrepareStateRoots(queueRoot); err != nil {
				t.Fatal(err)
			}
			q, err := myce.Open(filepath.Join(queueRoot, "queue.db"), filepath.Join(queueRoot, "checkpoints"), myce.RealClock{})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			const token = "issue102-synthetic"
			server := httptest.NewServer(myce.API{Queue: q, Token: token}.Handler())
			defer server.Close()
			c.Queue = server.URL
			c.EnvironmentFile = filepath.Join(account.Home, "synthetic.env")
			if err = os.WriteFile(c.EnvironmentFile, []byte("MYCE_TOKEN="+token+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			config, _ := json.Marshal(c)
			if err = os.WriteFile(c.Config, config, 0600); err != nil {
				t.Fatal(err)
			}
			records, reconciles, observations := 0, 0, 0
			childUnconfirmed := false
			x := myce.HostMutationExecutor{Probe: myce.MachineProbe{OS: "fixture"}, Clock: clock, Reconcile: func(ctx context.Context, v myce.HostInvocation) error {
				reconciles++
				return myce.ReconcileHostLocal(ctx, v.StateDir, clock, func(endpoint string) (myce.Client, error) {
					return myce.Client{URL: endpoint, Token: token}, nil
				}, v.Config.Queue)
			}}
			handler := func(ctx context.Context, v myce.HostInvocation, m myce.HostMutation) (myce.HostMutationResult, error) {
				if m.Operation == "record-exclusion" {
					records++
				}
				result, err := x.Execute(ctx, v, m)
				if m.Operation == "close" {
					childUnconfirmed = err != nil && strings.Contains(err.Error(), "local_exclusion_unconfirmed") && result.Revision > 0
				}
				return result, err
			}
			launch := issue102Child(t, r, account, handler)
			var out bytes.Buffer
			err = hostEnvelopeCommandWithTiming([]string{"close", "--local", "--worker-user", account.Name, "--config", c.Config, "--grace", "0", "--timeout", "3s", "--json"}, myce.MachineProbe{}, &out, r, launch, nil, func(ctx context.Context, v myce.HostInvocation) (myce.HostExclusionReport, error) {
				observations++
				input, err := v.Mutate(ctx, myce.HostMutation{Operation: "inspect-exclusion"}, launch, nil)
				if err != nil {
					return myce.HostExclusionReport{}, err
				}
				report := envelopeHardware(ctx, myce.HostIntent{HostRevision: input.HostRevision})
				report.At = clock.Now()
				for name := range report.Sources {
					at := report.At
					report.Sources[name] = &at
				}
				data, _ := json.Marshal(input.Input)
				digest := sha256.Sum256(data)
				report.Digest = hex.EncodeToString(digest[:])
				report.Verdict = verdict
				// Also prove that a later PASS is recorded, rather than only the
				// first UNKNOWN observation being relayed to reconciliation.
				if verdict == "PASS" && observations == 1 {
					report.Verdict = "UNKNOWN"
				}
				return report, nil
			}, clock, func(context.Context, time.Duration) error {
				clock.advance(100 * time.Millisecond)
				return nil
			})
			e := validateEnvelopeOutput(t, out.Bytes())
			if !childUnconfirmed || observations == 0 || records == 0 || reconciles == 0 || e.Accepted == nil || *e.Accepted != 2 {
				t.Fatalf("accepted close skipped privileged inspect/record: child_unconfirmed=%v observations=%d records=%d reconciles=%d accepted=%v err=%v", childUnconfirmed, observations, records, reconciles, e.Accepted, err)
			}
			s, readErr := myce.ReadHostIntent(c.HostStateDir)
			if readErr != nil || s.Desired != "closed" {
				t.Fatal("close intent was not retained", readErr)
			}
			if verdict == "PASS" {
				if err != nil || e.LocalExclusion != "PASS" || !s.LocalExclusion.Confirmed || records < 2 || s.HostSettlement() != "confirmed" || e.Settlement != "confirmed" {
					t.Fatal("fresh release was not recorded and settled", err, e.LocalExclusion, e.Settlement, s.HostSettlement(), records)
				}
			} else {
				if exitCode(err) != 1 || !errors.Is(err, myce.ErrHostDeadline) || !strings.Contains(err.Error(), "close accepted, release not confirmed") || s.LocalExclusion.Confirmed {
					t.Fatal("unconfirmed close returned success or lost deadline", err, exitCode(err))
				}
				found := false
				for _, failure := range e.Errors {
					found = found || failure.Code == "release_not_confirmed" && strings.Contains(failure.Message, "close accepted, release not confirmed")
				}
				if !found {
					t.Fatal("missing release_not_confirmed diagnostic", e.Errors)
				}
			}
		})
	}
}
