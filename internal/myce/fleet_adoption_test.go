package myce

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestWorkerAdoptionProof(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)}
	j := Job{ID: ID(), Attempt: 2, Machine: "worker", Session: "session"}
	for _, kind := range []string{"preparing", "started", "finished", "unobserved", "stale", "release-pending", "inline", "shared-inline", "released"} {
		t.Run(kind, func(t *testing.T) {
			g := &attemptCustodian{observed: true, last: custodianSnapshot{At: c.Now(), State: workerState{Job: &j, Session: j.Session}}}
			a := &attemptSupervisor{job: j, custodian: g}
			want := ""
			switch kind {
			case "started":
				g.last.Started = true
			case "finished":
				g.last.Finished = true
			case "unobserved":
				g.observed = false
				want = "readiness or release"
			case "stale":
				g.last.At = c.Now().Add(-max(3*time.Second, attemptLivenessMaxAge) - time.Nanosecond)
				want = "readiness or release"
			case "release-pending":
				g.last.Finished = true
				g.last.State.Result = &Result{ReleasePending: true}
				want = "readiness or release"
			case "inline", "shared-inline":
				a.custodian = nil
				want = "no detached custodian"
				if kind == "shared-inline" {
					a.worker.Machine.SharedHost = &SharedHostProfile{}
					if runtime.GOOS == "linux" {
						want = "aggregate CPU quota"
					}
				}
			case "released":
				a.custodian, a.released = nil, true
			}
			proof := workerAdoptionProof(map[attemptKey]*attemptSupervisor{jobKey(j): a}, c.Now(), time.Second)
			if proof.Protocol != custodianProtocol || (want == "" && proof.Reason != "") || (want != "" && !strings.Contains(proof.Reason, want)) {
				t.Fatalf("proof: %+v, want reason %q", proof, want)
			}
			if want == "" && kind != "released" && (len(proof.Attempts) != 1 || proof.Attempts[0] != runExecution(&j)) {
				t.Fatal("lost exact preparation/attempt custody", proof)
			}
		})
	}
}
