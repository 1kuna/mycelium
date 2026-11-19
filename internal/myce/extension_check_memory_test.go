package myce

import (
	"bytes"
	"encoding/json"
	"maps"
	"math"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #645: a single CUDA/unified check's total-cgroup peak transfers to XPU
// with a 2.5 margin, exceeding the allowance despite the measured need fitting.
func TestExtensionCheckMemoryCrossFamilyRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		hint, total                    float64
		oom, missingDevice, sameTarget bool
	}{
		{"hint", 12, 10.855361938476562, false, false, false},
		{"no hint", 0, 10.855361938476562, false, false, false},
		{"oversized cgroup despite small RSS", 12, 25, false, false, false},
		{"failed probe cannot repeat", 12, 10.855361938476562, true, false, false},
		{"missing device telemetry", 12, 10.855361938476562, true, true, false},
		{"same target different layout", 12, 10.855361938476562, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			e := registeredExtension(t, q, s.Extension)
			e.Version = "2"
			cuda := e.Variants[0]
			cuda.Target = ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "cuda"}
			if tc.sameTarget {
				e.Variants[0] = cuda
				m.CapabilityProfile.Arch = "arm64"
				m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 31 << 30}}
				for i := range m.Devices {
					m.Devices[i].Platform, m.Devices[i].MemoryMode = "cuda", "dedicated"
				}
			} else {
				e.Variants = append(e.Variants, cuda)
			}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			source := unifiedCUDAAdmissionMachine()
			source.ID, source.HostID, source.Session = "source", "source", "source-session"
			source.CapabilityProfile.Arch = "arm64"
			source.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 31 << 30}}
			source.Devices = cloneMachine(&m).Devices[:1]
			source.Devices[0].Platform, source.Devices[0].MemoryMode = "cuda", "unified"
			source.Devices[0].UnifiedMemory = true
			for _, owner := range []*Machine{&source, &m} {
				for i := range owner.Devices {
					target := owner.Devices[i].machine(*owner)
					report := newExtensionReport(e, &target, "extension:sample")
					report.Setup, report.Qualified, report.CheckID = true, true, "check"
					owner.Devices[i].ExtensionReports = []ExtensionReport{report}
				}
				machineCheck(t, q, *owner)
			}
			s.Extension, s.ExtensionDigest, s.Workload = extensionKey(e), e.Digest, nil
			s.RequireTarget = true
			s.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
			s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			s.SizeHint = SizeHint{MemoryGB: 8, HostMemoryGB: 40}
			s.Targets = []ExecutionTarget{cuda.Target}
			s.AllowedMachines = []string{source.ID}
			start := func(owner Machine) *Job {
				t.Helper()
				queued := submit(t, q, s)
				run := assignedJob(machineCheck(t, q, owner))
				if run == nil || run.ID != queued.ID {
					t.Fatalf("check not assigned: %+v", job(t, q, queued.ID))
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				return run
			}
			complete := func(run *Job, peak *AttemptMemoryPeak, oom bool) {
				t.Helper()
				clock.Advance(time.Second)
				r := result(run, !oom)
				r.MemoryPeak, r.OutOfMemory = peak, oom
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			rss, device := 5.0111541748046875, 5.7197265625
			seed := &AttemptMemoryPeak{HostGB: &rss, CgroupGB: &tc.total, DeviceGB: &device, Events: &MemoryEventCounts{}}
			if tc.missingDevice {
				seed.DeviceGB = nil
			}
			complete(start(source), seed, false)
			s.Targets = []ExecutionTarget{e.Variants[0].Target}
			s.AllowedMachines = []string{m.ID}
			s.SizeHint = SizeHint{HostMemoryGB: tc.hint}
			if tc.total > 24 {
				memoryExplorationRefused(t, q, s)
				return
			}
			run := start(m)
			wantSource := boundedGPUExploration
			if tc.missingDevice {
				wantSource += "; unknown device peak; exclusive card"
			}
			q.mu.Lock()
			explorations := len(q.s.MemoryExplorations)
			q.mu.Unlock()
			if run.MemoryEnvelope.Basis != "exploratory" || run.MemoryEnvelope.Source != wantSource || run.Spec.Needs.HostMemoryGB != 24-minimumHostMemoryGB || explorations != 1 {
				t.Fatalf("single cross-family prior still blocks bounded measurement: %+v", run.MemoryEnvelope)
			}
			host, residual := 7.25, .25
			peak := &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{SiblingAdjusted: true, DriverGB: &residual, OutsideGB: &residual, HostDropGB: &host}}
			complete(run, peak, tc.oom)
			if tc.oom {
				if got := job(t, q, run.ID); got.State != "failed" || got.OOMRetries != 0 || !strings.Contains(got.Reason, memoryCapacityRemedies) {
					t.Fatalf("bounded measurement retried after OOM: %+v", got)
				}
				q = memoryFactsRestart(t, q, clock)
				machineCheck(t, q, source)
				machineCheck(t, q, m)
				memoryExplorationRefused(t, q, s)
				if tc.sameTarget {
					// A failed discrete probe cannot erase the accepted unified
					// observation or replace it with the discrete spent prior.
					s.AllowedMachines = []string{source.ID}
					local := start(source)
					if local.MemoryEnvelope.Basis != "learned" || local.Spec.Needs.HostMemoryGB != tc.total+minimumHostMemoryGB {
						t.Fatalf("probe changed the other layout's accepted need: %+v", local.MemoryEnvelope)
					}
					complete(local, seed, false)
					s.AllowedMachines = []string{m.ID}
				}
				// The durable refusal remains valid without any live device
				// estimate after the source observation's normal expiry.
				clock.Advance(workCurveMaxAge + time.Second)
				machineCheck(t, q, source)
				machineCheck(t, q, m)
				memoryExplorationRefused(t, q, s)
				return
			}
			next := start(m)
			if next.MemoryEnvelope.Basis != "learned" || next.Spec.Needs.HostMemoryGB != 7.5+minimumHostMemoryGB {
				t.Fatalf("accepted target sample did not replace transferred prior: %+v", next.MemoryEnvelope)
			}
			// Once measured on this family, a genuinely oversized peak is binding.
			host = 25
			complete(next, peak, false)
			memoryExplorationRefused(t, q, s)
		})
	}
}

// A qualification check runs its small reference fixture, so it is sized from
// its own accepted checks, never from the workload's peak (#618). Live: a check
// peaking near 0.5 GiB was charged 16 GiB of a 24 GiB host allowance because
// it matched the workload's 12.8 GiB accepted peak. The workload in turn keeps
// its own envelope; a check's small peak never shrinks it.
func TestExtensionCheckMemoryLearnsFromChecksNotWorkload(t *testing.T) {
	q, clock, m, s := floorFreeFitFixture(t, 24)
	workloadHost, workloadDevice := 12.8, 4.5
	for id, p := range q.s.WorkObservations { // the workload's accepted peak
		p.MemoryPeak = &AttemptMemoryPeak{HostGB: &workloadHost, DeviceGB: &workloadDevice}
		q.s.WorkObservations[id] = p
	}
	checkSpec := s
	checkSpec.Workload = nil
	checkSpec.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	checkSpec.RequireTarget = true
	checkSpec.AllowedMachines = []string{m.ID}
	checkSpec.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
	checkSpec.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	runCheck := func(host, device float64) *Job {
		t.Helper()
		queued := submit(t, q, checkSpec)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("check not assigned: %s", job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &host, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		return j
	}

	first := runCheck(.4, .18)
	if e := first.MemoryEnvelope; e == nil || e.Source != physicalMemoryProbe || first.Spec.Needs.HostMemoryGB != 24-minimumHostMemoryGB {
		t.Fatalf("first check did not reserve physical headroom: host=%.2f GiB %+v", first.Spec.Needs.HostMemoryGB, e)
	}
	second := runCheck(.4, .18)
	if e := second.MemoryEnvelope; e == nil || e.Basis != "learned" || math.Abs(second.Spec.Needs.HostMemoryGB-(.4+minimumHostMemoryGB)) > 1e-9 || math.Abs(second.Spec.Needs.MemoryGB-(.18+minimumHostMemoryGB)) > 1e-9 {
		t.Fatalf("check not charged from its own accepted peak: host=%.3f device=%.3f GiB %+v", second.Spec.Needs.HostMemoryGB, second.Spec.Needs.MemoryGB, e)
	}

	queued := submit(t, q, s)
	work := assignedJob(machineCheck(t, q, m))
	if work == nil || work.ID != queued.ID {
		t.Fatalf("workload not assigned: %s", job(t, q, queued.ID).Reason)
	}
	if e := work.MemoryEnvelope; e == nil || e.Source != "accepted peak + measured uncertainty" || work.Spec.Needs.HostMemoryGB != workloadHost+minimumHostMemoryGB || work.Spec.Needs.MemoryGB != workloadDevice+minimumHostMemoryGB {
		t.Fatalf("check peaks changed the workload's envelope: host=%.2f device=%.2f GiB %+v", work.Spec.Needs.HostMemoryGB, work.Spec.Needs.MemoryGB, e)
	}
}

// A successful ceiling-bound workload must not leave an older accepted host
// estimate ahead of an explicit measurement hint. Qualification stays valid.
func TestExtensionHintMeasurementAfterConstrained(t *testing.T) {
	binary := os.Getenv("GATE_TEST_CLI_BINARY")
	binaryRoot := t.TempDir()
	cliResolve := func(t *testing.T, q *Queue, s Spec) ResolveReport {
		t.Helper()
		if binary == "" {
			binary = filepath.Join(binaryRoot, "myce")
			build := exec.CommandContext(t.Context(), "go", "build", "-p", "1", "-o", binary, "../../cmd/myce")
			if out, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build CLI: %v\n%s", err, out)
			}
		}
		server := httptest.NewServer(API{Queue: q, Token: "local-test-token"}.Handler())
		defer server.Close()
		path := filepath.Join(t.TempDir(), "measurement.json")
		data, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), binary, "spec", "check", "--resolve", "--json", "--queue", server.URL, "--token", "local-test-token", path)
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "MYCE_") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("spec check --resolve: %v\n%s\n%s", err, out, stderr.String())
		}
		var report ResolveReport
		if err := json.Unmarshal(out, &report); err != nil {
			t.Fatal(err)
		}
		return report
	}
	for _, history := range []string{"workload", "stored workload", "qualification", "none"} {
		for _, outcome := range []string{"constrained success", "legacy max success", "constrained failure", "OOM", "failed"} {
			for _, placement := range []string{"advanced", "legacy"} {
				t.Run(history+"/"+outcome+"/"+placement, func(t *testing.T) {
					q, clock, m, s := gpuHostFixture(t)
					start := func() *Job {
						t.Helper()
						queued := submit(t, q, s)
						run := assignedJob(machineCheck(t, q, m))
						if run == nil || run.ID != queued.ID {
							t.Fatalf("not assigned: %s", job(t, q, queued.ID).Reason)
						}
						if err := q.Started(result(run, false)); err != nil {
							t.Fatal(err)
						}
						return run
					}

					peak := 9.1 - minimumHostMemoryGB
					if history != "none" {
						work := s
						if history == "qualification" {
							s.Workload = nil
							s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
							s.ExtensionCheck = &ExtensionCheckRequest{Work: s.workType(), Directory: t.TempDir()}
							s.AllowedMachines = []string{m.ID}
						}
						seed := start()
						clock.Advance(time.Second)
						r := result(seed, true)
						// A host-only accepted sample produces the exploratory diagnostic from
						// the report without treating the following censored peak as accepted.
						r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak, Events: &MemoryEventCounts{}}
						if err := q.Complete(r); err != nil {
							t.Fatal(err)
						}
						s = work
					}
					first := start()
					wantEvidence := 1
					if history == "none" {
						wantEvidence = 0
					}
					if e := first.MemoryEnvelope; e.Basis != "exploratory" || e.Evidence != wantEvidence || history != "none" && math.Abs(e.HostGB-9.1) > 1e-9 {
						t.Fatalf("wrong reproduction envelope: %+v", e)
					}

					clock.Advance(600 * time.Second)
					stored := maps.Clone(q.s.WorkObservations)
					peak = 9.1
					r := result(first, strings.HasSuffix(outcome, "success"))
					r.OutOfMemory = outcome == "OOM"
					r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak, Events: &MemoryEventCounts{}}
					if outcome != "failed" {
						r.MemoryPeak.Constrained = outcome != "legacy max success"
						r.MemoryPeak.Events.Max = 5014
					}
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					if history == "stored workload" && r.Success {
						// Older queues preserved this aggregate after a successful
						// constrained completion. Reopen that exact persisted shape.
						if err := q.update(func() error {
							for id, point := range stored {
								q.putWorkObservation(id, point)
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
						q = memoryFactsRestart(t, q, clock)
					}
					if outcome == "OOM" {
						if err := q.Cancel(first.ID); err != nil {
							t.Fatal(err)
						}
					}
					accepted := 0
					for _, point := range q.s.WorkObservations {
						if !point.Key.Check && q.acceptedMemoryPoint(&point) {
							accepted += max(1, point.MemorySamples)
						}
					}
					if outcome == "failed" {
						// An ordinary failure teaches nothing and does not disprove a clean peak.
						want := 0
						if strings.HasSuffix(history, "workload") {
							want = 1
						}
						if accepted != want {
							t.Fatalf("failed result altered accepted count: %d", accepted)
						}
						return
					}
					if accepted != 0 {
						t.Errorf("ceiling-bound result left %d accepted samples", accepted)
					}
					s.SizeHint = SizeHint{HostMemoryGB: 14, MemoryGB: 30}
					if placement == "advanced" {
						s.Advanced = &Advanced{SizeHint: s.SizeHint}
						s.SizeHint = SizeHint{}
					}
					m.HostAvailableMemoryGB = 12 // The resolve hold exposes the required host envelope.
					machineCheck(t, q, m)        // Refresh inventory after the 600-second attempt.
					folded := s
					if _, err := folded.FoldAdvanced(); err != nil {
						t.Fatal(err)
					}
					report, err := q.Resolve(ResolveRequest{Spec: folded})
					if err != nil {
						t.Fatal(err)
					}
					if len(report.Targets) == 0 {
						t.Fatalf("no resolve targets: %+v", report)
					}
					if report.Hold == nil {
						t.Errorf("missing 14 GiB admission wait: %+v", report)
					} else {
						t.Logf("resolve hold: %s", report.Hold.FullText())
						if !strings.Contains(report.Hold.FullText(), "submitter hint") {
							t.Errorf("hinted resolve retained old source: %s", report.Hold.FullText())
						}
					}

					if strings.HasSuffix(history, "workload") && outcome == "constrained success" {
						cli := cliResolve(t, q, s)
						if cli.Hold == nil || !strings.Contains(cli.Hold.FullText(), "14.0 GiB needed") || !strings.Contains(cli.Hold.FullText(), "submitter hint") {
							t.Errorf("CLI did not resolve 14 GiB from hint: %+v", cli.Hold)
						}
					}
					queued := submit(t, q, folded)
					target := m.Devices[0].machine(*q.s.Machines[m.ID])
					candidate := seedMemoryCandidate(t, q, queued, &target)
					if e := candidate.MemoryEnvelope; e.HostGB != 14 || e.Source != "submitter hint" || e.Basis != "hint" || e.Evidence != 0 {
						t.Errorf("hinted candidate: %+v", e)
					}

					// A smaller hint cannot erase censored demand; a larger one cannot
					// bypass permanent physical/allowance capacity.
					low := *queued
					low.Spec.SizeHint = SizeHint{HostMemoryGB: 8, MemoryGB: 30}
					low.MemoryFallback = &low.Spec.SizeHint
					if e := seedMemoryCandidate(t, q, &low, &target).MemoryEnvelope; e.HostGB < 9.1 {
						t.Errorf("small hint erased measured floor: %+v", e)
					}
					partial := *queued
					partial.Spec.SizeHint = SizeHint{MemoryGB: 30}
					partial.MemoryFallback = &partial.Spec.SizeHint
					if e := seedMemoryCandidate(t, q, &partial, &target).MemoryEnvelope; e.HostGB <= peak {
						t.Errorf("device-only hint stopped host exploration: %+v", e)
					}
					high := low
					high.Spec.SizeHint = SizeHint{HostMemoryGB: 40, MemoryGB: 30}
					high.MemoryFallback = &high.Spec.SizeHint
					tooLarge := seedMemoryCandidate(t, q, &high, &target)
					capacity := q.qualificationCapacity(&target)
					if tooLarge.MemoryEnvelope.HostGB != 40 || hostFits(&capacity, tooLarge.Spec.Needs.MemoryGB, tooLarge.Spec.Needs.HostMemoryGB) {
						t.Errorf("large hint bypassed capacity: %+v", tooLarge.MemoryEnvelope)
					}
					if err := q.Cancel(queued.ID); err != nil {
						t.Fatal(err)
					}
					eligible := 0
					for _, target := range report.Targets {
						if !target.Eligible {
							continue
						}
						eligible++
					}
					if eligible != 2 {
						t.Errorf("wanted two eligible single-card targets: %+v", report.Targets)
					}
					if history == "stored workload" && outcome == "constrained success" {
						// Startup repair must be idempotent and leave later clean evidence.
						s = folded
						m.HostAvailableMemoryGB = 24
						machineCheck(t, q, m)
						clean := start()
						clock.Advance(time.Second)
						host, device := 10.0, 20.0
						r = result(clean, true)
						r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}}
						if err := q.Complete(r); err != nil {
							t.Fatal(err)
						}
						q = memoryFactsRestart(t, q, clock)
						machineCheck(t, q, m)
						target = m.Devices[0].machine(*q.s.Machines[m.ID])
						next := submit(t, q, folded)
						if e := seedMemoryCandidate(t, q, next, &target).MemoryEnvelope; e.Evidence == 0 || e.Basis != "learned" {
							t.Errorf("startup repair erased newer clean evidence: %+v", e)
						}
					}
				})
			}
		}
	}
}
