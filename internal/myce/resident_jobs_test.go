package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func residentFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m := modelFixture(t)
	m.FreeMemoryGB = 32
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	m.Capabilities = append(m.Capabilities, residentJobsCapability)
	check(t, q, m, nil)
	s := spec("experiment")
	s.Resumable = false
	s.Needs.MemoryGB, s.Needs.HostMemoryGB = 1, 1
	s.Resident = &ResidentBinding{Model: "test"}
	return q, clock, m, s
}

func residentAssigned(t *testing.T, q *Queue, m Machine, s Spec) *Job {
	t.Helper()
	j := submit(t, q, s)
	r := check(t, q, m, nil)
	if r.Job == nil || r.Job.ID != j.ID {
		t.Fatalf("resident driver not assigned: %+v; job=%+v", r, job(t, q, j.ID))
	}
	return r.Job
}

func residentReport(t *testing.T, q *Queue, m Machine, j *Job, state string) *ModelSlot {
	t.Helper()
	name := j.Spec.Resident.Engine.Model.Name
	slot := q.s.ModelSlots[targetSlotKey(&m, name)]
	if slot == nil {
		t.Fatal("resident engine not reserved")
	}
	c := Checkin{Machine: m, ModelReports: []ModelReport{{Name: name, Attempt: slot.Attempt, Device: slot.Device, State: state, CallsMeasured: true}}, period: time.Second}
	if j.State == "running" {
		c.Running = &Execution{ID: j.ID, Attempt: j.Attempt}
	}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	return slot
}

func TestResidentJobsReuseChargeAndMeasuredIdle(t *testing.T) {
	q, clock, m, s := residentFixture(t)
	first := residentAssigned(t, q, m, s)
	clock.Advance(4 * time.Second) // Measured load cost, not a production timeout.
	slot := residentReport(t, q, m, first, "ready")
	attempt := slot.Attempt
	if q.modelMemory(m.ID) != 8 || len(q.s.ModelSlots) != 1 {
		t.Fatal("cold engine was not charged once")
	}
	finish(t, q, first, true)
	s.Files = nil
	second := residentAssigned(t, q, m, s)
	if second.Spec.Resident.Engine.Model.Name != slot.Model || slot.Attempt != attempt || slot.State != "ready" || q.modelMemory(m.ID) != 8 {
		t.Fatalf("same stage reloaded or double charged: %+v", slot)
	}
	finish(t, q, second, true)
	// One four-second load sample has a twelve-second retention envelope:
	// observed cost plus the existing latency policy's initial uncertainty.
	clock.Advance(11 * time.Second)
	q.Tick()
	if slot.State != "ready" {
		t.Fatal("engine expired before the measured retention envelope")
	}
	clock.Advance(time.Second) // Twelve measured seconds, not the configured idle interval.
	q.Tick()
	if slot.State != "draining" || q.modelMemory(m.ID) != 8 {
		t.Fatal("idle release must retain charge until acknowledgement")
	}
	second.State = "succeeded"
	residentReport(t, q, m, second, "unloaded")
	if q.modelMemory(m.ID) != 0 {
		t.Fatal("acknowledged unload retained memory")
	}
}

func residentPiecesFixture(t *testing.T, mode string) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m, s := residentFixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s.Outputs = map[string]string{"out": "results"}
	s.FanOut = &FanOut{Pieces: []FanOutPiece{
		{Key: "a", Args: []string{"--input", "a"}},
		{Key: "b", Args: []string{"--input", "b"}},
		{Key: "c", Args: []string{"--input", "c"}},
	}}
	if mode == "plain-entry" {
		s.Files = FileManifest{}
		for _, p := range s.FanOut.Pieces {
			s.Files[PlainPiecePrefix+p.Key+"/input"] = putPieceTestFile(t, q, "input "+p.Key)
		}
		return q, clock, m, s
	}
	e := fakeExtension(t)
	e.Resumable = false
	if mode == "registered-extension" {
		e.Resident, s.Resident = s.Resident, nil
	}
	e.Files["extension.yaml"], _ = yaml.Marshal(e)
	e, err := extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	s.Run, s.Needs = RunSpec{}, ResourceNeeds{}
	s.Kind = "sample"
	s.Workload = &Workload{InputShape: "fixture", Units: 1}
	s.FanOut.MaxConcurrency = 1
	r := newExtensionReport(e, &m, s.workType())
	r.Setup, r.Qualified, r.CheckID = true, true, "resident-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	return q, clock, m, s
}

func TestResidentJobsPiecesReuseFrozenEngineAndAcceptIndependently(t *testing.T) {
	for _, mode := range []string{"plain-entry", "extension-entry", "registered-extension"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, m, s := residentPiecesFixture(t, mode)
			parent := submit(t, q, s)
			binding := parent.Spec.Resident
			// A later catalog edit must not change an unmaterialized child's engine.
			q.models.Models[0].StartCommand[0] = "changed-runtime"
			m.ModelConfigHash = modelConfigHash(q.models)
			var slot *ModelSlot
			var loadAttempt string
			accepted := map[string]PieceResult{}
			for range 3 {
				run := check(t, q, m, nil).Job
				if run == nil || run.Batch == nil || run.Batch.Parent != parent.ID || run.Spec.FanOut != nil || run.Spec.Resumable || run.Batch.Concurrency != 1 || len(run.Batch.Pieces) != 1 {
					t.Fatalf("piece is not an independent non-resumable job: %+v", run)
				}
				piece := run.Batch.Pieces[0]
				if !reflect.DeepEqual(run.Spec.Resident, binding) {
					t.Fatalf("piece %s changed frozen resident binding: %+v", piece.Key, run.Spec.Resident)
				}
				if mode == "plain-entry" {
					if !slices.Equal(run.Spec.Run.Command, append(slices.Clone(s.Run.Command), piece.Args...)) || run.Spec.Files["input"] != s.Files[PlainPiecePrefix+piece.Key+"/input"] || len(run.Spec.Files) != 1 {
						t.Fatal("piece arguments or private input lost")
					}
				} else if !slices.Equal(run.Spec.Args, piece.Args) {
					t.Fatalf("piece arguments lost: %v", run.Spec.Args)
				}
				if err := q.Started(result(run, true)); err != nil {
					t.Fatal(err)
				}
				if slot == nil {
					clock.Advance(4 * time.Second)
					slot = residentReport(t, q, m, run, "ready")
					loadAttempt = slot.Attempt
				}
				current := q.s.ModelSlots[targetSlotKey(&m, binding.Engine.Model.Name)]
				if current == nil || current.State != "ready" || current.Attempt != loadAttempt || len(q.s.ModelSlots) != 1 || q.modelMemory(m.ID) != 8 {
					t.Fatalf("engine reloaded or charged more than once: %+v", q.s.ModelSlots)
				}
				// Test the parent's concurrency cap on another eligible worker too.
				if mode != "plain-entry" {
					other := cloneMachine(&m)
					other.ID, other.Session = "other", "other-session"
					if candidate := q.fanOutCandidate(q.s.Jobs[parent.ID], &other); candidate != nil {
						t.Fatal("max_concurrency 1 admitted a sibling before completion")
					}
				}
				clock.Advance(time.Second)
				ref := completeWithOutput(t, q, run, "result "+piece.Key)
				accepted[piece.Key] = PieceResult{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, OutputRefs: FileManifest{"out": ref}, OutputFiles: map[string]string{}}
				got := job(t, q, parent.ID)
				if !reflect.DeepEqual(got.FanOut.Results, accepted) || job(t, q, run.ID).State != "succeeded" {
					t.Fatalf("piece result not independently accepted: %+v", got.FanOut.Results)
				}
				if len(accepted) < 3 && (got.State != "queued" || q.modelIdleExpired(slot)) {
					t.Fatal("remaining pieces did not retain the warm engine")
				}
			}
			got := job(t, q, parent.ID)
			if got.State != "succeeded" || len(got.OutputRefs) != 3 {
				t.Fatalf("incomplete gather: state=%s outputs=%v", got.State, got.OutputRefs)
			}
		})
	}
}

func TestResidentJobsFailedPiecePreservesAcceptedSiblings(t *testing.T) {
	for _, mode := range []string{"plain-entry", "registered-extension"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, m, s := residentPiecesFixture(t, mode)
			parent := submit(t, q, s)
			first := check(t, q, m, nil).Job
			if first == nil {
				t.Fatal("no first piece")
			}
			clock.Advance(time.Second)
			residentReport(t, q, m, first, "ready")
			ref := completeWithOutput(t, q, first, "accepted sibling")
			key := first.Batch.Pieces[0].Key
			accepted := job(t, q, parent.ID).FanOut.Results[key]
			failed := check(t, q, m, nil).Job
			if failed == nil {
				t.Fatal("no second piece")
			}
			clock.Advance(time.Second)
			finish(t, q, failed, false)
			// Plain pieces let all siblings settle; extension fan-out keeps its
			// existing fail-fast contract for work not yet accepted.
			if mode == "plain-entry" {
				last := check(t, q, m, nil).Job
				if last == nil {
					t.Fatal("failure cancelled the remaining plain piece")
				}
				completeWithOutput(t, q, last, "last sibling")
			}
			got := job(t, q, parent.ID)
			if got.State != "failed" || job(t, q, failed.ID).State != "failed" || job(t, q, first.ID).State != "succeeded" || job(t, q, first.ID).OutputRefs["out"] != ref || !reflect.DeepEqual(got.FanOut.Results[key], accepted) {
				t.Fatalf("failure invalidated an accepted sibling: state=%s results=%+v", got.State, got.FanOut.Results)
			}
		})
	}
}

func TestResidentJobsUnsupportedSpecs(t *testing.T) {
	for _, mode := range []string{"plain-entry", "registered-extension"} {
		for _, shape := range []string{"resumable", "multi-device", "cpu-fallback"} {
			t.Run(mode+"/"+shape, func(t *testing.T) {
				q, _, _, s := residentPiecesFixture(t, mode)
				switch shape {
				case "resumable":
					s.Resumable = true
				case "multi-device":
					count := 2
					s.Needs.GPU = &GPUSelection{Count: &count}
				case "cpu-fallback":
					s.AllowCPUFallback = true
				}
				_, err := q.Submit(s)
				want := "resident requires a catalog model and non-resumable single-device jobs without CPU fallback"
				if mode == "registered-extension" && shape == "cpu-fallback" {
					// Authoring rejects this before extension binding is resolved.
					want = "extension work runs on CPU through a declared cpu variant"
				} else if mode == "registered-extension" && shape == "multi-device" {
					want = "gpu_count_unsupported"
				}
				if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "use pieces") {
					t.Fatalf("unsupported resident shape has wrong refusal: %v", err)
				}
			})
		}
	}
}

func TestResidentJobsSeparateKindsAndFreezeCatalog(t *testing.T) {
	q, _, _, s := residentFixture(t)
	a := submit(t, q, s)
	s.Name = "other-stage"
	b := submit(t, q, s)
	if a.Spec.Resident.Engine.Model.Name == b.Spec.Resident.Engine.Model.Name {
		t.Fatal("different stages shared an engine")
	}
	original := a.Spec.Resident.Engine.Model.StartCommand[0]
	q.models.Models[0].StartCommand[0] = "changed-runtime"
	if a.Spec.Resident.Engine.Model.StartCommand[0] != original {
		t.Fatal("frozen engine aliases mutable catalog data")
	}
	s.Name = "job"
	c := submit(t, q, s)
	if c.Spec.Resident.Engine.Model.Name == a.Spec.Resident.Engine.Model.Name {
		t.Fatal("changed runtime reused an old engine")
	}
}

func TestResidentJobsReserveColdLoadAndYieldUnderPressure(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, clock, m, s := residentFixture(t)
			m.FreeMemoryGB = 8 // Driver alone fits; driver plus cold engine does not.
			check(t, q, m, nil)
			j := submit(t, q, s)
			if r := check(t, q, m, nil); r.Job != nil || len(q.s.ModelSlots) != 0 {
				t.Fatal("admitted driver without room for engine")
			}
			m.FreeMemoryGB = 32
			r := check(t, q, m, nil)
			if r.Job == nil || r.Job.ID != j.ID {
				t.Fatal("driver did not start when both allocations fit")
			}
			j = r.Job
			clock.Advance(4 * time.Second)
			slot := residentReport(t, q, m, j, "ready")
			finish(t, q, j, true)
			production := spec(priority)
			production.Needs.MemoryGB = 32
			production.Needs.HostMemoryGB = 1
			p := submit(t, q, production)
			if slot.State != "draining" || q.modelMemory(m.ID) != 8 {
				t.Fatal("production pressure did not drain engine with custody")
			}
			j.State = "succeeded"
			residentReport(t, q, m, j, "unloaded")
			if got := job(t, q, p.ID); got.State != "running" && check(t, q, m, nil).Job == nil {
				t.Fatal("production did not start after unload acknowledgement")
			}
		})
	}
}

func TestResidentJobsPendingDemandAndEphemeralRefusal(t *testing.T) {
	q, clock, m, s := residentFixture(t)
	j := residentAssigned(t, q, m, s)
	clock.Advance(time.Second)
	slot := residentReport(t, q, m, j, "ready")
	finish(t, q, j, true)
	waiting := submit(t, q, s)
	clock.Advance(time.Hour)
	// No heartbeat is required to prove the idle-decision contract itself.
	if q.modelIdleExpired(slot) {
		t.Fatal("queued same-stage demand lost its warm engine")
	}
	q.s.Jobs[waiting.ID].State = "cancelled"
	if !q.modelIdleExpired(slot) {
		t.Fatal("cancelled demand held memory indefinitely")
	}
	s.WorkerData = "ephemeral"
	s.Outputs = map[string]string{"result.json": "results"}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("resident engine escaped ephemeral cleanup contract")
	}
}

func TestResidentJobsExtensionAndQualificationBinding(t *testing.T) {
	q, _, m, _ := residentFixture(t)
	e := fakeExtension(t)
	e.Resumable = false
	e.Resident = &ResidentBinding{Model: "test"}
	e.Files["extension.yaml"], _ = yaml.Marshal(e)
	e, err := extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "sample-stage", Project: "test", Priority: "experiment", Kind: "sample"}
	j := submit(t, q, s)
	if j.Spec.Resident == nil || j.Spec.Resident.Engine == nil || j.Spec.Resident.Model != "test" {
		t.Fatal("registered extension did not freeze its engine")
	}
	// Qualification is a normal independent check attempt. It must be able to
	// exercise the same client/server contract before useful work is eligible.
	probe := Spec{Name: "extension-check", Project: "extension-check", Priority: "experiment", Extension: extensionKey(e), ExtensionDigest: e.Digest, AllowedMachines: []string{m.ID}, ExtensionCheck: &ExtensionCheckRequest{Work: "kind:sample", Directory: t.TempDir()}}
	check := submit(t, q, probe)
	if check.Spec.Resident == nil || check.Spec.Resident.Engine == nil || check.Spec.Resident.Engine.Model.Name == j.Spec.Resident.Engine.Model.Name {
		t.Fatal("qualification must inherit the engine with independent stage custody")
	}
	s.Resident = &ResidentBinding{Model: "conflicting"}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("extension resident contract was overridden")
	}
}

func TestResidentAutoCheckFitsAlongsideColdEngine(t *testing.T) {
	q, clock, m, _ := residentFixture(t)
	model := q.models.Models[0]
	model.MemoryGB, model.HostMemoryGB = 24, 16
	if err := q.ConfigureModels(ModelConfig{Models: []Model{model}}); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	m.CapabilityProfile.LogicalCPUs, m.CapabilityProfile.SystemMemoryBytes = 16, 31<<30
	m.TotalMemoryGB, m.FreeMemoryGB = 30, 30
	// Leave 2 GiB after the cold engine's host charge so the probe's own
	// minimum cannot mask the accelerator-memory rejection below.
	const hostRoomWithProbeGB = 18.0
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 24, hostRoomWithProbeGB
	check(t, q, m, nil)

	e := fakeExtension(t)
	e.Resumable, e.Resident = false, &ResidentBinding{Model: model.Name}
	e.Files["extension.yaml"], _ = yaml.Marshal(e)
	e, err := extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m.ExtensionFingerprint = "native-test"
	r := newExtensionReport(e, &m, "kind:sample")
	r.Setup = true
	m.ExtensionReports = []ExtensionReport{r}
	parent := submit(t, q, Spec{Name: "resident fan-out", Project: "test", Kind: "sample",
		Workload: &Workload{InputShape: "tiny", Units: 1},
		FanOut:   &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "first"}, {Key: "second"}}}})
	offers := check(t, q, m, nil).ExtensionQualificationPlan
	if len(offers) != 1 || offers[0].Report.Work != parent.Spec.workType() {
		t.Fatalf("expected one native check offer, got %+v", offers)
	}
	report := offers[0].Report
	queued := submit(t, q, Spec{Name: "extension-check", Project: "extension-check", Priority: "experiment",
		Extension: extensionKey(e), ExtensionDigest: e.Digest, AllowedMachines: []string{m.ID},
		SubmissionKey:  report.CheckID,
		Needs:          ResourceNeeds{Memory: "auto"},
		ExtensionCheck: &ExtensionCheckRequest{Work: report.Work, Directory: t.TempDir(), Automatic: &report}})
	m.FreeMemoryGB = 23
	clock.Advance(time.Second)
	if run := check(t, q, m, nil).Job; run != nil {
		t.Fatal("check admitted without room for its engine")
	}
	drainAdmissionWait(t, q)
	wait := job(t, q, queued.ID)
	if !strings.Contains(wait.Reason, "24.000 GiB accelerator memory; 23.000 GiB available") {
		t.Fatalf("wrong wait reason: %s", wait.Reason)
	}
	if wait.AdmissionWait == nil || wait.AdmissionWait.Latest == nil || !slices.ContainsFunc(wait.AdmissionWait.Latest.Targets, func(target AdmissionWaitTarget) bool {
		return target.AdmissionDecision != nil && target.AdmissionDecision.Predicate == predicateResidentDeviceMemory &&
			target.RequiredDeviceGB == 24 && target.AvailableDeviceGB == 23
	}) {
		t.Fatalf("missing device rejection and combined demand: %+v", wait.AdmissionWait)
	}
	m.FreeMemoryGB = 30
	m.HostAvailableMemoryGB = 13.071
	clock.Advance(time.Second)
	if run := check(t, q, m, nil).Job; run != nil || len(q.s.ModelSlots) != 0 {
		t.Fatal("check or engine admitted without sufficient host RAM")
	}
	ram, err := q.jobSnapshot(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ram.Hold == nil || ram.Hold.Kind != "native_qualification" ||
		!strings.Contains(ram.Reason, ShortID(queued.ID)) ||
		!strings.Contains(ram.Reason, "13.071 GiB available; 16.000 GiB for the engine and launch headroom") {
		t.Fatalf("parent's RAM hold lost the check's admission reason: %+v (%s)", ram.Hold, ram.Reason)
	}
	host := hostID(&m)
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain", DrainOwner: "test-maintenance"}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	check(t, q, m, nil)
	drained, err := q.jobSnapshot(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if drained.Hold == nil || drained.Hold.Kind != hostMaintenanceJobHold || drained.Hold.Owner != "operation owner" ||
		!strings.Contains(drained.Reason, "waiting for drained host") || strings.Contains(drained.Reason, "host RAM") {
		t.Fatalf("parent's maintenance hold lost the drain owner or used stale RAM evidence: %+v (%s)", drained.Hold, drained.Reason)
	}
	if parked := job(t, q, queued.ID); parked.State != "waiting" || parked.Attempt != 0 {
		t.Fatalf("drain did not preserve the unstarted check: %s attempt %d", parked.State, parked.Attempt)
	}
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "resume", IfDrainOwner: "test-maintenance"}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if run := check(t, q, m, nil).Job; run != nil {
		t.Fatal("ending maintenance did not free host RAM")
	}
	m.HostAvailableMemoryGB = hostRoomWithProbeGB
	clock.Advance(time.Second)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID || run.Attempt != 1 {
		t.Fatalf("cold resident check not assigned: %s", job(t, q, queued.ID).Reason)
	}
	if n := run.Spec.Needs; n.MemoryGB != 6 || n.HostMemoryGB <= 0 || n.HostMemoryGB+model.HostMemoryGB > m.HostAvailableMemoryGB {
		t.Fatalf("probe did not leave space for its engine: %+v", n)
	}
	if sizing := run.MemoryEnvelope.SizingInputs; sizing == nil || sizing.ResidentEngineHostGB != model.HostMemoryGB {
		t.Fatalf("probe did not retain the engine's host charge: %+v", sizing)
	}
	if slot := q.s.ModelSlots[targetSlotKey(&m, run.Spec.Resident.Engine.Model.Name)]; slot == nil || !slotReservesMemory(slot) {
		t.Fatal("engine was not reserved with the check")
	}
}

// Real HTTP process, queue call admission and attempt pipes. Independent
// accepted results stay in SQLite; invalid/stale calls cannot affect siblings.
func TestResidentJobsRealCPUProcessAndAttemptBridge(t *testing.T) {
	q, clock, m, s := residentFixture(t)
	model := serverTestModel(t, "ready")
	model.Name, model.Device, model.DeviceEnv = "test", "", nil
	model.SizeBytes, model.SHA256 = 1, strings.Repeat("0", 64)
	if err := q.ConfigureModels(ModelConfig{Models: []Model{model}}); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	storeRoot := t.TempDir()
	if err := q.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{"input": putPieceTestFile(t, q, "managed input")}
	s.Outputs = map[string]string{"out": "results"}
	first := residentAssigned(t, q, m, s)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{s.Project: {Weight: 1, MaxConcurrent: 1, MaxConcurrentExperiments: 1}}); err != nil {
		t.Fatal(err)
	}
	frozen := first.Spec.Resident.Engine.Model
	engine := NewModelEngine(frozen, t.TempDir(), RealClock{})
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := engine.Load(ctx, frozen); err != nil {
		t.Fatal(err)
	}
	defer engine.Unload()
	pid := engine.(*LlamaEngine).cmd.Process.Pid
	slot := q.s.ModelSlots[targetSlotKey(&m, frozen.Name)]
	rt := &modelRuntime{models: map[string]*workerModel{frozen.Name: {model: frozen, engine: engine, state: "ready", attempt: slot.Attempt}}, session: m.Session, token: "test"}
	worker := workerTestServer(http.HandlerFunc(rt.serve))
	defer worker.Close()
	rt.server = worker.Config
	m.ModelURL = worker.URL
	clock.Advance(time.Second)
	residentReport(t, q, m, first, "ready")
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	w := Worker{Client: Client{URL: api.URL, Token: "test"}, Clock: clock}
	endpoint, expectedStatus, cancelBefore := "/files/a%3Fb%2F%25?x=1", 200, false
	invoke := func(j *Job, payload string) string {
		t.Helper()
		if err := q.Started(result(j, true)); err != nil {
			t.Fatal(err)
		}
		// Pipe names also work under arbitrarily long managed attempt roots.
		work, err := os.MkdirTemp("", "myce-r-test-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(work)
		env, close, err := w.startResidentBridge(ctx, *j, work)
		if err != nil {
			t.Fatal(err)
		}
		defer close()
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://resident"+endpoint, strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+env["MYCE_RESIDENT_KEY"])
		req.Header.Set("X-Myce-Project", "forged")
		if cancelBefore {
			if _, err := q.CancelJob(j.ID); err != nil {
				t.Fatal(err)
			}
		}
		response, err := ResidentRequest(ctx, env["MYCE_RESIDENT_PIPE"], env["MYCE_RESIDENT_KEY"], req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		if response.StatusCode != expectedStatus {
			t.Fatalf("bridge status=%d: %s", response.StatusCode, data)
		}
		if strings.Contains(string(data), "test|") {
			t.Fatal("fleet token reached engine")
		}
		if expectedStatus == 200 && !strings.Contains(string(data), "/files/a%3Fb%2F%25?x=1|") {
			t.Fatalf("escaped endpoint lost: %s", data)
		}
		return string(data)
	}
	accept := func(j *Job, data string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "out")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Client.UploadFile(ctx, path, ref); err != nil {
			t.Fatal(err)
		}
		r := result(j, true)
		r.OutputRefs = FileManifest{"out": ref}
		if err = q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	dependent := pieceConsumer(s, first.ID, "")
	dependent.Resident = nil
	dependent.Files = nil
	consumer := submit(t, q, dependent)
	if dependencyReady(consumer) {
		t.Fatal("dependent ready before accepted artifact")
	}
	one := invoke(first, "one")
	if !strings.HasSuffix(one, "||one") {
		t.Fatal(one)
	}
	accept(first, one)
	if !dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("accepted resident output did not release dependency")
	}
	if _, err := q.CancelJob(consumer.ID); err != nil {
		t.Fatal(err)
	}
	second := residentAssigned(t, q, m, s)
	two := invoke(second, "two")
	if !strings.HasSuffix(two, "||two") {
		t.Fatal(two)
	}
	if engine.(*LlamaEngine).cmd.Process.Pid != pid || q.modelMemory(m.ID) != model.MemoryGB {
		t.Fatal("second job replaced process or double charged")
	}
	accept(second, two)
	bad := residentAssigned(t, q, m, s)
	endpoint, expectedStatus = "/invalid", 422
	invoke(bad, "bad input")
	finish(t, q, bad, false)
	cancelled := residentAssigned(t, q, m, s)
	expectedStatus, cancelBefore = 409, true
	invoke(cancelled, "cancelled input")
	finish(t, q, cancelled, false)
	dying := residentAssigned(t, q, m, s)
	// The existing model API reports a lost, non-retryable engine as unavailable.
	endpoint, expectedStatus, cancelBefore = "/exit", 503, false
	invoke(dying, "interrupted input")
	finish(t, q, dying, false)
	await(t, func() bool {
		reports, memory := rt.report()
		return len(reports) == 1 && reports[0].State == "failed" && reports[0].RuntimeFailed && memory == 0
	})
	reports, _ := rt.report()
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: reports, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	// Restarted worker reports clean custody only after the real process was reaped.
	previous := m.Session
	if err := rt.close(); err != nil {
		t.Fatal(err)
	}
	m.Session = "replacement-session"
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: previous, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	if job(t, q, first.ID).State != "succeeded" || job(t, q, second.ID).State != "succeeded" || !dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("failed/cancelled/dead engine damaged accepted siblings")
	}

	// Calls are ephemeral; accepted jobs and their frozen dependencies survive reopen.
	api.Close()
	q = reopenCancellationQueue(t, q, clock)
	if err := q.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	api = httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	w.Client.URL = api.URL
	if job(t, q, first.ID).OutputRefs["out"].SHA256 == "" || !dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("restart lost accepted artifacts/dependency")
	}
	stale := residentAttempt{first.ID, first.Machine, first.Session, first.Attempt}
	if q.residentAttemptLive(stale) {
		t.Fatal("completed job retained invocation authority")
	}
	if err := q.Complete(result(first, true)); err != nil && !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if job(t, q, second.ID).State != "succeeded" {
		t.Fatal("stale completion damaged accepted sibling")
	}
}

func TestResidentJobsIdentityIgnoresPerItemMemory(t *testing.T) {
	q, _, _, s := residentFixture(t)
	s.PrepareSource = true
	s.Files = FileManifest{".myce-source.tar": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	a := &Job{Spec: s}
	if err := q.freezeResident(a); err != nil {
		t.Fatal(err)
	}
	s.ProfileMemory = strings.Repeat("b", 64)
	s.Run.Command = append(s.Run.Command, "another-recording")
	s.Files = FileManifest{".myce-source.tar": s.Files[".myce-source.tar"], "input": {SHA256: strings.Repeat("c", 64), SizeBytes: 2}}
	b := &Job{Spec: s}
	if err := q.freezeResident(b); err != nil {
		t.Fatal(err)
	}
	if a.Spec.Resident.Engine.Model.Name != b.Spec.Resident.Engine.Model.Name {
		t.Fatal("per-item arguments/files/memory split the common engine")
	}
	s.Files[".myce-source.tar"] = FileRef{SHA256: strings.Repeat("d", 64), SizeBytes: 1}
	c := &Job{Spec: s}
	if err := q.freezeResident(c); err != nil {
		t.Fatal(err)
	}
	if a.Spec.Resident.Engine.Model.Name == c.Spec.Resident.Engine.Model.Name {
		t.Fatal("changed source reused engine")
	}
}

func TestResidentJobsLocalAliasBudgets(t *testing.T) {
	_, clock := fixture(t)
	m := sharedUnifiedMachine()
	w := sharedWorker(m, clock)
	w.Machine.HostMemoryBudgetGB = 14
	runtime := &modelRuntime{models: map[string]*workerModel{"derived": {model: Model{Name: "derived", MemoryGB: 8, HostMemoryGB: 4}, state: "loading", device: "0"}}}
	reports, device := runtime.report()
	if device != 8 {
		t.Fatal("runtime budget not charged once")
	}
	next := Job{Spec: sharedSpec("experiment", 1), AllocatedDevices: []string{"0"}}
	if err := localAttemptAdmission(w, nil, next, reports); err != nil {
		t.Fatal("fitting alias allocation refused", err)
	}
	w.Machine.HostMemoryBudgetGB = 4
	if err := localAttemptAdmission(w, nil, next, reports); err == nil {
		t.Fatal("resident alias missing from host guard")
	}
	w.Machine.HostMemoryBudgetGB = 14
	next.Spec.Needs.MemoryGB = 33
	if err := localAttemptAdmission(w, nil, next, reports); err == nil || !strings.Contains(err.Error(), "device_budget") {
		t.Fatal("alias missing from card guard", err)
	}
	encoded, _ := json.Marshal(reports)
	var untrusted []ModelReport
	_ = json.Unmarshal(encoded, &untrusted)
	if _, ok := w.localModelBudget(untrusted[0]); ok {
		t.Fatal("wire report supplied trusted local budget")
	}
}

// Runs inside the same kernel boundary as a real stage; pathname sockets are
// still denied, while the managed request/response pipes are usable.
func TestResidentJobsPayloadChild(t *testing.T) {
	if os.Getenv("RESIDENT_PAYLOAD_CHILD") == "" {
		return
	}
	if os.Getenv("MYCE_TOKEN") != "" {
		os.Exit(8)
	}
	req, _ := http.NewRequest("POST", "http://resident/infer", strings.NewReader("sandboxed input"))
	response, err := ResidentRequest(context.Background(), os.Getenv("MYCE_RESIDENT_PIPE"), os.Getenv("MYCE_RESIDENT_KEY"), req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 || string(data) != "accepted response" {
		t.Fatalf("%s %v", data, err)
	}
}
func TestResidentJobsDispatchStaysWorkerBound(t *testing.T) {
	for _, kind := range []string{"stage", "qualification"} {
		t.Run(kind, func(t *testing.T) {
			w := recoveryWorker(t)
			w.ServiceConfig = &ServiceConfig{Config: "test-config", Binary: "test-binary"}
			if !w.detachedAttemptSupported() {
				t.Fatal("fixture must otherwise permit detached dispatch")
			}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			// If dispatch regresses, refuse before invoking a native manager.
			store.state.Custodian = "unproved-custodian"
			j := recoveryJob(t, []string{"true"})
			j.Spec.Resident = &ResidentBinding{Model: "test"}
			if kind == "qualification" {
				j.Spec.ExtensionCheck = &ExtensionCheckRequest{Work: "kind:sample"}
			}
			// The worker's ordinary device admission must run, rather than
			// handing the resident job to an isolated custodian.
			p, err := w.startAssignedAttempt(context.Background(), store, j)
			if p != nil || err == nil || !strings.Contains(err.Error(), "assigned device is not in the machine inventory") {
				t.Fatalf("resident dispatch bypassed worker admission: process=%v error=%v", p != nil, err)
			}
		})
	}
}

func TestResidentJobsSandboxedPayload(t *testing.T) {
	w := recoveryWorker(t)
	api := httptest.NewServer(workerTestHandler(http.HandlerFunc(func(out http.ResponseWriter, in *http.Request) {
		if strings.HasPrefix(in.URL.Path, "/api/workers/resident/") {
			if in.Header.Get("Authorization") != "Bearer worker-private" || in.Header.Get("X-Myce-Job-Attempt") != "1" {
				t.Error("bridge lost attempt authorization")
			}
			data, _ := io.ReadAll(in.Body)
			if string(data) != "sandboxed input" {
				t.Error("request body changed")
			}
			out.Write([]byte("accepted response"))
			return
		}
		out.Header().Set("Content-Type", "application/json")
		out.Write([]byte(`{"ok":true,"start_gate":"` + startGateProtocol + `"}`))
	})))
	defer api.Close()
	w.Client = Client{URL: api.URL, Token: "worker-private"}
	exe, _ := os.Executable()
	j := recoveryJob(t, []string{exe, "-test.run=^TestResidentJobsPayloadChild$"})
	j.Spec.Isolation = "sandboxed"
	j.Spec.Resumable = false
	j.Spec.Run.Env = map[string]string{"RESIDENT_PAYLOAD_CHILD": "1", "GORACE": "atexit_sleep_ms=0"}
	j.Spec.Resident = &ResidentBinding{Model: "test", Engine: &ResidentEngine{Model: Model{Name: "derived"}}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	var p *process
	await(t, func() bool { p, err = w.start(store, j); return !errors.Is(err, errWorkerCheckpoint) })
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(w.Clock.Now(), 0, true)
	select {
	case finished := <-p.done:
		if finished.err != nil || !finished.result.Success {
			data, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
			t.Fatalf("payload: %+v %s", finished, data)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("sandboxed resident payload stalled")
	}
}

func TestResidentJobsFrozenEnvironmentRetention(t *testing.T) {
	q, _, m, s := residentFixture(t)
	recipe := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"engine-env": {"fake": recipe}}}); err != nil {
		t.Fatal(err)
	}
	config := q.models
	config.Models[0].Environment = "engine-env"
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	j := residentAssigned(t, q, m, s)
	plan := q.modelPlan(m.ID)
	if len(plan) != 1 || plan[0].EnvironmentRecipe == nil || plan[0].EnvironmentRecipe.Key != recipe.Key {
		t.Fatal("load plan lost frozen recipe")
	}
	// The original catalog can change while the frozen job remains live.
	if err := q.ConfigureModels(ModelConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{}); err != nil {
		t.Fatal(err)
	}
	if !q.environmentReferenced(context.Background(), recipe.Key) {
		t.Fatal("live resident environment was prunable")
	}
	model, ok := q.model(j.Spec.Resident.Engine.Model.Name)
	if !ok || model.Environment != "engine-env" {
		t.Fatal("catalog replacement lost frozen model")
	}
	if err := q.Started(result(j, true)); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	slot := residentReport(t, q, m, j, "ready")
	slot.State = "draining" // A memory-pressure reclaim between client calls.
	residentReport(t, q, m, j, "unloaded")
	call := &ModelCall{Model: model.Name, Project: j.Spec.Project, Priority: j.Spec.Priority, ResidentAttempt: &residentAttempt{j.ID, j.Machine, j.Session, j.Attempt}}
	if err := q.addCall(call); err != nil {
		t.Fatal(err)
	}
	plan = q.modelPlan(m.ID)
	if len(plan) != 1 || plan[0].Resident == nil || plan[0].Resident.Model.Name != model.Name || plan[0].EnvironmentRecipe == nil || plan[0].EnvironmentRecipe.Key != recipe.Key {
		t.Fatal("client call after reclaim lost its frozen engine/environment", plan)
	}
}
