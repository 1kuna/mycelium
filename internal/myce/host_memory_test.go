package myce

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestHostMemorySharedWorkers(t *testing.T) {
	q, c := fixture(t)
	a, b := machine("card0", "fake"), machine("card1", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "box"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	first := submit(t, q, s)
	c.Advance(time.Second)
	second := submit(t, q, s)
	r := check(t, q, a, nil)
	if r.Job == nil || r.Job.ID != first.ID {
		t.Fatal("first not assigned")
	}
	c.Advance(time.Second)
	if r := check(t, q, b, nil); r.Job != nil {
		t.Fatal("second GPU spent same host RAM")
	}
	if !strings.Contains(job(t, q, second.ID).Reason, "RAM") {
		t.Fatal("missing RAM wait reason")
	}
	finish(t, q, r.Job, true)
	if r := check(t, q, b, nil); r.Job == nil {
		t.Fatal("completed reservation not released")
	}
}
func TestHostMemoryProductionReclaimsOtherCard(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("card0", "fake"), machine("card1", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "box"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{a.ID}
	submit(t, q, s)
	running := check(t, q, a, nil).Job
	s.Priority = "production"
	s.AllowedMachines = []string{b.ID}
	prod := submit(t, q, s)
	requireState(t, q, running.ID, "stopping")
	if r := check(t, q, b, nil); r.Job != nil {
		t.Fatal("RAM released before stop acknowledgement")
	}
	if r := check(t, q, a, running); !r.Stop {
		t.Fatal("no stop")
	}
	if err := q.Complete(Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	if r := check(t, q, b, nil); r.Job == nil || r.Job.ID != prod.ID {
		t.Fatal("production not admitted")
	}
}
func TestHostMemoryUnifiedPool(t *testing.T) {
	q, _ := fixture(t)
	m := machine("mac", "fake")
	m.FreeMemoryGB = 32
	m.HostID = "mac"
	m.UnifiedMemory = true
	m.CapabilityProfile.MemoryLayout = "unified"
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 12
	check(t, q, m, nil)
	s := spec("experiment")
	s.Needs.MemoryGB = 8
	s.Needs.HostMemoryGB = 5
	j := submit(t, q, s)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("GPU and CPU allocations exceed common pool")
	}
	if !strings.Contains(job(t, q, j.ID).Reason, "RAM") {
		t.Fatal("missing RAM reason")
	}
}
func TestHostMemoryReaderReserve(t *testing.T) {
	source, err := newHostMemorySource(2, "linux", func(string) ([]byte, error) { return []byte("MemAvailable: 12582912 kB\n"), nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	n, err := source(context.Background())
	if err != nil || n != 10 {
		t.Fatalf("%v %v", n, err)
	}
}

func TestHostMemoryModelReservationsAndPins(t *testing.T) {
	for _, pin := range []bool{false, true} {
		t.Run(fmt.Sprint(pin), func(t *testing.T) {
			q, _ := fixture(t)
			model := Model{Name: "speech", Engine: "fake", Path: "weights", Platforms: []string{"fake"}, MemoryGB: 2, HostMemoryGB: 10}
			if err := q.ConfigureModels(ModelConfig{Models: []Model{model}, Idle: time.Hour, Wait: time.Minute}); err != nil {
				t.Fatal(err)
			}
			a, b := machine("a", "fake"), machine("b", "fake")
			for _, m := range []*Machine{&a, &b} {
				m.HostID = "host"
				m.HostMemoryMeasured = true
				m.HostAvailableMemoryGB = 12
				m.ModelURL = "http://worker"
				m.ModelConfigHash = modelConfigHash(q.models)
			}
			check(t, q, a, nil)
			check(t, q, b, nil)
			if pin {
				if err := q.Pin(ModelPin{Model: "speech", Project: "test", Machine: a.ID}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := q.addCall(&ModelCall{Model: "speech", Project: "test", Priority: "experiment"}); err != nil {
					t.Fatal(err)
				}
			}
			s := spec("experiment")
			s.Needs.HostMemoryGB = 4
			s.AllowedMachines = []string{b.ID}
			submit(t, q, s)
			if r := check(t, q, b, nil); r.Job != nil {
				t.Fatal("batch ignored server RAM")
			}
		})
	}
}
func TestHostMemoryWorkerRefusesLatePressure(t *testing.T) {
	w := Worker{HostMemorySource: func(context.Context) (float64, error) { return 3, nil }, Machine: Machine{}}
	j := Job{Spec: spec("experiment")}
	j.Spec.Needs.HostMemoryGB = 10
	if _, err := w.start(nil, j); err == nil || !strings.Contains(err.Error(), "RAM") {
		t.Fatalf("%v", err)
	}
}

func TestHostMemoryFailedTelemetryAndLegacy(t *testing.T) {
	q, _ := fixture(t)
	m := machine("legacy", "fake")
	check(t, q, m, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 1
	submit(t, q, s)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("host request admitted without telemetry")
	}
	s.Needs.HostMemoryGB = 0
	j := submit(t, q, s)
	if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("legacy behavior changed")
	}
}
func TestHostMemoryRestartReconciliation(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "host"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{"a"}
	submit(t, q, s)
	running := check(t, q, a, nil).Job
	a.Session = "new-session"
	r, err := q.Checkin(Checkin{Machine: a, Fresh: true, PreviousSession: running.Session})
	if err != nil || r.Job == nil {
		t.Fatalf("%+v %v", r, err)
	}
	if r.Job.Attempt <= running.Attempt {
		t.Fatal("stale attempt reused")
	}
	s.AllowedMachines = []string{"b"}
	submit(t, q, s)
	if check(t, q, b, nil).Job != nil {
		t.Fatal("restart duplicated allowance")
	}
	finish(t, q, r.Job, true)
	if check(t, q, b, nil).Job == nil {
		t.Fatal("reconciled reservation retained twice")
	}
}
func TestHostMemoryModelProductionReclaim(t *testing.T) {
	q, _ := fixture(t)
	model := Model{Name: "speech", Engine: "fake", Path: "weights", Platforms: []string{"fake"}, MemoryGB: 2, HostMemoryGB: 10}
	if err := q.ConfigureModels(ModelConfig{Models: []Model{model}, Idle: time.Hour, Wait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	a, b := machine("a", "fake"), machine("b", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "host"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
		m.ModelURL = "http://worker"
		m.ModelConfigHash = modelConfigHash(q.models)
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{"a"}
	submit(t, q, s)
	running := check(t, q, a, nil).Job
	if err := q.addCall(&ModelCall{Model: "speech", Project: "test", Priority: "production", Exclude: "a"}); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, running.ID, "stopping")
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range snap.Jobs {
		if j.Spec.Name == "model call: speech" && !strings.Contains(j.Reason, "RAM") {
			t.Fatalf("model RAM wait hidden: %s", j.Reason)
		}
	}
}

func TestHostMemoryUnifiedDoesNotChargeGPUtwice(t *testing.T) {
	q, _ := fixture(t)
	m := machine("mac", "fake")
	m.FreeMemoryGB = 12
	m.HostID = "mac"
	m.UnifiedMemory = true
	m.CapabilityProfile.MemoryLayout = "unified"
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 12
	check(t, q, m, nil)
	s := spec("experiment")
	s.Needs.MemoryGB = 8
	s.Needs.HostMemoryGB = 2
	submit(t, q, s)
	if r := check(t, q, m, nil); r.Job == nil {
		t.Fatal("8 GPU + 2 CPU should fit 12 GiB unified pool")
	}
}
func TestHostMemoryLossReleasesReservation(t *testing.T) {
	q, c := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	other := machine("other", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "host"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	check(t, q, other, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{"a"}
	submit(t, q, s)
	running := check(t, q, a, nil).Job
	s.AllowedMachines = []string{"b"}
	second := submit(t, q, s)
	var r Reply
	for range 31 {
		c.Advance(2 * time.Second)
		check(t, q, other, nil)
		r = check(t, q, b, nil)
	}
	if r.Job == nil || r.Job.ID != second.ID {
		t.Fatal("lost worker RAM reservation not reconciled")
	}
	requireState(t, q, running.ID, "queued")
}
func TestHostMemoryProductionUsesFreeHostBeforeReclaim(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = m.ID
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{"a"}
	submit(t, q, s)
	running := check(t, q, a, nil).Job
	s.Priority = "production"
	s.AllowedMachines = nil
	submit(t, q, s)
	requireState(t, q, running.ID, "running")
	if check(t, q, b, nil).Job == nil {
		t.Fatal("free physical host not used")
	}
}

// Running includes accepted preparation before Started; Docker's declared host
// total already includes its simultaneous main/sidecar ceilings. Both retain
// their whole charge until the exact execution releases.
func TestHostMemoryAllowanceAggregatePreparationsAndSidecars(t *testing.T) {
	for _, docker := range []bool{false, true} {
		t.Run(fmt.Sprint(docker), func(t *testing.T) {
			q, clock := fixture(t)
			a, b := machine("card-a", "fake"), machine("card-b", "fake")
			for _, m := range []*Machine{&a, &b} {
				if docker {
					m.Capabilities = []string{"docker"}
					m.CapabilityProfile.OS = "linux"
				}
				m.HostID = "host"
				m.HostMemoryMeasured = true
				m.HostAvailableMemoryGB = 64
				m.HostMemoryBudgetGB = 6
				m.MemoryCeilingSlackGB = .25
			}
			check(t, q, a, nil)
			check(t, q, b, nil)
			first := spec("experiment")
			if docker {
				first = dockerGroupJob(t).Spec
			}
			first.Needs.HostMemoryGB = 6
			first.AllowedMachines = []string{a.ID}
			submitted := submit(t, q, first)
			preparing := check(t, q, a, nil).Job
			if preparing != nil {
				t.Fatal("slack exceeded host allowance")
			}
			for _, m := range []*Machine{&a, &b} {
				m.HostMemoryBudgetGB = 7
			}
			check(t, q, b, nil)
			preparing = check(t, q, a, nil).Job
			if preparing == nil || preparing.ID != submitted.ID || preparing.StartedAt != nil {
				t.Fatal("missing accepted preparation", preparing)
			}
			second := spec("experiment")
			second.Needs.HostMemoryGB = 1
			second.AllowedMachines = []string{b.ID}
			submit(t, q, second)
			if check(t, q, b, nil).Job != nil {
				t.Fatal("preparation/sidecars double-spent sibling allowance")
			}
			snap, err := q.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range snap.Machines {
				if m.HostMemoryInUseGB != 6.25 {
					t.Fatal("missing whole preparation/sidecar charge", m.HostMemoryInUseGB)
				}
			}
			cancelAck(t, q, preparing.ID)
			receipt := result(preparing, false)
			receipt.ReleasePending = true
			receipt.Interrupted = true
			if err := q.Complete(receipt); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				clock.Advance(24 * time.Hour)
				check(t, q, b, nil)
			}
			snap, err = q.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range snap.Machines {
				if m.HostMemoryInUseGB != 6.25 {
					t.Fatal("unconfirmed release refunded", m.HostMemoryInUseGB)
				}
			}
			receipt.ReleasePending = false
			receipt.ReleaseOnly = true
			if err := q.Complete(receipt); err != nil {
				t.Fatal(err)
			}
			if check(t, q, b, nil).Job == nil {
				t.Fatal("exact release did not refund allowance")
			}
		})
	}
}
