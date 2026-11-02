package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// MY-30 (#105): memory_gb alone decides the cards. Two 8 GiB fake cards hold
// a 12 GiB job only together; an 8 GiB job keeps one card.
func bigSpec(priority string, gb float64) Spec {
	s := spec(priority)
	s.Needs.MemoryGB = gb
	return s
}

func TestMultiCardJobReservesBindsAndReleasesBothCards(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	big := submit(t, q, bigSpec("experiment", 12))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != big.ID || !slices.Equal(a.AllocatedDevices, []string{"0", "1"}) || !slices.Equal(a.Copies[len(a.Copies)-1].AllocatedDevices, []string{"0", "1"}) {
		t.Fatal("12 GiB job did not get both 8 GiB cards", a)
	}
	for _, card := range q.machineTargets(q.s.Machines[m.ID])[1:] {
		if card.FreeMemoryGB != 2 {
			t.Fatalf("card %s free %g, want an equal 6 GiB share charged", card.Device, card.FreeMemoryGB)
		}
	}
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range snapshot.Machines {
		for _, card := range status.Devices {
			if status.ID == m.ID && (len(card.Attempts) != 1 || len(card.Releases) != 1 || card.Releases[0].Job != big.ID) {
				t.Fatal("device status does not show the group's attempt and release on every card", card.ID, card.Attempts, card.Releases)
			}
		}
	}
	small := submit(t, q, spec("experiment"))
	if got := assignedJob(machineCheck(t, q, m, a)); got != nil {
		t.Fatal("a card of the running group admitted another batch", got.ID)
	}
	cpu := submit(t, q, cpuSpec(1, 1))
	c := assignedJob(machineCheck(t, q, m, a))
	if c == nil || c.ID != cpu.ID {
		t.Fatal("CPU work beside the group", c)
	}
	if err := q.Complete(result(c, true)); err != nil {
		t.Fatal(err)
	}

	// The worker binds every card: both visible, counted and verified.
	w := Worker{Machine: m3Machine()}
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return m3Machine().Devices, nil }
	bound, err := w.forAttempt(*a)
	if err != nil {
		t.Fatal(err)
	}
	if free, err := bound.MemorySource(context.Background()); err != nil || free != 16 {
		t.Fatal("launch memory check does not see both cards' room", free, err)
	}
	env := placementEnvironment(bound.Machine)
	if env["MYCE_GPU_COUNT"] != "2" || env["MYCE_DEVICES"] != "0,1" {
		t.Fatal("worker did not expose both cards", env)
	}
	if one, _ := w.forAttempt(Job{AllocatedDevices: []string{"1"}}); placementEnvironment(one.Machine)["MYCE_GPU_COUNT"] != "1" {
		t.Fatal("one-card attempt count")
	}
	cards := []MachineDevice{{Selector: "GPU-a"}, {Selector: "GPU-b"}}
	if cuda, _ := cardsEnvironment("cuda", cards); cuda["CUDA_VISIBLE_DEVICES"] != "GPU-a,GPU-b" || cuda["ONEAPI_DEVICE_SELECTOR"] != "!*:*" {
		t.Fatal("CUDA mask", cuda)
	}
	cards = []MachineDevice{{Selector: "3"}, {Selector: "5"}}
	if xpu, _ := cardsEnvironment("xpu", cards); xpu["ZE_AFFINITY_MASK"] != "3,5" || xpu["ONEAPI_DEVICE_SELECTOR"] != "level_zero:0,1" || xpu["CUDA_VISIBLE_DEVICES"] != "" {
		t.Fatal("XPU mask", xpu)
	}
	missing := Worker{Machine: m3Machine()}
	missing.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return m3Machine().Devices[:1], nil }
	if _, err := missing.forAttempt(*a); err == nil || !strings.Contains(err.Error(), "matches saved identity") {
		t.Fatal("worker launched with one of two assigned cards gone", err)
	}

	if err := q.Complete(result(a, true)); err != nil {
		t.Fatal(err)
	}
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != small.ID || !slices.Equal(first.AllocatedDevices, []string{"0"}) {
		t.Fatal("released cards did not admit one-card work", first)
	}
	next := submit(t, q, spec("experiment"))
	if second := assignedJob(machineCheck(t, q, m, first)); second == nil || second.ID != next.ID || !slices.Equal(second.AllocatedDevices, []string{"1"}) {
		t.Fatal("second card stayed reserved after the group's release", second)
	}
}

func TestMultiCardProductionPreemptsBothExperimentsAndWaitsForBothReleases(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	clock.Advance(time.Second)
	submit(t, q, spec("experiment"))
	b := assignedJob(machineCheck(t, q, m, a))
	if a == nil || b == nil || !slices.Equal(append(a.AllocatedDevices, b.AllocatedDevices...), []string{"0", "1"}) {
		t.Fatal("experiments did not fill both cards", a, b)
	}
	production := submit(t, q, bigSpec("production", 12))
	for _, victim := range []*Job{a, b} {
		got := job(t, q, victim.ID)
		if got.State != "stopping" || priorityStop(got) == nil || priorityStop(got).Demand != production.ID {
			t.Fatal("production did not stop the experiment on its card", got)
		}
	}
	ack := result(a, false)
	ack.ReleaseOnly = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m, b)); got != nil {
		t.Fatal("a card freed for production was taken before both released", got.ID, got.AllocatedDevices)
	}
	ack = result(b, false)
	ack.ReleaseOnly = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	got := assignedJob(machineCheck(t, q, m))
	if got == nil || got.ID != production.ID || !slices.Equal(got.AllocatedDevices, []string{"0", "1"}) {
		t.Fatal("production did not start on both released cards", got)
	}
}

func TestMultiCardPlacementFollowsMemory(t *testing.T) {
	q, _ := fixture(t)
	single := cpuMachine("single", "single", "fake", 8, 32)
	single.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-s", Fingerprint: "fake", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	dual := m3Machine()
	machineCheck(t, q, single)
	machineCheck(t, q, dual)

	big := submit(t, q, bigSpec("experiment", 12))
	if got := assignedJob(machineCheck(t, q, single)); got != nil {
		t.Fatal("single-card host took a job no single card holds", got.ID)
	}
	got := assignedJob(machineCheck(t, q, dual))
	if got == nil || got.ID != big.ID || !slices.Equal(got.AllocatedDevices, []string{"0", "1"}) {
		t.Fatal("two-card host did not hold the job across both cards", got)
	}
	if err := q.Complete(result(got, true)); err != nil {
		t.Fatal(err)
	}
	fits := submit(t, q, spec("experiment"))
	if got := assignedJob(machineCheck(t, q, dual)); got == nil || got.ID != fits.ID || len(got.AllocatedDevices) != 1 {
		t.Fatal("a job that fits one card did not get exactly one card", got)
	}

	tooBig := bigSpec("experiment", 20)
	tooBig.RequireTarget = true
	_, err := q.Submit(tooBig)
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Code != "no_qualified_target" || !strings.Contains(refusal.Message, "across all 2 cards of this host; one job runs on one host") {
		t.Fatal("a job larger than any host was not refused clearly", err)
	}
}

// With mixed card sizes, work the largest card holds never spreads, even when
// the smaller cards have the most free memory.
func TestMultiCardMixedSizesKeepWorkOnOneCardThatHoldsIt(t *testing.T) {
	cards := []*Machine{
		{ID: "mixed", GPU: "fake", Device: "0", TotalMemoryGB: 24, FreeMemoryGB: 4},
		{ID: "mixed", GPU: "fake", Device: "1", TotalMemoryGB: 8, FreeMemoryGB: 8},
		{ID: "mixed", GPU: "fake", Device: "2", TotalMemoryGB: 8, FreeMemoryGB: 8},
	}
	twenty := &Job{Spec: bigSpec("production", 20)}
	larger := &Job{Spec: bigSpec("production", 25)}
	for _, g := range groupTargets(cards) {
		if groupFits(twenty, g) {
			t.Fatalf("20 GiB job one 24 GiB card holds may spread across %s", g.Device)
		}
		if len(g.groupMembers) == 3 && !groupFits(larger, g) {
			t.Fatal("work no fewer cards hold cannot use all three", g.Device)
		}
	}
}

// One card free is no partial reservation, and restart keeps both cards' custody.
func TestMultiCardNoPartialReservationAcrossRaceOrRestart(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	holder := submit(t, q, spec("production"))
	h := assignedJob(machineCheck(t, q, m))
	if h == nil || h.ID != holder.ID {
		t.Fatal("holder", h)
	}
	big := submit(t, q, bigSpec("experiment", 12))
	clock.Advance(time.Second)
	small := submit(t, q, spec("experiment"))
	got := assignedJob(machineCheck(t, q, m, h))
	if got == nil || got.ID != small.ID || !slices.Equal(got.AllocatedDevices, []string{"1"}) {
		t.Fatal("waiting group job held the free card", got)
	}
	requireState(t, q, big.ID, "queued")
	if err := q.Complete(result(h, true)); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(result(got, true)); err != nil {
		t.Fatal(err)
	}
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != big.ID {
		t.Fatal("group job after both cards freed", a)
	}

	// The worker refuses an attempt whose card another local attempt holds.
	w := Worker{Machine: m3Machine(), Clock: clock}
	other := Job{AllocatedDevices: []string{"1"}, Spec: spec("experiment")}
	if err := localAttemptAdmission(w, []Job{other}, *a, nil); err == nil || !strings.Contains(err.Error(), "local_device_reservation") {
		t.Fatal("worker admitted a group over a held card", err)
	}

	q = reopenTierQueue(t, q, clock)
	running := job(t, q, big.ID)
	if running.State != "running" || !slices.Equal(running.AllocatedDevices, []string{"0", "1"}) {
		t.Fatal("restart lost the group's custody", running)
	}
	submit(t, q, spec("experiment"))
	if got := assignedJob(machineCheck(t, q, m, running)); got != nil {
		t.Fatal("restart freed a card of the running group", got.ID, got.AllocatedDevices)
	}
}

// The real API, worker and command runner launch one command that sees both cards.
func TestMultiCardDevLaunchSeesBothCards(t *testing.T) {
	root := t.TempDir()
	cfg := DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "local-test", Workers: 1, Devices: 2, HostID: "two-card", MemoryGB: 8, Interval: 10 * time.Millisecond}
	d, err := StartDev(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := bigSpec("experiment", 12)
	s.Run.Command = []string{"sh", "-c", `printf '%s %s' "$MYCE_GPU_COUNT" "$MYCE_DEVICES" > "$MYCE_CHECKPOINT_DIR/cards"`}
	var j Job
	if err := d.Client().Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		if err := d.Client().Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &j); err != nil {
			t.Fatal(err)
		}
		return terminal(j.State)
	})
	seen, _ := os.ReadFile(filepath.Join(cfg.Checkpoints, j.ID, fmt.Sprintf("attempt-%d", j.Attempt), "cards"))
	if j.State != "succeeded" || !slices.Equal(j.AllocatedDevices, []string{"0", "1"}) || string(seen) != "2 0,1" {
		t.Fatalf("state=%s devices=%v saw=%q reason=%s", j.State, j.AllocatedDevices, seen, j.Reason)
	}
}

// Review regression: a deadline demand (no start deadline) stops both cards,
// then one card is disabled. The other card must return to other work.
func TestMultiCardHoldEndsWhenHostCanNoLongerHoldDemand(t *testing.T) {
	for _, loss := range []string{"disabled", "quarantined", "missing"} {
		t.Run(loss, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			submit(t, q, spec("experiment"))
			a := assignedJob(machineCheck(t, q, m))
			clock.Advance(time.Second)
			submit(t, q, spec("experiment"))
			b := assignedJob(machineCheck(t, q, m, a))
			demand := submit(t, q, bigSpec("deadline", 12))
			if got := job(t, q, b.ID); got.State != "stopping" || priorityStop(got).Demand != demand.ID {
				t.Fatal("deadline demand did not stop both cards", got)
			}
			ack := result(a, false)
			ack.ReleaseOnly = true
			if err := q.Complete(ack); err != nil {
				t.Fatal(err)
			}
			if got := assignedJob(machineCheck(t, q, m, b)); got != nil {
				t.Fatal("card 0 not held while the demand can still use both cards", got.ID)
			}
			switch loss {
			case "disabled":
				rev := uint64(0)
				if _, err := q.ChangeDeviceMaintenance(hostID(&m), DeviceMaintenanceRequest{Action: "disable", Platform: "fake", Identity: "fake-1", IfRevision: &rev}); err != nil {
					t.Fatal(err)
				}
			case "quarantined":
				m.Devices = slices.Clone(m.Devices)
				m.Devices[1].Reason, m.Devices[1].FreeMemoryGB = "device_fault_xe", 0
			case "missing":
				m.Devices = slices.Clone(m.Devices[:1])
			}
			ack = result(b, false)
			ack.ReleaseOnly = true
			if err := q.Complete(ack); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			got := assignedJob(machineCheck(t, q, m))
			if got == nil || got.ID != a.ID && got.ID != b.ID || !slices.Equal(got.AllocatedDevices, []string{"0"}) {
				t.Fatal("card 0 stayed idle after the host could no longer hold the demand", got)
			}
			requireState(t, q, demand.ID, "queued")
		})
	}
}

// Planner cost of card groups (#105): four two-card hosts, every card held by
// production, 2,000 queued experiments (10% need two cards), nothing placeable.
func BenchmarkCardGroupCheckin(b *testing.B) {
	clock := &fakeClock{now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { q.Close() })
	q.checkpointProbe = func() bool { return true }
	q.nasAvailable = true
	hosts := []Machine{}
	for h := range 4 {
		m := m3Machine()
		m.ID, m.HostID, m.Session = fmt.Sprintf("host-%d", h), fmt.Sprintf("host-%d", h), fmt.Sprintf("session-%d", h)
		for i := range m.Devices {
			m.Devices[i].Identity = fmt.Sprintf("fake-%d-%d", h, i)
		}
		m.LastSeen = clock.Now()
		q.s.Machines[m.ID] = &m
		hosts = append(hosts, m)
		for _, card := range []string{"0", "1"} {
			id := fmt.Sprintf("held-%d-%s", h, card)
			q.s.Jobs[id] = &Job{ID: id, Spec: spec("production"), State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, StartedAt: &clock.now, AllocatedDevices: []string{card}}
		}
	}
	for i := range 2000 {
		id := fmt.Sprintf("queued-%06d", i)
		s := spec("experiment")
		s.Project = fmt.Sprintf("project-%d", i%5)
		if i%10 == 0 {
			s.Needs.MemoryGB = 12
		} else {
			s.Needs.MemoryGB = 4
		}
		q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
	}
	m := hosts[0]
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	for _, card := range []string{"0", "1"} {
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: "held-0-" + card, Attempt: 1}, Session: m.Session, Started: true})
	}
	checkin := func() {
		reply, err := q.Checkin(c)
		if err != nil {
			b.Fatal(err)
		}
		for _, a := range reply.Attempts {
			if a.Job != nil && a.Job.ID[:4] != "held" {
				b.Fatal("check-in admitted work", a.Job.ID)
			}
		}
	}
	checkin()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		checkin()
	}
}
