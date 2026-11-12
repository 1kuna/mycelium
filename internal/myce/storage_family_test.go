package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A per-item entry: each run has its own source, settings and input, so the exact
// storage key never repeats. Once the family has enough accepted runs, its
// peaks plus spread replace the declared scratch hint; before that the hint
// stands.
func TestStorageFamilyLearnsPerItemScratch(t *testing.T) {
	q, clock, m, base, out := storageFixture(t)
	m = reportDisk(t, m, clock, "volume", []string{"data"}, 400<<30, 200<<30)
	check(t, q, m, nil)
	c := deliveryClient(t, q)
	hint := uint64(1 << 20)
	item := func(n int) Spec {
		s := base
		_, source := retentionUpload(t, c, fmt.Sprintf("source revision %d", n))
		_, input := retentionUpload(t, c, strings.Repeat("x", n))
		s.Files = FileManifest{".myce-source.tar": source, fmt.Sprintf("item-%d.bin", n): input}
		s.Run.Env = map[string]string{"ITEM": fmt.Sprint(n)}
		s.Disk = &DiskNeeds{ScratchBytes: hint}
		return s
	}
	scratchFor := func(s Spec) (uint64, string) {
		probe := submit(t, q, s)
		q.mu.Lock()
		j, mm := q.s.Jobs[probe.ID], q.s.Machines[m.ID]
		w, e := q.diskWork(j, mm), q.storageEnvelope(j, mm)
		q.mu.Unlock()
		if err := q.Cancel(probe.ID); err != nil {
			t.Fatal(err)
		}
		return w.scratch, e.Basis
	}
	run := func(n int, scratch uint64) Job {
		j := startObservation(t, q, m, item(n))
		clock.Advance(time.Second)
		if err := q.Complete(storageResult(&j, out, scratch)); err != nil {
			t.Fatal(err)
		}
		return j
	}

	run(1, 10<<30)
	if got, basis := scratchFor(item(90)); got != hint || basis != "declared" {
		t.Fatalf("one family run overrode the hint: scratch %d basis %s", got, basis)
	}
	run(2, 20<<30)
	third := run(3, 30<<30)
	if len(q.s.StorageObservations) != 3 {
		t.Fatalf("per-item runs should keep separate exact keys: %d", len(q.s.StorageObservations))
	}
	if e := job(t, q, third.ID).StorageEnvelope; e == nil || e.Basis != "family" {
		t.Fatalf("third run was not admitted on the family basis: %+v", e)
	}

	// Peak 30 GiB plus the 20 GiB spread between items.
	fourth := item(4)
	if got, basis := scratchFor(fourth); got != 50<<30 || basis != "family" {
		t.Fatalf("new item not sized from the family: scratch %d basis %s", got, basis)
	}
	small := reportDisk(t, cpuMachine("small-worker", "small-host", "cpu", 8, 32), clock, "small-volume", []string{"data"}, 100<<30, 40<<30)
	check(t, q, small, nil)
	j := submit(t, q, fourth)
	q.mu.Lock()
	reason := q.diskHeadroomReason(q.s.Jobs[j.ID], q.s.Machines[small.ID])
	q.mu.Unlock()
	if !strings.Contains(reason, "disk: 50.0 GiB needed") {
		t.Fatalf("family need did not drive admission: %q", reason)
	}
	got := check(t, q, m, nil).Job
	if got == nil || got.ID != j.ID || got.StorageEnvelope == nil || got.StorageEnvelope.Basis != "family" || *got.StorageEnvelope.ScratchBytes != 50<<30 {
		t.Fatalf("fourth run not admitted on the family figure: %+v", got)
	}
	if !strings.Contains(got.StorageEnvelope.Summary(), "basis family") {
		t.Fatalf("status does not name the basis: %s", got.StorageEnvelope.Summary())
	}
}
