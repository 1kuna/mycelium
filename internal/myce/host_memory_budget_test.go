package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestHostMemoryAllowanceClosedReconfiguration(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	before, err := os.ReadFile(filepath.Join(dir, hostStateFile))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SetHostMemoryBudget(dir, 4); err == nil {
		t.Fatal("OPEN host accepted allowance reconfiguration")
	}
	after, err := os.ReadFile(filepath.Join(dir, hostStateFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected configuration changed host intent", err)
	}
	clock := &fakeClock{now: time.Unix(1000, 0)}
	if _, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", Grace: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	closed, err := ReadHostIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []float64{0, -1, math.NaN(), math.Inf(1), float64(math.MaxInt64)} {
		if _, err := SetHostMemoryBudget(dir, bad); err == nil {
			t.Fatal("invalid allowance accepted", bad)
		}
	}
	configured, err := SetHostMemoryBudget(dir, 4)
	if err != nil || configured.Bytes != 4<<30 || configured.Qualification != "UNQUALIFIED" {
		t.Fatal(configured, err)
	}
	state, err := ReadHostIntent(dir)
	if err != nil || state.Desired != "closed" || state.HostRevision != closed.HostRevision || !state.StopDeadline.Equal(*closed.StopDeadline) {
		t.Fatal("configuration changed close/deadline", state, err)
	}
	for range 3 {
		testConfirmClose(t, dir)
		if _, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: readTestHostRevision(t, dir)}); err != nil {
			t.Fatal(err)
		}
		if _, err := SetHostMemoryBudget(dir, 8); err == nil {
			t.Fatal("open reconfiguration accepted")
		}
		budget, source, err := EffectiveHostMemoryBudget(dir, 1)
		if err != nil || budget != configured || source != hostStateFile {
			t.Fatal("open changed allowance", budget, source, err)
		}
		if _, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed"}); err != nil {
			t.Fatal(err)
		}
		budget, _, err = EffectiveHostMemoryBudget(dir, 1)
		if err != nil || budget != configured {
			t.Fatal("close changed allowance", budget, err)
		}
	}
	st, err := os.Stat(filepath.Join(dir, hostStateFile))
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("host policy permissions", err)
	}
}

func TestHostMemoryAllowanceMigrationAndValidation(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	fallback, _, err := EffectiveHostMemoryBudget(dir, 3)
	if err == nil || fallback.Bytes != 3<<30 || fallback.Qualification != "UNQUALIFIED" {
		t.Fatal("conservative install input", fallback, err)
	}
	for _, bad := range []float64{-1, math.NaN(), math.Inf(1)} {
		if _, _, err := EffectiveHostMemoryBudget(dir, bad); err == nil {
			t.Fatal("invalid fallback accepted")
		}
	}
	original := []byte(`{"gib":3,"generation":19}`)
	path := filepath.Join(dir, "host-memory-budget.json")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := EffectiveHostMemoryBudget(dir, 27); err == nil {
		t.Fatal("old independent protocol remained active")
	}
	if _, err := ImportHostMemoryBudget(dir); err == nil {
		t.Fatal("import while open accepted")
	}
	if err := closeTestHost(dir); err != nil {
		t.Fatal(err)
	}
	budget, err := ImportHostMemoryBudget(dir)
	if err != nil || budget.Bytes != 3<<30 || budget.Qualification != "UNQUALIFIED" {
		t.Fatal("conservative input changed", budget, err)
	}
	if _, err := ImportHostMemoryBudget(dir); err == nil {
		t.Fatal("repeated import overwrote host policy")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, original) {
		t.Fatal("migration source altered", err)
	}
	effective, _, err := EffectiveHostMemoryBudget(dir, 27)
	if err != nil || effective != budget {
		t.Fatal("old file overrode imported policy", effective, err)
	}
	s, err := ReadHostIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.MemoryAllowanceBytes = -1
	if err := atomicHostJSON(dir, hostStateFile, s, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := EffectiveHostMemoryBudget(dir, 3); err == nil {
		t.Fatal("corrupt allowance accepted")
	}
}

func TestHostMemoryAllowanceSerializesAndRefusesPolicyMismatch(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	if err := closeTestHost(dir); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := SetHostMemoryBudget(dir, 4); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	budget, _, err := EffectiveHostMemoryBudget(dir, 1)
	if err != nil || budget.Bytes != 4<<30 {
		t.Fatal(budget, err)
	}
	w := Worker{HostStateDir: dir, Machine: Machine{HostMemoryBudgetGB: 2}}
	if err := w.checkHostMemoryBudget(0, 1); !errors.Is(err, errWorkerMemory) {
		t.Fatal("stale policy admitted", err)
	}
	if err := w.refreshHostMemoryBudget(); err != nil {
		t.Fatal(err)
	}
	if err := w.checkHostMemoryBudget(0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := SetHostMemoryBudget(dir, 2); err != nil {
		t.Fatal(err)
	}
	if err := w.checkHostMemoryBudget(0, 1); !errors.Is(err, errWorkerMemory) {
		t.Fatal("lower policy not fenced", err)
	}
	q, _ := fixture(t)
	a, b := machine("card-a", "fake"), machine("card-b", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "host"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 16
		m.HostMemoryBudgetGB = 4
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	b.HostMemoryBudgetGB = 2
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 1
	submit(t, q, s)
	if check(t, q, a, nil).Job != nil {
		t.Fatal("sibling policy conflict admitted work")
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range snap.Machines {
		if m.HostMemoryAllowanceBytes != 2<<30 || m.HostMemoryQualification != "UNQUALIFIED" || m.HostMemoryBudgetStatus() != "host allowance policy mismatch" {
			t.Fatal("missing effective bytes/mismatch", m)
		}
	}
	a.HostMemoryBudgetGB = 2
	if check(t, q, a, nil).Job == nil {
		t.Fatal("consistent policy stayed fenced")
	}
}

func TestHostMemoryAllowanceDevSwitches(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "host")
	d, err := StartDev(DevConfig{HostStateDir: dir, HostID: "dev-host", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture-token", Workers: 2, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	end, _ := ctx.Deadline()
	workCtx, workCancel := context.WithDeadline(ctx, end.Add(-5*time.Second))
	defer workCancel()
	closeAndSettle := func() error {
		if _, err := changeHostIntent(dir, d.Queue.clock, hostIntentChange{Desired: "closed"}); err != nil {
			return err
		}
		// Wait for the real worker runs to leave their spools before the fake kernel
		// inventory is allowed to attest exclusion. Never race startup with reopen.
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for _, name := range []string{"fake-1", "fake-2"} {
			for {
				unlock, e := hostOperationLock(filepath.Join(root, "workers", name), "worker.lock", false)
				if e == nil {
					unlock()
					break
				}
				if !errors.Is(e, syscall.EWOULDBLOCK) && !errors.Is(e, syscall.EAGAIN) {
					return e
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
		_, err := (HostControl{Dir: dir, Clock: d.Queue.clock, Observe: testHostExclusion, Service: &fakeHostService{}}).Disable(ctx, "fixture close", 0)
		if err != nil {
			return err
		}
		return ReconcileHostLocal(ctx, dir, d.Queue.clock, func(string) (Client, error) { return d.Client(), nil })
	}

	defer func() {
		if err := closeAndSettle(); err != nil {
			t.Error("finally close/settle", err)
		}
		s, err := ReadHostIntent(dir)
		if err != nil || s.Desired != "closed" || s.HostSettlement() != "confirmed" || len(s.Pending) != 0 {
			t.Error("finally not CLOSED/settled", s, err)
		}
		if err := d.Close(); err != nil {
			t.Error("dev teardown", err)
		}
	}()
	client := d.Client()
	var last Snapshot
	get := func() Snapshot {
		t.Helper()
		var snap Snapshot
		if err := client.Call(workCtx, "GET", "/api/status", nil, &snap); err != nil {
			for _, m := range last.Machines {
				t.Logf("last machine: %+v", m)
			}
			for _, j := range last.Jobs {
				t.Logf("last job: %+v", j)
			}
			select {
			case e := <-d.Errors():
				t.Logf("dev error: %v", e)
			default:
			}
			t.Fatal(err)
		}
		last = snap
		return snap
	}
	// StartDev returns before worker registration. Establish real API readiness
	// before the first close; otherwise there is no worker session to discharge.
	readyTick := time.NewTicker(5 * time.Millisecond)
	for len(get().Machines) != 2 {
		select {
		case <-workCtx.Done():
			readyTick.Stop()
			t.Fatal("initial worker readiness", workCtx.Err())
		case <-readyTick.C:
		}
	}
	readyTick.Stop()
	if err := closeAndSettle(); err != nil {
		t.Fatal(err)
	}
	configured, err := SetHostMemoryBudget(dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 3; cycle++ {
		effect := filepath.Join(root, "effect-"+string(rune('a'+cycle)))
		s := spec("experiment")
		s.Resumable = false
		s.Needs.HostMemoryGB = minimumHostMemoryGB
		s.Run.Command = []string{"/bin/sh", "-c", `test -z "$MYCE_TOKEN" && printf executed > "$EFFECT"`}
		s.Run.Env = map[string]string{"EFFECT": effect}
		var submitted Job
		if err := client.Call(workCtx, "POST", "/api/jobs", s, &submitted); err != nil {
			t.Fatal(err)
		}
		// Drive real queue transactions while closed; a huge allowance cannot open admission.
		for range 3 {
			snap := get()
			for _, j := range snap.Jobs {
				if j.ID == submitted.ID && (j.State != "queued" || j.Attempt != 0) {
					t.Fatal("CLOSED host admitted work", j)
				}
			}
		}
		if _, err := os.Stat(effect); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("closed command effect", err)
		}
		state, err := ReadHostIntent(dir)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := changeHostIntent(dir, d.Queue.clock, hostIntentChange{Desired: "open", Expected: state.HostRevision}); err != nil {
			t.Fatal(err)
		}
		deadline := time.NewTicker(10 * time.Millisecond)
		completed := false
		for !completed {
			select {
			case <-workCtx.Done():
				deadline.Stop()
				for _, m := range last.Machines {
					t.Logf("last machine: %+v", m)
				}
				for _, j := range last.Jobs {
					t.Logf("last job: %+v", j)
				}
				t.Fatal("dev switch total deadline", ctx.Err())
			case <-deadline.C:
			}
			snap := get()
			last = snap
			for _, j := range snap.Jobs {
				if j.ID == submitted.ID {
					if j.State == "failed" {
						t.Fatal("real command failed", j.Reason)
					}
					completed = j.State == "succeeded"
				}
			}
			for _, m := range snap.Machines {
				if m.HostMemoryBudgetGB == 64 && (m.HostMemoryAllowanceBytes != configured.Bytes || m.HostMemoryQualification != "UNQUALIFIED") {
					t.Fatal("status allowance", m)
				}
			}
		}
		deadline.Stop()
		data, err := os.ReadFile(effect)
		if err != nil || string(data) != "executed" {
			t.Fatal("missing real command effect", err)
		}
		if err := closeAndSettle(); err != nil {
			t.Fatal(err)
		}
		budget, _, err := EffectiveHostMemoryBudget(dir, 1)
		if err != nil || budget != configured {
			t.Fatal("switch rewrote allowance", budget, err)
		}
		t.Logf("cycle %d: queued while CLOSED; real API/SQLite/runner effect; exact allowance %d bytes; CLOSED settled", cycle+1, budget.Bytes)
	}
	state, err := ReadHostIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("generation")) {
		t.Fatal("independent memory generation persisted")
	}
}

func TestHostMemoryAllowanceInstallInputRequiresClosedConfiguration(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	before, err := os.ReadFile(filepath.Join(dir, hostStateFile))
	if err != nil {
		t.Fatal(err)
	}
	input, _, err := EffectiveHostMemoryBudget(dir, 3)
	if err == nil {
		t.Fatal("positive install-time input became runtime allowance while OPEN")
	}
	if input.Bytes != 3<<30 || input.Qualification != "UNQUALIFIED" {
		t.Fatal("conservative input lost", input)
	}
	after, e := os.ReadFile(filepath.Join(dir, hostStateFile))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("input read changed policy", e)
	}
	if err := closeTestHost(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := SetHostMemoryBudget(dir, input.GiB); err != nil {
		t.Fatal(err)
	}
	testConfirmClose(t, dir)
	if _, err := changeHostIntent(dir, RealClock{}, hostIntentChange{Desired: "open", Expected: readTestHostRevision(t, dir)}); err != nil {
		t.Fatal(err)
	}
	w := Worker{HostStateDir: dir, Machine: Machine{HostMemoryBudgetGB: 27}}
	if err := w.refreshHostMemoryBudget(); err != nil || w.Machine.HostMemoryBudgetGB != 3 {
		t.Fatal("new install flag replaced explicit policy", err)
	}
	if err := w.checkHostMemoryBudget(0, 1); err != nil {
		t.Fatal(err)
	}
}

// Synthetic launch fixtures use the same explicit CLOSED reconfiguration path.
func configureTestHostMemoryAllowance(t *testing.T, dir string, gib float64) HostRevision {
	t.Helper()
	if err := closeTestHost(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := SetHostMemoryBudget(dir, gib); err != nil {
		t.Fatal(err)
	}
	testConfirmClose(t, dir)
	opened, err := changeHostIntent(dir, RealClock{}, hostIntentChange{Desired: "open", Expected: readTestHostRevision(t, dir)})
	if err != nil {
		t.Fatal(err)
	}
	return opened.HostRevision
}
