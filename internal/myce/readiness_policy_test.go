package myce

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestAdaptiveReadinessProgressClock(t *testing.T) {
	for _, host := range []struct {
		name string
		gap  time.Duration
	}{
		{"tiny", 40 * time.Second},
		{"large", 2 * time.Millisecond},
	} {
		t.Run(host.name, func(t *testing.T) {
			clock := &manualWorkerClock{now: time.Unix(1, 0)}
			history := readinessHistory{}
			history.Load.observe(host.gap)
			p := newReadinessProgress(clock, history)
			output := &modelReadinessOutput{output: io.Discard, progress: p}
			for i := 1; i <= 100; i++ {
				clock.Advance(host.gap)
				fmt.Fprintf(output, "MYCE_LOAD_PROGRESS {\"stage\":1,\"completed\":%d,\"total\":100}\n", i)
				if _, stalled := p.state(); stalled {
					t.Fatal("advancing load killed by total elapsed time")
				}
			}
			// Replaying a percentage or ordinary logs is not a progress signal.
			budget, _ := p.state()
			clock.Advance(budget)
			fmt.Fprintln(output, "MYCE_LOAD_PROGRESS {\"stage\":1,\"completed\":100,\"total\":100}")
			fmt.Fprintln(output, "still alive, waiting")
			if _, stalled := p.state(); !stalled {
				t.Fatal("true stall hidden by repeated output")
			}
			p.record([]byte(`{"stage":2,"completed":0,"total":10}`))
			if _, stalled := p.state(); stalled {
				t.Fatal("new stage did not renew progress")
			}
		})
	}
}

func TestAdaptiveReadinessDockerPullExtractClock(t *testing.T) {
	for _, gap := range []time.Duration{30 * time.Second, time.Millisecond} {
		clock := &manualWorkerClock{now: time.Unix(1, 0)}
		history := readinessHistory{}
		history.Load.observe(gap)
		p := newReadinessProgress(clock, history)
		output := &dockerProgressOutput{output: io.Discard, progress: p}
		for _, stage := range []string{"Downloading", "Extracting"} {
			for i := 1; i <= 30; i++ {
				clock.Advance(gap)
				fmt.Fprintf(output, "{\"id\":\"layer\",\"status\":%q,\"progressDetail\":{\"current\":%d,\"total\":30}}\n", stage, i)
				if _, stalled := p.state(); stalled {
					t.Fatal("slow advancing pull/extract treated as stalled")
				}
			}
		}
		budget, _ := p.state()
		clock.Advance(budget)
		fmt.Fprintln(output, `{"id":"layer","status":"Extracting","progressDetail":{"current":30,"total":30}}`)
		fmt.Fprintln(output, `{"id":"layer","status":"Downloading","progressDetail":{"current":30,"total":30}}`)
		if _, stalled := p.state(); !stalled {
			t.Fatal("duplicate/regressed layer phase concealed a stall")
		}
	}
}

func TestAdaptiveReadinessDockerCommandProgress(t *testing.T) {
	d := &dockerAttempt{clock: RealClock{}, timeout: 20 * time.Millisecond}
	d.command = func(ctx context.Context, _ ...string) ([]byte, error) {
		for i := 0; i < 12; i++ {
			if err := readinessPause(ctx, 5*time.Millisecond); err != nil {
				return nil, err
			}
			ctx.Value(ioProgressKey{}).(func())()
		}
		return []byte("done"), nil
	}
	if b, err := d.callWithTimeout(d.timeout, "start"); err != nil || string(b) != "done" {
		t.Fatalf("progressing command: %s %v", b, err)
	}
	d.command = func(ctx context.Context, _ ...string) ([]byte, error) { <-ctx.Done(); return nil, context.Cause(ctx) }
	if _, err := d.callWithTimeout(d.timeout, "start"); err == nil {
		t.Fatal("silent command never expired")
	}
}

func TestAdaptiveReadinessBurstThenIdleClock(t *testing.T) {
	for _, hostCount := range []int{1, 200} {
		t.Run(fmt.Sprint(hostCount), func(t *testing.T) {
			q, clock, m := modelFixture(t)
			q.models.Idle, q.models.Wait = 0, 0
			if hostCount == 1 {
				m.EffectiveCPUCores = 2
				m.CapabilityProfile.SystemMemoryBytes = 16 << 30
			} else {
				m.EffectiveCPUCores = 64
				m.CapabilityProfile.SystemMemoryBytes = 512 << 30
			}
			check(t, q, m, nil)
			for i := 1; i < hostCount; i++ {
				peer := m
				peer.ID = fmt.Sprintf("peer-%d", i)
				q.s.Machines[peer.ID] = &peer
			}
			call := addModelCall(t, q, "experiment")
			slot := q.s.ModelSlots[slotKey(m.ID, "test")]
			clock.Advance(2 * time.Second)
			reportModel(t, q, m, "ready")
			if err := q.finishCall(call.ID); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 8; i++ {
				clock.Advance(time.Second)
				reportModel(t, q, m, "ready")
				call = addModelCall(t, q, "experiment")
				if call.Attempt != slot.Attempt {
					t.Fatalf("burst iteration %d call=%+v slot=%+v demand=%+v", i, call, slot, q.s.ModelUses["test"])
				}
				if err := q.finishCall(call.ID); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(500 * time.Millisecond)
			reportModel(t, q, m, "ready")
			if slot.State != "ready" {
				t.Fatal("expected near-term demand lost residency")
			}
			clock.Advance(10 * time.Second)
			reportModel(t, q, m, "ready")
			if slot.State != "draining" {
				t.Fatal("idle model ignored measured demand/reload cost")
			}
		})
	}
}

func TestAdaptiveReadinessHistoryIsLocalAndIdentityBound(t *testing.T) {
	dir := t.TempDir()
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	model := Model{Name: "model", SHA256: strings.Repeat("a", 64)}
	_, p, finish := superviseModelLoad(context.Background(), model, dir, clock)
	p.mu.Lock()
	p.budget = 2 * time.Minute
	p.mu.Unlock()
	clock.Advance(time.Minute)
	p.advance()
	finish(true)
	_, next, stop := superviseModelLoad(context.Background(), model, dir, clock)
	defer stop(false)
	if budget, _ := next.state(); budget < time.Minute {
		t.Fatal("same model lost successful local load history")
	}
	model.SHA256 = strings.Repeat("b", 64)
	_, changed, stopChanged := superviseModelLoad(context.Background(), model, dir, clock)
	defer stopChanged(false)
	if budget, _ := changed.state(); budget >= time.Minute {
		t.Fatal("changed weights inherited load history")
	}
	_, foreign, stopForeign := superviseModelLoad(context.Background(), model, t.TempDir(), clock)
	defer stopForeign(false)
	if budget, _ := foreign.state(); budget >= time.Minute {
		t.Fatal("load history transferred between hosts")
	}
}
