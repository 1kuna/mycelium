package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

// This command never constructs a client or reads installed authority/state.
func hostTelemetryCommand(ctx context.Context, args []string, probe myce.MachineProbe, out io.Writer) error {
	if len(args) == 0 || args[0] != "capture" {
		return errors.New("usage: myce telemetry capture --run-id ID --host-ref LABEL --queue-pid PID --worker-pid PID --duration D --interval D [--card LABEL=PCI]")
	}
	f := flag.NewFlagSet("myce telemetry capture", flag.ContinueOnError)
	c := myce.HostTelemetryCapture{Probe: probe}
	f.StringVar(&c.RunID, "run-id", "", "benchmark run identity")
	f.StringVar(&c.HostRef, "host-ref", "", "opaque host alias; never a host name/address")
	f.IntVar(&c.QueuePID, "queue-pid", 0, "local queue daemon PID")
	f.IntVar(&c.WorkerPID, "worker-pid", 0, "local worker daemon PID")
	f.BoolVar(&c.NativeWorkerOnly, "native-worker-only", false, "assert every managed attempt runs inside the worker tree; otherwise external attribution is missing")
	duration := f.Duration("duration", 0, "required finite capture duration, including probe time")
	interval := f.Duration("interval", 0, "required sampling cadence; use the same cadence in baseline and candidate")
	f.Func("card", "opaque card alias=physical PCI identity; repeat for every card", func(s string) error {
		ref, pci, ok := strings.Cut(s, "=")
		if !ok {
			return errors.New("card requires LABEL=PCI")
		}
		c.Cards = append(c.Cards, myce.HostTelemetryCard{Ref: ref, PCI: pci})
		return nil
	})
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || *duration <= 0 || *interval <= 0 || *interval > *duration {
		return errors.New("capture requires 0 < interval <= duration and no positional arguments")
	}
	if err := c.Init(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	start := time.Now()
	end := start.Add(*duration)
	ctx, cancel := context.WithDeadline(ctx, end)
	defer cancel()
	encoder := json.NewEncoder(out)
	for sequence, due := 0, start; due.Before(end); sequence, due = sequence+1, due.Add(*interval) {
		timer := time.NewTimer(max(0, time.Until(due)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		// The interval is also this slot's sensor-time budget. Slow/missing
		// tools cannot turn a bounded diagnostic into an unbounded observer.
		slot, cancelSlot := context.WithDeadline(ctx, minTime(due.Add(*interval), end))
		r, err := c.Sample(slot)
		cancelSlot()
		r.Sequence, r.IntervalSeconds = sequence, interval.Seconds()
		r.CaptureWindow = myce.TelemetryWindow{Start: start.UTC(), End: end.UTC()}
		if encodeErr := encoder.Encode(r); encodeErr != nil {
			return encodeErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
