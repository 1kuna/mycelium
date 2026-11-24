package myce

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"
)

// Local admission still belongs to the worker's aggregate ledger. A custodian
// must get a fresh exact-request receipt at every existing final launch gate.
// During worker replacement it waits; already-running guards need no worker.
type custodianGate struct {
	Nonce  string     `json:"nonce"`
	Launch HostLaunch `json:"launch"`
}
type custodianGateReceipt struct {
	Nonce     string           `json:"nonce"`
	Error     string           `json:"error,omitempty"`
	Admission *AdmissionDetail `json:"admission,omitempty"`
}
type custodianLedger struct {
	Charge  float64 `json:"charge"`
	Pending float64 `json:"pending"`
}

func (r custodianGateReceipt) refusal() error {
	if r.Error == "" {
		return nil
	}
	if detail := safeAdmissionDetail(r.Admission); detail != nil {
		return &admissionError{detail: *detail}
	}
	return errors.Join(errWorkerMemory, errors.New(r.Error))
}

func custodianLaunchGate(ctx context.Context, dir, id string, launch HostLaunch) error {
	r := custodianGate{Nonce: ID(), Launch: launch}
	if err := saveWorkerJSON(dir, custodianName(id, "gate"), r); err != nil {
		return err
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var receipt custodianGateReceipt
		if readCustodianJSON(dir, custodianName(id, "gate-receipt"), &receipt) == nil && receipt.Nonce == r.Nonce {
			if err := receipt.refusal(); err != nil {
				return err
			}
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (w Worker) serveCustodianGate(ctx context.Context, g *attemptCustodian, j Job) {
	if ctx.Err() != nil || w.serviceStop != nil && w.serviceStop.Err() != nil {
		return
	}
	if w.hostChargesExcept != nil {
		charge, pending := w.hostChargesExcept(jobKey(j))
		next := custodianLedger{Charge: charge, Pending: pending}
		var previous custodianLedger
		if readCustodianJSON(g.dir, custodianName(g.id, "ledger"), &previous) != nil || previous != next {
			_ = saveWorkerJSON(g.dir, custodianName(g.id, "ledger"), next)
		}
	}
	var r custodianGate
	if readCustodianJSON(g.dir, custodianName(g.id, "gate"), &r) != nil || r.Nonce == "" {
		return
	}
	l := r.Launch
	if l.Machine != j.Machine || l.Session != j.Session || l.Kind != "batch" || l.Key != j.ID || l.Attempt != strconv.Itoa(j.Attempt) {
		return
	}
	var previous custodianGateReceipt
	if readCustodianJSON(g.dir, custodianName(g.id, "gate-receipt"), &previous) == nil && previous.Nonce == r.Nonce {
		return
	}
	receipt := custodianGateReceipt{Nonce: r.Nonce}
	if w.batchAdmission != nil {
		if err := w.batchAdmission(ctx, l); err != nil {
			receipt.Error = err.Error()
			receipt.Admission = admissionDetail(err, j)
		}
	}
	if w.checkAvailability() != nil {
		receipt.Error = "worker is unavailable"
		receipt.Admission = nil
	}
	if ctx.Err() == nil && (w.serviceStop == nil || w.serviceStop.Err() == nil) {
		_ = saveWorkerJSON(g.dir, custodianName(g.id, "gate-receipt"), receipt)
	}
}

func custodianHostCharges(dir, id string) func(attemptKey) (float64, float64) {
	return func(attemptKey) (float64, float64) {
		var ledger struct {
			Charge  *float64 `json:"charge"`
			Pending *float64 `json:"pending"`
		}
		if readCustodianJSON(dir, custodianName(id, "ledger"), &ledger) != nil || ledger.Charge == nil || ledger.Pending == nil || !validMemoryValue(*ledger.Charge) || !validMemoryValue(*ledger.Pending) {
			return math.NaN(), math.NaN() // Unknown, never fabricated full-allowance charges.
		}
		return *ledger.Charge, *ledger.Pending
	}
}
