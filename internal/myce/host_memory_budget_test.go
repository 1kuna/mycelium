package myce

import (
	"errors"
	"testing"
)

func TestHostMemoryBudgetAdmissionDiagnostic(t *testing.T) {
	w := Worker{Machine: machine("configured", "fake")}
	w.Machine.HostMemoryBudgetGB = 16
	err := w.checkHostMemoryBudget(8, 22)
	if !errors.Is(err, errWorkerMemory) {
		t.Fatal("configured allowance accepted oversized attempt", err)
	}
	d := admissionDetail(err, Job{Spec: spec("experiment")})
	if d == nil || d.Subcheck != "configured_host_allowance" || d.Required != 22 || d.Available == nil || *d.Available != 16 || d.Unit != "GiB" || d.Unavailable {
		t.Fatal("configured allowance lost compared values", d)
	}
}
