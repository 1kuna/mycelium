package myce

import (
	"context"
	"errors"
	"testing"
)

func TestInventoryObservationCUDAFailureClassification(t *testing.T) {
	p := MachineProbe{Command: func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.Join(context.DeadlineExceeded, errors.New("private diagnostic"))
	}}
	_, err := cudaAccelerators(p, t.Context(), []pciAccelerator{{vendor: "0x10de"}})
	if err == nil || inventoryErrorClass(err) != "deadline" {
		t.Fatal("CUDA command timeout lost its classification", err)
	}
}
