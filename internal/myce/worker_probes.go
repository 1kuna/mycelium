package myce

import "context"

// Worker defaults are factories so package tests can inject a complete synthetic
// host before any worker goroutine starts. Inventory remains explicitly injected.
var workerProfileProbe = LocalProfileProbe
var workerHostMemorySource = NewHostMemorySource
var workerOwnerSignals = LocalOwnerSignals

var workerMachineProbe = LocalMachineProbe

// Ordinary builds use the real NAS preparation; package fixtures replace it
// before workers start. A worker override still exercises storage faults.
var workerCheckpointPrepare = prepareCheckpoint

// WorkerProbeDefaults groups host sources used when a worker has no override.
// Embedded callers may inject these before starting any worker goroutines.
type WorkerProbeDefaults struct {
	Profile      func() ProfileProbe
	HostMemory   func(float64) (MemorySource, error)
	OwnerSignals func(context.Context) OwnerSignals
	Identity     func() MachineProbe
}

// SetWorkerProbeDefaults installs complete defaults during process setup.
// It must not run concurrently with workers or fingerprint readers.
func SetWorkerProbeDefaults(p WorkerProbeDefaults) {
	workerProfileProbe = p.Profile
	workerHostMemorySource = p.HostMemory
	workerOwnerSignals = p.OwnerSignals
	extensionIdentityProbe = p.Identity
}
