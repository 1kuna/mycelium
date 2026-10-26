package myce

import "context"

// Worker defaults are factories so package tests can inject a complete synthetic
// host before any worker goroutine starts. Inventory remains explicitly injected.
var workerProfileProbe = LocalProfileProbe
var workerHostMemorySource = NewHostMemorySource
var workerOwnerSignals = LocalOwnerSignals

var workerMachineProbe = LocalMachineProbe

// WorkerProbeDefaults groups host sources used when a worker has no override.
// Embedded callers may inject these before starting any worker goroutines.
type WorkerProbeDefaults struct {
	Disk         func(string) (DiskReading, error)
	Profile      func() ProfileProbe
	HostMemory   func(float64) (MemorySource, error)
	OwnerSignals func(context.Context) OwnerSignals
	Identity     func() MachineProbe
	DeviceFaults func() DeviceFaultSource
}

// SetWorkerProbeDefaults installs complete defaults during process setup.
// It must not run concurrently with workers or fingerprint readers.
func SetWorkerProbeDefaults(p WorkerProbeDefaults) {
	if p.Disk != nil {
		workerDiskProbe = p.Disk
	}
	workerProfileProbe = p.Profile
	workerHostMemorySource = p.HostMemory
	workerOwnerSignals = p.OwnerSignals
	extensionIdentityProbe = p.Identity
	if p.DeviceFaults != nil {
		workerDeviceFaultSource = p.DeviceFaults
	}
}
