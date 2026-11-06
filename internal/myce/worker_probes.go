package myce

import "context"

// Worker defaults are factories so package tests can inject a complete synthetic
// host before any worker goroutine starts. Inventory remains explicitly injected.
var workerProfileProbe = LocalProfileProbe
var workerHostMemorySource = NewHostMemorySource
var workerOwnerSignals = LocalOwnerSignals
var workerProcessMemory = localProcessMemory

var workerMachineProbe = LocalMachineProbe

// WorkerProbeDefaults groups host sources used when a worker has no override.
// Embedded callers may inject these before starting any worker goroutines.
type WorkerProbeDefaults struct {
	HostHealth func(context.Context) (HostHealthReading, error)
	MountRoots func(context.Context) ([]string, error)
	// Nil disables the current worker-tree RSS observation.
	MemoryRSS    MemorySource
	Disk         func(string) (DiskReading, error)
	Profile      func() ProfileProbe
	HostMemory   func(float64) (MemorySource, error)
	OwnerSignals func(context.Context) OwnerSignals
	Identity     func() MachineProbe
	DeviceFaults func() DeviceFaultSource
	// Nil disables per-attempt process sampling on hosts without cgroups.
	ProcessMemory func() *processMemoryProbe
	// Nil disables process-tree CPU observation; cgroup accounting remains available.
	ProcessCPU func(context.Context, int, string) (float64, string, error)
	// Nil disables host I/O pressure and sampled per-attempt process-tree CPU.
	IOPressure  func(Clock) *IOPressure
	ResourceCPU func(context.Context, map[int]string, map[string]float64) (float64, error)
}

// SetWorkerProbeDefaults installs complete defaults during process setup.
// It must not run concurrently with workers or fingerprint readers.
func SetWorkerProbeDefaults(p WorkerProbeDefaults) {
	workerHostHealthSource = p.HostHealth
	if p.MountRoots != nil {
		workerMountRoots = p.MountRoots
	}
	if p.Disk != nil {
		workerDiskProbe = p.Disk
	}
	workerProfileProbe = p.Profile
	workerHostMemorySource = p.HostMemory
	workerOwnerSignals = p.OwnerSignals
	workerProcessMemory = p.ProcessMemory
	workerMemoryRSS = p.MemoryRSS
	workerLivenessCPU = p.ProcessCPU
	workerIOPressure = p.IOPressure
	workerResourceCPU = p.ResourceCPU
	extensionIdentityProbe = p.Identity
	if p.DeviceFaults != nil {
		workerDeviceFaultSource = p.DeviceFaults
	}
}
