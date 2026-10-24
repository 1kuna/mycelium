package myce

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type DevConfig struct {
	Devices                         int
	ProductionInfrastructureRetries *int
	FileStore                       string
	FileStoreLimit                  int64
	FileRetention                   time.Duration
	NASCheckpoints                  bool
	ExtensionRoot, JobFileRoot      string

	HostPolicies                          map[string]HostPolicy
	HostID                                string
	ProjectPolicies                       map[string]ProjectPolicy
	EnvironmentConfig                     EnvironmentCatalog
	ModelConfig                           ModelConfig
	Address, Database, Checkpoints, Token string
	GPU                                   string
	Environments, Models                  []string
	CPUReserveCores, CPUBudgetCores       int
	Workers                               int
	MemoryGB                              float64
	Interval                              time.Duration
}

// Dev runs the real API and command-executing workers with fake GPU inventory
// and an advance-only queue clock.
type Dev struct {
	workerWG  sync.WaitGroup
	stopQueue context.CancelFunc
	Queue     *Queue
	URL       string
	client    Client
	server    *http.Server
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	errors    chan error
}

func StartDev(c DevConfig) (*Dev, error) {
	return startDev(c, nil)
}

// Configure test clocks and storage probes before queue/worker goroutines start.
func startDev(c DevConfig, configureQueue func(*Queue)) (*Dev, error) {
	if c.GPU == "" {
		c.GPU = "fake"
	}
	if !validGPU(c.GPU) {
		return nil, errors.New("invalid simulated GPU platform")
	}
	if c.Workers < 1 || (c.MemoryGB <= 0 && c.GPU != "cpu") || c.Token == "" || c.CPUReserveCores < 0 || c.CPUBudgetCores < 0 {
		return nil, errors.New("dev requires workers, positive memory, and a token")
	}
	if c.GPU == "cpu" {
		c.MemoryGB = 0
	}
	if c.Devices == 0 {
		c.Devices = 1
	}
	if c.Devices < 1 || c.Devices > 1 && (c.Workers != 1 || c.GPU != "fake") {
		return nil, errors.New("multiple fake GPUs require one fake machine worker")
	}
	clock := &devClock{}
	if c.HostID == "" {
		c.HostID = "dev-host"
	}
	// Dev owns isolated fake worker spools and uses the ordinary state-root gate.
	roots := []string{filepath.Dir(c.Database), c.FileStore, c.Database + ".uploads"}
	for i := 0; i < c.Workers; i++ {
		dir := filepath.Join(filepath.Dir(c.Database), "workers", fmt.Sprintf("fake-%d", i+1))
		roots = append(roots, dir, filepath.Join(dir, "environment-cache"), filepath.Join(dir, "model-cache"), filepath.Join(dir, "file-cache"))
	}
	if err := PrepareStateRoots(roots...); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.Checkpoints, 0700); err != nil {
		return nil, err
	}
	q, err := Open(c.Database, c.Checkpoints, clock)
	if err != nil {
		return nil, err
	}
	if configureQueue != nil {
		configureQueue(q)
	}
	if c.ProductionInfrastructureRetries != nil {
		if err = q.SetProductionInfrastructureRetries(*c.ProductionInfrastructureRetries); err != nil {
			q.Close()
			return nil, err
		}
	}
	if c.FileRetention != 0 {
		if err = q.ConfigureFileRetention(c.FileRetention); err != nil {
			q.Close()
			return nil, err
		}
	}
	if c.FileStore != "" {
		limit := c.FileStoreLimit
		if limit == 0 {
			limit = 100 << 30
		}
		if err = q.ConfigureFileStore(c.FileStore, limit, !c.NASCheckpoints); err != nil {
			q.Close()
			return nil, err
		}
	}
	if c.ModelConfig.Idle <= 0 {
		c.ModelConfig.Idle = 10 * time.Minute
	}
	if c.ModelConfig.Wait <= 0 {
		c.ModelConfig.Wait = 5 * time.Minute
	}
	if len(c.ModelConfig.Models) == 0 {
		c.ModelConfig.Models = []Model{{Name: "dev-model", Engine: "fake", Path: "fake", MemoryGB: 1, Platforms: []string{c.GPU}}}
	}
	for _, m := range c.ModelConfig.Models {
		if m.Engine != "fake" {
			q.Close()
			return nil, errors.New("dev requires fake model engines")
		}
	}
	if err = q.ConfigureEnvironments(c.EnvironmentConfig); err != nil {
		q.Close()
		return nil, err
	}
	if c.HostPolicies != nil {
		if err = q.ConfigureHosts(c.HostPolicies); err != nil {
			q.Close()
			return nil, err
		}
	}
	if c.ProjectPolicies != nil {
		if err = q.ConfigureProjects(c.ProjectPolicies); err != nil {
			q.Close()
			return nil, err
		}
	}
	if err = q.ConfigureModels(c.ModelConfig); err != nil {
		q.Close()
		return nil, err
	}
	listener, err := net.Listen("tcp", c.Address)
	if err != nil {
		q.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dev{Queue: q, URL: "http://" + listener.Addr().String(), cancel: cancel, errors: make(chan error, c.Workers+2)}
	d.client = Client{URL: d.URL, Token: c.Token}
	targets := []ExtensionCheckTarget{}
	for i := 0; i < c.Workers; i++ {
		cache, err := filepath.Abs(filepath.Join(filepath.Dir(c.Database), "workers", fmt.Sprintf("fake-%d", i+1), "environment-cache"))
		if err != nil {
			listener.Close()
			q.Close()
			cancel()
			return nil, err
		}
		for device := 0; device < c.Devices; device++ {
			targets = append(targets, ExtensionCheckTarget{Host: c.HostID, Platform: c.GPU, Device: fmt.Sprint(device), EnvironmentCache: cache, OS: runtime.GOOS, Arch: runtime.GOARCH})
		}
	}
	clock.targets = targets
	d.server = &http.Server{Handler: API{Queue: q, Token: c.Token}.Handler(), ReadHeaderTimeout: 5 * time.Second}
	queueCtx, stopQueue := context.WithCancel(context.Background())
	d.stopQueue = stopQueue
	d.wg.Add(2)
	go func() {
		defer d.wg.Done()
		if err := d.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			d.errors <- err
		}
	}()
	go func() {
		defer d.wg.Done()
		if err := q.Run(queueCtx); err != nil {
			d.errors <- err
		}
	}()
	for i := 0; i < c.Workers; i++ {
		w := Worker{CPUIsolationProbe: func(context.Context) error { return nil }, ExtensionRoot: c.ExtensionRoot, JobFileRoot: c.JobFileRoot, ModelConfig: c.ModelConfig, StateDir: filepath.Join(filepath.Dir(c.Database), "workers", fmt.Sprintf("fake-%d", i+1)), Client: d.client, Machine: Machine{CPUReserveCores: c.CPUReserveCores, CPUBudgetCores: c.CPUBudgetCores, HostMemoryMeasured: true, HostID: c.HostID, Device: "0", ID: fmt.Sprintf("fake-%d", i+1), GPU: c.GPU, FreeMemoryGB: c.MemoryGB, Environments: c.Environments, Models: c.Models}, Interval: c.Interval}
		if c.Devices > 1 {
			w.Machine.ID, w.Machine.Device = c.HostID, ""
			devices := []MachineDevice{}
			for index := 0; index < c.Devices; index++ {
				id := fmt.Sprint(index)
				devices = append(devices, MachineDevice{ID: id, Platform: "fake", Identity: "fake-" + id, Fingerprint: "fake", Selector: id, MemoryMode: "fixed", TotalMemoryGB: c.MemoryGB, BudgetGB: c.MemoryGB, FreeMemoryGB: c.MemoryGB})
			}
			w.Machine.Devices = devices
			w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return append([]MachineDevice(nil), devices...), nil }
		}
		d.workerWG.Add(1)
		go func() {
			defer d.workerWG.Done()
			if err := w.Run(ctx); err != nil {
				d.errors <- err
			}

		}()
	}
	return d, nil
}
func (d *Dev) Client() Client       { return d.client }
func (d *Dev) Errors() <-chan error { return d.errors }
func (d *Dev) Close() error {
	// Keep the HTTP service alive while workers leave and finish uploading.
	d.cancel()
	d.workerWG.Wait()
	d.stopQueue()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := d.server.Shutdown(ctx)
	d.wg.Wait()
	return errors.Join(err, d.Queue.Close())
}
