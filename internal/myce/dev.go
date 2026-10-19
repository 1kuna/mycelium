package myce

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type DevConfig struct {
	FileStore                             string
	FileStoreLimit                        int64
	ExtensionRoot, JobFileRoot            string
	HostStateDir                          string
	HostPolicies                          map[string]HostPolicy
	HostID                                string
	ProjectPolicies                       map[string]ProjectPolicy
	EnvironmentConfig                     EnvironmentCatalog
	ModelConfig                           ModelConfig
	Address, Database, Checkpoints, Token string
	GPU                                   string
	Environments, Models                  []string
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
	if c.GPU == "" {
		c.GPU = "fake"
	}
	if !validGPU(c.GPU) {
		return nil, errors.New("invalid simulated GPU platform")
	}
	if c.Workers < 1 || c.MemoryGB <= 0 || c.Token == "" {
		return nil, errors.New("dev requires workers, positive memory, and a token")
	}
	clock := &devClock{}
	freshHost := false
	if c.HostID == "" {
		c.HostID = "dev-host"
	}
	// Dev owns its isolated fake host; real runtimes never initialize missing state.
	if c.HostStateDir == "" {
		c.HostStateDir = filepath.Join(filepath.Dir(c.Database), "dev-host")
	}
	roots := []string{filepath.Dir(c.Database), c.HostStateDir, c.FileStore, c.Database + ".uploads"}
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
	if _, e := os.Stat(filepath.Join(c.HostStateDir, hostStateFile)); errors.Is(e, os.ErrNotExist) {
		_, e := InitializeHostIntent(c.HostStateDir, c.HostID, clock)
		if e != nil {
			return nil, e
		}
		freshHost = true
	}
	if _, e := ReadHostIntent(c.HostStateDir); e != nil {
		return nil, e
	}
	q, err := Open(c.Database, c.Checkpoints, clock)
	if err != nil {
		return nil, err
	}
	if c.FileStore != "" {
		limit := c.FileStoreLimit
		if limit == 0 {
			limit = 100 << 30
		}
		if err = q.ConfigureFileStore(c.FileStore, limit, true); err != nil {
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
	if err = q.ConfigureLocalHost(c.HostStateDir, d.URL); err != nil {
		listener.Close()
		q.Close()
		cancel()
		return nil, err
	}
	if freshHost {
		s, e := ReadHostIntent(c.HostStateDir)
		if e != nil {
			listener.Close()
			q.Close()
			cancel()
			return nil, e
		}
		boot, e := HostBootID()
		if e == nil {
			e = mutateHostIntent(c.HostStateDir, s.HostRevision, func(current *HostIntent) error {
				current.LocalExclusion = &HostLocalExclusion{Simulated: true, HostObservation: HostObservation{HostRevision: s.HostRevision, Boot: boot, State: "closed", At: q.clock.Now()}, Confirmed: true}
				return nil
			})
		}
		// Dev has just provisioned an empty isolated fake host; the real queue
		// transaction still must acknowledge it before opening.
		if e == nil {
			e = q.Tick()
		}
		if e == nil {
			_, e = changeHostIntent(c.HostStateDir, q.clock, hostIntentChange{Desired: "open", Expected: s.HostRevision})
		}
		if e != nil {
			listener.Close()
			q.Close()
			cancel()
			return nil, e
		}
	}

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
		w := Worker{ExtensionRoot: c.ExtensionRoot, JobFileRoot: c.JobFileRoot, HostStateDir: c.HostStateDir, ModelConfig: c.ModelConfig, StateDir: filepath.Join(filepath.Dir(c.Database), "workers", fmt.Sprintf("fake-%d", i+1)), Client: d.client, Machine: Machine{HostID: c.HostID, Device: "0", ID: fmt.Sprintf("fake-%d", i+1), GPU: c.GPU, FreeMemoryGB: c.MemoryGB, Environments: c.Environments, Models: c.Models}, Interval: c.Interval}
		d.workerWG.Add(1)
		go func() {
			defer d.workerWG.Done()
			for ctx.Err() == nil {
				if err := w.Run(ctx); err != nil {
					d.errors <- err
					return
				}
				for ctx.Err() == nil {
					reason, err := HostDisabled(w.HostStateDir)
					if err != nil {
						d.errors <- err
						return
					}
					if reason == "" {
						break
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(50 * time.Millisecond):
					}
				}
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
