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
	ModelConfig                           ModelConfig
	Address, Database, Checkpoints, Token string
	GPU                                   string
	Environments, Models                  []string
	Workers                               int
	MemoryGB                              float64
	Interval                              time.Duration
}

// Dev runs the real API and command-executing workers. The only fake is GPU inventory.
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
	if err := os.MkdirAll(c.Checkpoints, 0700); err != nil {
		return nil, err
	}
	q, err := Open(c.Database, c.Checkpoints, RealClock{})
	if err != nil {
		return nil, err
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
		w := Worker{ModelConfig: c.ModelConfig, StateDir: filepath.Join(filepath.Dir(c.Database), "workers", fmt.Sprintf("fake-%d", i+1)), Client: d.client, Machine: Machine{ID: fmt.Sprintf("fake-%d", i+1), GPU: c.GPU, FreeMemoryGB: c.MemoryGB, Environments: c.Environments, Models: c.Models}, Interval: c.Interval}
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
