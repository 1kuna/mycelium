package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// The watchdog reads the last known obligations without waiting for dockerd.
type dockerDisableProgress struct {
	mu     sync.Mutex
	units  map[string]string
	status string
	left   []string
}

func (p *dockerDisableProgress) unit(name, result string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.units == nil {
		p.units = map[string]string{}
	}
	p.units[name] = result
}
func (p *dockerDisableProgress) unitReport() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	units := map[string]string{}
	for k, v := range p.units {
		units[k] = v
	}
	return units
}
func (p *dockerDisableProgress) set(status string, left []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = status
	p.left = append([]string(nil), left...)
}
func (p *dockerDisableProgress) report(left []string) (string, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status, append(left, p.left...)
}

func (h HostControl) confirmDocker(ctx context.Context, workers []HostWorker, stopAt time.Time) error {
	h.dockerProgress.set("unconfirmed", []string{"Docker inventory not yet confirmed"})
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" {
		endpoint = "unix:///var/run/docker.sock"
	}
	endpoints := []string{endpoint}
	attempts := map[string]*dockerAttempt{}
	orphanNote := ""
	var env []string
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if k != "MYCE_TOKEN" && k != "DOCKER_CONTEXT" {
			env = append(env, v)
		}
	}
	// Read all custody, including an accepted create whose client died before its receipt.
	for _, w := range workers {
		paths, err := filepath.Glob(filepath.Join(w.State, "*.docker.json"))
		if err != nil {
			return err
		}
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var st dockerExecution
			if err = json.Unmarshal(data, &st); err != nil {
				return fmt.Errorf("Docker custody %s: %w", filepath.Base(path), err)
			}
			if st.Identity == "" || filepath.Base(st.Identity) != st.Identity || filepath.Base(path) != st.Identity+".docker.json" || st.Endpoint == "" {
				return errors.New("invalid Docker custody")
			}
			if !slices.Contains(endpoints, st.Endpoint) {
				endpoints = append(endpoints, st.Endpoint)
			}
			if st.Released {
				continue
			}
			key := st.Endpoint + "\x00" + st.Identity
			if attempts[key] != nil {
				return fmt.Errorf("duplicate Docker custody for attempt %s", st.Identity)
			}
			attempts[key] = &dockerAttempt{state: st, dir: w.State, clock: h.Clock, ctx: ctx, timeout: 2 * time.Second, env: env}
		}
	}
	obligations := func() []string {
		var left []string
		for _, d := range attempts {
			if !d.state.Released {
				left = append(left, fmt.Sprintf("Docker attempt %s container %s: %s", d.state.Identity, d.state.Container, d.status()))
			}
		}
		slices.Sort(left)
		return left
	}
	h.dockerProgress.set("unconfirmed", append(obligations(), "Docker label inventory unconfirmed"))
	binary, err := exec.LookPath("docker")
	if errors.Is(err, exec.ErrNotFound) && len(attempts) == 0 {
		h.dockerProgress.set("not applicable (Docker CLI not installed; no unresolved custody)", nil)
		return nil
	}
	if err != nil {
		return fmt.Errorf("Docker CLI unavailable; %s: %w", strings.Join(obligations(), "; "), err)
	}
	for _, d := range attempts {
		d.binary = binary
		d.stop(h.Clock.Now(), max(0, stopAt.Sub(h.Clock.Now())), !h.Clock.Now().Before(stopAt))
	}
	liveContainers := map[string]string{}
	wake := time.NewTicker(10 * time.Millisecond)
	defer wake.Stop()
	for {
		var failures []string
		// Inventory every Mycelium attempt on the endpoint, not just registered spools.
		for _, endpoint := range endpoints {
			scan := &dockerAttempt{inventory: func(ids []string) {
				left := append(obligations(), "Docker label inventory unconfirmed")
				for _, id := range ids {
					left = append(left, "Docker container "+id+": attempt label inspection unconfirmed")
				}
				for _, c := range liveContainers {
					left = append(left, c)
				}
				h.dockerProgress.set("unconfirmed", left)
			}, binary: binary, env: env, ctx: ctx, timeout: 2 * time.Second, state: dockerExecution{Endpoint: endpoint}}
			var containers []dockerInspection
			var e error
			containers, e = scan.containers()
			if e != nil {
				failures = append(failures, "Docker label inventory unconfirmed: "+e.Error())
				continue
			}
			var networks []dockerNetworkInspection
			networks, e = scan.networks()
			if e != nil {
				failures = append(failures, "Docker network inventory unconfirmed: "+e.Error())
			}
			for _, n := range networks {
				key := endpoint + "\x00" + n.Labels["myce.attempt"]
				d := attempts[key]
				if d == nil {
					orphanNote = "; " + dockerOrphanEvidenceNote
					d = &dockerAttempt{binary: binary, env: env, ctx: ctx, clock: h.Clock, timeout: 2 * time.Second, state: dockerExecution{Identity: n.Labels["myce.attempt"], Endpoint: endpoint, Network: n.Name}}
					d.stop(h.Clock.Now(), max(0, stopAt.Sub(h.Clock.Now())), !h.Clock.Now().Before(stopAt))
					attempts[key] = d
				}
				d.state.Released = false
			}
			for key := range liveContainers {
				if strings.HasPrefix(key, endpoint+"\x00") {
					delete(liveContainers, key)
				}
			}
			for _, c := range containers {
				containerKey := endpoint + "\x00" + c.ID
				if *c.State.Running {
					liveContainers[containerKey] = fmt.Sprintf("Docker attempt %s container %s: execution unconfirmed", c.Config.Labels["myce.attempt"], c.ID)
				} else {
					delete(liveContainers, containerKey)
				}
				id := c.Config.Labels["myce.attempt"]
				key := endpoint + "\x00" + id
				d := attempts[key]
				if d == nil {
					orphanNote = "; " + dockerOrphanEvidenceNote
					d = &dockerAttempt{binary: binary, env: env, ctx: ctx, clock: h.Clock, timeout: 2 * time.Second, state: dockerExecution{Identity: id, Endpoint: endpoint, Container: c.ID}}
					d.stop(h.Clock.Now(), max(0, stopAt.Sub(h.Clock.Now())), !h.Clock.Now().Before(stopAt))
					attempts[key] = d
				}
				// A previously stopped object may have started while another obligation was settling.
				if *c.State.Running {
					d.state.Released = false
				}
			}
		}
		known := obligations()
		for _, c := range liveContainers {
			known = append(known, c)
		}
		h.dockerProgress.set("unconfirmed"+orphanNote, append(known, failures...))
		for _, d := range attempts {
			if d.state.Released {
				continue
			}
			if _, e := d.reconcile(); e != nil {
				failures = append(failures, fmt.Sprintf("Docker attempt %s container %s: %v", d.state.Identity, d.state.Container, e))
			}
		}
		left := append(obligations(), failures...)
		for _, c := range liveContainers {
			left = append(left, c)
		}
		slices.Sort(left)
		h.dockerProgress.set("unconfirmed"+orphanNote, left)
		if ctx.Err() != nil {
			return fmt.Errorf("Docker cleanup unconfirmed: %s: %w", strings.Join(left, "; "), ctx.Err())
		}
		if len(left) == 0 {
			h.dockerProgress.set("confirmed (no labeled execution; pending operations settled)"+orphanNote, nil)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Docker cleanup unconfirmed: %s: %w", strings.Join(left, "; "), ctx.Err())
		case <-wake.C:
		}
	}
}
