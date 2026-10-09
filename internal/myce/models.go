package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type Model struct {
	Name      string   `yaml:"name" json:"name"`
	Engine    string   `yaml:"engine" json:"engine"`
	Path      string   `yaml:"path" json:"path"`
	MemoryGB  float64  `yaml:"memory_gb" json:"memory_gb"`
	Platforms []string `yaml:"platforms" json:"platforms"`
	Args      []string `yaml:"args,omitempty" json:"args,omitempty"`
}
type ModelConfig struct {
	Models []Model       `yaml:"models"`
	Idle   time.Duration `yaml:"idle_timeout"`
	Wait   time.Duration `yaml:"max_wait"`
}

func ReadModels(path string) (ModelConfig, error) {
	c := ModelConfig{Idle: 10 * time.Minute, Wait: 5 * time.Minute}
	if path == "" {
		return c, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return c, errors.New("expected one model config")
	}
	return c, validateModels(c)
}
func validateModels(c ModelConfig) error {
	if c.Idle <= 0 || c.Wait <= 0 {
		return errors.New("model timeouts must be positive")
	}
	seen := map[string]bool{}
	for _, m := range c.Models {
		if m.Name == "" || seen[m.Name] || m.Path == "" || m.MemoryGB <= 0 || math.IsNaN(m.MemoryGB) || math.IsInf(m.MemoryGB, 0) || len(m.Platforms) == 0 {
			return errors.New("models require unique names, path, positive memory and platforms")
		}
		seen[m.Name] = true
		if m.Engine != "llama.cpp" && m.Engine != "fake" {
			return fmt.Errorf("unsupported engine %q", m.Engine)
		}
		for _, arg := range m.Args {
			for _, reserved := range []string{"--host", "--port", "--model", "--alias", "-m"} {
				if arg == reserved || strings.HasPrefix(arg, reserved+"=") {
					return fmt.Errorf("model args must not override %s", reserved)
				}
			}
		}
		for _, p := range m.Platforms {
			if !validGPU(p) {
				return fmt.Errorf("invalid model platform %q", p)
			}
		}
	}
	return nil
}

type ModelSlot struct {
	Model       string    `json:"model"`
	Machine     string    `json:"machine"`
	Session     string    `json:"session"`
	State       string    `json:"state"`
	Priority    string    `json:"priority"`
	LastUsed    time.Time `json:"last_used"`
	InFlight    int       `json:"calls_in_flight"`
	Error       string    `json:"error,omitempty"`
	IdleSeconds float64   `json:"idle_seconds"`
}
type ModelReport struct {
	Name  string `json:"name"`
	State string `json:"state"`
	Error string `json:"error,omitempty"`
}
type ModelCall struct {
	ID, Model, Project, Priority     string
	Deadline                         *time.Time
	Queued                           time.Time
	Machine, Session, Exclude, Error string
}

func slotKey(machine, model string) string { return machine + "/" + model }
func (q *Queue) ConfigureModels(c ModelConfig) error {
	if e := validateModels(c); e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.models = c
	q.calls = map[string]*ModelCall{}
	if q.s.ModelSlots == nil {
		q.s.ModelSlots = map[string]*ModelSlot{}
	}
	// HTTP requests do not survive queue restart. Reconnected workers reconcile slots.
	for _, s := range q.s.ModelSlots {
		s.InFlight = 0
	}
	return nil
}
func (q *Queue) model(name string) (Model, bool) {
	for _, m := range q.models.Models {
		if m.Name == name {
			return m, true
		}
	}
	return Model{}, false
}
func (q *Queue) modelMemory(machine string) float64 {
	n := 0.0
	for _, s := range q.s.ModelSlots {
		if s.Machine == machine {
			m, _ := q.model(s.Model)
			n += m.MemoryGB
		}
	}
	return n
}

// Native free memory already reflects resident and unloading engines. Until a
// load is reported resident, reserve its declared budget conservatively. For
// configured budgets none of the model usage is included in the report.
func (q *Queue) modelReservation(m *Machine, reports []ModelReport) float64 {
	if !m.MemoryMeasured {
		return q.modelMemory(m.ID)
	}
	resident := map[string]bool{}
	for _, r := range reports {
		if contains([]string{"ready", "draining", "unloading", "unload_failed"}, r.State) {
			resident[r.Name] = true
		}
	}
	reservation := 0.0
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && !resident[s.Model] {
			model, _ := q.model(s.Model)
			reservation += model.MemoryGB
		}
	}
	return reservation
}
func (q *Queue) reconcileModels(m *Machine, reports []ModelReport) {
	if m.ModelURL == "" {
		return
	}
	for key, s := range q.s.ModelSlots {
		if s.Machine != m.ID {
			continue
		}
		if s.Session != m.Session {
			delete(q.s.ModelSlots, key)
			continue
		}
		for _, r := range reports {
			if r.Name == s.Model {
				if r.State == "failed" {
					for _, c := range q.calls {
						if c.Model == s.Model && c.Machine == "" {
							c.Error = "model load failed: " + r.Error
						}
					}
					delete(q.s.ModelSlots, key)
				} else if r.State == "unload_failed" {
					s.State = "unload_failed"
					s.Error = r.Error
				} else if r.State == "ready" && s.State == "loading" {
					s.State = "ready"
					s.LastUsed = q.clock.Now()
				} else if r.State == "unloaded" && s.State == "draining" && s.InFlight == 0 {
					delete(q.s.ModelSlots, key)
				}
			}
		}
	}
	hints := []string{}
	for _, name := range m.Models {
		if _, managed := q.model(name); !managed {
			hints = append(hints, name)
		}
	}
	m.Models = hints
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && s.State == "ready" {
			m.Models = append(m.Models, s.Model)
		}
	}
}
func (q *Queue) maintainModels() {
	if q.s.ModelSlots == nil {
		q.s.ModelSlots = map[string]*ModelSlot{}
	}
	now := q.clock.Now()
	for k, s := range q.s.ModelSlots {
		m := q.s.Machines[s.Machine]
		if m == nil || m.Gone || m.Session != s.Session {
			delete(q.s.ModelSlots, k)
			continue
		}
		if m.Leaving || s.State == "ready" && s.InFlight == 0 && now.Sub(s.LastUsed) >= q.models.Idle {
			s.State = "draining"
		}
	}
	if q.networkQuiet {
		return
	}
	pending := []*ModelCall{}
	for _, c := range q.calls {
		if c.Machine == "" && c.Error == "" {
			pending = append(pending, c)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return before(callJob(pending[i]), callJob(pending[j]), &Machine{}) })
	for _, c := range pending {
		model, ok := q.model(c.Model)
		if !ok {
			c.Error = "unknown model"
			continue
		}
		// Ready affinity precedes loading a second copy.
		machines := []*Machine{}
		for _, m := range q.s.Machines {
			machines = append(machines, m)
		}
		sort.Slice(machines, func(i, j int) bool { return machines[i].ID < machines[j].ID })
		for _, m := range machines {
			if m.Gone || m.Leaving || m.ID == c.Exclude || m.ModelURL == "" || !contains(model.Platforms, m.GPU) || q.unreported[m.ID] {
				continue
			}
			s := q.s.ModelSlots[slotKey(m.ID, c.Model)]
			if s != nil && s.State == "ready" && !q.batchBeforeCall(c, m) {
				c.Machine = m.ID
				c.Session = m.Session
				if s.InFlight == 0 || rank(c.Priority) < rank(s.Priority) {
					s.Priority = c.Priority
				}
				s.InFlight++
				s.LastUsed = now
				break
			}
		}
		if c.Machine != "" {
			continue
		}

		eligible := []*Machine{}
		loading := false
		for _, m := range machines {
			if m.Gone || m.Leaving || m.ID == c.Exclude || m.ModelURL == "" || !contains(model.Platforms, m.GPU) || q.unreported[m.ID] || q.batchBeforeCall(c, m) {
				continue
			}
			if s := q.s.ModelSlots[slotKey(m.ID, c.Model)]; s != nil {
				if s.State == "loading" {
					if rank(c.Priority) < rank(s.Priority) {
						s.Priority = c.Priority
					}
					loading = true
				}
				continue
			}
			eligible = append(eligible, m)
		}
		if loading {
			continue
		}
		allocated := false
		// Check all free workers before draining a model or stopping any batch.
		for _, m := range eligible {
			if m.FreeMemoryGB >= model.MemoryGB {
				q.s.ModelSlots[slotKey(m.ID, c.Model)] = &ModelSlot{Model: c.Model, Machine: m.ID, Session: m.Session, State: "loading", Priority: c.Priority, LastUsed: now}
				m.FreeMemoryGB -= model.MemoryGB
				allocated = true
				break
			}
		}
		if allocated {
			continue
		}
		for _, m := range eligible {
			possible := m.FreeMemoryGB
			for _, s := range q.s.ModelSlots {
				if s.Machine == m.ID && s.State != "unload_failed" && rank(s.Priority) > rank(c.Priority) {
					mm, _ := q.model(s.Model)
					possible += mm.MemoryGB
				}
			}
			var victim *Job
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.State == "running" && j.Spec.Resumable && !m.CheckpointUnavailable && checkpointAvailable(q.root) && rank(j.Spec.Priority) > rank(c.Priority) {
					victim = j
					possible += j.Spec.Needs.MemoryGB
				}
			}
			if possible < model.MemoryGB {
				continue
			}
			q.makeModelRoom(m, model.MemoryGB, c.Priority)
			if victim != nil && m.FreeMemoryGB+q.modelMemory(m.ID) < model.MemoryGB {
				q.stop(victim, "making room for "+c.Priority+" model call")
			}
			// Prefer draining models alone; if that cannot suffice, save the batch.
			reclaimable := m.FreeMemoryGB
			for _, s := range q.s.ModelSlots {
				if s.Machine == m.ID && s.State == "draining" {
					mm, _ := q.model(s.Model)
					reclaimable += mm.MemoryGB
				}
			}
			if reclaimable < model.MemoryGB && victim != nil {
				q.stop(victim, "making room for "+c.Priority+" model call")
			}
			break
		}

	}

	if !checkpointAvailable(q.root) {
		return
	}
	waiting := []*Job{}
	for _, j := range q.s.Jobs {
		if j.State == "queued" {
			waiting = append(waiting, j)
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return before(waiting[i], waiting[j], &Machine{}) })
	for _, j := range waiting {
		room := false
		for _, m := range q.s.Machines {
			busy := false
			for _, r := range q.s.Jobs {
				if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") {
					busy = true
				}
			}
			if !busy && fits(j, m, m.FreeMemoryGB) {
				room = true
				break
			}
		}
		if room {
			continue
		}
		machines := []*Machine{}
		for _, m := range q.s.Machines {
			machines = append(machines, m)
		}
		sort.Slice(machines, func(i, j int) bool { return machines[i].ID < machines[j].ID })
		for _, m := range machines {
			possible := m.FreeMemoryGB
			for _, s := range q.s.ModelSlots {
				if s.Machine == m.ID && s.State != "unload_failed" && rank(s.Priority) > rank(j.Spec.Priority) {
					mm, _ := q.model(s.Model)
					possible += mm.MemoryGB
				}
			}
			if m.FreeMemoryGB < j.Spec.Needs.MemoryGB && fits(j, m, possible) {
				q.makeModelRoom(m, j.Spec.Needs.MemoryGB, j.Spec.Priority)
				break
			}
		}
	}

}
func callJob(c *ModelCall) *Job {
	j := &Job{ID: c.ID, Queued: c.Queued, Spec: Spec{Priority: c.Priority, Deadline: c.Deadline}}
	j.Spec.Needs.Model = c.Model
	return j
}

// A pending request may precede a batch only on a worker where it can actually
// run or make room. Counting every resident model incorrectly blocks batches
// behind requests that cannot reclaim higher-priority work.
func (q *Queue) callCanUse(m *Machine, c *ModelCall) bool {
	model, ok := q.model(c.Model)
	if !ok || c.Machine != "" || c.Error != "" || m.Gone || m.Leaving || m.ID == c.Exclude || m.ModelURL == "" || !contains(model.Platforms, m.GPU) || q.unreported[m.ID] {
		return false
	}
	if s := q.s.ModelSlots[slotKey(m.ID, c.Model)]; s != nil && s.State == "ready" {
		return true
	}
	possible := m.FreeMemoryGB
	for _, s := range q.s.ModelSlots {
		if s.Machine != m.ID || s.State == "unload_failed" {
			continue
		}
		if s.Model == c.Model && s.State == "loading" {
			return true
		}
		if s.InFlight == 0 && rank(s.Priority) > rank(c.Priority) {
			resident, _ := q.model(s.Model)
			possible += resident.MemoryGB
		}
	}
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && j.State == "running" && j.Spec.Resumable && !m.CheckpointUnavailable && checkpointAvailable(q.root) && rank(j.Spec.Priority) > rank(c.Priority) {
			possible += j.Spec.Needs.MemoryGB
		}
	}
	return possible >= model.MemoryGB
}
func (q *Queue) batchBeforeCall(c *ModelCall, m *Machine) bool {
	for _, j := range q.s.Jobs {
		memory := m.FreeMemoryGB
		for _, s := range q.s.ModelSlots {
			if s.Machine == m.ID && s.State != "unload_failed" && rank(s.Priority) > rank(j.Spec.Priority) {
				model, _ := q.model(s.Model)
				memory += model.MemoryGB
			}
		}
		if j.State == "queued" && fits(j, m, memory) && before(j, callJob(c), m) {
			return true
		}
	}
	return false
}
func (q *Queue) makeModelRoom(m *Machine, need float64, priority string) {
	slots := []*ModelSlot{}
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && s.State != "unload_failed" && rank(s.Priority) > rank(priority) {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool {
		if rank(slots[i].Priority) != rank(slots[j].Priority) {
			return rank(slots[i].Priority) > rank(slots[j].Priority)
		}
		return slots[i].Model < slots[j].Model
	})
	room := m.FreeMemoryGB
	for _, s := range slots {
		if room >= need {
			break
		}
		s.State = "draining"
		model, _ := q.model(s.Model)
		room += model.MemoryGB
	}
}
func (q *Queue) modelPlan(machine string) []ModelSlot {
	out := []ModelSlot{}
	for _, s := range q.s.ModelSlots {
		if s.Machine == machine {
			out = append(out, *s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}
func (q *Queue) addCall(c *ModelCall) error {
	return q.update(func() error {
		if _, ok := q.model(c.Model); !ok {
			return errors.New("unknown model")
		}
		c.ID = ID()
		c.Queued = q.clock.Now()
		q.calls[c.ID] = c
		q.maintain()
		return nil
	})
}
func (q *Queue) finishCall(id string) error {
	return q.update(func() error {
		c := q.calls[id]
		if c != nil {
			if s := q.s.ModelSlots[slotKey(c.Machine, c.Model)]; s != nil && s.Session == c.Session {
				s.InFlight = max(0, s.InFlight-1)
				s.LastUsed = q.clock.Now()
			}
			delete(q.calls, id)
			q.refreshModelPriority(c)
		}
		q.maintain()
		return nil
	})
}

// Only current use protects a model from reclamation. A completed high-priority
// request must not protect remaining experiments (or an entirely idle model).
func (q *Queue) refreshModelPriority(c *ModelCall) {
	s := q.s.ModelSlots[slotKey(c.Machine, c.Model)]
	if s == nil || s.Session != c.Session {
		return
	}
	s.Priority = "experiment"
	for _, live := range q.calls {
		if live.Machine == c.Machine && live.Session == c.Session && live.Model == c.Model && rank(live.Priority) < rank(s.Priority) {
			s.Priority = live.Priority
		}
	}
}
func modelError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message, "type": "mycelium_error"}})
}
func (a API) modelCall(w http.ResponseWriter, r *http.Request) {
	data, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if e != nil {
		modelError(w, 400, e.Error())
		return
	}
	var body struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if e = json.Unmarshal(data, &body); e != nil {
		modelError(w, 400, "invalid OpenAI JSON request")
		return
	}
	c := &ModelCall{Model: body.Model, Project: r.Header.Get("X-Myce-Project"), Priority: r.Header.Get("X-Myce-Priority")}
	if c.Project == "" || rank(c.Priority) == 3 {
		modelError(w, 400, "X-Myce-Project and X-Myce-Priority are required")
		return
	}
	if c.Priority == "deadline" {
		d, e := time.Parse(time.RFC3339, r.Header.Get("X-Myce-Deadline"))
		if e != nil {
			modelError(w, 400, "deadline priority requires X-Myce-Deadline (RFC3339)")
			return
		}
		c.Deadline = &d
	}
	if body.Stream && r.URL.Path != "/v1/chat/completions" {
		modelError(w, 400, "streaming is supported for chat only")
		return
	}
	if e = a.Queue.addCall(c); e != nil {
		modelError(w, 400, e.Error())
		return
	}
	defer a.Queue.finishCall(c.ID)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	attempt := 0
	for {
		if r.Context().Err() != nil {
			return
		}
		var call ModelCall
		var target string
		var timedOut bool
		e = a.Queue.update(func() error {
			a.Queue.maintain()
			call = *a.Queue.calls[c.ID]
			timedOut = a.Queue.clock.Now().Sub(call.Queued) >= a.Queue.models.Wait
			if m := a.Queue.s.Machines[call.Machine]; m != nil && !m.Gone && m.Session == call.Session {
				target = m.ModelURL
			}
			return nil
		})
		if e != nil {
			modelError(w, 503, e.Error())
			return
		}
		if call.Error != "" {
			modelError(w, 503, call.Error)
			return
		}
		if timedOut {
			modelError(w, 504, "maximum model wait exceeded")
			return
		}
		if target == "" && call.Machine != "" {
			if attempt >= 1 {
				modelError(w, 503, "model call failed after one retry on another worker")
				return
			}
			attempt++
			if e = a.Queue.retryCall(c.ID); e != nil {
				modelError(w, 503, e.Error())
				return
			}
			continue
		}
		if target != "" {
			proxyCtx, cancel := context.WithCancel(r.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				wake := time.NewTicker(20 * time.Millisecond)
				defer wake.Stop()
				for {
					select {
					case <-proxyCtx.Done():
						return
					case <-wake.C:
						a.Queue.mu.Lock()
						m := a.Queue.s.Machines[call.Machine]
						lost := m == nil || m.Gone || m.Session != call.Session
						a.Queue.mu.Unlock()
						if lost {
							cancel()
							return
						}
					}
				}
			}()
			req, e := http.NewRequestWithContext(proxyCtx, "POST", target+r.URL.Path, bytes.NewReader(data))
			if e != nil {
				cancel()
				<-done
				modelError(w, 503, e.Error())
				return
			}
			req.Header.Set("Authorization", "Bearer "+a.Token)
			req.Header.Set("X-Myce-Session", call.Session)
			req.Header.Set("Content-Type", "application/json")
			res, e := http.DefaultClient.Do(req)
			if e == nil && res.StatusCode < 500 {
				if !body.Stream {
					response, readErr := io.ReadAll(res.Body)
					if readErr == nil {
						w.Header().Set("Content-Type", res.Header.Get("Content-Type"))
						w.WriteHeader(res.StatusCode)
						_, _ = w.Write(response)
						res.Body.Close()
						cancel()
						<-done
						return
					}
					e = readErr // Buffer non-stream responses so a lost worker can be retried safely.
				} else {
					started := false
					buf := make([]byte, 16384)
					for {
						n, readErr := res.Body.Read(buf)
						if n > 0 {
							if !started {
								w.Header().Set("Content-Type", res.Header.Get("Content-Type"))
								w.WriteHeader(res.StatusCode)
								started = true
							}
							if _, e = w.Write(buf[:n]); e != nil {
								res.Body.Close()
								cancel()
								<-done
								return
							}
							if f, ok := w.(http.Flusher); ok {
								f.Flush()
							}
						}
						if readErr != nil {
							if readErr != io.EOF && started {
								_, _ = fmt.Fprint(w, "\ndata: {\"error\":{\"message\":\"worker lost after streaming response started; unsafe to retry\"}}\n\n")
								if f, ok := w.(http.Flusher); ok {
									f.Flush()
								}
							}
							if started || readErr == io.EOF {
								if !started {
									w.Header().Set("Content-Type", res.Header.Get("Content-Type"))
									w.WriteHeader(res.StatusCode)
								}
								res.Body.Close()
								cancel()
								<-done
								return
							}
							e = readErr
							break
						}
					}
				}
			}
			if res != nil {
				res.Body.Close()
			}
			cancel()
			<-done
			if r.Context().Err() != nil {
				return
			}
			if attempt >= 1 {
				modelError(w, 503, "model call failed after one retry on another worker")
				return
			}
			attempt++
			e = a.Queue.retryCall(c.ID)
			if e != nil {
				modelError(w, 503, e.Error())
				return
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
	}
}

func (q *Queue) retryCall(id string) error {
	return q.update(func() error {
		live := q.calls[id]
		if live == nil {
			return errors.New("model call no longer active")
		}
		previous := *live
		if s := q.s.ModelSlots[slotKey(live.Machine, live.Model)]; s != nil && s.Session == live.Session {
			s.InFlight = max(0, s.InFlight-1)
			s.LastUsed = q.clock.Now()
		}
		live.Exclude = live.Machine
		live.Machine = ""
		live.Session = ""
		q.refreshModelPriority(&previous)
		q.maintain()
		return nil
	})
}

func modelConfigHash(c ModelConfig) string {
	models := append([]Model(nil), c.Models...)
	sort.Slice(models, func(i, j int) bool { return models[i].Name < models[j].Name })
	data, _ := json.Marshal(models)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
