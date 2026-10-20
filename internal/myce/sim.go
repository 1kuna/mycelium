package myce

// The simulator owns execution timers, never placement or priority decisions.
import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"slices"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

type SimFleet struct {
	Hosts    map[string]HostPolicy `yaml:"hosts"`
	Machines []SimMachine          `yaml:"machines"`
}
type SimMachine struct {
	Name      string      `yaml:"name"`
	Host      string      `yaml:"host"`
	Platform  string      `yaml:"platform"`
	MemoryGB  float64     `yaml:"memory_gb"`
	HostGB    float64     `yaml:"host_memory_gb"`
	Unified   bool        `yaml:"unified"`
	Speed     float64     `yaml:"speed"`
	Departure string      `yaml:"departure"` // planned (default) or loss
	Windows   []SimWindow `yaml:"windows"`
}
type SimWindow struct {
	Days  []int   `yaml:"days"` // Monday=0 through Sunday=6
	Start float64 `yaml:"start_hour"`
	End   float64 `yaml:"end_hour"`
}
type SimWorkload struct {
	Policy   *PriorityPolicy          `yaml:"policy,omitempty"`
	Jitter   *float64                 `yaml:"duration_jitter"`
	Projects map[string]ProjectPolicy `yaml:"projects"`
	Models   []SimModel               `yaml:"models"`
	Streams  []SimStream              `yaml:"streams"`
}
type SimModel struct {
	Name        string   `yaml:"name"`
	MemoryGB    float64  `yaml:"memory_gb"`
	HostGB      float64  `yaml:"host_memory_gb"`
	SizeBytes   int64    `yaml:"size_bytes"`
	LoadSeconds float64  `yaml:"load_seconds"`
	Platforms   []string `yaml:"platforms"`
}
type SimStream struct {
	Name       string   `yaml:"name"`
	Project    string   `yaml:"project"`
	Priority   string   `yaml:"priority"`
	Class      string   `yaml:"class,omitempty"`
	Model      string   `yaml:"model"`
	Call       bool     `yaml:"call"`
	Platforms  []string `yaml:"platforms"`
	MemoryGB   float64  `yaml:"memory_gb"`
	HostGB     float64  `yaml:"host_memory_gb"`
	Duration   float64  `yaml:"duration_seconds"`
	Checkpoint float64  `yaml:"checkpoint_seconds"`
	Every      float64  `yaml:"every_hours"`
	Start      float64  `yaml:"start_hour"`
	Count      int      `yaml:"count"`
	Deadline   float64  `yaml:"deadline_seconds"`
	Batch      int      `yaml:"batch_files"` // >1 enables paired one-job/per-file comparison
}
type SimOptions struct {
	Days  float64
	Seed  int64
	Split bool
}
type SimWait struct {
	Median    float64 `json:"median_seconds"`
	P95       float64 `json:"p95_seconds"`
	Max       float64 `json:"max_seconds"`
	Started   int     `json:"started"`
	Unstarted int     `json:"unstarted"`
}
type SimProjectMetric struct {
	Weight                  float64 `json:"weight"`
	GPUHours                float64 `json:"gpu_hours"`
	Share                   float64 `json:"share_percent"`
	WeightedShare           float64 `json:"weight_percent"`
	ExperimentGPUHours      float64 `json:"experiment_gpu_hours"`
	ExperimentShare         float64 `json:"experiment_share_percent"`
	ExperimentCompleted     int     `json:"experiments_completed"`
	ExperimentPerDay        float64 `json:"experiments_per_day"`
	ExperimentFinishSeconds float64 `json:"experiment_mean_submit_to_finish_seconds"`
}
type SimMachineMetric struct {
	AvailableHours   float64 `json:"available_gpu_hours"`
	BusyHours        float64 `json:"busy_gpu_hours"`
	LoadingHours     float64 `json:"loading_gpu_hours"`
	Utilization      float64 `json:"utilization_percent"`
	IdleWaitingHours float64 `json:"idle_fit_waiting_gpu_hours"`
}
type SimBatchMetric struct {
	SubmittedSeconds float64  `json:"submitted_seconds"`
	FinishedSeconds  *float64 `json:"finished_seconds"`
}
type SimReport struct {
	Batches                   map[string]SimBatchMetric    `json:"batches"`
	Days                      float64                      `json:"days"`
	Seed                      int64                        `json:"seed"`
	Split                     bool                         `json:"split"`
	Production                SimWait                      `json:"production_wait"`
	DeadlineMisses            int                          `json:"deadline_misses"`
	Submitted                 int                          `json:"submitted"`
	Completed                 int                          `json:"completed"`
	Unfinished                int                          `json:"unfinished"`
	Failed                    int                          `json:"failed"`
	NeverPlaceable            []string                     `json:"never_placeable"`
	Projects                  map[string]*SimProjectMetric `json:"projects"`
	Machines                  map[string]*SimMachineMetric `json:"machines"`
	ModelLoads                int                          `json:"model_loads"`
	ModelReloads              int                          `json:"model_reloads"`
	LoadingSeconds            float64                      `json:"loading_seconds"`
	Preemptions               int                          `json:"preemptions"`
	LostSeconds               float64                      `json:"preemption_lost_seconds"`
	LeaveLostSeconds          float64                      `json:"leave_lost_seconds"`
	InfrastructureLostSeconds float64                      `json:"infrastructure_lost_seconds"`
	IdleWaitingHours          float64                      `json:"idle_fit_waiting_gpu_hours"`
	BatchCompleted            int                          `json:"batches_completed"`
	BatchUnfinished           int                          `json:"batches_unfinished"`
	BatchMeanSeconds          float64                      `json:"batch_mean_submit_to_finish_seconds"`
}

func ReadSimYAML(path string, out any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	d := yaml.NewDecoder(io.LimitReader(f, 1<<20))
	d.KnownFields(true)
	if e = d.Decode(out); e != nil {
		return e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("expected one simulation YAML document")
	}
	return nil
}

type simClock struct{ now time.Time }

func (c *simClock) Now() time.Time { return c.now }

type simTask struct {
	stream                                            SimStream
	at, timeStarted, timeFinished                     float64
	started, finished, failed, placeable              bool
	id                                                string
	work, done, segment, segmentAt, loadUntil, stopAt float64
	machine, batch                                    string
	attempt                                           int
}
type simLoad struct {
	until float64
	seen  bool
}
type simWorker struct {
	spec    SimMachine
	online  bool
	session int
	task    *simTask
	loads   map[string]*simLoad
	cached  map[string]bool
}
type simArrival struct {
	at   float64
	task *simTask
}
type simBatch struct {
	at, finish float64
	remaining  int
}

func validSimNumber(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) && x >= 0 }
func validateSim(f SimFleet, w SimWorkload, o SimOptions) error {
	if !validSimNumber(o.Days) || o.Days <= 0 || o.Days > 90 {
		return errors.New("days must be in (0,90]")
	}
	if len(f.Machines) == 0 || len(w.Streams) == 0 {
		return errors.New("fleet and streams must not be empty")
	}
	if w.Jitter != nil && (!validSimNumber(*w.Jitter) || *w.Jitter > 1) {
		return errors.New("duration_jitter must be in [0,1]")
	}
	seen := map[string]bool{}
	for _, m := range f.Machines {
		if m.Name == "" || seen[m.Name] || !validGPU(m.Platform) || !validSimNumber(m.MemoryGB) || m.MemoryGB == 0 || !validSimNumber(m.HostGB) || !validSimNumber(m.Speed) || m.Speed == 0 {
			return errors.New("machines require unique name, platform, positive memory and speed")
		}
		seen[m.Name] = true
		if m.Departure != "" && m.Departure != "planned" && m.Departure != "loss" {
			return errors.New("departure must be planned or loss")
		}
		for _, v := range m.Windows {
			if len(v.Days) == 0 || !validSimNumber(v.Start) || !validSimNumber(v.End) || v.Start >= v.End || v.End > 24 {
				return errors.New("availability requires days and 0 <= start < end <= 24")
			}
			for _, d := range v.Days {
				if d < 0 || d > 6 {
					return errors.New("days are Monday=0 through Sunday=6")
				}
			}
		}
	}
	models := map[string]bool{}
	for _, m := range w.Models {
		if m.Name == "" || models[m.Name] || !validSimNumber(m.MemoryGB) || m.MemoryGB == 0 || !validSimNumber(m.HostGB) || !validSimNumber(m.LoadSeconds) || m.SizeBytes < 0 || len(m.Platforms) == 0 {
			return errors.New("invalid simulation model")
		}
		for _, p := range m.Platforms {
			if !validGPU(p) {
				return errors.New("invalid model platform")
			}
		}
		models[m.Name] = true
	}
	seen = map[string]bool{}
	if w.Policy != nil && w.Projects != nil && w.Policy.Projects != nil {
		return errors.New("use policy.projects or projects, not both")
	}
	for _, s := range w.Streams {
		identity := Spec{Project: s.Project, Priority: s.Priority, Class: s.Class}
		if err := normalizeJobClass(&identity); err != nil {
			return err
		}
		if s.Call && s.Class != "" && s.Class != s.Priority {
			return errors.New("custom model-call classes are not supported")
		}
		if s.Name == "" || seen[s.Name] || s.Project == "" || (s.Class == "" && legacyRank(s.Priority) == 3) || !validSimNumber(s.Duration) || s.Duration <= 0 || !validSimNumber(s.Every) || s.Every <= 0 || !validSimNumber(s.Start) || !validSimNumber(s.MemoryGB) || !validSimNumber(s.HostGB) || !validSimNumber(s.Checkpoint) || !validSimNumber(s.Deadline) || s.Count < 1 || s.Batch < 0 {
			return errors.New("invalid stream: require unique name, project, priority, positive duration/every/count")
		}
		seen[s.Name] = true
		if s.Model != "" && !models[s.Model] {
			return fmt.Errorf("unknown stream model %s", s.Model)
		}
		if s.Call && (s.Model == "" || s.Batch > 1 || s.Checkpoint > 0) {
			return errors.New("calls require a model and cannot checkpoint or batch")
		}
		if !s.Call && (s.MemoryGB <= 0 || len(s.Platforms) == 0) {
			return errors.New("batch streams require memory and platforms")
		}
		for _, p := range s.Platforms {
			if !validGPU(p) {
				return errors.New("invalid stream platform")
			}
		}
		if s.Priority == "deadline" && s.Deadline <= 0 {
			return errors.New("deadline stream requires positive deadline_seconds")
		}
	}
	return nil
}
func simOnline(m SimMachine, t float64) bool {
	if len(m.Windows) == 0 {
		return true
	}
	day := int(t/86400) % 7
	hour := math.Mod(t, 86400) / 3600
	for _, v := range m.Windows {
		if slices.Contains(v.Days, day) && hour >= v.Start && hour < v.End {
			return true
		}
	}
	return false
}
func simResult(t *simTask, w *simWorker, success bool) Result {
	return Result{Execution: Execution{ID: t.id, Attempt: t.attempt}, Machine: w.spec.Name, Session: fmt.Sprint(w.session), Success: success}
}

// Simulate executes real queue methods in a single deterministic event loop.
// No wall-clock waits, engines, HTTP listeners, child processes or GPU probes.
func Simulate(f SimFleet, w SimWorkload, o SimOptions) (SimReport, error) {
	r := SimReport{Days: o.Days, Seed: o.Seed, Split: o.Split, Projects: map[string]*SimProjectMetric{}, Machines: map[string]*SimMachineMetric{}, NeverPlaceable: []string{}, Batches: map[string]SimBatchMetric{}}
	if e := validateSim(f, w, o); e != nil {
		return r, e
	}
	clock := &simClock{time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	epoch := clock.now
	// A pretend NAS avoids filesystem probes; persistence is disabled only here.
	root := "/simulated-checkpoints"
	var e error
	q := &Queue{simulation: true, clock: clock, root: root, productionGrace: 10 * time.Second, productionInfrastructureRetries: DefaultProductionInfrastructureRetries, lossTimeouts: [3]time.Duration{ProductionLossAfter, DeadlineLossAfter, LossAfter}, unreported: map[string]bool{}, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}}}
	if e = q.ConfigureHosts(f.Hosts); e != nil {
		return r, e
	}
	models := map[string]SimModel{}
	mc := ModelConfig{Idle: 10 * time.Minute, Wait: 5 * time.Minute}
	for _, m := range w.Models {
		models[m.Name] = m
		mc.Models = append(mc.Models, Model{Name: m.Name, Engine: "fake", Path: "simulated", MemoryGB: m.MemoryGB, HostMemoryGB: m.HostGB, SizeBytes: m.SizeBytes, Platforms: m.Platforms, MaxInFlight: 1})
	}
	if e = q.ConfigureModels(mc); e != nil {
		return r, e
	}
	if e = q.ConfigureProjects(w.Projects); e != nil {
		return r, e
	}
	if w.Policy != nil {
		if _, e = q.SetPriorityPolicy(q.policyRevision(), *w.Policy); e != nil {
			return r, e
		}
	}

	workers := []*simWorker{}
	f.Machines = append([]SimMachine(nil), f.Machines...)
	sort.Slice(f.Machines, func(i, j int) bool { return f.Machines[i].Name < f.Machines[j].Name })
	for _, m := range f.Machines {
		cached := map[string]bool{}
		for _, model := range w.Models {
			if slices.Contains(model.Platforms, m.Platform) {
				cached[model.Name] = true
			}
		}
		workers = append(workers, &simWorker{spec: m, loads: map[string]*simLoad{}, cached: cached})
		r.Machines[m.Name] = &SimMachineMetric{}
	}
	ensureProject := func(p string) *SimProjectMetric {
		if r.Projects[p] == nil {
			r.Projects[p] = &SimProjectMetric{Weight: q.weight(p)}
		}
		return r.Projects[p]
	}
	rng := rand.New(rand.NewSource(o.Seed))
	horizon := o.Days * 86400
	arrivals := []simArrival{}
	tasks := []*simTask{}
	batches := map[string]*simBatch{}
	boundaries := []float64{horizon}
	for _, worker := range workers {
		for day := 0; float64(day)*86400 < horizon; day++ {
			for _, v := range worker.spec.Windows {
				if slices.Contains(v.Days, day%7) {
					boundaries = append(boundaries, float64(day)*86400+v.Start*3600, float64(day)*86400+v.End*3600)
				}
			}
		}
	}
	for _, s := range w.Streams {
		ensureProject(s.Project)
		for at := s.Start * 3600; at < horizon; at += s.Every * 3600 {
			for n := 0; n < s.Count; n++ {
				count := max(1, s.Batch)
				durations := make([]float64, count)
				total := 0.0
				for k := range durations {
					jitter := 0.2
					if w.Jitter != nil {
						jitter = *w.Jitter
					}
					durations[k] = s.Duration * (1 - jitter + 2*jitter*rng.Float64())
					total += durations[k]
				}
				key := ""
				if s.Batch > 1 {
					key = fmt.Sprintf("%s-%.0f-%d", s.Name, at, n)
					batches[key] = &simBatch{at: at, remaining: 1}
					if o.Split {
						batches[key].remaining = count
					}
				}
				if s.Batch <= 1 || !o.Split {
					durations = []float64{total}
				}
				for _, duration := range durations {
					t := &simTask{stream: s, at: at, work: duration, batch: key, stopAt: math.Inf(1)}
					if len(tasks) >= 20000 {
						return r, errors.New("simulation exceeds 20000 jobs; reduce days or arrival count")
					}
					tasks = append(tasks, t)
					arrivals = append(arrivals, simArrival{at, t})
				}
			}
		}
	}
	if len(tasks) > 20000 {
		return r, errors.New("simulation exceeds 20000 jobs; reduce days or arrival count")
	}
	sort.SliceStable(arrivals, func(i, j int) bool { return arrivals[i].at < arrivals[j].at })
	sort.Float64s(boundaries)
	now := 0.0
	ai := 0
	bi := 0
	loadsSeen := map[string]bool{}
	callSerial := 0
	waits := []float64{}
	finish := func(t *simTask) {
		t.finished = true
		t.timeFinished = now
		r.Completed++
		if t.batch != "" {
			b := batches[t.batch]
			b.remaining--
			b.finish = max(b.finish, now)
		}
		if t.stream.Priority == "experiment" {
			p := ensureProject(t.stream.Project)
			p.ExperimentCompleted++
			p.ExperimentFinishSeconds += now - t.at
		}
	}
	start := func(t *simTask, worker *simWorker, load float64) {
		t.machine = worker.spec.Name
		t.segment = t.done
		t.segmentAt = now
		t.loadUntil = now + load
		t.stopAt = math.Inf(1)
		if !t.started {
			t.started = true
			t.timeStarted = now
			if t.stream.Priority == "production" {
				waits = append(waits, now-t.at)
			}
		}
	}
	machineReport := func(worker *simWorker) Machine {
		m := worker.spec
		report := Machine{ID: m.Name, HostID: m.Host, Session: fmt.Sprint(worker.session), GPU: m.Platform, TotalMemoryGB: m.MemoryGB, FreeMemoryGB: m.MemoryGB, HostMemoryMeasured: true, HostAvailableMemoryGB: m.HostGB, UnifiedMemory: m.Unified, ModelURL: "http://sim.invalid", ModelConfigHash: modelConfigHash(mc), CacheFreeBytes: 1 << 50, CopyBytesPerSecond: 40e6}
		layout := "separate"
		if m.Unified {
			layout = "unified"
		}
		// Synthetic hosts provide explicit CPU facts too; real hosts never infer
		// capacity from the accelerator count or memory size.
		report.CapabilityProfile = &CapabilityProfile{OS: "linux", LogicalCPUs: 8, MemoryLayout: layout, SystemMemoryBytes: uint64(m.HostGB * (1 << 30)), Accelerators: []AcceleratorProfile{{Type: m.Platform, MemoryBytes: uint64(m.MemoryGB * (1 << 30))}}}
		if worker.task != nil {
			report.FreeMemoryGB = max(0, report.FreeMemoryGB-worker.task.stream.MemoryGB)
		}
		for name := range worker.cached {
			report.CachedModels = append(report.CachedModels, name)
		}
		sort.Strings(report.CachedModels)
		return report
	}
	for now <= horizon {
		clock.now = epoch.Add(time.Duration(math.Ceil(now * 1e9)))
		// Continuous healthy heartbeat coverage between meaningful events. This skips
		// repetitive no-op transport polling, not scheduler time or usage accounting.
		for _, worker := range workers {
			if worker.online {
				if m := q.s.Machines[worker.spec.Name]; m != nil {
					m.LastSeen = clock.now
				}
			}
		}
		// Finish timers before departures or timeout maintenance at the same instant.
		for _, worker := range workers {
			if t := worker.task; t != nil && t.loadUntil+(t.work-t.segment)/worker.spec.Speed <= now+1e-7 {
				j := q.s.Jobs[t.id]
				if j.State == "running" {
					if e = q.Complete(simResult(t, worker, true)); e != nil {
						return r, e
					}
					worker.task = nil
					t.machine = ""

					finish(t)
					delete(q.s.Jobs, t.id)
				}
			}
		}
		for _, t := range tasks {
			if t.stream.Call && t.machine != "" && !t.finished && !t.failed {
				for _, worker := range workers {
					if worker.spec.Name == t.machine && t.segmentAt+t.work/worker.spec.Speed <= now+1e-7 {
						if e = q.finishCall(t.id); e != nil {
							return r, e
						}
						finish(t)
						break
					}
				}
			}
		}
		if e = q.Tick(); e != nil {
			return r, e
		}
		for _, worker := range workers {
			online := simOnline(worker.spec, now)
			if worker.online && !online && worker.spec.Departure == "loss" {
				if t := worker.task; t != nil {
					elapsed := max(0, now-t.loadUntil)
					saved := 0.0
					if t.stream.Checkpoint > 0 {
						saved = math.Floor(elapsed/t.stream.Checkpoint) * t.stream.Checkpoint
						t.done = t.segment + saved*worker.spec.Speed
					} else {
						t.done = 0
					}
					r.InfrastructureLostSeconds += elapsed - saved
					worker.task = nil
					t.machine = ""
				}
				for _, t := range tasks {
					if t.stream.Call && !t.finished && !t.failed && t.machine == worker.spec.Name {
						r.InfrastructureLostSeconds += now - t.segmentAt
						if e = q.finishCall(t.id); e != nil {
							return r, e
						}
						t.failed = true
						t.machine = ""
						r.Failed++
					}
				}
				worker.loads = map[string]*simLoad{}
			} else if worker.online && !online {
				if e = q.Leave(worker.spec.Name, true); e != nil {
					return r, e
				}
				if t := worker.task; t != nil {
					t.done = t.segment + max(0, now-t.loadUntil)*worker.spec.Speed
					if t.stream.Checkpoint == 0 {
						r.LeaveLostSeconds += max(0, now-t.loadUntil)
						t.done = 0
					}
					if e = q.Complete(simResult(t, worker, false)); e != nil {
						return r, e
					}
					worker.task = nil
					t.machine = ""
				}
				for _, t := range tasks {
					if t.stream.Call && !t.finished && t.machine == worker.spec.Name {
						if e = q.finishCall(t.id); e != nil {
							return r, e
						}
						t.machine = ""
						t.done = 0
						if e = q.addCall(&ModelCall{ID: t.id, Model: t.stream.Model, Project: t.stream.Project, Priority: t.stream.Priority, Deadline: deadlineAt(epoch, t), Queued: epoch.Add(time.Duration(t.at * 1e9)), WaitingSince: clock.now}); e != nil {
							return r, e
						}
					}
				}
				// One final empty check-in acknowledges departure and model unloads.
				reports := []ModelReport{}
				for _, slot := range q.modelPlan(worker.spec.Name) {
					reports = append(reports, ModelReport{Name: slot.Model, Attempt: slot.Attempt, State: "unloaded"})
				}
				if _, e = q.Checkin(Checkin{Machine: machineReport(worker), ModelReports: reports}); e != nil {
					return r, e
				}
				worker.loads = map[string]*simLoad{}

			}
			if !worker.online && online {
				worker.session++
			}
			worker.online = online
		}
		// Register every newly present worker before submissions/placement.
		for _, worker := range workers {
			if worker.online && q.s.Machines[worker.spec.Name] == nil {
				if _, e = q.Checkin(Checkin{Machine: machineReport(worker)}); e != nil {
					return r, e
				}
			} else if worker.online && (q.s.Machines[worker.spec.Name].Gone || q.s.Machines[worker.spec.Name].Session != fmt.Sprint(worker.session)) {
				if _, e = q.Checkin(Checkin{Machine: machineReport(worker), Fresh: true, PreviousSession: q.s.Machines[worker.spec.Name].Session}); e != nil {
					return r, e
				}
			}
		}
		for ai < len(arrivals) && arrivals[ai].at <= now {
			t := arrivals[ai].task
			ai++
			r.Submitted++
			for _, worker := range workers {
				m := machineReport(worker)
				m.Gone = false
				m.Leaving = false
				m.FreeHostMemoryGB = m.HostAvailableMemoryGB
				j := simSpec(t, epoch)
				if t.stream.Call {
					model := models[t.stream.Model]
					j.Needs.MemoryGB = model.MemoryGB
					j.Needs.HostMemoryGB = model.HostGB
					j.Needs.Platform = Platform{Required: model.Platforms}
				}
				if fits(&Job{Spec: j}, &m, m.TotalMemoryGB) {
					t.placeable = true
				}
			}
			if !t.placeable {
				r.NeverPlaceable = append(r.NeverPlaceable, fmt.Sprintf("%s@%.0f", t.stream.Name, t.at))
			}
			if t.stream.Call {
				callSerial++
				t.id = fmt.Sprintf("call-%012d", callSerial)
				e = q.addCall(&ModelCall{ID: t.id, Model: t.stream.Model, Project: t.stream.Project, Priority: t.stream.Priority, Deadline: deadlineAt(epoch, t), Queued: clock.now, WaitingSince: clock.now})
			} else {
				var j *Job
				j, e = q.Submit(simSpec(t, epoch))
				if e == nil {
					t.id = j.ID
				}
			}
			if e != nil {
				return r, e
			}
		}
		// Settle instantaneous responses, including immediate kill, load completion,
		// unload acknowledgements, dispatch, and call start. Bound guards regressions.
		for round := 0; round < 100; round++ {
			changed := false
			for _, worker := range workers {
				if !worker.online {
					continue
				}
				if t := worker.task; t != nil {
					j := q.s.Jobs[t.id]
					if (j.State == "stopping" || j.State == "queued") && math.IsInf(t.stopAt, 1) {
						t.stopAt = now
						if j.StopAt != nil {
							t.stopAt = min(float64(j.StopAt.Sub(epoch).Nanoseconds())/1e9, now+1)
						}
						changed = true
					}
					doneAt := t.loadUntil + (t.work-t.segment)/worker.spec.Speed
					if doneAt <= now+1e-7 || t.stopAt <= now+1e-7 {
						success := doneAt <= now+1e-7 && (j.State == "running" || plannedPriorityStop(j))
						t.done = min(t.work, t.segment+max(0, now-t.loadUntil)*worker.spec.Speed)
						if !success {
							// SIGTERM saves current progress; non-resumable work starts over.
							if t.stream.Checkpoint == 0 {
								lost := max(0, now-t.loadUntil)
								r.LostSeconds += lost
								t.done = 0
							}
						}
						outcome := simResult(t, worker, success)
						outcome.Interrupted = !success && j.State == "stopping"
						if e = q.Complete(outcome); e != nil && !(errors.Is(e, ErrStale) && j.State == "queued") {
							return r, e
						}
						worker.task = nil
						t.machine = ""

						if success {
							finish(t)
							delete(q.s.Jobs, t.id)
						}
						changed = true
					}
				}
				reports := []ModelReport{}
				for _, slot := range q.modelPlan(worker.spec.Name) {
					load := worker.loads[slot.Model]
					if slot.State == "draining" && slot.InFlight == 0 {
						reports = append(reports, ModelReport{Name: slot.Model, Attempt: slot.Attempt, State: "unloaded"})
						delete(worker.loads, slot.Model)
						changed = true
					} else if slot.State == "loading" {
						if load == nil {
							delay := models[slot.Model].LoadSeconds / worker.spec.Speed
							load = &simLoad{until: now + delay}
							worker.loads[slot.Model] = load
							r.ModelLoads++
							if loadsSeen[slotKey(worker.spec.Name, slot.Model)] {
								r.ModelReloads++
							}
							loadsSeen[slotKey(worker.spec.Name, slot.Model)] = true
							worker.cached[slot.Model] = true
							changed = true
						}
						if load.until <= now && !load.seen {
							reports = append(reports, ModelReport{Name: slot.Model, Attempt: slot.Attempt, State: "ready"})
							load.seen = true
							changed = true
						}
					}
				}
				c := Checkin{Machine: machineReport(worker), ModelReports: reports}
				if t := worker.task; t != nil {
					c.Running = &Execution{ID: t.id, Attempt: t.attempt}
					c.Started = true // Batch process starts before its native model initialization.
				}
				reply, err := q.Checkin(c)
				if err != nil {
					return r, err
				}
				if worker.task == nil && reply.Job != nil {
					var t *simTask
					for _, candidate := range tasks {
						if candidate.id == reply.Job.ID {
							t = candidate
							break
						}
					}
					if t == nil {
						return r, errors.New("untracked assignment")
					}
					t.attempt = reply.Job.Attempt
					load := 0.0
					if t.stream.Model != "" {
						load = models[t.stream.Model].LoadSeconds / worker.spec.Speed
						r.ModelLoads++
						if loadsSeen[slotKey(worker.spec.Name, t.stream.Model)] {
							r.ModelReloads++
						}
						loadsSeen[slotKey(worker.spec.Name, t.stream.Model)] = true
						worker.cached[t.stream.Model] = true
					}
					worker.task = t
					start(t, worker, load)
					changed = true
				}
			}
			for _, t := range tasks {
				if !t.stream.Call || t.id == "" || t.finished || t.failed {
					continue
				}
				c := q.calls[t.id]
				if c == nil {
					continue
				}
				if c.Error != "" {
					t.failed = true
					r.Failed++
					if e = q.finishCall(t.id); e != nil {
						return r, e
					}
					changed = true
					continue
				}
				if c.Machine != "" && t.machine == "" {
					for _, worker := range workers {
						if worker.spec.Name == c.Machine {
							start(t, worker, 0)
							break
						}
					}
					changed = true
				}
				if t.machine != "" {
					speed := 1.0
					for _, worker := range workers {
						if worker.spec.Name == t.machine {
							speed = worker.spec.Speed
						}
					}
					if t.segmentAt+t.work/speed <= now+1e-7 {
						if e = q.finishCall(t.id); e != nil {
							return r, e
						}
						finish(t)
						changed = true
					}
				}
			}
			if !changed {
				break
			}
			if round == 99 {
				return r, errors.New("simulation failed to settle")
			}
		}
		if now >= horizon {
			break
		}

		next := horizon
		for _, c := range q.calls {
			if c.StopAt != nil && c.Error == "" {
				at := float64(c.StopAt.Sub(epoch).Nanoseconds()) / 1e9
				if at > now+1e-7 {
					next = min(next, at)
				}
			}
		}
		if ai < len(arrivals) {
			next = min(next, arrivals[ai].at)
		}
		for bi < len(boundaries) && boundaries[bi] <= now+1e-7 {
			bi++
		}
		if bi < len(boundaries) {
			next = min(next, boundaries[bi])
		}
		for _, worker := range workers {
			if !worker.online {
				if m := q.s.Machines[worker.spec.Name]; m != nil && !m.Gone {
					lossAt := float64(m.LastSeen.Sub(epoch).Nanoseconds())/1e9 + q.machineLossTimeout(m.ID).Seconds()
					if lossAt > now+1e-7 {
						next = min(next, lossAt)
					}
				}
				continue
			}
			if t := worker.task; t != nil {
				next = min(next, t.loadUntil+(t.work-t.segment)/worker.spec.Speed, t.stopAt)
				if t.loadUntil > now+1e-7 {
					next = min(next, t.loadUntil)
				}
			}
			for name, load := range worker.loads {
				if slot := q.s.ModelSlots[slotKey(worker.spec.Name, name)]; slot != nil {
					if slot.State == "loading" {
						next = min(next, load.until)
					} else if slot.State == "ready" && slot.InFlight == 0 {
						next = min(next, float64(slot.LastUsed.Sub(epoch).Nanoseconds())/1e9+600)
					}
				}
			}
		}
		for _, t := range tasks {
			if t.stream.Call && t.machine != "" && !t.finished && !t.failed {
				for _, worker := range workers {
					if worker.spec.Name == t.machine {
						next = min(next, t.segmentAt+t.work/worker.spec.Speed)
					}
				}
			}
		}
		if next <= now+1e-8 {
			return r, fmt.Errorf("simulation clock stalled at %.9f", now)
		}
		dt := next - now
		for _, worker := range workers {
			if !worker.online {
				continue
			}
			m := r.Machines[worker.spec.Name]
			m.AvailableHours += dt / 3600
			active := []*simTask{}
			if worker.task != nil {
				active = append(active, worker.task)
			}
			for _, t := range tasks {
				if t.stream.Call && t.machine == worker.spec.Name && !t.finished && !t.failed {
					active = append(active, t)
				}
			}
			loading := worker.task != nil && worker.task.loadUntil > now
			for name := range worker.loads {
				if s := q.s.ModelSlots[slotKey(worker.spec.Name, name)]; s != nil && s.State == "loading" {
					loading = true
				}
			}
			if loading {
				m.LoadingHours += dt / 3600
				r.LoadingSeconds += dt
			}
			if len(active) > 0 {
				m.BusyHours += dt / 3600
				for _, t := range active {
					p := ensureProject(t.stream.Project)
					p.GPUHours += dt / 3600 / float64(len(active))
					if t.stream.Priority == "experiment" {
						p.ExperimentGPUHours += dt / 3600 / float64(len(active))
					}
				}
			}
			// Idle means no command/call/load. Runnable includes caps and real memory fit.
			if len(active) == 0 && !loading {
				waiting := simIdleWaiting(q, q.s.Machines[worker.spec.Name])
				if waiting {
					m.IdleWaitingHours += dt / 3600
					r.IdleWaitingHours += dt / 3600
				}
			}
		}
		now = next
	}
	r.Preemptions = len(q.s.Preemptions)
	r.Unfinished = r.Submitted - r.Completed - r.Failed
	for _, t := range tasks {
		if t.stream.Priority == "production" && !t.started {
			r.Production.Unstarted++
		}
		if t.stream.Priority == "deadline" && t.at+t.stream.Deadline <= horizon && (!t.finished || t.timeFinished > t.at+t.stream.Deadline) {
			r.DeadlineMisses++
		}
	}
	sort.Float64s(waits)
	r.Production.Started = len(waits)
	if len(waits) > 0 {
		r.Production.Median = simPercentile(waits, .5)
		r.Production.P95 = simPercentile(waits, .95)
		r.Production.Max = waits[len(waits)-1]
	}
	total, weights, exp := 0.0, 0.0, 0.0
	keys := []string{}
	for k := range r.Projects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		p := r.Projects[k]
		total += p.GPUHours
		weights += p.Weight
		exp += p.ExperimentGPUHours
	}
	for _, p := range r.Projects {
		if total > 0 {
			p.Share = p.GPUHours / total * 100
		}
		if exp > 0 {
			p.ExperimentShare = p.ExperimentGPUHours / exp * 100
		}
		p.WeightedShare = p.Weight / weights * 100
		p.ExperimentPerDay = float64(p.ExperimentCompleted) / o.Days
		if p.ExperimentCompleted > 0 {
			p.ExperimentFinishSeconds /= float64(p.ExperimentCompleted)
		}
	}
	for _, m := range r.Machines {
		if m.AvailableHours > 0 {
			m.Utilization = m.BusyHours / m.AvailableHours * 100
		}
	}
	batchKeys := []string{}
	for k := range batches {
		batchKeys = append(batchKeys, k)
	}
	sort.Strings(batchKeys)
	for _, k := range batchKeys {
		b := batches[k]
		metric := SimBatchMetric{SubmittedSeconds: b.at}
		if b.remaining == 0 {
			finish := b.finish
			metric.FinishedSeconds = &finish
		}
		r.Batches[k] = metric
		if b.remaining == 0 {
			r.BatchCompleted++
			r.BatchMeanSeconds += b.finish - b.at
		} else {
			r.BatchUnfinished++
		}
	}
	if r.BatchCompleted > 0 {
		r.BatchMeanSeconds /= float64(r.BatchCompleted)
	}
	return r, nil
}
func deadlineAt(epoch time.Time, t *simTask) *time.Time {
	if t.stream.Priority != "deadline" {
		return nil
	}
	v := epoch.Add(time.Duration((t.at + t.stream.Deadline) * 1e9))
	return &v
}
func simSpec(t *simTask, epoch time.Time) Spec {
	s := Spec{Name: t.stream.Name, Project: t.stream.Project, Priority: t.stream.Priority, Class: t.stream.Class, Deadline: deadlineAt(epoch, t), Resumable: t.stream.Checkpoint > 0, ExpectedMinutes: t.work / 60}
	s.Run.Command = []string{"simulated-timer"}
	s.Needs.MemoryGB = t.stream.MemoryGB
	s.Needs.HostMemoryGB = t.stream.HostGB
	s.Needs.Model = t.stream.Model
	s.Needs.Platform = Platform{Required: t.stream.Platforms}
	return s
}
func simPercentile(values []float64, p float64) float64 {
	index := max(0, int(math.Ceil(float64(len(values))*p))-1)
	return values[index]
}
func PrintSim(w io.Writer, r SimReport) {
	fmt.Fprintf(w, "Metric                                Value\nProduction wait median / p95 / max    %.2f / %.2f / %.2f s (%d unstarted)\nDeadline misses                       %d\nCompleted / unfinished / impossible   %d / %d / %d\nIdle with fitting work waiting        %.3f GPU-h\nModel loads / reloads / load time      %d / %d / %.1f s\nPreemptions / lost work                %d / %.1f s\nBatch completion mean / unfinished    %.1f s / %d\n", r.Production.Median, r.Production.P95, r.Production.Max, r.Production.Unstarted, r.DeadlineMisses, r.Completed, r.Unfinished, len(r.NeverPlaceable), r.IdleWaitingHours, r.ModelLoads, r.ModelReloads, r.LoadingSeconds, r.Preemptions, r.LostSeconds, r.BatchMeanSeconds, r.BatchUnfinished)
	keys := []string{}
	for k := range r.Projects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintln(w, "Project            GPU-h  share% weight% exp/day exp finish(s)")
	for _, k := range keys {
		p := r.Projects[k]
		fmt.Fprintf(w, "%-18s %6.1f %6.1f %7.1f %7.1f %13.1f\n", k, p.GPUHours, p.Share, p.WeightedShare, p.ExperimentPerDay, p.ExperimentFinishSeconds)
	}
	keys = nil
	for k := range r.Machines {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintln(w, "Machine            utilization% loading(h) idle-fit(h)")
	for _, k := range keys {
		m := r.Machines[k]
		fmt.Fprintf(w, "%-18s %12.1f %10.2f %11.3f\n", k, m.Utilization, m.LoadingHours, m.IdleWaitingHours)
	}
}

// Uses production eligibility and caps; it does not choose where to dispatch.
func simIdleWaiting(q *Queue, m *Machine) bool {
	if q.candidate(m, nil) != nil {
		return true
	}
	for _, c := range q.calls {
		if q.callCanUse(m, c) {
			return true
		}
	}
	return false
}
