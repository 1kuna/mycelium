package myce

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const residentJobsCapability = "resident_jobs_v1"

// ResidentBinding opts a command into an authored catalog engine. Engine is
// queue-owned frozen execution data, never an authoring field. The command is
// an ordinary attempt: it stages inputs, calls the bridge, validates and writes
// declared outputs. Only the model process survives that attempt.
type ResidentBinding struct {
	Model  string          `json:"model" yaml:"model"`
	Engine *ResidentEngine `json:"engine,omitempty" yaml:"-"`
}

type ResidentEngine struct {
	Model        Model                        `json:"model"`
	Environments map[string]EnvironmentRecipe `json:"environments,omitempty"`
}

func ownsResidentSlot(s Spec, slot *ModelSlot) bool {
	return s.Resident != nil && s.Resident.Engine != nil && s.Resident.Engine.Model.Name == slot.Model
}

func validateResident(s Spec) error {
	if s.Resident == nil {
		return nil
	}
	if strings.TrimSpace(s.Resident.Model) == "" || s.Resumable || s.FanOut != nil || s.AllowCPUFallback || s.Needs.GPU != nil && s.Needs.GPU.Count != nil && *s.Needs.GPU.Count != 1 {
		return errors.New("resident requires a catalog model and independent, non-resumable single-device stage jobs; use pieces for independent inputs")
	}
	return nil
}

// Called only after key replay and normal extension/environment resolution.
// Inputs, per-item args and job IDs do not enter residency identity. A changed
// entry, extension, environment or model does. No hashing of model bytes here.
func (q *Queue) freezeResident(j *Job) error {
	for _, e := range j.Extensions {
		if e.Resident == nil {
			continue
		}
		if j.Spec.Resident != nil && j.Spec.Resident.Model != e.Resident.Model {
			return errors.New("resident model conflicts with the frozen extension")
		}
		j.Spec.Resident = &ResidentBinding{Model: e.Resident.Model}
	}
	if j.Spec.Resident == nil {
		return nil
	}
	if err := validateResident(j.Spec); err != nil {
		return err
	}
	var model Model
	ok := false
	for _, entry := range q.models.Models {
		if entry.Name == j.Spec.Resident.Model {
			model, ok = entry, true
			break
		}
	}
	if !ok || model.Engine != "server" && model.Engine != "llama.cpp" && model.Engine != "vllm" && model.Engine != "fake" {
		return errors.New("resident model must name an existing catalog engine")
	}
	engine := &ResidentEngine{Model: model, Environments: q.environments.Environments[model.Environment]}
	frozen, err := json.Marshal(engine)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(frozen, &engine); err != nil {
		return err
	}
	extensions := make([]string, 0, len(j.Extensions))
	for _, e := range j.Extensions {
		extensions = append(extensions, e.Digest)
	}
	run, execution, environment := j.Spec.Run, j.Spec.Execution, j.Spec.Environment
	if j.Spec.PrepareSource || len(extensions) > 0 {
		run, execution = RunSpec{}, nil
	}
	identity, err := json.Marshal(struct {
		Project, Name, Kind, Extension string
		Source                         FileRef
		Run                            RunSpec
		Execution                      *ExecutionSpec
		Environment                    Environment
		Recipes                        map[string]EnvironmentRecipe
		Extensions                     []string
		Engine                         *ResidentEngine
	}{j.Spec.Project, j.Spec.Name, j.Spec.Kind, j.Spec.ExtensionDigest, j.Spec.Files[".myce-source.tar"], run, execution, environment, j.EnvironmentRecipes, extensions, engine})
	if err != nil {
		return err
	}
	engine.Model.MaxInFlight = 1 // The frozen budget covers one request activation peak.
	engine.Model.Name = fmt.Sprintf("resident-%x", sha256.Sum256(identity))
	j.Spec.Resident = &ResidentBinding{Model: model.Name, Engine: engine}
	return nil
}

func (q *Queue) residentModelByName(name string) (Model, bool) {
	for _, slot := range q.s.ModelSlots {
		if slot.Model == name && slot.Resident != nil {
			return slot.Resident.Model, true
		}
	}
	for _, j := range q.s.Jobs {
		if r := j.Spec.Resident; r != nil && r.Engine != nil && r.Engine.Model.Name == name {
			return r.Engine.Model, true
		}
	}
	return Model{}, false
}

// Cold residency and the driver are admitted together. A batch of staged
// drivers must never consume the room needed to load their common engine.
func (q *Queue) residentPlanningJob(j *Job, m *Machine) *Job {
	r := j.Spec.Resident
	if r == nil || r.Engine == nil {
		return j
	}
	if slot := q.s.ModelSlots[targetSlotKey(m, r.Engine.Model.Name)]; slot != nil && slotReservesMemory(slot) {
		return j
	}
	copy := *j
	model := r.Engine.Model
	copy.Spec.Needs.MemoryGB += model.MemoryGB
	copy.Spec.Needs.HostMemoryGB += model.HostMemoryGB + m.MemoryCeilingSlackGB
	copy.Spec.Needs.CPUCores = j.Spec.Needs.cores() + 1
	return &copy
}

func (q *Queue) residentJobFits(j *Job, m *Machine) bool {
	r := j.Spec.Resident
	if r == nil {
		return true
	}
	if r.Engine == nil || !contains(m.Capabilities, residentJobsCapability) || len(m.groupMembers) > 0 || m.ModelURL == "" || !q.modelUsableMachine(r.Engine.Model, m) {
		return false
	}
	if slot := q.s.ModelSlots[targetSlotKey(m, r.Engine.Model.Name)]; slot != nil {
		if slot.Session != m.Session || slot.State != "ready" && slot.State != "loading" {
			return false
		}
	} else if q.modelLoadDiskHeadroomReason(r.Engine.Model, m) != "" {
		return false
	}
	planned := q.residentPlanningJob(j, m)
	n := planned.Spec.Needs
	return m.FreeMemoryGB >= n.MemoryGB && hostFits(m, n.MemoryGB, n.HostMemoryGB) && cpuAllocationFits(m, n.cores())
}

func (q *Queue) reserveResident(j *Job, m *Machine) {
	r := j.Spec.Resident
	if r == nil || r.Engine == nil {
		return
	}
	key := targetSlotKey(m, r.Engine.Model.Name)
	slot := q.s.ModelSlots[key]
	if slot != nil && slotReservesMemory(slot) {
		return
	}
	slot = &ModelSlot{Resident: r.Engine, Model: r.Engine.Model.Name, Device: targetDevice(m), Machine: m.ID, Session: m.Session, State: "loading", Attempt: ID(), Priority: j.Spec.Priority, Class: j.Spec.Class, Project: j.Spec.Project, ProductionUsed: j.Spec.Priority == "production", LastUsed: q.clock.Now()}
	q.s.ModelSlots[key] = slot
	q.initEnginePreemption(slot)
	q.recordModelInterval(slot, "assigned")
	m.FreeMemoryGB -= r.Engine.Model.MemoryGB
}

// Waiting dependencies are real future demand. They protect only this engine's
// idle retention, never a pin or a priority/memory reclamation exemption.
func (q *Queue) residentJobDemand(slot *ModelSlot) bool {
	for _, j := range q.liveJobs() {
		if !ownsResidentSlot(j.Spec, slot) || cancelled(j) || j.ReleaseConfirmed && j.State != "queued" || j.State != "queued" && j.State != "running" {
			continue
		}
		m := slotMachine(q.s.Machines[slot.Machine], slot)
		if m != nil && j.Spec.allowsMachine(m.ID) && q.projectAllows(j.Spec.Project, m.ID) && executionTargetMatches(j.Spec, m) {
			return true
		}
	}
	return false
}

type residentAttempt struct {
	Job, Machine, Session string
	Attempt               int
}
type residentAttemptKey struct{}

func (q *Queue) residentAttemptLive(a residentAttempt) bool {
	j := q.s.Jobs[a.Job]
	return j != nil && j.Spec.Resident != nil && j.Spec.Resident.Engine != nil && j.State == "running" && !j.ReleaseConfirmed && !cancelled(j) && j.StartedAt != nil && j.Machine == a.Machine && j.Session == a.Session && j.Attempt == a.Attempt
}

// Fleet authentication stays on the worker side of the pipes. A payload can
// select an inference endpoint, never the engine, project, tier or machine.
func (a API) residentCall(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(r.Header.Get("X-Myce-Job-Attempt"))
	bound := residentAttempt{r.PathValue("id"), r.Header.Get("X-Myce-Machine"), r.Header.Get("X-Myce-Session"), n}
	unlock := requestReadLock(r.Context(), &a.Queue.mu)
	live := err == nil && a.Queue.residentAttemptLive(bound)
	var model string
	headers := proxyHeaders(r.Header)
	if live {
		j := a.Queue.s.Jobs[bound.Job]
		model = j.Spec.Resident.Engine.Model.Name
		for name := range headers {
			if strings.HasPrefix(strings.ToLower(name), "x-myce-") {
				headers.Del(name)
			}
		}
		headers.Set("X-Myce-Project", j.Spec.Project)
		headers.Set("X-Myce-Priority", j.Spec.Priority)
		headers.Set("X-Myce-Class", j.Spec.Class)
		if j.Spec.Deadline != nil {
			headers.Set("X-Myce-Deadline", j.Spec.Deadline.Format("2006-01-02T15:04:05Z07:00"))
		}
	}
	unlock()
	if !live {
		modelError(w, http.StatusConflict, "resident job attempt is no longer authorized")
		return
	}
	copy := r.Clone(context.WithValue(r.Context(), residentAttemptKey{}, bound))
	u := *copy.URL
	u.Path = "/models/" + model + "/" + r.PathValue("path")
	escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/api/workers/resident/"+url.PathEscape(bound.Job)+"/")
	u.RawPath = "/models/" + model + "/" + escaped
	copy.URL = &u
	copy.Header = headers
	a.modelCall(w, copy)
}

// The pipes are inside the attempt's managed workdir and kernel boundary; their
// random per-attempt key also excludes accidental same-user callers. The transport
// needs no network or pathname-socket permission. It exposes
// neither a fleet credential nor a raw engine URL and makes no transport retry.
func (w Worker) startResidentBridge(ctx context.Context, j Job, work string) (map[string]string, func(), error) {
	if j.Spec.Resident == nil {
		return nil, func() {}, nil
	}
	path := filepath.Join(work, ".myce-resident")
	if err := os.Mkdir(path, 0700); err != nil {
		return nil, nil, err
	}
	conn, err := residentPipes(path, true)
	if err != nil {
		os.RemoveAll(path)
		return nil, nil, err
	}
	listener := &residentListener{conn: conn, done: make(chan struct{})}
	key := ID()
	bridgeCtx, cancel := context.WithCancel(ctx)
	server := &http.Server{BaseContext: func(net.Listener) context.Context { return bridgeCtx }}
	server.Handler = http.HandlerFunc(func(out http.ResponseWriter, in *http.Request) {
		if subtle.ConstantTimeCompare([]byte(in.Header.Get("Authorization")), []byte("Bearer "+key)) != 1 || !strings.HasPrefix(in.URL.Path, "/") || strings.HasPrefix(in.URL.Path, "//") {
			modelError(out, 403, "invalid resident attempt capability")
			return
		}
		endpoint := "/api/workers/resident/" + url.PathEscape(j.ID) + in.URL.RequestURI()
		req, err := http.NewRequestWithContext(in.Context(), in.Method, strings.TrimRight(w.Client.URL, "/")+endpoint, in.Body)
		if err != nil {
			modelError(out, 400, "invalid resident request")
			return
		}
		req.Header = proxyHeaders(in.Header)
		req.Header.Set("Authorization", "Bearer "+w.Client.Token)
		req.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		req.Header.Set("User-Agent", "myce/"+BuildVersion)
		req.Header.Set("X-Myce-Machine", j.Machine)
		req.Header.Set("X-Myce-Session", j.Session)
		req.Header.Set("X-Myce-Job-Attempt", strconv.Itoa(j.Attempt))
		req.ContentLength = in.ContentLength
		client := http.Client{Transport: clientTransport}
		if w.Client.HTTP != nil {
			client = *w.Client.HTTP
		}
		client.Timeout = 0
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		response, err := client.Do(req)
		if err != nil {
			modelError(out, 502, "resident call interrupted; request was not replayed")
			return
		}
		defer response.Body.Close()
		for k, values := range proxyHeaders(response.Header) {
			out.Header()[k] = values
		}
		out.WriteHeader(response.StatusCode)
		_, _ = io.Copy(out, response.Body)
	})
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	stop := func() { cancel(); _ = server.Close(); <-done; _ = os.RemoveAll(path) }
	executable, err := os.Executable()
	if err != nil {
		stop()
		return nil, nil, err
	}
	return map[string]string{"MYCE_RESIDENT_PIPE": path, "MYCE_RESIDENT_KEY": key, "MYCE_RESIDENT_MODEL": j.Spec.Resident.Engine.Model.Name, "MYCE_RESIDENT_CLIENT": executable}, stop, nil
}

// One HTTP connection per attempt, with sequential requests. Unlike a pathname
// socket, named pipes stay within existing filesystem grants on both platforms.
// Nonblocking open lets Go's poller interrupt blocked I/O when custody ends.
type residentConn struct {
	request, response *os.File
	server            bool
}

func residentPipes(path string, create bool) (*residentConn, error) {
	c := &residentConn{server: create}
	for n, dest := range map[string]**os.File{"request": &c.request, "response": &c.response} {
		name := filepath.Join(path, n)
		if create {
			if err := unix.Mkfifo(name, 0600); err != nil {
				c.Close()
				return nil, err
			}
		}
		fd, err := unix.Open(name, unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			c.Close()
			return nil, err
		}
		*dest = os.NewFile(uintptr(fd), name)
		info, err := (*dest).Stat()
		if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
			c.Close()
			return nil, errors.New("invalid resident pipe")
		}
	}
	return c, nil
}
func (c *residentConn) Read(p []byte) (int, error) {
	if c.server {
		return c.request.Read(p)
	}
	return c.response.Read(p)
}
func (c *residentConn) Write(p []byte) (int, error) {
	if c.server {
		return c.response.Write(p)
	}
	return c.request.Write(p)
}
func (c *residentConn) Close() error {
	var a, b error
	if c.request != nil {
		a = c.request.Close()
	}
	if c.response != nil {
		b = c.response.Close()
	}
	return errors.Join(a, b)
}
func (c *residentConn) LocalAddr() net.Addr  { return &net.UnixAddr{Name: "resident", Net: "pipe"} }
func (c *residentConn) RemoteAddr() net.Addr { return c.LocalAddr() }
func (c *residentConn) SetDeadline(t time.Time) error {
	return errors.Join(c.request.SetDeadline(t), c.response.SetDeadline(t))
}
func (c *residentConn) SetReadDeadline(t time.Time) error {
	if c.server {
		return c.request.SetReadDeadline(t)
	}
	return c.response.SetReadDeadline(t)
}
func (c *residentConn) SetWriteDeadline(t time.Time) error {
	if c.server {
		return c.response.SetWriteDeadline(t)
	}
	return c.request.SetWriteDeadline(t)
}

type residentListener struct {
	conn      net.Conn
	once      sync.Once
	done      chan struct{}
	closeOnce sync.Once
}

func (l *residentListener) Accept() (net.Conn, error) {
	var c net.Conn
	l.once.Do(func() { c = l.conn })
	if c != nil {
		return c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *residentListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return l.conn.Close()
}
func (l *residentListener) Addr() net.Addr { return l.conn.LocalAddr() }

// ResidentRequest is the local payload client. It never reads installed fleet
// configuration or retries. Callers must make sequential requests per attempt;
// a failed/interrupted exchange fails that attempt rather than reusing its stream.
func ResidentRequest(ctx context.Context, path, key string, req *http.Request) (*http.Response, error) {
	c, err := residentPipes(path, false)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	req = req.Clone(ctx)
	req.Header = req.Header.Clone()
	req.Header.Set("Authorization", "Bearer "+key)
	if err = req.Write(c); err != nil {
		stop()
		c.Close()
		return nil, err
	}
	response, err := http.ReadResponse(bufio.NewReader(c), req)
	if err != nil {
		stop()
		c.Close()
		return nil, err
	}
	response.Body = &residentResponseBody{ReadCloser: response.Body, close: func() { stop(); c.Close() }}
	return response, nil
}

type residentResponseBody struct {
	io.ReadCloser
	close func()
}

func (b *residentResponseBody) Close() error { err := b.ReadCloser.Close(); b.close(); return err }
