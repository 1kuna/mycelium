package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
)

var hostBootOnce sync.Once
var hostBoot string
var hostBootErr error

func HostBootID() (string, error) {
	hostBootOnce.Do(func() { hostBoot, hostBootErr = readHostBootID() })
	return hostBoot, hostBootErr
}
func readHostBootID() (string, error) {
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		return strings.TrimSpace(string(b)), err
	}
	b, err := exec.Command("/usr/sbin/sysctl", "-n", "kern.boottime").Output()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
func previousCloseSettled(s HostIntent) error {
	if s.LastClose == nil || s.LastClose.Settlement != "confirmed" || s.LastClose.SettledAt == nil || len(s.Pending) > 0 || len(s.Holds) > 0 {
		return errors.New("previous_close_unsettled")
	}
	if s.LastClose.HostRevision != s.HostRevision {
		return ErrHostRevision
	}
	return nil
}
func (s HostIntent) HostSettlement() string {
	if s.LastClose == nil {
		return "unknown"
	}
	return s.LastClose.Settlement
}
func mutateHostIntent(dir string, revision HostRevision, fn func(*HostIntent) error) error {
	unlock, err := hostOperationLock(dir, "intake.lock", true)
	if err != nil {
		return err
	}
	defer unlock()
	s, err := ReadHostIntent(dir)
	if err != nil {
		return err
	}
	if s.HostRevision != revision {
		return ErrHostRevision
	}
	if err = fn(&s); err != nil {
		return err
	}
	return atomicHostJSON(dir, hostStateFile, s, nil)
}
func mergeObligations(a, b []HostObligation) []HostObligation {
	out := slices.Clone(a)
	seen := map[string]bool{}
	for _, p := range out {
		seen[p.Queue+"\x00"+obligationKey(p)] = true
	}
	for _, p := range b {
		key := p.Queue + "\x00" + obligationKey(p)
		if !seen[key] {
			out = append(out, p)
			seen[key] = true
		}
	}
	return out
}

// Read both state and durable attempt/result files. A service may complete and
// clear state.json during Stop; neither its initial nor its final snapshot alone
// is a complete inventory.
func hostCustody(workers []HostWorker) ([]HostObligation, []Result, error) {
	var obligations []HostObligation
	var results []Result
	for _, w := range workers {
		if w.Queue == "" {
			continue
		}
		paths := []string{filepath.Join(w.State, "state.json")}
		for _, pattern := range []string{"attempt-*.json", "*.result.json", "*.docker.json"} {
			more, err := filepath.Glob(filepath.Join(w.State, pattern))
			if err != nil {
				return nil, nil, err
			}
			paths = append(paths, more...)
		}
		for _, path := range paths {
			b, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) && filepath.Base(path) == "state.json" {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			var r *Result
			if strings.HasSuffix(path, ".result.json") {
				var result Result
				if err = json.Unmarshal(b, &result); err != nil {
					return nil, nil, err
				}
				r = &result
			} else if strings.HasSuffix(path, ".docker.json") {
				var d dockerExecution
				if err = json.Unmarshal(b, &d); err != nil {
					return nil, nil, err
				}
				r = d.Owner
			} else {
				var st workerState
				if err = json.Unmarshal(b, &st); err != nil {
					return nil, nil, err
				}
				r = st.Result
				if st.Job != nil {
					obligations = mergeObligations(obligations, []HostObligation{{Queue: w.Queue, Result: Result{Machine: w.Name, Session: st.Session, Execution: Execution{ID: st.Job.ID, Attempt: st.Job.Attempt}}}})
				}
			}
			if r != nil && r.ID != "" && r.Attempt > 0 && r.Machine == w.Name && r.Session != "" {
				obligations = mergeObligations(obligations, []HostObligation{{Queue: w.Queue, Result: Result{Machine: r.Machine, Session: r.Session, Execution: r.Execution}}})
				if strings.HasSuffix(path, ".result.json") {
					results = append(results, *r)
				}
			}
		}
	}
	return obligations, results, nil
}
func newHostExchange(s HostIntent, boot string) HostExchange {
	return HostExchange{Host: s.Host, HostRevision: s.HostRevision, Desired: s.Desired, Boot: boot, Request: ID(), AcceptedAt: s.AcceptedAt, Reason: s.Reason, LastClose: s.LastClose}
}
func validateHostAck(c HostExchange, a HostAcknowledgement) error {
	if a.Host != c.Host || a.HostRevision != c.HostRevision || a.Desired != c.Desired || a.Boot != c.Boot || a.Request != c.Request || a.Pending == nil || a.Covered == nil {
		return errors.New("incomplete or stale host acknowledgement")
	}
	return nil
}
func saveHostAck(dir, queue string, c HostExchange, a HostAcknowledgement, clock Clock) error {
	boot, err := HostBootID()
	if err != nil {
		return err
	}
	if c.Boot != boot {
		return errors.New("stale boot acknowledgement")
	}
	if err := validateHostAck(c, a); err != nil {
		return err
	}
	return mutateHostIntent(dir, c.HostRevision, func(s *HostIntent) error {
		if s.QueueAcks == nil {
			s.QueueAcks = map[string]HostAcknowledgement{}
		}
		s.QueueAcks[queue] = a
		covered := map[string]bool{}
		for _, p := range a.Covered {
			covered[obligationKey(p)] = true
		}
		pending := s.Pending[:0]
		for _, p := range s.Pending {
			if p.Queue != queue || !covered[obligationKey(p)] {
				pending = append(pending, p)
			}
		}
		s.Pending = pending
		for _, p := range a.Pending {
			p.Queue = queue
			s.Pending = mergeObligations(s.Pending, []HostObligation{p})
		}
		if s.Desired != "closed" {
			return nil
		}
		if s.LastClose == nil {
			s.LastClose = &HostClose{HostRevision: s.HostRevision, Settlement: "pending"}
		}
		priorSettledAt := s.LastClose.SettledAt
		s.LastClose.Settlement = "pending"
		s.LastClose.SettledAt = nil
		local := s.LocalExclusion
		if local == nil || !local.Confirmed || local.Boot != c.Boot || local.HostRevision != c.HostRevision || len(s.Pending) > 0 || len(s.Holds) > 0 {
			return nil
		}
		queues, err := (HostControl{Dir: dir}).Queues()
		if err != nil {
			return err
		}
		if !slices.Contains(queues, queue) {
			queues = append(queues, queue)
		}
		for _, endpoint := range queues {
			ack, ok := s.QueueAcks[endpoint]
			if !ok || ack.HostRevision != c.HostRevision || ack.Boot != c.Boot || ack.Desired != "closed" || ack.Pending == nil || len(ack.Pending) > 0 {
				return nil
			}
		}
		now := clock.Now().UTC()
		s.LastClose.Settlement = "confirmed"
		s.LastClose.SettledAt = &now
		if priorSettledAt != nil {
			s.LastClose.SettledAt = priorSettledAt
		}
		return nil
	})
}

// ReconcileHostLocal is shared by the timer, workers and co-located admission.
// Cleanup is supplied by the host control path, independently of queue reachability.
// Each queue returns its exact obligations before any release is offered.
func ReconcileHostLocal(ctx context.Context, dir string, clock Clock, client func(string) (Client, error)) error {
	if clock == nil {
		return errors.New("clock required")
	}
	boot, err := HostBootID()
	if err != nil {
		return err
	}
	workers, err := HostWorkers(dir)
	if err != nil {
		return err
	}
	queues, err := (HostControl{Dir: dir}).Queues()
	if err != nil {
		return err
	}
	s, err := ReadHostIntent(dir)
	if err != nil {
		return err
	}
	for _, p := range s.Pending {
		if !slices.Contains(queues, p.Queue) {
			queues = append(queues, p.Queue)
		}
	}
	var errs []error
	for _, endpoint := range queues {
		c, err := client(endpoint)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err = reconcileHostClient(ctx, dir, boot, clock, workers, c); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// A worker exchanges with its own queue before check-in. An unrelated queue
// outage cannot suppress this heartbeat; the host timer still retries all queues.
func reconcileWorkerHost(ctx context.Context, dir string, clock Clock, client Client) error {
	boot, err := HostBootID()
	if err != nil {
		return err
	}
	workers, err := HostWorkers(dir)
	if err != nil {
		return err
	}
	return reconcileHostClient(ctx, dir, boot, clock, workers, client)
}
func reconcileHostClient(ctx context.Context, dir, boot string, clock Clock, workers []HostWorker, client Client) error {
	return reconcileHostEndpoint(dir, client.URL, boot, clock, workers, func(request HostExchange) (HostAcknowledgement, error) {
		var ack HostAcknowledgement
		err := client.Call(ctx, "POST", "/api/hosts/"+request.Host+"/reconcile", request, &ack)
		return ack, err
	})
}
func reconcileHostEndpoint(dir, endpoint, boot string, clock Clock, workers []HostWorker, exchange func(HostExchange) (HostAcknowledgement, error)) error {
	s, err := ReadHostIntent(dir)
	if err != nil {
		return err
	}
	request := newHostExchange(s, boot)
	ack, err := exchange(request)
	if err != nil {
		return err
	}
	if err = validateHostAck(request, ack); err != nil {
		return err
	}
	if err = saveHostAck(dir, endpoint, request, ack, clock); err != nil {
		return err
	}
	local := s.LocalExclusion
	if s.Desired != "closed" || local == nil || !local.Confirmed || local.HostRevision != s.HostRevision || local.Boot != boot {
		return nil
	}
	if err = hostReleaseEvidence(&request, s, endpoint, boot, ack.Pending, workers); err != nil {
		return err
	}
	request.Request = ID()
	ack, err = exchange(request)
	if err != nil {
		return err
	}
	return saveHostAck(dir, endpoint, request, ack, clock)
}

// ConfigureLocalHost selects a state location, never a delivery mode. Production
// queues call this unconditionally; an absent/corrupt provisioned state fails closed.
func (q *Queue) ConfigureLocalHost(dir, endpoint string) error {
	if dir == "" {
		var err error
		dir, err = HostStateDirectory()
		if err != nil {
			return err
		}
	}
	q.mu.Lock()
	q.localHostDir, q.localHostQueue = dir, endpoint
	q.mu.Unlock()
	return nil
}
func (q *Queue) reconcileLocalAdmission() (func() error, error) {
	if q.localHostDir == "" {
		return nil, nil
	}
	s, err := ReadHostIntent(q.localHostDir)
	if err != nil {
		// Absence is valid only for queue-only hosts, with no legacy registries.
		paths, e := legacyHostPaths(q.localHostDir)
		workers, _ := filepath.Glob(filepath.Join(q.localHostDir, "worker-*.json"))
		if errors.Is(err, ErrHostState) && len(paths) == 0 && len(workers) == 0 && e == nil {
			if _, e = os.Stat(filepath.Join(q.localHostDir, hostStateFile)); errors.Is(e, os.ErrNotExist) {
				return nil, nil
			}
		}
		return nil, err
	}
	boot, err := HostBootID()
	if err != nil {
		return nil, err
	}
	workers, err := HostWorkers(q.localHostDir)
	if err != nil {
		return nil, err
	}
	request := newHostExchange(s, boot)
	if err = hostReleaseEvidence(&request, s, q.localHostQueue, boot, q.hostObligations(s.Host), workers); err != nil {
		return nil, err
	}
	ack, err := q.reconcileHost(request)
	if err != nil {
		return nil, err
	}
	return func() error {
		ack.Pending = q.hostObligations(s.Host)
		ack.Covered = q.hostCoverage(s.Host)
		return saveHostAck(q.localHostDir, q.localHostQueue, request, ack, q.clock)
	}, nil
}

// Worker-owned timer credentials stay in the configured secret mechanism.
func HostQueueClient(workers []HostWorker, token string) func(string) (Client, error) {
	return func(endpoint string) (Client, error) {
		for _, w := range workers {
			if w.Queue == endpoint && w.Config != "" {
				config, err := ReadServiceConfig(w.Config)
				if err != nil {
					return Client{}, err
				}
				value, err := ReadServiceToken(config)
				if err != nil {
					return Client{}, err
				}
				return Client{URL: endpoint, Token: value}, nil
			}
		}
		if token == "" {
			return Client{}, fmt.Errorf("queue credentials unavailable")
		}
		return Client{URL: endpoint, Token: token}, nil
	}
}

func hostReleaseEvidence(request *HostExchange, s HostIntent, endpoint, boot string, pending []HostObligation, workers []HostWorker) error {
	if s.Desired != "closed" || s.LocalExclusion == nil || !s.LocalExclusion.Confirmed || s.LocalExclusion.Boot != boot || s.LocalExclusion.HostRevision != s.HostRevision {
		return nil
	}
	known := map[string]bool{}
	for _, p := range s.LocalExclusion.Released {
		if p.Queue == endpoint {
			known[obligationKey(p)] = true
		}
	}
	for _, p := range pending {
		covered := known[obligationKey(p)]
		// A completed full worker cleanup also covers its session's preparations
		// and engine slots, whose exact attempts come from the queue, never a host-wide clear.
		if p.Engine != nil || p.Preparation {
			for _, w := range workers {
				machine, session := p.Result.Machine, p.Result.Session
				if p.Engine != nil {
					machine, session = p.Engine.Machine, p.Engine.Session
				}
				if w.Queue == endpoint && w.Name == machine && w.Session == session {
					covered = true
				}
			}
		}
		if covered {
			request.Released = append(request.Released, p)
		}
	}
	_, results, err := hostCustody(workers)
	if err != nil {
		return err
	}
	for _, r := range results {
		for _, p := range request.Released {
			if p.Engine == nil && p.Result.ID == r.ID && p.Result.Machine == r.Machine && p.Result.Session == r.Session && p.Result.Attempt == r.Attempt {
				// The saved result can predate this successful cleanup retry.
				// Current exact exclusion evidence supersedes its pending flag.
				r.ReleasePending = false
				request.Results = append(request.Results, r)
				break
			}
		}
	}
	return nil
}
