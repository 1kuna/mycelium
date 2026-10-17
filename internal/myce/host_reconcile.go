package myce

import (
	"bytes"
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
	if s.LastClose == nil || s.LastClose.Settlement != "confirmed" || s.LastClose.SettledAt == nil || len(s.Pending) > 0 || blockingHostHolds(s.Holds) {
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
	before, _ := json.Marshal(s)
	if err = fn(&s); err != nil {
		return err
	}
	after, _ := json.Marshal(s)
	if bytes.Equal(before, after) {
		return nil
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

// Local files are undelivered results, never the proof of session release.
func hostCustody(workers []HostWorker) ([]HostObligation, []Result, []string, error) {
	var obligations []HostObligation
	var results []Result
	var holds []string
	for _, w := range workers {
		if w.Queue == "" {
			continue
		}
		if _, err := os.Stat(w.State); errors.Is(err, os.ErrNotExist) {
			holds = append(holds, "missing_worker_spool")
			continue
		} else if err != nil {
			return nil, nil, nil, err
		}
		var st workerState
		b, err := os.ReadFile(filepath.Join(w.State, "state.json"))
		if err == nil {
			err = json.Unmarshal(b, &st)
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil, err
		}
		add := func(r Result) {
			if r.ID != "" && r.Attempt > 0 && r.Machine == w.Name && r.Session != "" {
				obligations = mergeObligations(obligations, []HostObligation{{Queue: w.Queue, Result: r}})
			}
		}
		currentResult := ""
		if st.Job != nil {
			add(Result{Machine: w.Name, Session: st.Session, Execution: Execution{ID: st.Job.ID, Attempt: st.Job.Attempt}})
			currentResult = st.Identity + ".result.json"
			r := st.Result
			if r == nil && st.Identity != "" {
				if b, e := os.ReadFile(filepath.Join(w.State, currentResult)); e == nil {
					var saved Result
					if e = json.Unmarshal(b, &saved); e != nil {
						return nil, nil, nil, e
					}
					r = &saved
				} else if !errors.Is(e, os.ErrNotExist) {
					return nil, nil, nil, e
				}
			}
			if r != nil && r.ID == st.Job.ID && r.Attempt == st.Job.Attempt && r.Machine == w.Name && r.Session == st.Session {
				add(*r)
				results = append(results, *r)
			}
		}
		paths, err := filepath.Glob(filepath.Join(w.State, "*.docker.json"))
		if err != nil {
			return nil, nil, nil, err
		}
		for _, path := range paths {
			b, e := os.ReadFile(path)
			if e != nil {
				return nil, nil, nil, e
			}
			var d dockerExecution
			if e = json.Unmarshal(b, &d); e != nil {
				return nil, nil, nil, e
			}
			if !d.Released && d.Owner != nil {
				add(*d.Owner)
			}
		}
		paths, err = filepath.Glob(filepath.Join(w.State, "*.result.json"))
		if err != nil {
			return nil, nil, nil, err
		}
		for _, path := range paths {
			if filepath.Base(path) != currentResult {
				holds = append(holds, "diagnostic:historical_result")
			}
		}
	}
	return obligations, results, holds, nil
}
func blockingHostHolds(holds []string) bool {
	for _, hold := range holds {
		if !strings.HasPrefix(hold, "diagnostic:") {
			return true
		}
	}
	return false
}
func localExclusionConfirmed(s HostIntent, boot string) bool {
	return s.LocalExclusion != nil && s.LocalExclusion.Confirmed && s.LocalExclusion.Boot == boot && s.LocalExclusion.HostRevision == s.HostRevision
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
		a.Request = "" // The nonce is validated, not heartbeat-persisted.
		s.QueueAcks[queue] = a
		if s.QueueObserved[queue].Revision <= s.Revision {
			delete(s.QueueObserved, queue)
		}
		for _, p := range mergeObligations(s.Pending, localCustody(s)) {
			if p.Queue != queue {
				continue
			}
			matched := false
			for _, q := range append(slices.Clone(a.Covered), a.Pending...) {
				matched = matched || obligationKey(p) == obligationKey(q)
			}
			if !matched && !slices.Contains(s.Holds, "diagnostic:unmatched_local_custody") {
				s.Holds = append(s.Holds, "diagnostic:unmatched_local_custody")
			}
		}
		pending := []HostObligation{}
		for _, p := range s.Pending {
			if p.Queue != queue {
				pending = append(pending, p)
			}
		}
		for _, p := range a.Pending {
			p.Queue = queue
			pending = mergeObligations(pending, []HostObligation{p})
		}
		s.Pending = pending
		if s.Desired != "closed" {
			return nil
		}
		if s.LastClose == nil {
			s.LastClose = &HostClose{HostRevision: s.HostRevision, Settlement: "pending"}
		}
		if s.LastClose.Settlement == "confirmed" && len(s.Pending) == 0 && !blockingHostHolds(s.Holds) {
			return nil
		}
		priorSettledAt := s.LastClose.SettledAt
		s.LastClose.Settlement = "pending"
		s.LastClose.SettledAt = nil
		local := s.LocalExclusion
		if local == nil || !local.Confirmed || local.Boot != c.Boot || local.HostRevision != c.HostRevision || len(s.Pending) > 0 || blockingHostHolds(s.Holds) {
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
	s, err := ReadHostIntent(dir)
	if err != nil {
		return err
	}
	if hostOpenConverged(s) {
		return nil
	}
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
	s, err = ReadHostIntent(dir)
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
		var rejected *HTTPError
		if errors.As(err, &rejected) {
			observeHostQueueRevision(dir, request, client.URL, rejected.QueueRevision)
		}
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
	hostReleaseEvidence(&request, s, endpoint, boot, workers)
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
	if s, e := ReadHostIntent(dir); e == nil {
		q.localHostID = s.Host
	}
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
	q.localHostID = s.Host
	boot, err := HostBootID()
	if err != nil {
		return nil, err
	}
	workers, err := HostWorkers(q.localHostDir)
	if err != nil {
		return nil, err
	}
	request := newHostExchange(s, boot)
	hostReleaseEvidence(&request, s, q.localHostQueue, boot, workers)
	ack, err := q.reconcileHost(request)
	if err != nil {
		observeHostQueueRevision(q.localHostDir, request, q.localHostQueue, q.s.HostIntents[s.Host].HostRevision)
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

func hostReleaseEvidence(request *HostExchange, s HostIntent, endpoint, boot string, workers []HostWorker) {
	if s.Desired != "closed" || !localExclusionConfirmed(s, boot) {
		return
	}
	for _, session := range s.LocalExclusion.Sessions {
		for _, w := range workers {
			if w.Queue == endpoint && w.Host == s.Host && w.Name == session.Machine {
				request.Sessions = append(request.Sessions, session)
				break
			}
		}
	}
}

func localCustody(s *HostIntent) []HostObligation {
	if s.LocalExclusion != nil {
		return s.LocalExclusion.Released
	}
	return nil
}

func hostStatusCode(s HostIntent) string {
	for _, revision := range s.QueueObserved {
		if revision.Revision > s.Revision {
			return "host_revision_behind_queue"
		}
	}
	return ""
}
func hostOpenConverged(s HostIntent) bool {
	if s.Desired != "open" || s.HostSettlement() != "confirmed" || len(s.QueueObserved) > 0 || len(s.QueueAcks) == 0 {
		return false
	}
	for _, ack := range s.QueueAcks {
		if ack.HostRevision != s.HostRevision || ack.Desired != "open" {
			return false
		}
	}
	return true
}

func observeHostQueueRevision(dir string, request HostExchange, queue string, revision HostRevision) {
	if revision.Revision <= request.Revision {
		return
	}
	_ = mutateHostIntent(dir, request.HostRevision, func(s *HostIntent) error {
		if s.QueueObserved == nil {
			s.QueueObserved = map[string]HostRevision{}
		}
		s.QueueObserved[queue] = revision
		return nil
	})
}
