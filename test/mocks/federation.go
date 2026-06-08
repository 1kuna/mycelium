package mocks

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type AdmissionController struct {
	OfferVal           domain.LeaseOffer
	LeaseVal           domain.Lease
	OfferErr           error
	CommitErr          error
	ReleaseErr         error
	PreemptErr         error
	LeaseForJobVal     domain.Lease
	LeaseForJobFound   bool
	LeaseForJobErr     error
	LeaseForInstVal    domain.Lease
	LeaseForInstFound  bool
	LeaseForInstErr    error
	JobStatusVal       domain.JobStatus
	JobStatusFound     bool
	JobStatusErr       error
	BindErr            error
	AllowDirectPreempt bool
	Offers             map[string]domain.LeaseOffer
	Requests           []domain.AdmissionRequest
	Calls              []string
}

func (m *AdmissionController) Offer(_ context.Context, req domain.AdmissionRequest) (domain.LeaseOffer, error) {
	job := req.Job
	claim := req.Claim
	m.Calls = append(m.Calls, "offer:"+job.ID)
	m.Requests = append(m.Requests, req)
	if m.OfferErr != nil {
		return domain.LeaseOffer{}, m.OfferErr
	}
	if m.OfferVal.OfferID != "" {
		m.recordOffer(m.OfferVal)
		return m.OfferVal, nil
	}
	offer := domain.LeaseOffer{
		OfferID:        fmt.Sprintf("offer_%s", job.ID),
		JobID:          job.ID,
		NodeID:         req.NodeID,
		Claim:          claim,
		AcceleratorSet: append([]int(nil), req.AcceleratorSet...),
		InstanceID:     req.InstanceID,
		Fence:          1,
	}
	if offer.NodeID == "" {
		offer.NodeID = "node_test"
	}
	m.recordOffer(offer)
	return offer, nil
}

func (m *AdmissionController) Commit(_ context.Context, offerID string, fence uint64) (domain.Lease, error) {
	m.Calls = append(m.Calls, fmt.Sprintf("commit:%s:%d", offerID, fence))
	if m.CommitErr != nil {
		return domain.Lease{}, m.CommitErr
	}
	if m.LeaseVal.ID != "" {
		return m.LeaseVal, nil
	}
	offer, ok := m.Offers[offerID]
	if !ok {
		return domain.Lease{}, fmt.Errorf("unknown offer %q", offerID)
	}
	if offer.Fence != fence {
		return domain.Lease{}, domain.ErrStaleFence
	}
	return domain.Lease{ID: "lease_" + offerID, JobID: offer.JobID, NodeID: offer.NodeID, InstanceID: offer.InstanceID, AcceleratorSet: offer.AcceleratorSet, Claim: offer.Claim}, nil
}

func (m *AdmissionController) Release(_ context.Context, leaseID string) error {
	m.Calls = append(m.Calls, "release:"+leaseID)
	return m.ReleaseErr
}

func (m *AdmissionController) Preempt(_ context.Context, leaseID, reason string) error {
	m.Calls = append(m.Calls, "preempt:"+leaseID+":"+reason)
	if m.PreemptErr != nil {
		return m.PreemptErr
	}
	if !m.AllowDirectPreempt {
		return fmt.Errorf("direct lease preemption is disabled; use policy-aware owner admission preemptions")
	}
	return nil
}

func (m *AdmissionController) PreemptForJob(_ context.Context, job domain.Job, leaseID, reason string) error {
	m.Calls = append(m.Calls, "preempt-for-job:"+job.ID+":"+leaseID+":"+reason)
	return m.PreemptErr
}

func (m *AdmissionController) LeaseForJob(_ context.Context, jobID string) (domain.Lease, bool, error) {
	m.Calls = append(m.Calls, "lease-for-job:"+jobID)
	return m.LeaseForJobVal, m.LeaseForJobFound, m.LeaseForJobErr
}

func (m *AdmissionController) LeaseForInstance(_ context.Context, instanceID string) (domain.Lease, bool, error) {
	m.Calls = append(m.Calls, "lease-for-instance:"+instanceID)
	return m.LeaseForInstVal, m.LeaseForInstFound, m.LeaseForInstErr
}

func (m *AdmissionController) JobStatus(_ context.Context, jobID string) (domain.JobStatus, bool, error) {
	m.Calls = append(m.Calls, "job-status:"+jobID)
	return m.JobStatusVal, m.JobStatusFound, m.JobStatusErr
}

func (m *AdmissionController) BindInstance(_ context.Context, leaseID, instanceID string) error {
	m.Calls = append(m.Calls, "bind-instance:"+leaseID+":"+instanceID)
	return m.BindErr
}

func (m *AdmissionController) recordOffer(offer domain.LeaseOffer) {
	if m.Offers == nil {
		m.Offers = map[string]domain.LeaseOffer{}
	}
	m.Offers[offer.OfferID] = offer
}

type Coordinator struct {
	Decision    domain.PlacementDecision
	Lease       domain.Lease
	Outcome     domain.CommitOutcome
	Err         error
	PlanErr     error
	CommitErr   error
	ReleaseErr  error
	RunningErr  error
	CompleteErr error
	FailErr     error
	Calls       []string
}

func (m *Coordinator) ClaimJob(_ context.Context, jobID string) error {
	m.Calls = append(m.Calls, "claim:"+jobID)
	return m.Err
}

func (m *Coordinator) Plan(_ context.Context, jobID string) (domain.PlacementDecision, error) {
	m.Calls = append(m.Calls, "plan:"+jobID)
	if m.PlanErr != nil {
		return domain.PlacementDecision{}, m.PlanErr
	}
	return m.Decision, nil
}

func (m *Coordinator) Commit(_ context.Context, plan domain.PlacementDecision) (domain.CommitOutcome, error) {
	m.Calls = append(m.Calls, "commit:"+plan.JobID)
	if m.CommitErr != nil {
		return domain.CommitOutcome{}, m.CommitErr
	}
	if m.Outcome.Decision.JobID != "" || m.Outcome.Lease.ID != "" {
		return m.Outcome, nil
	}
	return domain.CommitOutcome{Decision: plan, Lease: m.Lease}, nil
}

func (m *Coordinator) MarkRunning(_ context.Context, jobID string) error {
	m.Calls = append(m.Calls, "running:"+jobID)
	return m.RunningErr
}

func (m *Coordinator) Release(_ context.Context, jobID string) error {
	m.Calls = append(m.Calls, "release:"+jobID)
	return m.ReleaseErr
}

func (m *Coordinator) Complete(_ context.Context, jobID string) error {
	m.Calls = append(m.Calls, "complete:"+jobID)
	return m.CompleteErr
}

func (m *Coordinator) Fail(_ context.Context, jobID string, err error) error {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	m.Calls = append(m.Calls, "fail:"+jobID+":"+msg)
	return m.FailErr
}

type JobRegistry struct {
	Records   []domain.JobRecord
	Err       error
	WatchErr  error
	WatchCh   chan domain.JobRecord
	Calls     []string
	mu        sync.Mutex
	nextWatch int
	watchers  map[int]chan domain.JobRecord
}

func (m *JobRegistry) Put(_ context.Context, rec domain.JobRecord) error {
	m.mu.Lock()
	m.Calls = append(m.Calls, "put:"+rec.JobID)
	if m.Err != nil {
		m.mu.Unlock()
		return m.Err
	}
	rec = cloneMockJobRecord(rec)
	replaced := false
	for i, existing := range m.Records {
		if existing.JobID == rec.JobID {
			if newerMockJobRecord(rec, existing) {
				m.Records[i] = rec
			}
			replaced = true
			break
		}
	}
	if !replaced {
		m.Records = append(m.Records, rec)
	}
	watchers := make([]chan domain.JobRecord, 0, len(m.watchers))
	for _, ch := range m.watchers {
		watchers = append(watchers, ch)
	}
	m.mu.Unlock()
	for _, ch := range watchers {
		select {
		case ch <- cloneMockJobRecord(rec):
		default:
		}
	}
	return nil
}

func (m *JobRegistry) Watch(ctx context.Context, fromCursor string) (<-chan domain.JobRecord, error) {
	cursor, err := parseMockJobCursor(fromCursor)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.Calls = append(m.Calls, "watch:"+fromCursor)
	if m.WatchErr != nil {
		m.mu.Unlock()
		return nil, m.WatchErr
	}
	if m.WatchCh != nil {
		m.mu.Unlock()
		return m.WatchCh, nil
	}
	pending := mockJobRecordsAfter(m.Records, cursor)
	ch := make(chan domain.JobRecord, len(pending)+16)
	for _, rec := range pending {
		ch <- rec
	}
	if m.watchers == nil {
		m.watchers = map[int]chan domain.JobRecord{}
	}
	id := m.nextWatch
	m.nextWatch++
	m.watchers[id] = ch
	m.mu.Unlock()
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		if current, ok := m.watchers[id]; ok {
			delete(m.watchers, id)
			close(current)
		}
		m.mu.Unlock()
	}()
	return ch, nil
}

func (m *JobRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, "snapshot")
	if m.Err != nil {
		return nil, m.Err
	}
	return mockJobRecordsAfter(m.Records, time.Time{}), nil
}

type PeerDiscovery struct {
	PeersVal  []domain.Peer
	Err       error
	WatchErr  error
	WatchCh   chan domain.Peer
	Calls     []string
	mu        sync.Mutex
	nextWatch int
	watchers  map[int]chan domain.Peer
}

func (m *PeerDiscovery) Advertise(_ context.Context, self domain.Peer) error {
	m.mu.Lock()
	m.Calls = append(m.Calls, "advertise:"+self.ID)
	if m.Err != nil {
		m.mu.Unlock()
		return m.Err
	}
	m.PeersVal = append(m.PeersVal, self)
	watchers := make([]chan domain.Peer, 0, len(m.watchers))
	for _, ch := range m.watchers {
		watchers = append(watchers, ch)
	}
	m.mu.Unlock()
	for _, ch := range watchers {
		select {
		case ch <- self:
		default:
		}
	}
	return nil
}

func (m *PeerDiscovery) Peers(context.Context) ([]domain.Peer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, "peers")
	if m.Err != nil {
		return nil, m.Err
	}
	return append([]domain.Peer(nil), m.PeersVal...), nil
}

func (m *PeerDiscovery) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.Calls = append(m.Calls, "watch-peers")
	if m.WatchErr != nil {
		m.mu.Unlock()
		return nil, m.WatchErr
	}
	if m.WatchCh != nil {
		m.mu.Unlock()
		return m.WatchCh, nil
	}
	ch := make(chan domain.Peer, 16)
	if m.watchers == nil {
		m.watchers = map[int]chan domain.Peer{}
	}
	id := m.nextWatch
	m.nextWatch++
	m.watchers[id] = ch
	m.mu.Unlock()
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		if current, ok := m.watchers[id]; ok {
			delete(m.watchers, id)
			close(current)
		}
		m.mu.Unlock()
	}()
	return ch, nil
}

var _ ports.AdmissionController = (*AdmissionController)(nil)
var _ ports.LeaseInspector = (*AdmissionController)(nil)
var _ ports.JobStatusInspector = (*AdmissionController)(nil)
var _ ports.LeaseBinder = (*AdmissionController)(nil)
var _ ports.Coordinator = (*Coordinator)(nil)
var _ ports.JobRegistry = (*JobRegistry)(nil)
var _ ports.PeerDiscovery = (*PeerDiscovery)(nil)

func parseMockJobCursor(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	cursor, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse job registry cursor: %w", err)
	}
	return cursor, nil
}

func mockJobRecordsAfter(records []domain.JobRecord, cursor time.Time) []domain.JobRecord {
	out := make([]domain.JobRecord, 0, len(records))
	for _, rec := range records {
		if cursor.IsZero() || rec.UpdatedAt.After(cursor) {
			out = append(out, cloneMockJobRecord(rec))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].JobID < out[j].JobID
		}
		return out[i].UpdatedAt.Before(out[j].UpdatedAt)
	})
	return out
}

func newerMockJobRecord(next, current domain.JobRecord) bool {
	if mockTerminalJobStatus(current.Status) && !mockTerminalJobStatus(next.Status) {
		return false
	}
	if next.Fence != current.Fence {
		return next.Fence > current.Fence
	}
	if !next.UpdatedAt.Equal(current.UpdatedAt) {
		return next.UpdatedAt.After(current.UpdatedAt)
	}
	return mockRecordTieKey(next) > mockRecordTieKey(current)
}

func mockTerminalJobStatus(status domain.JobStatus) bool {
	return status == domain.JobDone || status == domain.JobFailed
}

func mockRecordTieKey(rec domain.JobRecord) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", rec.Coordinator, rec.AssignedNode, rec.Status, rec.Request, rec.Fence)
}

func cloneMockJobRecord(rec domain.JobRecord) domain.JobRecord {
	rec.Request = append([]byte(nil), rec.Request...)
	return rec
}
