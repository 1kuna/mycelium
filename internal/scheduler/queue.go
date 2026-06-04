package scheduler

import (
	"sort"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const agingInterval = time.Minute

type Queue struct {
	mu    sync.Mutex
	clock ports.Clock
	next  int64
	items []queuedJob
}

type queuedJob struct {
	job        domain.Job
	payload    []byte
	enqueuedAt time.Time
	seq        int64
}

func NewQueue(clock ports.Clock) *Queue {
	return &Queue{clock: clock}
}

func (q *Queue) Enqueue(job domain.Job) {
	q.EnqueueWithPayload(job, nil)
}

func (q *Queue) EnqueueWithPayload(job domain.Job, payload []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.next++
	q.items = append(q.items, queuedJob{job: job, payload: append([]byte(nil), payload...), enqueuedAt: q.clock.Now(), seq: q.next})
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue) Dequeue() (domain.Job, bool) {
	job, _, ok := q.DequeueWithPayload()
	return job, ok
}

func (q *Queue) DequeueWithPayload() (domain.Job, []byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return domain.Job{}, nil, false
	}
	q.sort()
	item := q.items[0]
	q.items = q.items[1:]
	return item.job, append([]byte(nil), item.payload...), true
}

func (q *Queue) sort() {
	now := q.clock.Now()
	sort.SliceStable(q.items, func(i, j int) bool {
		left := q.effectivePriority(q.items[i], now)
		right := q.effectivePriority(q.items[j], now)
		if left != right {
			return left > right
		}
		return q.items[i].seq < q.items[j].seq
	})
}

func (q *Queue) effectivePriority(item queuedJob, now time.Time) int {
	waited := int(now.Sub(item.enqueuedAt) / agingInterval)
	return priorityRank(item.job.Priority)*100 + waited
}
