package myce

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEventsRequestTiming(t *testing.T) {
	q, _ := fixture(t)
	s := &requestSpan{started: time.Now()}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), requestSpanKey{}, s))
	cancel()
	r := httptest.NewRequest("GET", "/api/workers/worker/events?session=session", nil).WithContext(ctx)
	r.SetPathValue("id", "worker")
	API{Queue: q}.workerEvent(httptest.NewRecorder(), r)
	if !s.measured || s.hold <= 0 {
		t.Fatal("event queue read was not attributed")
	}
}
