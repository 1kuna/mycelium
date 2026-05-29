package trace

import (
	"errors"
	"testing"
	"time"
)

func TestTraceDoRecordsSuccessAndError(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tr := New(func() time.Time {
		now = now.Add(10 * time.Millisecond)
		return now
	})

	if err := tr.Do("success", map[string]any{"k": "v"}, func() error { return nil }); err != nil {
		t.Fatalf("success err = %v", err)
	}
	wantErr := errors.New("boom")
	if err := tr.Do("error", nil, func() error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("error err = %v", err)
	}
	if len(tr.Steps) != 2 {
		t.Fatalf("steps = %+v", tr.Steps)
	}
	if tr.Steps[0].Status != "success" || tr.Steps[0].DurationMS <= 0 {
		t.Fatalf("success step = %+v", tr.Steps[0])
	}
	if tr.Steps[1].Status != "error" || tr.Steps[1].Error != "boom" {
		t.Fatalf("error step = %+v", tr.Steps[1])
	}
}
