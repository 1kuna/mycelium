package clock

import (
	"time"

	"mycelium/internal/ports"
)

type System struct{}

func (System) Now() time.Time {
	return time.Now()
}

func (System) NewTimer(d time.Duration) ports.Timer {
	return systemTimer{timer: time.NewTimer(d)}
}

type systemTimer struct {
	timer *time.Timer
}

func (t systemTimer) C() <-chan time.Time {
	return t.timer.C
}

func (t systemTimer) Stop() bool {
	return t.timer.Stop()
}

var _ ports.Clock = System{}
