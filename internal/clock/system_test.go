package clock

import (
	"testing"
	"time"
)

func TestSystemClockTimer(t *testing.T) {
	clk := System{}
	if clk.Now().IsZero() {
		t.Fatal("system clock returned zero time")
	}
	timer := clk.NewTimer(time.Hour)
	if timer.C() == nil {
		t.Fatal("timer channel is nil")
	}
	if !timer.Stop() {
		t.Fatal("fresh timer should stop")
	}
}

func TestSystemClockTimerStopAfterFire(t *testing.T) {
	timer := System{}.NewTimer(time.Nanosecond)
	<-timer.C()
	if timer.Stop() {
		t.Fatal("fired timer should not stop")
	}
}
