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
	if !timer.Stop() {
		t.Fatal("fresh timer should stop")
	}
}
