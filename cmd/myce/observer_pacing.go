package main

import "time"

// Recovery must not turn many old completions for one binding into consecutive
// agent turns. Spread that binding's backlog over its measured delivery service
// cost times remaining demand. There is no fleet-specific rate or sleep cap.
// The cost is slower backlog clearance; fresh completions and other bindings
// remain eligible. This measures notifier service, not downstream agent runtime.
type observerBindingPace struct {
	next    time.Time
	total   time.Duration
	samples int64
	pass    uint64
}

func (p *observerBindingPace) ready(now time.Time, pass uint64) bool {
	return p.pass != pass && !now.Before(p.next)
}
func (p *observerBindingPace) observed(start, end time.Time, remaining int) {
	if cost := end.Sub(start); cost > 0 {
		p.total += cost
		p.samples++
	}
	if p.samples > 0 && remaining > 0 {
		p.next = end.Add((p.total / time.Duration(p.samples)) * time.Duration(remaining))
	} else {
		p.next = end
	}
}
