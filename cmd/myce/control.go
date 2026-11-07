package main

import (
	"fmt"
	"mycelium/internal/myce"
)

func printControl(page myce.ControlPage) {
	t := page.Telemetry
	if t == nil {
		fmt.Println("control: unknown (queue telemetry unavailable)")
		return
	}
	fmt.Println(t.Summary())
	fmt.Printf("  retained sequences %d..%d next=%d pending=%d overwritten=%d\n", page.First, page.HighWater, page.Next, t.Pending, t.Overwritten)
	var longest *myce.ControlFlight
	for i := range t.InFlight {
		f := &t.InFlight[i]
		if longest == nil || f.Enqueued.Before(longest.Enqueued) {
			longest = f
		}
	}
	if longest != nil {
		fmt.Printf("  oldest in-flight: acquisition=%d request=%d operation=%s mode=%s phase=%s age=%.1fms\n", longest.ID, longest.Request, longest.Operation, longest.Mode, longest.Phase, t.At.Sub(longest.Enqueued).Seconds()*1000)
	}
	for i := len(t.Recent) - 1; i >= 0; i-- {
		e := t.Recent[i]
		if e.Kind == "rejection" {
			fmt.Printf("  last 429: route=%s client=%s family=%s handler=%d wait=%.1fms baseline=%.1fms frontier=%s\n", e.Operation, e.Client, e.Family, e.Handler, e.PressureNS.Seconds()*1000, e.BaselineNS/1e6, e.Frontier)
			break
		}
	}
}
