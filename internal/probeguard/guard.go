// Package probeguard provides a test seam at the real hardware boundary.
package probeguard

// Before is set once, before tests start. Production leaves it nil. Injected
// probes bypass this boundary; accidental local probes panic in tests.
var Before func()

func Check() {
	if Before != nil {
		Before()
	}
}
