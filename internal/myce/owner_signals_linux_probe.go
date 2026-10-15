package myce

import (
	"context"
	"math"
	"path/filepath"
	"strconv"
	"strings"
)

// Linux uses logind session hints, sysfs power_supply and hwmon. Commands and
// files are injected for laptop tests. Neither raw input devices nor X cookies
// are read. Missing/unsupported desktop hints conservatively stay unavailable.
type linuxOwnerProbe struct {
	read      func(string) ([]byte, error)
	glob      func(string) ([]string, error)
	command   func(context.Context, string, ...string) ([]byte, error)
	monotonic func() (float64, error)
	host      OwnerSignalSource
}

func (p linuxOwnerProbe) detect(ctx context.Context) OwnerSignals {
	s := OwnerSignals{Power: "unknown", Heat: "unknown"}
	release, err := p.read("/proc/sys/kernel/osrelease")
	if err != nil {
		return s
	}
	if strings.Contains(strings.ToLower(string(release)), "microsoft") {
		// Only the explicitly configured authenticated host bridge may supply WSL
		// readings. Never fall back to guest probes, including on bridge failure.
		if p.host != nil {
			return p.host(ctx)
		}
		return s
	}
	s.IdleSeconds, s.IdleKnown = p.idle(ctx)
	s.Power, s.Laptop = p.power()
	s.Heat = p.heat()
	return s
}
func properties(raw []byte) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			m[k] = v
		}
	}
	return m
}
func (p linuxOwnerProbe) idle(ctx context.Context) (float64, bool) {
	uptime, err := p.monotonic()
	if err != nil || math.IsNaN(uptime) || math.IsInf(uptime, 0) || uptime < 0 {
		return 0, false
	}
	// logind's hint uses CLOCK_MONOTONIC. /proc/uptime includes suspend,
	// so subtracting it would incorrectly turn sleep into owner inactivity.
	raw, err := p.command(ctx, "loginctl", "list-sessions", "--no-legend", "--no-pager")
	if err != nil {
		return 0, false
	}
	idle, users := uptime, 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		raw, err = p.command(ctx, "loginctl", "show-session", f[0], "--no-pager", "-p", "Class", "-p", "Type", "-p", "State", "-p", "IdleHint", "-p", "IdleSinceHintMonotonic")
		if err != nil {
			return 0, false
		}
		props := properties(raw)
		switch props["Class"] {
		case "greeter", "lock-screen", "manager", "manager-early", "background", "background-light":
			continue
		case "user", "user-early", "user-light":
		default:
			return 0, false
		}
		users++
		// Uncovered TTY/desktop sessions never become "idle" by inference.
		if props["State"] == "" || !contains([]string{"tty", "x11", "wayland"}, props["Type"]) {
			return 0, false
		}
		if props["IdleHint"] == "no" {
			return 0, true
		}
		if props["IdleHint"] != "yes" {
			return 0, false
		}
		since, err := strconv.ParseUint(props["IdleSinceHintMonotonic"], 10, 64)
		if err != nil || since == 0 || float64(since)/1e6 > uptime {
			return 0, false
		}
		// Count time since the idle hint, not an invented last-input timestamp.
		idle = min(idle, uptime-float64(since)/1e6)
	}
	return idle, users > 0
}
func (p linuxOwnerProbe) value(path string) string {
	b, e := p.read(path)
	if e != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
func (p linuxOwnerProbe) power() (string, bool) {
	paths, err := p.glob("/sys/class/power_supply/*")
	if err != nil {
		return "unknown", false
	}
	laptop, battery, ac, known := false, false, false, true
	for _, path := range paths {
		switch p.value(filepath.Join(path, "type")) {
		case "Battery":
			laptop = true
			// Even a removed battery establishes a battery-equipped machine.
			if p.value(filepath.Join(path, "present")) == "0" {
				continue
			}
			status := p.value(filepath.Join(path, "status"))
			if status == "Discharging" {
				battery = true
			}
			if !contains([]string{"Discharging", "Charging", "Full", "Not charging"}, status) {
				known = false
			}
		case "Mains", "USB", "USB_C", "USB_PD", "UPS":
			switch p.value(filepath.Join(path, "online")) {
			case "1":
				ac = true
			case "0":
				battery = true
			default:
				known = false
			}
		default:
			known = false
		}
	}
	if !known {
		return "unknown", laptop
	}
	if ac {
		return "ac", laptop
	}
	if battery {
		return "battery", laptop
	}
	if laptop {
		return "unknown", true
	}
	return "desktop", false
}
func (p linuxOwnerProbe) heat() string {
	paths, err := p.glob("/sys/class/hwmon/hwmon*/temp*_input")
	if err != nil {
		return "unknown"
	}
	covered, unknown, hot, cooling := false, false, false, false
	for _, path := range paths {
		// Qualify only CPU/GPU sensors, rather than arbitrary disk/ambient readings.
		name := p.value(filepath.Join(filepath.Dir(path), "name"))
		if !contains([]string{"coretemp", "k10temp", "zenpower", "amdgpu", "i915", "xe", "nouveau", "nvidia", "cpu_thermal"}, name) {
			continue
		}
		covered = true
		stem := strings.TrimSuffix(path, "_input")
		readNumber := func(suffix string) (int64, bool) {
			n, e := strconv.ParseInt(p.value(stem+suffix), 10, 64)
			return n, e == nil
		}
		input, ok := readNumber("_input")
		fault := p.value(stem + "_fault")
		if !ok || input < -40000 || input > 200000 || (fault != "" && fault != "0") {
			unknown = true
			continue
		}
		guard, ok := readNumber("_max")
		if !ok {
			if critical, valid := readNumber("_crit"); valid {
				guard = critical - 10000
				ok = true
			}
		}
		if !ok || guard <= 0 || guard > 200000 {
			unknown = true
			continue
		}
		if input >= guard || p.value(stem+"_max_alarm") == "1" || p.value(stem+"_crit_alarm") == "1" || p.value(stem+"_alarm") == "1" {
			hot = true
		}
		margin := int64(5000)
		if hyst, valid := readNumber("_max_hyst"); valid && hyst > 0 && hyst < guard {
			margin = max(margin, guard-hyst)
		}
		if input > guard-margin {
			cooling = true
		}
	}
	if hot {
		return "serious"
	}
	if !covered || unknown {
		return "unknown"
	}
	if cooling {
		return "fair"
	}
	return "nominal"
}
