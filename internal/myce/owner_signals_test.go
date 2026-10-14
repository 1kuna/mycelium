package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func linuxSignalsFixture() (linuxOwnerProbe, map[string]string) {
	files := map[string]string{"/proc/sys/kernel/osrelease": "6.8.0", "/proc/uptime": "1000.00 0", "/sys/class/power_supply/BAT0/type": "Battery", "/sys/class/power_supply/BAT0/present": "1", "/sys/class/power_supply/BAT0/status": "Full", "/sys/class/power_supply/AC/type": "Mains", "/sys/class/power_supply/AC/online": "1", "/sys/class/hwmon/hwmon0/name": "coretemp", "/sys/class/hwmon/hwmon0/temp1_input": "60000", "/sys/class/hwmon/hwmon0/temp1_max": "90000"}
	p := linuxOwnerProbe{
		monotonic: func() (float64, error) { return 1000, nil },
		read: func(path string) ([]byte, error) {
			s, ok := files[path]
			if !ok {
				return nil, errors.New("missing")
			}
			return []byte(s), nil
		},
		glob: func(pattern string) ([]string, error) {
			switch pattern {
			case "/sys/class/power_supply/*":
				return []string{"/sys/class/power_supply/BAT0", "/sys/class/power_supply/AC"}, nil
			default:
				return []string{"/sys/class/hwmon/hwmon0/temp1_input"}, nil
			}
		},
		command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			if name != "loginctl" {
				return nil, errors.New("unexpected command")
			}
			if args[0] == "list-sessions" {
				return []byte("1 1000 test seat0 tty1\n"), nil
			}
			return []byte("Class=user\nType=wayland\nState=active\nIdleHint=yes\nIdleSinceHintMonotonic=600000000\n"), nil
		},
	}
	return p, files
}
func TestLinuxOwnerSignals(t *testing.T) {
	p, files := linuxSignalsFixture()
	s := p.detect(context.Background())
	if !s.IdleKnown || s.IdleSeconds != 400 || s.Power != "ac" || !s.Laptop || s.Heat != "nominal" {
		t.Fatal(s)
	}
	files["/sys/class/power_supply/AC/online"] = "0"
	files["/sys/class/power_supply/BAT0/status"] = "Discharging"
	if s = p.detect(context.Background()); s.Power != "battery" {
		t.Fatal(s)
	}
	files["/sys/class/power_supply/BAT0/status"] = "Unknown"
	if s = p.detect(context.Background()); s.Power != "unknown" || !s.Laptop {
		t.Fatal(s)
	}
	files["/sys/class/hwmon/hwmon0/temp1_input"] = "90000"
	if s = p.detect(context.Background()); s.Heat != "serious" {
		t.Fatal(s)
	}
	files["/sys/class/hwmon/hwmon0/temp1_input"] = "88000"
	if s = p.detect(context.Background()); s.Heat != "fair" {
		t.Fatal(s)
	}
	files["/sys/class/hwmon/hwmon0/temp1_input"] = "85000"
	if s = p.detect(context.Background()); s.Heat != "nominal" {
		t.Fatal(s)
	}
	files["/sys/class/hwmon/hwmon0/temp1_fault"] = "1"
	if s = p.detect(context.Background()); s.Heat != "unknown" {
		t.Fatal(s)
	}
	delete(files, "/sys/class/hwmon/hwmon0/temp1_fault")
	delete(files, "/sys/class/hwmon/hwmon0/temp1_max")
	if s = p.detect(context.Background()); s.Heat != "unknown" {
		t.Fatal(s)
	}
	files["/sys/class/hwmon/hwmon0/temp1_crit"] = "100000"
	if s = p.detect(context.Background()); s.Heat != "nominal" {
		t.Fatal(s)
	}
	files["/proc/sys/kernel/osrelease"] = "6.6.87.2-microsoft-standard-WSL2"
	if s = p.detect(context.Background()); s.IdleKnown || s.Power != "unknown" || s.Heat != "unknown" {
		t.Fatal("WSL used guest signals", s)
	}
}
func TestLinuxIdleSessionCoverage(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		known      bool
		seconds    float64
	}{
		{"owner active", "Class=user\nType=wayland\nState=active\nIdleHint=no\n", true, 0},
		{"missing hint", "Class=user\nType=wayland\nState=active\n", false, 0},
		{"unknown class", "Class=other\n", false, 0},
		{"uncovered session", "Class=user\nType=unspecified\nState=active\nIdleHint=yes\n", false, 0},
		{"future timestamp", "Class=user\nType=wayland\nState=active\nIdleHint=yes\nIdleSinceHintMonotonic=1100000000\n", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := linuxSignalsFixture()
			p.command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
				if args[0] == "list-sessions" {
					return []byte("1\n"), nil
				}
				return []byte(tc.text), nil
			}
			seconds, known := p.idle(context.Background())
			if known != tc.known || seconds != tc.seconds {
				t.Fatal(seconds, known)
			}
		})
	}
	p, _ := linuxSignalsFixture()
	p.command = func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("denied") }
	if s := p.detect(context.Background()); s.IdleKnown {
		t.Fatal("missing loginctl admitted")
	}
	p, _ = linuxSignalsFixture()
	old := p.command
	p.command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
		if args[0] == "list-sessions" {
			return []byte("1\n2\n"), nil
		}
		b, e := old(ctx, n, args...)
		if args[1] == "2" {
			b = []byte(strings.ReplaceAll(string(b), "IdleHint=yes", "IdleHint=no"))
		}
		return b, e
	}
	if seconds, known := p.idle(context.Background()); !known || seconds != 0 {
		t.Fatal("second owner session ignored", seconds, known)
	}
}

func TestLinuxIdleUsesLogindClockDomain(t *testing.T) {
	p, files := linuxSignalsFixture()
	files["/proc/uptime"] = "4600.00 0" // includes an hour asleep
	idle, known := p.idle(context.Background())
	if !known || idle != 400 {
		t.Fatal("sleep inflated logind idle duration", idle, known)
	}
	p.monotonic = func() (float64, error) { return 0, errors.New("clock unavailable") }
	if _, known := p.idle(context.Background()); known {
		t.Fatal("unknown monotonic time admitted")
	}
}
