package myce

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const HostTelemetrySchema = "mycelium-host-telemetry-v1"

var hostTelemetryPCI = regexp.MustCompile(`^[0-9a-f]{4,8}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]$`)

// These artifacts are local observations, never admission or release authority.
// Host/card readings have no attempt attribution. Join separately retained
// memory_attempts/resource_attempts by their exact job/attempt/machine/session.
type TelemetryWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type TelemetryReading struct {
	Value     *float64        `json:"value,omitempty"`
	Missing   string          `json:"missing,omitempty"`
	Unit      string          `json:"unit"`
	Source    string          `json:"source"`
	SampledAt *time.Time      `json:"sampled_at,omitempty"`
	Window    TelemetryWindow `json:"window"`
}

type TelemetryProcess struct {
	PID     int              `json:"pid"`
	BirthID string           `json:"birth_id,omitempty"`
	RSS     TelemetryReading `json:"rss"`
}

type TelemetryCard struct {
	CardRef  string           `json:"card_ref"`
	Total    TelemetryReading `json:"memory_total"`
	Used     TelemetryReading `json:"memory_used"`
	Free     TelemetryReading `json:"memory_free"`
	Activity TelemetryReading `json:"activity"`
}

type HostTelemetrySample struct {
	Schema          string           `json:"schema"`
	RunID           string           `json:"run_id"`
	HostRef         string           `json:"host_ref"`
	CaptureID       string           `json:"capture_id"`
	Sequence        int              `json:"sequence"`
	CaptureWindow   TelemetryWindow  `json:"capture_window"`
	IntervalSeconds float64          `json:"interval_seconds"`
	Window          TelemetryWindow  `json:"window"`
	ElapsedSeconds  float64          `json:"elapsed_seconds"`
	Queue           TelemetryProcess `json:"queue"`
	Worker          TelemetryProcess `json:"worker"`
	WorkerTreeRSS   TelemetryReading `json:"worker_tree_rss"`
	HostUsed        TelemetryReading `json:"host_used"`
	ExternalMemory  TelemetryReading `json:"external_memory"`
	Cards           []TelemetryCard  `json:"cards"`
}

type HostTelemetryCard struct{ Ref, PCI string }

type HostTelemetryCapture struct {
	Probe                     MachineProbe
	Clock                     Clock
	RunID, HostRef, CaptureID string
	QueuePID, WorkerPID       int
	Cards                     []HostTelemetryCard
	// Caller assertion that every managed attempt runs inside the worker tree;
	// without it, managed memory outside the tree cannot be excluded.
	NativeWorkerOnly        bool
	Birth                   func(int) (string, error)
	queueBirth, workerBirth string
	started                 time.Time
}

func (c *HostTelemetryCapture) Init() error {
	c.Probe = c.Probe.withFilesystem()
	if c.Probe.OS != "linux" || c.Probe.ReadFile == nil || c.Probe.Command == nil {
		return errors.New("host telemetry capture requires local Linux probes")
	}
	if c.Clock == nil {
		c.Clock = RealClock{}
	}
	if c.Birth == nil {
		c.Birth = processBirth
	}
	if c.QueuePID <= 0 || c.WorkerPID <= 0 || c.QueuePID == c.WorkerPID || c.RunID == "" || c.HostRef == "" {
		return errors.New("capture requires distinct queue/worker PIDs and run/host references")
	}
	refs, addresses := map[string]bool{}, map[string]bool{}
	for _, card := range c.Cards {
		pci := normalizePCIAddress(card.PCI)
		if card.Ref == "" || !hostTelemetryPCI.MatchString(pci) || refs[card.Ref] || addresses[pci] {
			return errors.New("capture requires distinct card references and PCI identities")
		}
		refs[card.Ref], addresses[pci] = true, true
	}
	if c.CaptureID == "" {
		c.CaptureID = ID()
	}
	// Failed birth reads remain unknown for this capture; never bind a later PID.
	c.queueBirth, _ = c.Birth(c.QueuePID)
	c.workerBirth, _ = c.Birth(c.WorkerPID)
	c.started = c.Clock.Now()
	return nil
}

func (c *HostTelemetryCapture) reading(unit, source string, start time.Time, value *float64, reason string) TelemetryReading {
	end := c.Clock.Now().UTC()
	r := TelemetryReading{Value: value, Missing: reason, Unit: unit, Source: source, Window: TelemetryWindow{Start: start.UTC(), End: end}}
	if value != nil {
		r.SampledAt = &end
	}
	return r
}

func telemetryValue(v float64) *float64 { return &v }

func (c *HostTelemetryCapture) Sample(ctx context.Context) (HostTelemetrySample, error) {
	start := c.Clock.Now()
	r := HostTelemetrySample{Schema: HostTelemetrySchema, RunID: c.RunID, HostRef: c.HostRef, CaptureID: c.CaptureID, Cards: []TelemetryCard{}}
	// One shared inventory avoids adding independently sampled process trees.
	qb, qe := c.Birth(c.QueuePID)
	wb, we := c.Birth(c.WorkerPID)
	data, inventoryErr := c.Probe.Command(ctx, "ps", "-ax", "-o", "pid=,ppid=,rss=")
	qa, qae := c.Birth(c.QueuePID)
	wa, wae := c.Birth(c.WorkerPID)
	process := func(pid int, expected, before, after string, beforeErr, afterErr error) (TelemetryProcess, bool) {
		p := TelemetryProcess{PID: pid}
		reason := "process_identity_unavailable"
		changed := expected != "" && (beforeErr == nil && before != expected || afterErr == nil && after != expected)
		if changed {
			reason = "process_identity_changed"
		}
		if expected != "" && beforeErr == nil && afterErr == nil && before == expected && after == expected {
			digest := sha256.Sum256([]byte(expected))
			p.BirthID = hex.EncodeToString(digest[:])
			if inventoryErr == nil {
				for _, line := range strings.Split(string(data), "\n") {
					f := strings.Fields(line)
					if len(f) == 3 && f[0] == strconv.Itoa(pid) {
						n, err := strconv.ParseUint(f[2], 10, 64)
						if err == nil {
							p.RSS = c.reading("bytes", "process RSS; shared pages may overlap", start, telemetryValue(float64(n)*1024), "")
							return p, false
						}
					}
				}
			}
			reason = "process_inventory_unavailable"
		}
		p.RSS = c.reading("bytes", "process RSS", start, nil, reason)
		return p, changed
	}
	var queueChanged, workerChanged bool
	r.Queue, queueChanged = process(c.QueuePID, c.queueBirth, qb, qa, qe, qae)
	r.Worker, workerChanged = process(c.WorkerPID, c.workerBirth, wb, wa, we, wae)
	r.WorkerTreeRSS = c.reading("bytes", "worker and descendant RSS; sampled upper estimate", start, nil, "worker_tree_unavailable")
	if r.Worker.RSS.Value != nil && inventoryErr == nil {
		if gb, err := workerTreeRSS(data, c.WorkerPID); err == nil {
			r.WorkerTreeRSS = c.reading("bytes", r.WorkerTreeRSS.Source, start, telemetryValue(gb*(1<<30)), "")
		}
	}
	hostStart := c.Clock.Now()
	r.HostUsed = c.reading("bytes", "MemTotal minus MemAvailable; excludes reclaimable RAM", hostStart, nil, "host_memory_unavailable")
	if data, err := c.Probe.ReadFile("/proc/meminfo"); err == nil {
		if used, err := parseMeasureHost(data); err == nil {
			r.HostUsed = c.reading("bytes", r.HostUsed.Source, hostStart, telemetryValue(used*(1<<30)), "")
		}
	}
	if r.HostUsed.Value == nil {
		r.HostUsed = c.reading("bytes", r.HostUsed.Source, hostStart, nil, r.HostUsed.Missing)
	}
	r.ExternalMemory = c.reading("bytes", "host used minus worker-tree RSS; includes separate queue processes and other memory outside that tree", start, nil, "attribution_unknown")
	if c.NativeWorkerOnly && r.HostUsed.Value != nil && r.WorkerTreeRSS.Value != nil && *r.HostUsed.Value >= *r.WorkerTreeRSS.Value {
		r.ExternalMemory = c.reading("bytes", r.ExternalMemory.Source, start, telemetryValue(*r.HostUsed.Value-*r.WorkerTreeRSS.Value), "")
	}
	for _, card := range c.Cards {
		a := TelemetryCard{CardRef: card.Ref}
		at := c.Clock.Now()
		readings := []*TelemetryReading{&a.Total, &a.Used, &a.Free}
		for _, p := range readings {
			*p = c.reading("bytes", "xpu-smi raw physical card memory; no allowance/reserve subtraction", at, nil, "card_memory_unavailable")
		}
		if values, err := captureXPUMemory(ctx, c.Probe, card.PCI); err == nil {
			for i, p := range readings {
				*p = c.reading("bytes", p.Source, at, telemetryValue(values[i]), "")
			}
		}
		for _, p := range readings {
			if p.Value == nil {
				*p = c.reading("bytes", p.Source, at, nil, p.Missing)
			}
		}
		at = c.Clock.Now()
		a.Activity = c.reading("percent", "Xe PMU maximum across all enumerated hardware engines", at, nil, "card_activity_unavailable")
		if c.Probe.XeActivity != nil {
			if active, err := c.Probe.XeActivity(ctx, card.PCI); err == nil && validMemoryValue(active) && active <= 100 {
				a.Activity = c.reading("percent", a.Activity.Source, at, &active, "")
			}
		}
		if a.Activity.Value == nil {
			a.Activity = c.reading("percent", a.Activity.Source, at, nil, a.Activity.Missing)
		}
		r.Cards = append(r.Cards, a)
	}
	end := c.Clock.Now()
	r.Window = TelemetryWindow{Start: start.UTC(), End: end.UTC()}
	r.ElapsedSeconds = end.Sub(c.started).Seconds()
	if queueChanged || workerChanged {
		return r, errors.New("process_identity_changed: capture stopped; no PID rebinding")
	}
	return r, ctx.Err()
}

func captureXPUMemory(ctx context.Context, p MachineProbe, pci string) ([3]float64, error) {
	var values [3]float64
	b, err := p.Command(ctx, "xpu-smi", "--id="+pci, "--query-gpu=pci.bus_id,memory.total,memory.used,memory.free", "--format=csv,noheader,nounits")
	if err != nil {
		return values, err
	}
	rows, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil || len(rows) != 1 || len(rows[0]) != 4 || normalizePCIAddress(strings.TrimSpace(rows[0][0])) != normalizePCIAddress(pci) {
		return values, errors.New("card memory identity unavailable")
	}
	for i := range values {
		n, err := strconv.ParseFloat(strings.TrimSpace(rows[0][i+1]), 64)
		if err != nil || !validMemoryValue(n) {
			return values, errors.New("card memory counter unavailable")
		}
		values[i] = n * (1 << 20)
	}
	if values[1] > values[0] || values[2] > values[0] {
		return values, errors.New("card memory counter invalid")
	}
	return values, nil
}
