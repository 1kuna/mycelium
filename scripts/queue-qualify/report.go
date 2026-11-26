package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	statusPass         = "pass"
	statusFail         = "fail"
	statusWarn         = "warn"
	statusInfo         = "info"
	statusNotExercised = "not-exercised"
)

type latencyStats struct {
	Count  int     `json:"count"`
	Errors int     `json:"errors"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	P999Ms float64 `json:"p999_ms"`
	MaxMs  float64 `json:"max_ms"`
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// summarize uses nearest-rank percentiles, so every reported value is a
// latency that actually occurred.
func summarize(values []float64, errors int) latencyStats {
	s := latencyStats{Count: len(values), Errors: errors}
	if len(values) == 0 {
		return s
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)
	rank := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(v)))) - 1
		return v[max(0, min(i, len(v)-1))]
	}
	var sum float64
	for _, x := range v {
		sum += x
	}
	s.MeanMs = sum / float64(len(v))
	s.P50Ms, s.P95Ms, s.P99Ms, s.P999Ms = rank(0.50), rank(0.95), rank(0.99), rank(0.999)
	s.MaxMs = v[len(v)-1]
	return s
}

// check is one pass/fail threshold. Prevents names the failure the limit
// guards against, so nobody tunes a number without knowing what it protects.
type check struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Measured string `json:"measured"`
	Limit    string `json:"limit"`
	Prevents string `json:"prevents"`
}

type outputFlags struct {
	dir, condition, volume string
}

func addOutputFlags(fs *flag.FlagSet) *outputFlags {
	o := &outputFlags{}
	fs.StringVar(&o.dir, "out", "queue-qualify-reports", "directory for the JSON and Markdown reports")
	fs.StringVar(&o.condition, "condition", "", "run condition label, for example idle, bulk-same-volume, mount-broken")
	fs.StringVar(&o.volume, "volume", "", "generic data-volume label, for example busy or quiet (never a real volume name)")
	return o
}

type report struct {
	Tool       string         `json:"tool"`
	Schema     int            `json:"schema"`
	Command    string         `json:"command"`
	Condition  string         `json:"condition,omitempty"`
	Volume     string         `json:"volume,omitempty"`
	Client     string         `json:"client"`
	StartedAt  time.Time      `json:"started_at"`
	FinishedAt time.Time      `json:"finished_at"`
	Server     *serverInfo    `json:"server,omitempty"`
	Params     map[string]any `json:"params,omitempty"`
	Results    any            `json:"results,omitempty"`
	Checks     []check        `json:"checks"`
	Verdict    string         `json:"verdict"`
	Notes      []string       `json:"notes,omitempty"`

	out *outputFlags
	md  func(*strings.Builder)
}

func newReport(command string, o *outputFlags) *report {
	return &report{
		Tool: "queue-qualify", Schema: 1, Command: command,
		Condition: o.condition, Volume: o.volume,
		Client:    runtime.GOOS + "/" + runtime.GOARCH,
		StartedAt: time.Now().UTC(), Params: map[string]any{}, out: o,
	}
}

func verdict(cs []check) string {
	v := statusPass
	for _, c := range cs {
		switch c.Status {
		case statusFail:
			return statusFail
		case statusNotExercised:
			v = "incomplete"
		}
	}
	return v
}

// finish writes the JSON and Markdown reports and prints where they went.
// A failing check returns errChecksFailed so scripts see a nonzero exit.
func (r *report) finish(out io.Writer) error {
	r.FinishedAt = time.Now().UTC()
	r.Verdict = verdict(r.Checks)
	if err := os.MkdirAll(r.out.dir, 0o755); err != nil {
		return err
	}
	name := r.Command
	if r.Condition != "" {
		name += "-" + safeName(r.Condition)
	}
	if r.Volume != "" {
		name += "-" + safeName(r.Volume)
	}
	name += "-" + r.StartedAt.Format("20060102T150405Z")
	base := filepath.Join(r.out.dir, name)
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(base+".json", append(b, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(base+".md", []byte(r.markdown()), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(out, "%s: %s\n  %s.json\n  %s.md\n", r.Command, r.Verdict, base, base)
	for _, c := range r.Checks {
		if c.Status == statusFail || c.Status == statusNotExercised {
			fmt.Fprintf(out, "  %s: %s (measured %s, limit %s)\n", c.Status, c.Name, c.Measured, c.Limit)
		}
	}
	if r.Verdict == statusFail {
		return errChecksFailed
	}
	return nil
}

func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, s)
}

func (r *report) markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# queue-qualify %s: %s\n\n", r.Command, strings.ToUpper(r.Verdict))
	fmt.Fprintf(&b, "| Field | Value |\n| --- | --- |\n")
	if r.Condition != "" {
		fmt.Fprintf(&b, "| Condition | %s |\n", r.Condition)
	}
	if r.Volume != "" {
		fmt.Fprintf(&b, "| Volume | %s |\n", r.Volume)
	}
	fmt.Fprintf(&b, "| Client | %s |\n| Started | %s |\n| Finished | %s |\n",
		r.Client, r.StartedAt.Format(time.RFC3339), r.FinishedAt.Format(time.RFC3339))
	keys := make([]string, 0, len(r.Params))
	for k := range r.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "| %s | %v |\n", k, r.Params[k])
	}
	if s := r.Server; s != nil {
		fmt.Fprintf(&b, "\n## Server\n\n%s, endpoint %s, superuser %v, clock offset %.1f ms.\n",
			s.Version, s.EndpointClass, s.Superuser, s.ClockOffsetMs)
		if s.SystemIdentifier != "" {
			fmt.Fprintf(&b, "System identifier %s, started %s.\n", s.SystemIdentifier, s.StartedAt.UTC().Format(time.RFC3339))
		}
		if s.RoundTrip.Count > 0 {
			fmt.Fprintf(&b, "SELECT 1 round trip p50/p99/max: %.2f / %.2f / %.2f ms over %d.\n",
				s.RoundTrip.P50Ms, s.RoundTrip.P99Ms, s.RoundTrip.MaxMs, s.RoundTrip.Count)
		}
		b.WriteString("\n| Setting | Value |\n| --- | --- |\n")
		keys := make([]string, 0, len(s.Settings))
		for k := range s.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "| %s | %s |\n", k, s.Settings[k])
		}
	}
	if r.md != nil {
		r.md(&b)
	}
	b.WriteString("\n## Checks\n\n| Check | Status | Measured | Limit | Prevents |\n| --- | --- | --- | --- | --- |\n")
	for _, c := range r.Checks {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", c.Name, c.Status, c.Measured, c.Limit, c.Prevents)
	}
	if len(r.Notes) > 0 {
		b.WriteString("\n## Notes\n\n")
		for _, n := range r.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
	}
	return b.String()
}

func latencyTable(b *strings.Builder, rows [][2]any) {
	b.WriteString("\n| Series | Count | Errors | Mean | p50 | p95 | p99 | p99.9 | Max |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range rows {
		s := row[1].(latencyStats)
		fmt.Fprintf(b, "| %s | %d | %d | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
			row[0], s.Count, s.Errors, s.MeanMs, s.P50Ms, s.P95Ms, s.P99Ms, s.P999Ms, s.MaxMs)
	}
	b.WriteString("\nAll latencies in milliseconds.\n")
}

// cmdSummarize compares saved reports side by side, one row per run.
func cmdSummarize(_ context.Context, args []string, out io.Writer) error {
	fs := newFlags("summarize", out)
	o := addOutputFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return fmt.Errorf("pass one or more report JSON files")
	}
	var runs []summaryRow
	for _, path := range fs.Args() {
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var raw struct {
			Command, Condition, Volume, Verdict string
			StartedAt                           time.Time `json:"started_at"`
			Results                             json.RawMessage
			Checks                              []check
		}
		if err := json.Unmarshal(b, &raw); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
		row := summaryRow{File: filepath.Base(path), Command: raw.Command, Condition: raw.Condition,
			Volume: raw.Volume, Verdict: raw.Verdict, Started: raw.StartedAt}
		for _, c := range raw.Checks {
			if c.Status == statusFail || c.Status == statusNotExercised {
				row.Failing = append(row.Failing, c.Name)
			}
		}
		if raw.Command == "commit" {
			var cr commitResults
			if err := json.Unmarshal(raw.Results, &cr); err == nil {
				row.Commit = &cr
			}
		}
		runs = append(runs, row)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].Started.Before(runs[j].Started) })
	r := newReport("summary", o)
	r.Results = runs
	for _, row := range runs {
		st := statusPass
		switch row.Verdict {
		case statusFail:
			st = statusFail
		case "incomplete":
			st = statusNotExercised
		}
		r.Checks = append(r.Checks, check{Name: row.Command + " " + row.Condition + " " + row.Volume, Status: st,
			Measured: row.Verdict, Limit: "pass", Prevents: "see " + row.File})
	}
	r.md = func(b *strings.Builder) {
		b.WriteString("\n## Commit runs\n\n| Condition | Volume | Verdict | Append p50 | p95 | p99 | max | Renew p99 | max | Rate /s | WAL kB/s |\n| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, row := range runs {
			if c := row.Commit; c != nil {
				fmt.Fprintf(b, "| %s | %s | %s | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.1f |\n",
					row.Condition, row.Volume, row.Verdict, c.Append.P50Ms, c.Append.P95Ms, c.Append.P99Ms,
					c.Append.MaxMs, c.Renew.P99Ms, c.Renew.MaxMs, c.AchievedRate, c.WALkBps)
			}
		}
		b.WriteString("\n## All runs\n\n| Started | Command | Condition | Volume | Verdict | Failing or not exercised | Report |\n| --- | --- | --- | --- | --- | --- | --- |\n")
		for _, row := range runs {
			fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s |\n", row.Started.Format(time.RFC3339),
				row.Command, row.Condition, row.Volume, row.Verdict, strings.Join(row.Failing, "; "), row.File)
		}
	}
	return r.finish(out)
}

type summaryRow struct {
	File      string         `json:"file"`
	Command   string         `json:"command"`
	Condition string         `json:"condition,omitempty"`
	Volume    string         `json:"volume,omitempty"`
	Verdict   string         `json:"verdict"`
	Started   time.Time      `json:"started_at"`
	Failing   []string       `json:"failing,omitempty"`
	Commit    *commitResults `json:"commit,omitempty"`
}
